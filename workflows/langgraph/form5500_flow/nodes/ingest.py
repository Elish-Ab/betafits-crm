from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional
from typing import Dict, List

from psycopg2 import sql

from .. import db
from lib.config.form5500_config import IngestConfig
from lib.models.form5500_state import ColMeta, IngestState, UpsertSummary, ValidationSummary

logger = logging.getLogger(__name__)


class IngestError(RuntimeError):
    pass


def prepare_files(state: IngestState, config: IngestConfig) -> IngestState:
    csv_path = config.csv_path
    if not csv_path.exists():
        raise IngestError(f"CSV file not found: {csv_path}")

    layout_path = config.layout_path
    if layout_path and not layout_path.exists():
        raise IngestError(f"Layout file not found: {layout_path}")

    state.update(
        {
            "csv_path": csv_path,
            "layout_path": layout_path,
            "schema": config.database.schema,
            "table": config.table.strip().lower(),
            "db_url": config.database.db_url,
            "dry_run": config.dry_run,
            "create_table_if_missing": config.create_table_if_missing,
            "configured_pk": config.pk_columns,
            "table_created": False,
        }
    )
    return state


def open_connection(state: IngestState, _: IngestConfig) -> IngestState:
    conn = db.connect_db(state.get("db_url"))
    conn.autocommit = False
    state["connection"] = conn
    return state


def inspect_schema(state: IngestState, _: IngestConfig) -> IngestState:
    conn = state["connection"]
    csv_cols: List[str] = state.get("csv_columns", [])
    layout_types: Dict[str, str] = state.get("layout_types", {})

    configured_pk = state.get("configured_pk") or []
    if configured_pk:
        missing_from_csv = [c for c in configured_pk if c not in csv_cols]
        if missing_from_csv:
            raise IngestError(
                f"Configured primary key column(s) not found in CSV header: {missing_from_csv}. "
                "Ensure the CSV contains all PK columns."
            )

    with conn.cursor() as cur:
        if not db.table_exists(cur, state["schema"], state["table"]):
            if not state.get("create_table_if_missing"):
                raise IngestError(
                    f"Table {state['schema']}.{state['table']} not found. "
                    "Pass --create-table-if-missing to create it from the CSV header."
                )
            if not csv_cols:
                raise IngestError("CSV header not loaded; cannot create table without column names.")

            pk_for_create = state.get("configured_pk") or []
            db.create_table(cur, state["schema"], state["table"], csv_cols, layout_types, pk_for_create)
            state["table_created"] = True

        pk_cols = db.fetch_pk_columns(cur, state["schema"], state["table"])
        if not pk_cols:
            if state.get("configured_pk"):
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY ({})").format(
                        sql.Identifier(state["schema"]),
                        sql.Identifier(state["table"]),
                        sql.SQL(", ").join(sql.Identifier(c) for c in state["configured_pk"]),
                    )
                )
                pk_cols = state["configured_pk"]
            else:
                raise IngestError(
                    f"No PRIMARY KEY found for {state['schema']}.{state['table']} (expected one per rule). "
                    "Provide --pk to set the primary key."
                )
    with conn.cursor() as cur:
        pk_cols = db.fetch_pk_columns(cur, state["schema"], state["table"])
        if not pk_cols:
            raise IngestError(f"No PRIMARY KEY found for {state['schema']}.{state['table']} (expected one per rule).")
        column_meta = db.fetch_table_columns(cur, state["schema"], state["table"])

    state["pk_columns"] = pk_cols
    state["column_meta"] = column_meta
    return state


def load_headers(state: IngestState, config: IngestConfig) -> IngestState:
    csv_columns = db.read_csv_header(config.csv_path)
    layout_types: Dict[str, str] = {}
    if config.layout_path:
        layout_types = db.parse_layout(config.layout_path)

    state["csv_columns"] = csv_columns
    state["layout_types"] = layout_types
    return state


def sync_schema(state: IngestState, _: IngestConfig) -> IngestState:
    conn = state["connection"]
    with conn.cursor() as cur:
        added_columns, refreshed_meta = db.ensure_columns(
            cur,
            state["schema"],
            state["table"],
            state["csv_columns"],
            state["column_meta"],
            state.get("layout_types", {}),
        )
    state["added_columns"] = added_columns
    state["column_meta"] = refreshed_meta
    return state


def stage_csv(state: IngestState, _: IngestConfig) -> IngestState:
    conn = state["connection"]
    stage_name = db.create_stage_name()
    with conn.cursor() as cur:
        db.create_temp_stage(cur, stage_name, state["csv_columns"])
        db.copy_csv_into_stage(cur, stage_name, state["csv_path"], state["csv_columns"])
    state["stage_name"] = stage_name
    return state


def validate(state: IngestState, _: IngestConfig) -> IngestState:
    conn = state["connection"]
    with conn.cursor() as cur:
        validation = db.validate_stage(
            cur,
            state["schema"],
            state["table"],
            state["stage_name"],
            state["pk_columns"],
            state["column_meta"],
        )
    if validation.pk_blank_count > 0:
        raise IngestError(
            f"Found {validation.pk_blank_count} row(s) in the file with blank/NULL primary key columns: {state['pk_columns']}"
        )
    if validation.duplicate_keys > 0:
        raise IngestError(
            "File contains duplicate primary keys. Deduplicate the CSV by PK before loading "
            f"({validation.duplicate_keys} duplicate row(s))."
        )

    state["validation"] = asdict(validation)
    return state


def upsert(state: IngestState, _: IngestConfig) -> IngestState:
    conn = state["connection"]
    validation = ValidationSummary(**state["validation"])

    with conn.cursor() as cur:
        summary = db.execute_upsert(
            cur,
            state["schema"],
            state["table"],
            state["stage_name"],
            state["csv_columns"],
            state["pk_columns"],
            state["column_meta"],
            validation,
        )

    if summary.discrepancies:
        conn.rollback()
        raise IngestError("Validation discrepancy detected after upsert: " + "; ".join(summary.discrepancies))

    if state.get("dry_run"):
        conn.rollback()
    else:
        conn.commit()

    state["upsert_result"] = asdict(summary)
    return state


def apply_calculated_fields(state: IngestState, config: IngestConfig) -> IngestState:
    if not config.apply_calculated:
        state["calculated_runs"] = {}
        return state

    conn = state["connection"]
    base_path = Path(__file__).resolve().parents[3] / "assets" / "calculated_fields"
    results: Dict[str, str] = {}

    if state.get("dry_run"):
        state["calculated_runs"] = {name: "skipped (dry-run)" for name in config.apply_calculated}
        conn.rollback()
        return state

    for script_name in config.apply_calculated:
        script_path = (base_path / f"{script_name}.sql").resolve()
        if not script_path.exists():
            raise IngestError(f"Calculated field script not found: {script_name}")

        sql_text = script_path.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql.SQL(sql_text))
        results[script_name] = "applied"

    if state.get("dry_run"):
        conn.rollback()
    else:
        conn.commit()

    state["calculated_runs"] = results
    return state


def finalize(state: IngestState, _: IngestConfig) -> IngestState:
    conn = state.get("connection")
    if conn:
        try:
            conn.close()
        except Exception:
            logger.exception("Failed to close DB connection")
    return state
