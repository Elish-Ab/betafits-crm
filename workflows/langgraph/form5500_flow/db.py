from __future__ import annotations

import csv
import os
import re
import uuid
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2 import sql

from lib.models.form5500_state import ColMeta, UpsertSummary, ValidationSummary

VALID_COL_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def env_first(*names: str) -> Optional[str]:
    for name in names:
        val = os.getenv(name)
        if val:
            return val
    return None


def connect_db(db_url: Optional[str] = None):
    dsn = db_url or env_first("SUPABASE_DB_URL", "DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)

    host = env_first("SUPABASE_DB_HOST", "PGHOST")
    port = env_first("SUPABASE_DB_PORT", "PGPORT") or "5432"
    dbname = env_first("SUPABASE_DB_NAME", "PGDATABASE")
    user = env_first("SUPABASE_DB_USER", "PGUSER")
    password = env_first("SUPABASE_DB_PASSWORD", "PGPASSWORD")

    missing = [k for k, v in [("host", host), ("dbname", dbname), ("user", user), ("password", password)] if not v]
    if missing:
        raise RuntimeError(
            "Missing DB connection settings. Provide SUPABASE_DB_URL (or DATABASE_URL), "
            "or set host/dbname/user/password via PG* or SUPABASE_DB_* env vars. "
            f"Missing: {', '.join(missing)}"
        )

    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def read_csv_header(csv_path: Path) -> List[str]:
    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        header = next(reader, None)
        if not header:
            raise ValueError("CSV appears to be empty or missing a header row.")
        cols = [c.strip().strip("\ufeff").lower() for c in header if c is not None]

    seen = set()
    out = []
    for col in cols:
        if not col:
            raise ValueError("Found an empty column name in CSV header.")
        if col in seen:
            raise ValueError(f"Duplicate column in CSV header after lowercasing: {col}")
        if not VALID_COL_RE.match(col):
            raise ValueError(
                f"Invalid/unsafe column name '{col}'. Expected lowercase snake_case like 'plan_name'."
            )
        seen.add(col)
        out.append(col)
    return out


def parse_layout(layout_path: Path) -> Dict[str, str]:
    type_map: Dict[str, str] = {}
    with layout_path.open("r", encoding="utf-8-sig") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("="):
                continue
            if line.upper().startswith("FIELD_POSITION"):
                continue

            row = next(csv.reader([line], delimiter=",", quotechar='"'))
            if len(row) < 3:
                continue

            field_name = (row[1] or "").strip().lower()
            field_type = (row[2] or "").strip().upper()

            if not field_name:
                continue

            if field_type == "NUMERIC":
                ptype = "numeric"
            else:
                ptype = "text"

            type_map[field_name] = ptype

    if not type_map:
        raise ValueError("Layout file parsed to zero fields. Check the layout format.")
    return type_map


def fetch_table_columns(cur, schema: str, table: str) -> Dict[str, ColMeta]:
    cur.execute(
        """
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {str(name).lower(): ColMeta(str(data_type), str(udt_name)) for name, data_type, udt_name in cur.fetchall()}


def fetch_pk_columns(cur, schema: str, table: str) -> List[str]:
    cur.execute(
        """
        SELECT a.attname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        JOIN unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ord) ON TRUE
        JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = cols.attnum
        WHERE con.contype = 'p'
          AND nsp.nspname = %s
          AND rel.relname = %s
        ORDER BY cols.ord
        """,
        (schema, table),
    )
    return [r[0].lower() for r in cur.fetchall()]


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def create_table(
    cur,
    schema: str,
    table: str,
    columns: List[str],
    layout_types: Dict[str, str],
    pk_columns: Optional[List[str]] = None,
) -> None:
    col_defs = []
    for col in columns:
        ptype = layout_types.get(col, "text")
        if ptype not in ("text", "numeric"):
            ptype = "text"
        col_defs.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(ptype)))

    pk_clause = sql.SQL("")
    if pk_columns:
        pk_clause = sql.SQL(", PRIMARY KEY ({})").format(
            sql.SQL(", ").join(sql.Identifier(c) for c in pk_columns)
        )

    cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({}{})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(col_defs),
            pk_clause,
        )
    )


def ensure_columns(
    cur,
    schema: str,
    table: str,
    csv_cols: List[str],
    colmeta: Dict[str, ColMeta],
    layout_types: Dict[str, str],
) -> Tuple[List[str], Dict[str, ColMeta]]:
    missing = [c for c in csv_cols if c not in colmeta]
    added: List[str] = []

    for col in missing:
        ptype = layout_types.get(col, "text")
        if ptype not in ("text", "numeric"):
            ptype = "text"

        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}")
            .format(sql.Identifier(schema), sql.Identifier(table), sql.Identifier(col), sql.SQL(ptype))
        )
        added.append(col)

    if added:
        colmeta = fetch_table_columns(cur, schema, table)

    return added, colmeta


def _cast_type_for(meta: ColMeta) -> Optional[str]:
    dt = meta.data_type.lower()
    udt = meta.udt_name.lower()

    if dt in ("integer", "bigint", "smallint"):
        return dt
    if dt in ("numeric", "double precision", "real"):
        return dt
    if dt == "boolean":
        return "boolean"
    if dt == "date":
        return "date"
    if dt.startswith("timestamp"):
        if udt in ("timestamptz", "timestamp"):
            return udt
        return "timestamp"
    if udt in ("uuid", "json", "jsonb"):
        return udt
    return None


def create_temp_stage(cur, stage_name: str, csv_cols: List[str]):
    col_defs = [sql.SQL("{} text").format(sql.Identifier(c)) for c in csv_cols]
    cur.execute(
        sql.SQL("CREATE TEMP TABLE {} ({} ) ON COMMIT DROP")
        .format(sql.Identifier(stage_name), sql.SQL(", ").join(col_defs))
    )


def copy_csv_into_stage(cur, stage_name: str, csv_path: Path, csv_cols: List[str]):
    copy_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '""', ESCAPE '""', NULL '')"
    ).format(
        sql.Identifier(stage_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in csv_cols),
    )

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        cur.copy_expert(copy_sql.as_string(cur.connection), f)


def _pk_blank_condition(stage_alias: str, pk_cols: List[str]) -> sql.SQL:
    return sql.SQL(" OR ").join(
        sql.SQL("({a}.{c} IS NULL OR btrim({a}.{c}) = '')").format(a=sql.Identifier(stage_alias), c=sql.Identifier(c))
        for c in pk_cols
    )


def _count_distinct_pk(cur, stage_name: str, pk_cols: List[str]) -> int:
    row_expr = sql.SQL("ROW({})").format(
        sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(c)) for c in pk_cols)
    )
    q = sql.SQL("SELECT COUNT(DISTINCT {row_expr}) FROM {stage} AS s").format(row_expr=row_expr, stage=sql.Identifier(stage_name))
    cur.execute(q)
    return int(cur.fetchone()[0])


def _count_new_keys(cur, schema: str, table: str, stage_name: str, pk_cols: List[str], pk_meta: Dict[str, ColMeta]) -> int:
    stage_key_exprs = []
    for c in pk_cols:
        meta = pk_meta[c]
        cast_t = _cast_type_for(meta)
        base = sql.SQL("NULLIF(btrim(s.{}), '')").format(sql.Identifier(c))
        expr = sql.SQL("{}::{}").format(base, sql.SQL(cast_t)) if cast_t else base
        stage_key_exprs.append(expr)

    stage_keys_select = sql.SQL(", ").join(
        sql.SQL("{expr} AS {col}").format(expr=stage_key_exprs[i], col=sql.Identifier(pk_cols[i]))
        for i in range(len(pk_cols))
    )

    using_clause = sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
    not_null_conds = sql.SQL(" AND ").join(sql.SQL("k.{} IS NOT NULL").format(sql.Identifier(c)) for c in pk_cols)

    q = sql.SQL(
        """
        WITH k AS (
            SELECT DISTINCT {stage_keys_select}
            FROM {stage} AS s
            WHERE NOT ({not_blank})
        )
        SELECT COUNT(*)
        FROM k
        LEFT JOIN {schema}.{table} AS t
        USING ({using_clause})
        WHERE t.{first_pk} IS NULL
          AND {not_null_conds}
        """
    ).format(
        stage_keys_select=stage_keys_select,
        stage=sql.Identifier(stage_name),
        not_blank=_pk_blank_condition("s", pk_cols),
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        using_clause=using_clause,
        first_pk=sql.Identifier(pk_cols[0]),
        not_null_conds=not_null_conds,
    )

    cur.execute(q)
    return int(cur.fetchone()[0])


def _count_rows(cur, schema: str, table: str) -> int:
    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
    return int(cur.fetchone()[0])


def validate_stage(cur, schema: str, table: str, stage_name: str, pk_cols: List[str], colmeta: Dict[str, ColMeta]) -> ValidationSummary:
    validation = ValidationSummary()

    validation.existing_total_before = _count_rows(cur, schema, table)

    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(stage_name)))
    validation.incoming_rows = int(cur.fetchone()[0])

    validation.incoming_distinct_keys = _count_distinct_pk(cur, stage_name, pk_cols)
    validation.duplicate_keys = validation.incoming_rows - validation.incoming_distinct_keys

    cur.execute(
        sql.SQL("SELECT COUNT(*) FROM {} AS s WHERE {}").format(
            sql.Identifier(stage_name), _pk_blank_condition("s", pk_cols)
        )
    )
    validation.pk_blank_count = int(cur.fetchone()[0])

    pk_meta = {c: colmeta[c] for c in pk_cols}
    validation.expected_new_rows = _count_new_keys(cur, schema, table, stage_name, pk_cols, pk_meta)
    validation.existing_keys_in_file = validation.incoming_distinct_keys - validation.expected_new_rows

    return validation


def build_upsert_sql(
    schema: str,
    table: str,
    stage_name: str,
    csv_cols: List[str],
    pk_cols: List[str],
    colmeta: Dict[str, ColMeta],
) -> sql.SQL:
    missing_pk = [c for c in pk_cols if c not in csv_cols]
    if missing_pk:
        raise ValueError(f"CSV is missing primary key column(s) required for upsert: {missing_pk}")

    insert_cols = [c for c in csv_cols if c in colmeta]
    if not insert_cols:
        raise ValueError("No CSV columns matched target table columns (after schema sync).")

    select_exprs = []
    for c in insert_cols:
        meta = colmeta[c]
        cast_t = _cast_type_for(meta)
        base = sql.SQL("NULLIF(btrim(s.{}), '')").format(sql.Identifier(c))
        expr = sql.SQL("{}::{}").format(base, sql.SQL(cast_t)) if cast_t else base
        select_exprs.append(expr)

    updatable = [c for c in insert_cols if c not in pk_cols]

    if updatable:
        assignments = [
            sql.SQL("{} = COALESCE(EXCLUDED.{}, tgt.{})").format(sql.Identifier(c), sql.Identifier(c), sql.Identifier(c))
            for c in updatable
        ]
        change_conds = [
            sql.SQL("(EXCLUDED.{} IS NOT NULL AND EXCLUDED.{} IS DISTINCT FROM tgt.{})").format(
                sql.Identifier(c), sql.Identifier(c), sql.Identifier(c)
            )
            for c in updatable
        ]
        on_conflict = sql.SQL("DO UPDATE SET {} WHERE {}").format(
            sql.SQL(", ").join(assignments), sql.SQL(" OR ").join(change_conds)
        )
    else:
        on_conflict = sql.SQL("DO NOTHING")

    insert_stmt = sql.SQL(
        """
        INSERT INTO {schema}.{table} AS tgt ({cols})
        SELECT {selects}
        FROM {stage} AS s
        ON CONFLICT ({pk}) {action}
        RETURNING (xmax = 0) AS inserted
        """
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols),
        selects=sql.SQL(", ").join(select_exprs),
        stage=sql.Identifier(stage_name),
        pk=sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols),
        action=on_conflict,
    )

    stmt = sql.SQL(
        """
        WITH upserted AS (
            {insert_stmt}
        )
        SELECT
            COUNT(*) FILTER (WHERE inserted)     AS inserted_rows,
            COUNT(*) FILTER (WHERE NOT inserted) AS updated_rows
        FROM upserted
        """
    ).format(insert_stmt=insert_stmt)

    return stmt


def execute_upsert(
    cur,
    schema: str,
    table: str,
    stage_name: str,
    csv_cols: List[str],
    pk_cols: List[str],
    colmeta: Dict[str, ColMeta],
    validation: ValidationSummary,
) -> UpsertSummary:
    upsert_sql = build_upsert_sql(schema, table, stage_name, csv_cols, pk_cols, colmeta)
    cur.execute(upsert_sql)
    inserted_rows, updated_rows = cur.fetchone()
    inserted_rows = int(inserted_rows or 0)
    updated_rows = int(updated_rows or 0)
    unchanged_rows = validation.existing_keys_in_file - updated_rows
    existing_total_after = _count_rows(cur, schema, table)
    delta_rows = existing_total_after - validation.existing_total_before

    discrepancies: List[str] = []
    if inserted_rows != validation.expected_new_rows:
        discrepancies.append(f"inserted_rows ({inserted_rows}) != expected_new_rows ({validation.expected_new_rows})")
    if delta_rows != inserted_rows:
        discrepancies.append(f"table row delta ({delta_rows}) != inserted_rows ({inserted_rows})")
    if updated_rows < 0 or updated_rows > validation.existing_keys_in_file:
        discrepancies.append(f"updated_rows ({updated_rows}) out of range (0..{validation.existing_keys_in_file})")
    if unchanged_rows < 0:
        discrepancies.append(f"unchanged_rows computed negative ({unchanged_rows}); check key math")

    return UpsertSummary(
        inserted_rows=inserted_rows,
        updated_rows=updated_rows,
        unchanged_rows=unchanged_rows,
        existing_total_after=existing_total_after,
        delta_rows=delta_rows,
        discrepancies=discrepancies,
    )


def create_stage_name() -> str:
    return f"stage_{uuid.uuid4().hex[:10]}"
