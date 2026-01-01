from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

from psycopg2 import sql

from lib.config.form5500_config import CalculatedFieldsConfig
from lib.models.form5500_state import CalcState
from ..db import connect_db

logger = logging.getLogger(__name__)


class CalculatedFieldsError(RuntimeError):
    pass


def prepare(state: CalcState, config: CalculatedFieldsConfig) -> CalcState:
    state["scripts"] = list(config.scripts)
    state["schema"] = config.schema
    state["db_url"] = config.db_url
    return state


def run_scripts(state: CalcState, _: CalculatedFieldsConfig) -> CalcState:
    base_path = Path(__file__).resolve().parents[3] / "assets" / "calculated_fields"
    db_url = state.get("db_url")

    conn = connect_db(db_url)
    conn.autocommit = False
    results: Dict[str, str] = {}

    try:
        for script_name in state["scripts"]:
            script_path = (base_path / f"{script_name}.sql").resolve()
            if not script_path.exists():
                raise CalculatedFieldsError(f"Calculated field script not found: {script_name}")
            sql_text = script_path.read_text(encoding="utf-8")
            with conn.cursor() as cur:
                cur.execute(sql.SQL(sql_text))
            results[script_name] = "applied"
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    state["results"] = results
    return state


def finalize(state: CalcState, _: CalculatedFieldsConfig) -> CalcState:
    return state
