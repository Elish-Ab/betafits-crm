#!/usr/bin/env python3
"""
Supabase / Postgres bulk CSV upsert with:
- Automatic primary key (including composite PK) inference
- Automatic schema sync (adds missing columns based on a layout .txt file)
- Fast load for 1M–10M+ rows: COPY into a TEMP staging table, then one set-based UPSERT
- Update rule: overwrite existing values, but DO NOT overwrite with blanks / NULLs from the CSV

Assumptions (per user):
- Target schema is f_5500
- Database column names are lowercase (script normalizes CSV headers and layout names to lowercase)
- CSV is comma-delimited, double quotes as the text qualifier, header row present

Environment variables (recommended):
- SUPABASE_DB_URL (preferred) OR DATABASE_URL
  OR the standard PG* set:
    PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD
  OR Supabase-namespaced:
    SUPABASE_DB_HOST / SUPABASE_DB_PORT / SUPABASE_DB_NAME / SUPABASE_DB_USER / SUPABASE_DB_PASSWORD
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import psycopg2
from psycopg2 import sql


VALID_COL_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class ColMeta:
    data_type: str       # information_schema.columns.data_type
    udt_name: str        # information_schema.columns.udt_name


def _env(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def connect_db(db_url: Optional[str] = None):
    """Create a psycopg2 connection."""
    dsn = db_url or _env("SUPABASE_DB_URL", "DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)

    host = _env("SUPABASE_DB_HOST", "PGHOST")
    port = _env("SUPABASE_DB_PORT", "PGPORT") or "5432"
    dbname = _env("SUPABASE_DB_NAME", "PGDATABASE")
    user = _env("SUPABASE_DB_USER", "PGUSER")
    password = _env("SUPABASE_DB_PASSWORD", "PGPASSWORD")

    missing = [k for k, v in [("host", host), ("dbname", dbname), ("user", user), ("password", password)] if not v]
    if missing:
        raise RuntimeError(
            "Missing DB connection settings. Provide SUPABASE_DB_URL (or DATABASE_URL), "
            "or set host/dbname/user/password via PG* or SUPABASE_DB_* env vars. "
            f"Missing: {', '.join(missing)}"
        )

    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def read_csv_header(csv_path: str) -> List[str]:
    """Read the first row (header) of the CSV file and normalize to lowercase names."""
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        header = next(reader, None)
        if not header:
            raise ValueError("CSV appears to be empty or missing a header row.")
        cols = [c.strip().strip("\ufeff").lower() for c in header if c is not None]

    # Validate, de-dup
    seen = set()
    out = []
    for c in cols:
        if not c:
            raise ValueError("Found an empty column name in CSV header.")
        if c in seen:
            raise ValueError(f"Duplicate column in CSV header after lowercasing: {c}")
        if not VALID_COL_RE.match(c):
            raise ValueError(
                f"Invalid/unsafe column name '{c}'. "
                "Expected lowercase snake_case like 'plan_name'."
            )
        seen.add(c)
        out.append(c)
    return out


def parse_layout(layout_path: str) -> Dict[str, str]:
    """
    Parse the layout .txt file into a {field_name_lower: postgres_type} map.

    Expected format (based on uploaded example):
      FIELD_POSITION,FIELD_NAME,TYPE,SIZE (only for text fields)
      ===========================================
      1,ACK_ID,TEXT,30
      2,FORM_PLAN_YEAR_BEGIN_DATE,TEXT,10
      ...
      44,SOME_NUMERIC_FIELD,NUMERIC

    Mapping rules:
      - TEXT,<size> -> TEXT (best for flexibility and load robustness)
      - NUMERIC -> NUMERIC
      - Unknown -> TEXT
    """
    type_map: Dict[str, str] = {}
    with open(layout_path, "r", encoding="utf-8-sig") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("="):
                continue
            if line.upper().startswith("FIELD_POSITION"):
                continue

            # Use csv parsing to be safe around spacing
            row = next(csv.reader([line], delimiter=",", quotechar='"'))
            if len(row) < 3:
                continue

            # row: [pos, field_name, type, size?]
            field_name = (row[1] or "").strip().lower()
            ftype = (row[2] or "").strip().upper()

            if not field_name:
                continue

            if ftype == "NUMERIC":
                ptype = "numeric"
            else:
                # TEXT and everything else: default to text
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
    out: Dict[str, ColMeta] = {}
    for name, data_type, udt_name in cur.fetchall():
        out[str(name).lower()] = ColMeta(str(data_type), str(udt_name))
    return out


def fetch_pk_columns(cur, schema: str, table: str) -> List[str]:
    """
    Return primary key columns in ordinal order.
    Works for single and composite primary keys.
    """
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
    cols = [r[0].lower() for r in cur.fetchall()]
    return cols


def ensure_columns(cur, schema: str, table: str, csv_cols: List[str], colmeta: Dict[str, ColMeta], layout_types: Dict[str, str]) -> Tuple[List[str], Dict[str, ColMeta]]:
    """
    Add missing columns to target table. Returns (added_cols, refreshed_colmeta).
    """
    missing = [c for c in csv_cols if c not in colmeta]
    added: List[str] = []

    for c in missing:
        ptype = layout_types.get(c, "text")
        # We intentionally use TEXT for all layout TEXT fields (SIZE treated as documentation)
        if ptype not in ("text", "numeric"):
            ptype = "text"

        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}")
               .format(sql.Identifier(schema), sql.Identifier(table), sql.Identifier(c), sql.SQL(ptype))
        )
        added.append(c)

    if added:
        # Refresh metadata
        colmeta = fetch_table_columns(cur, schema, table)

    return added, colmeta


def _cast_type_for(meta: ColMeta) -> Optional[str]:
    """
    Return a Postgres cast type string or None if no cast should be applied.

    We cast only when helpful/needed; staging columns are TEXT.
    """
    dt = meta.data_type.lower()
    udt = meta.udt_name.lower()

    # Common explicit types
    if dt in ("integer", "bigint", "smallint"):
        return dt
    if dt in ("numeric", "double precision", "real"):
        return dt
    if dt == "boolean":
        return "boolean"
    if dt == "date":
        return "date"
    if dt.startswith("timestamp"):
        # Use udt to distinguish timestamp vs timestamptz if available
        if udt in ("timestamptz", "timestamp"):
            return udt
        return "timestamp"
    if udt in ("uuid", "json", "jsonb"):
        return udt

    # Text types: skip cast
    return None


def create_temp_stage(cur, stage_name: str, csv_cols: List[str]):
    """
    Create a TEMP staging table with all CSV columns as TEXT.
    """
    col_defs = [sql.SQL("{} text").format(sql.Identifier(c)) for c in csv_cols]
    cur.execute(
        sql.SQL("CREATE TEMP TABLE {} ({} ) ON COMMIT DROP")
           .format(sql.Identifier(stage_name), sql.SQL(", ").join(col_defs))
    )


def copy_csv_into_stage(cur, stage_name: str, csv_path: str, csv_cols: List[str]):
    """
    COPY the CSV into the temp staging table.
    """
    copy_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')"
    ).format(
        sql.Identifier(stage_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in csv_cols),
    )

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        cur.copy_expert(copy_sql.as_string(cur.connection), f)



def build_upsert_sql(schema: str, table: str, stage_name: str, csv_cols: List[str], pk_cols: List[str], colmeta: Dict[str, ColMeta]):
    """
    Build the set-based UPSERT from stage -> target, returning a *single row* with inserted/updated counts.

    Applies "overwrite but don't overwrite blanks/nulls" by:
      - staging SELECT: NULLIF(btrim(s.col), '') (and cast if needed)
      - DO UPDATE: col = COALESCE(EXCLUDED.col, tgt.col)
      - DO UPDATE WHERE: only when an incoming non-blank/non-null value differs from target

    This enables accurate updated-vs-unchanged counting and avoids pointless updates.
    """
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
            sql.SQL("{} = COALESCE(EXCLUDED.{}, tgt.{})")
               .format(sql.Identifier(c), sql.Identifier(c), sql.Identifier(c))
            for c in updatable
        ]
        change_conds = [
            sql.SQL("(EXCLUDED.{} IS NOT NULL AND EXCLUDED.{} IS DISTINCT FROM tgt.{})")
               .format(sql.Identifier(c), sql.Identifier(c), sql.Identifier(c))
            for c in updatable
        ]
        on_conflict = sql.SQL("DO UPDATE SET {} WHERE {}").format(
            sql.SQL(", ").join(assignments),
            sql.SQL(" OR ").join(change_conds),
        )
    else:
        on_conflict = sql.SQL("DO NOTHING")

    insert_stmt = sql.SQL("""
        INSERT INTO {schema}.{table} AS tgt ({cols})
        SELECT {selects}
        FROM {stage} AS s
        ON CONFLICT ({pk}) {action}
        RETURNING (xmax = 0) AS inserted
    """).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols),
        selects=sql.SQL(", ").join(select_exprs),
        stage=sql.Identifier(stage_name),
        pk=sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols),
        action=on_conflict,
    )

    stmt = sql.SQL("""
        WITH upserted AS (
            {insert_stmt}
        )
        SELECT
            COUNT(*) FILTER (WHERE inserted)     AS inserted_rows,
            COUNT(*) FILTER (WHERE NOT inserted) AS updated_rows
        FROM upserted
    """).format(insert_stmt=insert_stmt)

    return stmt


def _pk_blank_condition(stage_alias: str, pk_cols: List[str]) -> sql.SQL:
    conds = []
    for c in pk_cols:
        conds.append(
            sql.SQL("({a}.{c} IS NULL OR btrim({a}.{c}) = '')").format(
                a=sql.Identifier(stage_alias), c=sql.Identifier(c)
            )
        )
    return sql.SQL(" OR ").join(conds)


def _count_distinct_pk(cur, stage_name: str, pk_cols: List[str]) -> int:
    row_expr = sql.SQL("ROW({})").format(
        sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(c)) for c in pk_cols)
    )
    q = sql.SQL("SELECT COUNT(DISTINCT {row_expr}) FROM {stage} AS s").format(
        row_expr=row_expr, stage=sql.Identifier(stage_name)
    )
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
    not_null_conds = sql.SQL(" AND ").join(
        sql.SQL("k.{} IS NOT NULL").format(sql.Identifier(c)) for c in pk_cols
    )

    q = sql.SQL("""
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
    """).format(
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
    cur.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    )
    return int(cur.fetchone()[0])


def main():
    ap = argparse.ArgumentParser(description="Bulk upsert a CSV into a Supabase/Postgres table (schema sync + COPY + UPSERT).")
    ap.add_argument("--schema", default="f_5500", help="Target schema (default: f_5500)")
    ap.add_argument("--table", required=True, help="Target table name (lowercase). Example: audit_reports")
    ap.add_argument("--csv", required=True, dest="csv_path", help="Path to CSV file")
    ap.add_argument("--layout", required=False, dest="layout_path", help="Path to layout .txt file (recommended)")
    ap.add_argument("--db-url", required=False, dest="db_url", help="Optional database URL/DSN (overrides env vars)")
    ap.add_argument("--no-schema-sync", action="store_true", help="Skip adding missing columns (not recommended)")
    ap.add_argument("--dry-run", action="store_true", help="Print planned actions and exit (no DB writes)")
    args = ap.parse_args()

    schema = args.schema.strip()
    table = args.table.strip().lower()
    csv_path = args.csv_path
    layout_path = args.layout_path

    if not VALID_COL_RE.match(table):
        raise ValueError("Table name should be lowercase snake_case (letters, digits, underscore).")

    t0 = time.time()

    csv_cols = read_csv_header(csv_path)

    layout_types: Dict[str, str] = {}
    if layout_path:
        layout_types = parse_layout(layout_path)

    conn = connect_db(args.db_url)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Basic existence check
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema=%s AND table_name=%s
                """,
                (schema, table),
            )
            if not cur.fetchone():
                raise ValueError(f"Table not found: {schema}.{table}")

            pk_cols = fetch_pk_columns(cur, schema, table)
            if not pk_cols:
                raise ValueError(f"No PRIMARY KEY found for {schema}.{table} (expected one per your rule).")

            colmeta = fetch_table_columns(cur, schema, table)

            added_cols = []
            if not args.no_schema_sync:
                if layout_path is None:
                    print("WARNING: --layout not provided. New columns (if any) will default to TEXT.", file=sys.stderr)
                added_cols, colmeta = ensure_columns(cur, schema, table, csv_cols, colmeta, layout_types)

            # Re-check PK columns exist and are in CSV
            missing_pk = [c for c in pk_cols if c not in csv_cols]
            if missing_pk:
                raise ValueError(f"CSV is missing PK column(s) required for upsert: {missing_pk}")

            # Build stage + upsert SQL
            stage_name = f"stage_{uuid.uuid4().hex}"
            upsert_sql = build_upsert_sql(schema, table, stage_name, csv_cols, pk_cols, colmeta)

            if args.dry_run:
                print(f"Schema: {schema}")
                print(f"Table:  {table}")
                print(f"CSV columns ({len(csv_cols)}): {csv_cols[:20]}{'...' if len(csv_cols)>20 else ''}")
                print(f"PK columns ({len(pk_cols)}): {pk_cols}")
                if added_cols:
                    print(f"Would add {len(added_cols)} column(s): {added_cols}")
                else:
                    print("No missing columns to add.")
                print("\nUpsert SQL (formatted):\n")
                print(upsert_sql.as_string(conn))
                conn.rollback()
                return

            # Run in one transaction
            if added_cols:
                print(f"Added {len(added_cols)} column(s) to {schema}.{table}: {added_cols}")

            print(f"Primary key inferred for {schema}.{table}: {pk_cols}")
            print(f"Creating temp stage table: {stage_name} ...")
            create_temp_stage(cur, stage_name, csv_cols)

            print(f"COPYing CSV into stage (this is the long step for huge files): {csv_path}")
            copy_csv_into_stage(cur, stage_name, csv_path, csv_cols)

            # --- Validation + reporting (always) ---
            print("")
            print("--- Load validation (pre-upsert) ---")
            existing_total_before = _count_rows(cur, schema, table)

            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(stage_name)))
            incoming_rows = int(cur.fetchone()[0])

            incoming_distinct_keys = _count_distinct_pk(cur, stage_name, pk_cols)
            duplicate_keys = incoming_rows - incoming_distinct_keys

            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {} AS s WHERE {}").format(
                    sql.Identifier(stage_name),
                    _pk_blank_condition("s", pk_cols),
                )
            )
            pk_blank_count = int(cur.fetchone()[0])

            if pk_blank_count > 0:
                raise RuntimeError(f"FAIL: Found {pk_blank_count} row(s) in the file with blank/NULL primary key columns: {pk_cols}")

            if duplicate_keys > 0:
                raise RuntimeError(
                    f"FAIL: File contains duplicate primary keys ({duplicate_keys} duplicate row(s)). "
                    "Postgres upsert would be ambiguous (and can error). Deduplicate the CSV by PK before loading."
                )

            pk_meta = {c: colmeta[c] for c in pk_cols}
            expected_new_rows = _count_new_keys(cur, schema, table, stage_name, pk_cols, pk_meta)
            existing_keys_in_file = incoming_distinct_keys - expected_new_rows

            print(f"Existing rows in {schema}.{table} BEFORE: {existing_total_before}")
            print(f"Incoming file rows: {incoming_rows}")
            print(f"Incoming distinct PKs: {incoming_distinct_keys}")
            print(f"Expected new rows (inserts): {expected_new_rows}")
            print(f"Expected existing keys (potential updates/unchanged): {existing_keys_in_file}")

            # --- Run UPSERT and get inserted/updated counts from DB ---
            print("")
            print("Running UPSERT from stage -> target ...")
            cur.execute(upsert_sql)
            inserted_rows, updated_rows = cur.fetchone()
            inserted_rows = int(inserted_rows or 0)
            updated_rows = int(updated_rows or 0)
            unchanged_rows = existing_keys_in_file - updated_rows

            existing_total_after = _count_rows(cur, schema, table)
            delta_rows = existing_total_after - existing_total_before

            print("")
            print("--- Load results (post-upsert) ---")
            print(f"Inserted rows:  {inserted_rows}")
            print(f"Updated rows:   {updated_rows}")
            print(f"Unchanged rows: {unchanged_rows}")
            print(f"Existing rows in {schema}.{table} AFTER: {existing_total_after} (delta: {delta_rows})")

            discrepancies = []
            if inserted_rows != expected_new_rows:
                discrepancies.append(f"inserted_rows ({inserted_rows}) != expected_new_rows ({expected_new_rows})")
            if delta_rows != inserted_rows:
                discrepancies.append(f"table row delta ({delta_rows}) != inserted_rows ({inserted_rows})")
            if updated_rows < 0 or updated_rows > existing_keys_in_file:
                discrepancies.append(f"updated_rows ({updated_rows}) out of range (0..{existing_keys_in_file})")
            if unchanged_rows < 0:
                discrepancies.append(f"unchanged_rows computed negative ({unchanged_rows}); check key math")

            if discrepancies:
                nl = chr(10)
                raise RuntimeError("FAIL: Discrepancy detected:" + nl + "  - " + (nl + "  - ").join(discrepancies))

            conn.commit()
            print("Validation: PASS (no discrepancies detected).")
            dt = time.time() - t0
            print(f"Done. Committed successfully in {dt:.1f}s.")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
