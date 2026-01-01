"""
Author: Betafits Engineering
Last updated: 2025-12-30

Main CLI entry point for 5500 data pipelines.
Orchestrates ingestion, calculated fields, and legacy script execution.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List, Sequence

# Import from lib (new structure)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config.form5500_config import CalculatedFieldsConfig, IngestConfig, ScriptConfig
from workflows.langgraph.form5500_flow.graph import run_calc, run_ingest, run_legacy_script, run_script

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

LEGACY_BASE = Path(__file__).resolve().parents[2] / "services" / "form5500_legacy"


def _legacy_script(*parts: str) -> Path:
    """
    Author: Betafits Engineering
    Last updated: 2025-12-30
    
    Resolves path to legacy script.
    
    Args:
        parts: Path components
        
    Returns:
        Path to legacy script
    """
    return LEGACY_BASE.joinpath(*parts)


def parse_args() -> argparse.Namespace:
    """
    Author: Betafits Engineering
    Last updated: 2025-12-30
    
    Parses CLI arguments for all pipeline commands.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(description="LangGraph pipelines for 5500 workflows")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingest a CSV into Postgres/Supabase")
    ingest.add_argument("--csv", dest="csv_path", required=True, help="Path to CSV file to ingest")
    ingest.add_argument("--table", required=True, help="Target table name (lowercase snake_case)")
    ingest.add_argument("--schema", default="f_5500", help="Target schema (default: f_5500)")
    ingest.add_argument("--layout", dest="layout_path", required=False, help="Layout file for schema sync")
    ingest.add_argument("--db-url", dest="db_url", required=False, help="Database URL/DSN (optional)")
    ingest.add_argument("--dry-run", action="store_true", help="Validate and stage only; no commit")
    ingest.add_argument(
        "--apply-calculated",
        nargs="*",
        dest="apply_calculated",
        default=None,
        help="Calculated field SQL scripts to run after upsert (omit .sql extension)",
    )
    ingest.add_argument(
        "--pk",
        nargs="+",
        dest="pk_columns",
        required=False,
        help="Primary key column(s) to enforce when creating a table or when none exists",
    )
    ingest.add_argument(
        "--create-table-if-missing",
        action="store_true",
        dest="create_table_if_missing",
        help="Create the target table from the CSV header if it does not exist",
    )

    calc = sub.add_parser("calc", help="Run calculated field SQL scripts without ingesting")
    calc.add_argument(
        "--scripts",
        nargs="+",
        required=True,
        help="Calculated field SQL scripts to run (omit .sql extension, resolved from assets/calculated_fields)",
    )
    calc.add_argument("--db-url", dest="db_url", required=False, help="Database URL/DSN (optional)")
    calc.add_argument("--schema", default="f_5500", help="Target schema (default: f_5500)")

    script = sub.add_parser("script", help="Run a legacy script via LangGraph (matching, benchmarking, reports, PEO, etc.)")
    script.add_argument("--path", dest="script_path", required=True, help="Path to the legacy script to execute")
    script.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to the legacy script")

    match = sub.add_parser("match", help="Run prospects 5500 matching pipeline (legacy logic)")
    match.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to the matcher")

    benchmark = sub.add_parser("benchmark", help="Run retirement benchmarking ETL (legacy logic)")
    benchmark.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to benchmarking")

    reports = sub.add_parser("reports-401k", help="Generate 401k at a glance reports (legacy logic)")
    reports.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to report generator")

    peo = sub.add_parser("peo", help="Download and attach PEO 5500s (legacy logic)")
    peo.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to PEO script")

    audit_upload = sub.add_parser("audit-upload", help="Upload audit report CSVs to Supabase (legacy logic)")
    audit_upload.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to upload script")

    audit_airtable = sub.add_parser("audit-airtable", help="Update Airtable audit report URLs (legacy logic)")
    audit_airtable.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to Airtable updater")

    dataset = sub.add_parser("dataset-upload", help="Upload 5500 dataset CSV to Supabase (legacy logic)")
    dataset.add_argument("--", dest="script_args", nargs=argparse.REMAINDER, help="Arguments passed to dataset uploader")

    return parser.parse_args()


def main() -> None:
    """
    Author: Betafits Engineering
    Last updated: 2025-12-30
    
    Main CLI entry point. Routes commands to appropriate handlers.
    """
    args = parse_args()
    if args.command == "ingest":
        config = IngestConfig.from_args(
            csv_path=args.csv_path,
            table=args.table,
            schema=args.schema,
            layout_path=args.layout_path,
            db_url=args.db_url,
            dry_run=args.dry_run,
            apply_calculated=args.apply_calculated,
            pk_columns=args.pk_columns,
            create_table_if_missing=args.create_table_if_missing,
        )
        result = run_ingest(config)
        trimmed = {
            k: v
            for k, v in result.items()
            if k not in {"connection", "layout_types", "column_meta"}
        }
        print(json.dumps(trimmed, indent=2, default=str))
    elif args.command == "calc":
        config = CalculatedFieldsConfig.from_args(args.scripts, db_url=args.db_url, schema=args.schema)
        result = run_calc(config)
        print(json.dumps(result, indent=2, default=str))
    elif args.command == "script":
        config = ScriptConfig.from_args(args.script_path, args.script_args)
        result = run_script(config)
        print(json.dumps(result, indent=2, default=str))
    elif args.command == "match":
        script_path = _legacy_script("prospects_5500_matching", "prospects_main_5500_matcher.py")
        print(json.dumps(run_legacy_script(script_path, args.script_args), indent=2, default=str))
    elif args.command == "benchmark":
        script_path = _legacy_script("retirement_benchmarking_etl", "compute_industry_benchmark_percentiles.py")
        print(json.dumps(run_legacy_script(script_path, args.script_args), indent=2, default=str))
    elif args.command == "reports-401k":
        script_path = _legacy_script("401k_at_a_glance_reports", "generate_and_attach_401k_reports.py")
        print(json.dumps(run_legacy_script(script_path, args.script_args), indent=2, default=str))
    elif args.command == "peo":
        script_path = _legacy_script("peo_5500_download_and_attach.py")
        print(json.dumps(run_legacy_script(script_path, args.script_args), indent=2, default=str))
    elif args.command == "audit-upload":
        script_path = _legacy_script("audit_reports_sync_pipeline", "upload_audit_report_csv_to_supabase.py")
        print(json.dumps(run_legacy_script(script_path, args.script_args), indent=2, default=str))
    elif args.command == "audit-airtable":
        script_path = _legacy_script("audit_reports_sync_pipeline", "update_airtable_audit_report_urls.py")
        print(json.dumps(run_legacy_script(script_path, args.script_args), indent=2, default=str))
    elif args.command == "dataset-upload":
        script_path = _legacy_script("5500dataset_upload_to_supabase.py")
        print(json.dumps(run_legacy_script(script_path, args.script_args), indent=2, default=str))
    else:
        raise SystemExit(f"Unsupported pipeline '{args.command}'.")


if __name__ == "__main__":
    main()
