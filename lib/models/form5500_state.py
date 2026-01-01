"""
Author: Betafits Engineering
Last updated: 2025-12-30

State models for LangGraph workflows.
Contains TypedDict and dataclass definitions for pipeline state management.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, TypedDict

import psycopg2


class IngestState(TypedDict, total=False):
    """
    Shared state that flows through the LangGraph ingestion pipeline.
    
    Tracks CSV processing, database connections, validation results,
    and upsert outcomes across all pipeline nodes.
    """
    csv_path: Path
    layout_path: Optional[Path]
    schema: str
    table: str
    db_url: Optional[str]
    csv_columns: List[str]
    layout_types: Dict[str, str]
    pk_columns: List[str]
    column_meta: Dict[str, "ColMeta"]
    stage_name: str
    added_columns: List[str]
    validation: Dict[str, int]
    upsert_result: Dict[str, int]
    calculated_runs: Dict[str, str]
    dry_run: bool
    create_table_if_missing: bool
    configured_pk: Optional[List[str]]
    table_created: bool
    connection: psycopg2.extensions.connection


@dataclass(frozen=True)
class ColMeta:
    """
    Column metadata from PostgreSQL information_schema.
    
    Attributes:
        data_type: SQL data type (e.g., 'text', 'integer')
        udt_name: User-defined type name
    """
    data_type: str
    udt_name: str


@dataclass
class ValidationSummary:
    """
    Validation metrics computed before upsert operation.
    
    Tracks incoming data quality, duplicate keys, and expected database changes.
    """
    incoming_rows: int = 0
    incoming_distinct_keys: int = 0
    duplicate_keys: int = 0
    pk_blank_count: int = 0
    expected_new_rows: int = 0
    existing_keys_in_file: int = 0
    existing_total_before: int = 0


@dataclass
class UpsertSummary:
    """
    Upsert operation results and discrepancy tracking.
    
    Contains actual database changes and validation checks.
    """
    inserted_rows: int
    updated_rows: int
    unchanged_rows: int
    existing_total_after: int
    delta_rows: int
    discrepancies: List[str] = field(default_factory=list)


class ScriptState(TypedDict, total=False):
    """
    State for running legacy scripts via LangGraph orchestration.
    
    Captures script path, arguments, and execution output.
    """
    script_path: Path
    script_args: List[str]
    stdout: str
    stderr: str


class CalcState(TypedDict, total=False):
    """
    State for calculated field SQL script execution.
    
    Tracks which SQL scripts to run and their results.
    """
    scripts: List[str]
    schema: str
    db_url: Optional[str]
    results: Dict[str, str]
