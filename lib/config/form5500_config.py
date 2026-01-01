"""
Author: Betafits Engineering
Last updated: 2025-12-30

Configuration classes for ingestion, calculated fields, and script execution.
All configuration is immutable (dataclass frozen where practical).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence


@dataclass
class DatabaseConfig:
    """
    Database connection configuration.
    
    Attributes:
        db_url: Optional database DSN (falls back to env vars if None)
        schema: Target schema for operations (default: f_5500)
    """
    db_url: Optional[str] = None
    schema: str = "f_5500"

    def resolved_url(self) -> Optional[str]:
        """Returns the database URL."""
        return self.db_url


@dataclass
class IngestConfig:
    """
    Configuration for CSV ingestion graph.
    
    Attributes:
        csv_path: Path to CSV file
        table: Target table name
        layout_path: Optional layout file for column types
        database: Database configuration
        dry_run: If True, rollback after validation
        apply_calculated: SQL scripts to run after upsert
        pk_columns: Primary key columns
        create_table_if_missing: Auto-create table if absent
    """
    csv_path: Path
    table: str
    layout_path: Optional[Path] = None
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    dry_run: bool = False
    apply_calculated: List[str] | None = None
    pk_columns: Optional[List[str]] = None
    create_table_if_missing: bool = False

    @classmethod
    def from_args(
        cls,
        csv_path: str,
        table: str,
        schema: str = "f_5500",
        layout_path: Optional[str] = None,
        db_url: Optional[str] = None,
        dry_run: bool = False,
        apply_calculated: Optional[Iterable[str]] = None,
        pk_columns: Optional[Iterable[str]] = None,
        create_table_if_missing: bool = False,
    ) -> "IngestConfig":
        """
        Author: Betafits Engineering
        Last updated: 2025-12-30
        
        Creates IngestConfig from CLI arguments.
        
        Args:
            csv_path: CSV file path
            table: Target table name
            schema: Database schema
            layout_path: Optional layout file
            db_url: Database connection URL
            dry_run: Rollback flag
            apply_calculated: Scripts to apply
            pk_columns: Primary key columns
            create_table_if_missing: Auto-create flag
            
        Returns:
            IngestConfig instance
        """
        return cls(
            csv_path=Path(csv_path).expanduser(),
            table=table,
            layout_path=Path(layout_path).expanduser() if layout_path else None,
            database=DatabaseConfig(db_url=db_url, schema=schema),
            dry_run=dry_run,
            apply_calculated=list(apply_calculated) if apply_calculated else None,
            pk_columns=list(pk_columns) if pk_columns else None,
            create_table_if_missing=create_table_if_missing,
        )


@dataclass
class ScriptConfig:
    """
    Configuration for running legacy scripts via LangGraph.
    
    Attributes:
        script_path: Path to Python script
        args: Command-line arguments
    """
    script_path: Path
    args: Sequence[str] | None = None

    @classmethod
    def from_args(cls, script_path: str, args: Optional[Iterable[str]] = None) -> "ScriptConfig":
        """
        Author: Betafits Engineering
        Last updated: 2025-12-30
        
        Creates ScriptConfig from arguments.
        
        Args:
            script_path: Path to script
            args: Script arguments
            
        Returns:
            ScriptConfig instance
        """
        return cls(script_path=Path(script_path).expanduser(), args=list(args) if args else None)


@dataclass
class CalculatedFieldsConfig:
    """
    Configuration for calculated field SQL execution.
    
    Attributes:
        scripts: List of SQL script names (without .sql extension)
        db_url: Optional database URL
        schema: Target schema
    """
    scripts: List[str]
    db_url: Optional[str] = None
    schema: str = "f_5500"

    @classmethod
    def from_args(
        cls,
        scripts: Iterable[str],
        db_url: Optional[str] = None,
        schema: str = "f_5500"
    ) -> "CalculatedFieldsConfig":
        """
        Author: Betafits Engineering
        Last updated: 2025-12-30
        
        Creates CalculatedFieldsConfig from arguments.
        
        Args:
            scripts: SQL script names
            db_url: Database URL
            schema: Target schema
            
        Returns:
            CalculatedFieldsConfig instance
        """
        return cls(scripts=list(scripts), db_url=db_url, schema=schema)
