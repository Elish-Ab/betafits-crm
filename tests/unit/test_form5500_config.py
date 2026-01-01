"""Unit tests for Form 5500 configuration."""
import pytest
from pathlib import Path
from lib.config.form5500_config import (
    DatabaseConfig,
    IngestConfig,
    CalculatedFieldsConfig,
    ScriptConfig,
)


class TestDatabaseConfig:
    """Test DatabaseConfig for Form 5500."""
    
    def test_database_config_defaults(self):
        """Test default database configuration."""
        config = DatabaseConfig()
        assert config.schema == "f_5500"
        assert config.db_url is None
        
    def test_database_config_with_url(self):
        """Test database config with connection URL."""
        config = DatabaseConfig(
            db_url="postgresql://user:pass@localhost/db",
            schema="f_5500"
        )
        assert config.db_url == "postgresql://user:pass@localhost/db"
        assert config.resolved_url() == "postgresql://user:pass@localhost/db"
        
    def test_database_config_custom_schema(self):
        """Test custom schema configuration."""
        config = DatabaseConfig(schema="f_5500_test")
        assert config.schema == "f_5500_test"


class TestIngestConfig:
    """Test IngestConfig for CSV ingestion."""
    
    def test_ingest_config_minimal(self):
        """Test IngestConfig with minimal required fields."""
        config = IngestConfig(
            csv_path=Path("/data/form5500.csv"),
            table="f_5500_2023",
        )
        assert config.csv_path.name == "form5500.csv"
        assert config.table == "f_5500_2023"
        assert config.dry_run is False
        assert config.create_table_if_missing is False
        
    def test_ingest_config_with_layout(self):
        """Test IngestConfig with layout file."""
        config = IngestConfig(
            csv_path=Path("/data/form5500.csv"),
            table="f_5500_2023",
            layout_path=Path("/layouts/layout.txt"),
        )
        assert config.layout_path is not None
        assert config.layout_path.name == "layout.txt"
        
    def test_ingest_config_with_pk(self):
        """Test IngestConfig with primary key columns."""
        config = IngestConfig(
            csv_path=Path("/data/form5500.csv"),
            table="f_5500_2023",
            pk_columns=["ack_id", "plan_num"],
        )
        assert len(config.pk_columns) == 2
        assert "ack_id" in config.pk_columns
        
    def test_ingest_config_dry_run(self):
        """Test IngestConfig in dry run mode."""
        config = IngestConfig(
            csv_path=Path("/data/form5500.csv"),
            table="f_5500_2023",
            dry_run=True,
        )
        assert config.dry_run is True
        
    def test_ingest_config_from_args(self):
        """Test IngestConfig.from_args class method."""
        config = IngestConfig.from_args(
            csv_path="/data/form5500.csv",
            table="f_5500_2023",
            schema="f_5500",
            pk_columns=["ack_id"],
            dry_run=False,
            create_table_if_missing=True,
        )
        assert config.csv_path.name == "form5500.csv"
        assert config.table == "f_5500_2023"
        assert config.database.schema == "f_5500"
        assert config.pk_columns == ["ack_id"]
        assert config.create_table_if_missing is True
        
    def test_ingest_config_with_calculated_fields(self):
        """Test IngestConfig with calculated field scripts."""
        config = IngestConfig(
            csv_path=Path("/data/form5500.csv"),
            table="f_5500_2023",
            apply_calculated=["calc_plan_size", "calc_risk_score"],
        )
        assert config.apply_calculated is not None
        assert len(config.apply_calculated) == 2
        assert "calc_plan_size" in config.apply_calculated


class TestCalculatedFieldsConfig:
    """Test CalculatedFieldsConfig."""
    
    def test_calculated_fields_config_creation(self):
        """Test CalculatedFieldsConfig initialization."""
        config = CalculatedFieldsConfig(
            scripts=["calc_plan_size.sql"],
            schema="f_5500",
        )
        assert len(config.scripts) == 1
        assert config.schema == "f_5500"
        assert config.scripts[0] == "calc_plan_size.sql"


class TestScriptConfig:
    """Test ScriptConfig for legacy script execution."""
    
    def test_script_config_creation(self):
        """Test ScriptConfig initialization."""
        config = ScriptConfig(
            script_path=Path("/scripts/process_5500.py"),
            args=["--year", "2023"],
        )
        assert config.script_path.name == "process_5500.py"
        assert len(config.args) == 2
        assert config.args[0] == "--year"


def test_form5500_config_integration():
    """Test Form 5500 config components working together."""
    # Database config
    db_config = DatabaseConfig(
        db_url="postgresql://localhost/betafits",
        schema="f_5500",
    )
    
    # Ingest config
    ingest_config = IngestConfig(
        csv_path=Path("/data/form5500_2023.csv"),
        table="f_5500_2023",
        layout_path=Path("/layouts/f5500_layout.txt"),
        database=db_config,
        pk_columns=["ack_id"],
        dry_run=False,
        create_table_if_missing=True,
        apply_calculated=["calc_plan_size", "calc_sponsor_metrics"],
    )
    
    # Verify integration
    assert ingest_config.database.schema == "f_5500"
    assert ingest_config.database.db_url == "postgresql://localhost/betafits"
    assert ingest_config.csv_path.name == "form5500_2023.csv"
    assert len(ingest_config.apply_calculated) == 2
