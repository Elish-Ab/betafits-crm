"""Unit tests for Form 5500 models and state management."""
import pytest
from pathlib import Path
from lib.models.form5500_state import (
    IngestState,
    ColMeta,
    ValidationSummary,
    UpsertSummary,
    ScriptState,
)


class TestForm5500StateModels:
    """Test suite for Form 5500 state data structures."""
    
    def test_col_meta_creation(self):
        """Test ColMeta dataclass creation."""
        col_meta = ColMeta(data_type="text", udt_name="varchar")
        assert col_meta.data_type == "text"
        assert col_meta.udt_name == "varchar"
        
    def test_col_meta_immutable(self):
        """Test that ColMeta is frozen/immutable."""
        col_meta = ColMeta(data_type="integer", udt_name="int4")
        with pytest.raises(Exception):  # Should be frozen
            col_meta.data_type = "text"
    
    def test_validation_summary_defaults(self):
        """Test ValidationSummary default values."""
        summary = ValidationSummary()
        assert summary.incoming_rows == 0
        assert summary.incoming_distinct_keys == 0
        assert summary.duplicate_keys == 0
        assert summary.pk_blank_count == 0
        assert summary.expected_new_rows == 0
        
    def test_validation_summary_with_data(self):
        """Test ValidationSummary with actual data."""
        summary = ValidationSummary(
            incoming_rows=1000,
            incoming_distinct_keys=950,
            duplicate_keys=50,
            pk_blank_count=5,
            expected_new_rows=100,
            existing_keys_in_file=900,
            existing_total_before=5000,
        )
        assert summary.incoming_rows == 1000
        assert summary.duplicate_keys == 50
        assert summary.expected_new_rows == 100
        
    def test_upsert_summary_defaults(self):
        """Test UpsertSummary initialization."""
        summary = UpsertSummary(
            inserted_rows=100,
            updated_rows=50,
            unchanged_rows=850,
            existing_total_after=5100,
            delta_rows=100,
        )
        assert summary.inserted_rows == 100
        assert summary.updated_rows == 50
        assert summary.unchanged_rows == 850
        assert len(summary.discrepancies) == 0
        
    def test_upsert_summary_with_discrepancies(self):
        """Test UpsertSummary with discrepancies."""
        summary = UpsertSummary(
            inserted_rows=100,
            updated_rows=50,
            unchanged_rows=850,
            existing_total_after=5100,
            delta_rows=100,
            discrepancies=["Expected 100 new rows, got 95"],
        )
        assert len(summary.discrepancies) == 1
        assert "Expected 100 new rows" in summary.discrepancies[0]


class TestIngestState:
    """Test IngestState TypedDict structure."""
    
    def test_ingest_state_basic(self):
        """Test IngestState with required fields."""
        state: IngestState = {
            "csv_path": Path("/data/form5500.csv"),
            "schema": "f_5500",
            "table": "f_5500_2023",
            "dry_run": False,
        }
        assert state["csv_path"].name == "form5500.csv"
        assert state["schema"] == "f_5500"
        assert state["table"] == "f_5500_2023"
        assert state["dry_run"] is False
        
    def test_ingest_state_with_pk(self):
        """Test IngestState with primary key configuration."""
        state: IngestState = {
            "csv_path": Path("/data/form5500.csv"),
            "schema": "f_5500",
            "table": "f_5500_2023",
            "pk_columns": ["ack_id", "plan_num"],
            "configured_pk": ["ack_id", "plan_num"],
        }
        assert len(state["pk_columns"]) == 2
        assert "ack_id" in state["pk_columns"]


class TestScriptState:
    """Test ScriptState for legacy script execution."""
    
    def test_script_state_creation(self):
        """Test ScriptState structure."""
        state: ScriptState = {
            "script_path": Path("/scripts/process_5500.py"),
            "script_args": ["--year", "2023"],
            "stdout": "Processing complete",
            "stderr": "",
        }
        assert state["script_path"].name == "process_5500.py"
        assert len(state["script_args"]) == 2
        assert state["stdout"] == "Processing complete"


def test_form5500_workflow_state_transitions():
    """Test state transitions through Form 5500 workflow."""
    # Initial state
    state: IngestState = {
        "csv_path": Path("/data/form5500_2023.csv"),
        "schema": "f_5500",
        "table": "f_5500_2023",
        "pk_columns": ["ack_id"],
        "dry_run": False,
        "create_table_if_missing": True,
        "table_created": False,
    }
    
    # Simulate CSV read
    state["csv_columns"] = ["ack_id", "plan_name", "sponsor_ein"]
    state["added_columns"] = []
    
    # Simulate validation
    state["validation"] = {
        "incoming_rows": 1000,
        "duplicate_keys": 10,
    }
    
    # Simulate upsert
    state["upsert_result"] = {
        "inserted": 100,
        "updated": 50,
    }
    
    assert state["csv_columns"][0] == "ack_id"
    assert state["validation"]["incoming_rows"] == 1000
    assert state["upsert_result"]["inserted"] == 100
