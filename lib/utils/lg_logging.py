"""
Universal LangGraph Run and State Snapshot utilities.

This module provides helper functions for working with the universal
lg_runs and state_snapshots tables in Supabase.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from lib.integrations.supabase.supabase_client import get_supabase_client


def generate_run_id() -> str:
    """Generate a unique run ID in the format: LG-RUN-YYYY-MM-DD-XXXX."""
    now = datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")
    unique_suffix = str(uuid.uuid4())[:8].upper()
    return f"LG-RUN-{date_str}-{unique_suffix}"


def create_lg_run(
    workflow: str,
    triggered_by: str,
    environment: str = "production",
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Create a new LangGraph run entry.
    
    Args:
        workflow: Name of the workflow (e.g., "Email Processing", "Form 5500 Ingestion")
        triggered_by: What triggered the run (e.g., "Gmail API", "CLI", "API Request")
        environment: Deployment environment (e.g., "production", "development", "staging")
        metadata: Additional context about the run
        
    Returns:
        run_id: The generated run ID
    """
    supabase = get_supabase_client()
    run_id = generate_run_id()
    
    run_data = {
        "run_id": run_id,
        "workflow": workflow,
        "triggered_by": triggered_by,
        "environment": environment,
        "status": "started",
        "step_log": [],
        "metadata": metadata or {},
        "started_at": datetime.utcnow().isoformat(),
    }
    
    result = supabase.table("lg_runs").insert(run_data).execute()
    
    if not result.data:
        raise Exception(f"Failed to create LG run: {run_id}")
    
    return run_id


def update_lg_run_status(
    run_id: str,
    status: str,
    error_details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Update the status of a LangGraph run.
    
    Args:
        run_id: The run ID to update
        status: New status (started, running, completed, failed, cancelled)
        error_details: Error information if status is 'failed'
    """
    supabase = get_supabase_client()
    
    update_data: Dict[str, Any] = {"status": status}
    
    if status in ("completed", "failed", "cancelled"):
        now = datetime.utcnow()
        update_data["completed_at"] = now.isoformat()
        
        # Calculate duration if we have started_at
        result = supabase.table("lg_runs").select("started_at").eq("run_id", run_id).execute()
        if result.data:
            started_at = datetime.fromisoformat(result.data[0]["started_at"].replace("Z", "+00:00"))
            duration = (now - started_at.replace(tzinfo=None)).total_seconds()
            update_data["duration_seconds"] = round(duration, 3)
    
    if error_details:
        update_data["error_details"] = error_details
    
    supabase.table("lg_runs").update(update_data).eq("run_id", run_id).execute()


def log_lg_step(
    run_id: str,
    step_name: str,
    step_status: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Add a step execution log to a LangGraph run.
    
    Args:
        run_id: The run ID to log to
        step_name: Name of the step (e.g., "classify_email", "extract_entities")
        step_status: Status of the step (e.g., "started", "completed", "failed")
        metadata: Additional step metadata (duration, inputs, outputs, etc.)
    """
    supabase = get_supabase_client()
    
    # Fetch current step_log
    result = supabase.table("lg_runs").select("step_log").eq("run_id", run_id).execute()
    
    if not result.data:
        raise Exception(f"Run not found: {run_id}")
    
    step_log = result.data[0]["step_log"] or []
    
    # Add new step entry
    step_entry = {
        "step": step_name,
        "status": step_status,
        "timestamp": datetime.utcnow().isoformat(),
        "metadata": metadata or {},
    }
    step_log.append(step_entry)
    
    # Update the run
    supabase.table("lg_runs").update({"step_log": step_log}).eq("run_id", run_id).execute()


def create_state_envelope(
    workflow_id: str,
    run_id: str,
    actor: str,
    phase: str,
    payload: Dict[str, Any],
    schema_version: str = "1.0",
) -> Dict[str, Any]:
    """
    Create a universal state envelope.
    
    Args:
        workflow_id: Identifier for the workflow type
        run_id: The LG run ID
        actor: Who/what is performing this action
        phase: Current workflow phase
        payload: The actual state data
        schema_version: Version of the envelope schema
        
    Returns:
        State envelope dictionary
    """
    return {
        "schema_version": schema_version,
        "workflow_id": workflow_id,
        "run_id": run_id,
        "actor": actor,
        "phase": phase,
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload,
    }


def save_state_snapshot(
    run_id: str,
    state_envelope: Dict[str, Any],
    phase: str,
    checkpoint_id: Optional[str] = None,
) -> str:
    """
    Save a state snapshot to the database.
    
    Args:
        run_id: The LG run ID
        state_envelope: Universal state envelope
        phase: Current workflow phase
        checkpoint_id: Optional LangGraph checkpoint ID
        
    Returns:
        snapshot_id: The generated snapshot ID
    """
    supabase = get_supabase_client()
    snapshot_id = f"SNAP-{uuid.uuid4()}"
    
    snapshot_data = {
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "state_envelope": state_envelope,
        "phase": phase,
        "checkpoint_id": checkpoint_id,
    }
    
    result = supabase.table("state_snapshots").insert(snapshot_data).execute()
    
    if not result.data:
        raise Exception(f"Failed to create state snapshot for run: {run_id}")
    
    return snapshot_id


def get_state_snapshots(
    run_id: str,
    phase: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve state snapshots for a run.
    
    Args:
        run_id: The LG run ID
        phase: Optional filter by phase
        
    Returns:
        List of state snapshots
    """
    supabase = get_supabase_client()
    
    query = supabase.table("state_snapshots").select("*").eq("run_id", run_id)
    
    if phase:
        query = query.eq("phase", phase)
    
    result = query.order("created_at", desc=False).execute()
    
    return result.data or []


def get_lg_run(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a LangGraph run by ID.
    
    Args:
        run_id: The run ID
        
    Returns:
        Run data or None if not found
    """
    supabase = get_supabase_client()
    result = supabase.table("lg_runs").select("*").eq("run_id", run_id).execute()
    
    return result.data[0] if result.data else None


def get_recent_runs(
    workflow: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Retrieve recent LangGraph runs.
    
    Args:
        workflow: Optional filter by workflow name
        status: Optional filter by status
        limit: Maximum number of runs to return
        
    Returns:
        List of run records
    """
    supabase = get_supabase_client()
    
    query = supabase.table("lg_runs").select("*")
    
    if workflow:
        query = query.eq("workflow", workflow)
    
    if status:
        query = query.eq("status", status)
    
    result = query.order("started_at", desc=True).limit(limit).execute()
    
    return result.data or []
