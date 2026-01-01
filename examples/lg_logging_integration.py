"""
Example: Integrating LG Run Logging into a LangGraph workflow.

This shows how to use the universal lg_runs and state_snapshots tables
in your LangGraph workflows.
"""

from typing import Any, Dict

from langgraph.graph import StateGraph

from lib.utils.lg_logging import (
    create_lg_run,
    create_state_envelope,
    log_lg_step,
    save_state_snapshot,
    update_lg_run_status,
)


# Example: Email Processing Workflow
def build_email_workflow_with_logging():
    """Build an email processing workflow with universal logging."""
    
    # Define your state
    class EmailState(Dict):
        email_id: str
        classification: str
        entities: list
        # ... other fields
    
    # Create the graph
    graph = StateGraph(EmailState)
    
    # Define nodes with logging
    def classify_email(state: EmailState) -> EmailState:
        """Classify email and log progress."""
        run_id = state.get("run_id")
        
        # Log step start
        log_lg_step(run_id, "classify_email", "started")
        
        try:
            # Do your classification work
            classification = "sales_inquiry"  # Example
            state["classification"] = classification
            
            # Save state snapshot
            envelope = create_state_envelope(
                workflow_id="email_processing",
                run_id=run_id,
                actor="classify_email_node",
                phase="classification",
                payload=state,
            )
            save_state_snapshot(run_id, envelope, "classification")
            
            # Log step completion
            log_lg_step(
                run_id,
                "classify_email",
                "completed",
                metadata={"classification": classification},
            )
            
        except Exception as e:
            log_lg_step(
                run_id,
                "classify_email",
                "failed",
                metadata={"error": str(e)},
            )
            raise
        
        return state
    
    def extract_entities(state: EmailState) -> EmailState:
        """Extract entities and log progress."""
        run_id = state.get("run_id")
        
        log_lg_step(run_id, "extract_entities", "started")
        
        try:
            # Do extraction
            entities = [{"name": "Acme Corp", "type": "company"}]
            state["entities"] = entities
            
            # Save state snapshot
            envelope = create_state_envelope(
                workflow_id="email_processing",
                run_id=run_id,
                actor="extract_entities_node",
                phase="extraction",
                payload=state,
            )
            save_state_snapshot(run_id, envelope, "extraction")
            
            log_lg_step(
                run_id,
                "extract_entities",
                "completed",
                metadata={"entity_count": len(entities)},
            )
            
        except Exception as e:
            log_lg_step(
                run_id,
                "extract_entities",
                "failed",
                metadata={"error": str(e)},
            )
            raise
        
        return state
    
    # Add nodes to graph
    graph.add_node("classify", classify_email)
    graph.add_node("extract", extract_entities)
    
    # Define edges
    graph.set_entry_point("classify")
    graph.add_edge("classify", "extract")
    graph.set_finish_point("extract")
    
    return graph.compile()


def run_email_workflow(email_id: str, email_data: Dict[str, Any]):
    """
    Run the email workflow with logging.
    
    This is how you'd call your workflow from your API or worker.
    """
    
    # 1. Create LG Run
    run_id = create_lg_run(
        workflow="Email Processing",
        triggered_by="Gmail API",
        environment="production",
        metadata={
            "email_id": email_id,
            "received_at": email_data.get("received_at"),
        },
    )
    
    try:
        # 2. Update status to running
        update_lg_run_status(run_id, "running")
        
        # 3. Build and execute workflow
        workflow = build_email_workflow_with_logging()
        
        initial_state = {
            "run_id": run_id,
            "email_id": email_id,
            **email_data,
        }
        
        # 4. Save initial state snapshot
        envelope = create_state_envelope(
            workflow_id="email_processing",
            run_id=run_id,
            actor="workflow_start",
            phase="initialized",
            payload=initial_state,
        )
        save_state_snapshot(run_id, envelope, "initialized")
        
        # 5. Execute workflow
        final_state = workflow.invoke(initial_state)
        
        # 6. Save final state snapshot
        envelope = create_state_envelope(
            workflow_id="email_processing",
            run_id=run_id,
            actor="workflow_end",
            phase="completed",
            payload=final_state,
        )
        save_state_snapshot(run_id, envelope, "completed")
        
        # 7. Mark run as completed
        update_lg_run_status(run_id, "completed")
        
        return final_state
        
    except Exception as e:
        # Log failure
        update_lg_run_status(
            run_id,
            "failed",
            error_details={
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        raise


# Example: Form 5500 Workflow
def run_form5500_workflow(config_path: str):
    """Run Form 5500 ingestion with logging."""
    
    # 1. Create LG Run
    run_id = create_lg_run(
        workflow="Form 5500 Ingestion",
        triggered_by="CLI",
        environment="development",
        metadata={"config_path": config_path},
    )
    
    try:
        update_lg_run_status(run_id, "running")
        
        # 2. Your workflow steps
        steps = [
            "prepare_files",
            "load_data",
            "validate_schema",
            "calculate_fields",
            "upsert_to_db",
            "finalize",
        ]
        
        state = {"run_id": run_id, "config_path": config_path}
        
        for step in steps:
            log_lg_step(run_id, step, "started")
            
            # Do the actual work for this step
            # ... your step logic here ...
            
            # Save snapshot after each step
            envelope = create_state_envelope(
                workflow_id="form5500_ingestion",
                run_id=run_id,
                actor=step,
                phase=step,
                payload=state,
            )
            save_state_snapshot(run_id, envelope, step)
            
            log_lg_step(run_id, step, "completed")
        
        update_lg_run_status(run_id, "completed")
        
    except Exception as e:
        update_lg_run_status(
            run_id,
            "failed",
            error_details={"error": str(e)},
        )
        raise
