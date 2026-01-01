"""
Example: Using Airtable LangGraph Logging in Workflows

This shows how to integrate the Airtable logging system into your
LangGraph workflows.
"""

from datetime import datetime
from typing import Any, Dict

from lib.utils.lg_logging_airtable import (
    complete_lg_run,
    create_lg_run,
    create_state_envelope,
    create_state_snapshot,
    fail_lg_run,
    get_recent_runs,
    get_snapshots_for_run,
)


def example_email_workflow():
    """Example: Email processing workflow with Airtable logging."""
    
    print("=" * 60)
    print("Example: Email Processing Workflow")
    print("=" * 60)
    
    # 1. Create a new run
    print("\n1. Creating LG Run...")
    run_record_id = create_lg_run(
        environment="Dev",
        input_summary="Process incoming email from customer",
        input_payload={
            "email_id": "msg_abc123",
            "from": "customer@example.com",
            "subject": "Question about pricing"
        }
    )
    print(f"✓ Run created: {run_record_id}")
    
    # 2. Workflow execution with snapshots
    print("\n2. Processing workflow steps...")
    
    # Step 1: Classify email
    print("  - Classifying email...")
    classification_state = {
        "email_id": "msg_abc123",
        "classification": "sales_inquiry",
        "confidence": 0.95
    }
    
    envelope = create_state_envelope(
        workflow_id="email_processing",
        actor="classify_node",
        phase="classification",
        payload=classification_state
    )
    
    snapshot_id_1 = create_state_snapshot(
        run_record_id=run_record_id,
        node="classify_email",
        snapshot_type="Checkpoint",
        state_envelope=envelope,
        snapshot_index=1,
        payload_version="1.0"
    )
    print(f"    ✓ Snapshot 1 saved: {snapshot_id_1}")
    
    # Step 2: Extract entities
    print("  - Extracting entities...")
    extraction_state = {
        **classification_state,
        "entities": [
            {"name": "Acme Corp", "type": "company"},
            {"name": "John Smith", "type": "person"}
        ]
    }
    
    envelope = create_state_envelope(
        workflow_id="email_processing",
        actor="extract_entities_node",
        phase="extraction",
        payload=extraction_state
    )
    
    snapshot_id_2 = create_state_snapshot(
        run_record_id=run_record_id,
        node="extract_entities",
        snapshot_type="Checkpoint",
        state_envelope=envelope,
        snapshot_index=2,
        payload_version="1.0"
    )
    print(f"    ✓ Snapshot 2 saved: {snapshot_id_2}")
    
    # Step 3: Generate response
    print("  - Generating response...")
    final_state = {
        **extraction_state,
        "draft_subject": "RE: Question about pricing",
        "draft_body": "Thank you for your inquiry..."
    }
    
    envelope = create_state_envelope(
        workflow_id="email_processing",
        actor="draft_response_node",
        phase="drafting",
        payload=final_state
    )
    
    snapshot_id_3 = create_state_snapshot(
        run_record_id=run_record_id,
        node="draft_response",
        snapshot_type="Final",
        state_envelope=envelope,
        snapshot_index=3,
        payload_version="1.0"
    )
    print(f"    ✓ Snapshot 3 saved: {snapshot_id_3}")
    
    # 3. Complete the run
    print("\n3. Completing run...")
    complete_lg_run(
        run_record_id,
        output_payload={
            "draft_id": "draft_xyz789",
            "status": "ready_for_approval"
        }
    )
    print("✓ Run completed successfully!")
    
    return run_record_id


def example_failed_workflow():
    """Example: Workflow that encounters an error."""
    
    print("\n" + "=" * 60)
    print("Example: Failed Workflow")
    print("=" * 60)
    
    # Create run
    print("\n1. Creating LG Run...")
    run_record_id = create_lg_run(
        environment="Dev",
        input_summary="Test error handling",
        input_payload={"test": "error_case"}
    )
    print(f"✓ Run created: {run_record_id}")
    
    # Simulate error
    print("\n2. Simulating error...")
    try:
        # Some operation that fails
        raise ValueError("API connection timeout")
    except Exception as e:
        print(f"  ✗ Error occurred: {e}")
        
        # Log the failure
        fail_lg_run(
            run_record_id,
            error_message=str(e),
            error_payload={
                "error_type": type(e).__name__,
                "step": "api_call",
                "details": "Connection to external API timed out after 30s"
            }
        )
        print("  ✓ Error logged to Airtable")
    
    return run_record_id


def example_query_runs():
    """Example: Querying runs from Airtable."""
    
    print("\n" + "=" * 60)
    print("Example: Query Recent Runs")
    print("=" * 60)
    
    # Get recent runs
    print("\n1. Fetching recent runs...")
    runs = get_recent_runs(environment="Dev", limit=5)
    
    print(f"\nFound {len(runs)} run(s):")
    for i, run in enumerate(runs, 1):
        fields = run['fields']
        run_id = fields.get('Run ID', 'N/A')
        created = fields.get('Created At', 'N/A')
        env = fields.get('Environment', 'N/A')
        summary = fields.get('Input Summary', 'N/A')
        
        print(f"\n{i}. Run ID: {run_id}")
        print(f"   Created: {created}")
        print(f"   Environment: {env}")
        print(f"   Summary: {summary}")
        
        # Get snapshots for this run
        if run['id']:
            snapshots = get_snapshots_for_run(run['id'])
            print(f"   Snapshots: {len(snapshots)}")


def example_form5500_workflow():
    """Example: Form 5500 ingestion workflow."""
    
    print("\n" + "=" * 60)
    print("Example: Form 5500 Ingestion Workflow")
    print("=" * 60)
    
    # Create run
    print("\n1. Creating LG Run...")
    run_record_id = create_lg_run(
        environment="production",
        input_summary="Form 5500 CSV ingestion - Q1 2026",
        input_payload={
            "file_path": "/data/form5500_q1_2026.csv",
            "row_count": 15000
        }
    )
    print(f"✓ Run created: {run_record_id}")
    
    # Workflow steps
    steps = ["validate", "transform", "calculate", "upsert"]
    
    print("\n2. Processing steps...")
    for i, step in enumerate(steps, 1):
        print(f"  - Step {i}: {step}...")
        
        envelope = create_state_envelope(
            workflow_id="form5500_ingestion",
            actor=f"{step}_node",
            phase=step,
            payload={
                "step": step,
                "rows_processed": i * 3750,
                "status": "completed"
            }
        )
        
        create_state_snapshot(
            run_record_id=run_record_id,
            node=step,
            snapshot_type="Checkpoint",
            state_envelope=envelope,
            snapshot_index=i
        )
        print(f"    ✓ Snapshot {i} saved")
    
    # Complete
    print("\n3. Completing run...")
    complete_lg_run(
        run_record_id,
        output_payload={
            "rows_inserted": 15000,
            "rows_updated": 0,
            "duration_seconds": 45.3
        }
    )
    print("✓ Run completed successfully!")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Airtable LangGraph Logging Examples")
    print("=" * 60)
    
    # Run examples
    try:
        # Example 1: Successful workflow
        run_id_1 = example_email_workflow()
        
        # Example 2: Failed workflow
        run_id_2 = example_failed_workflow()
        
        # Example 3: Query runs
        example_query_runs()
        
        # Example 4: Form 5500 workflow
        run_id_3 = example_form5500_workflow()
        
        print("\n" + "=" * 60)
        print("✅ All examples completed!")
        print("=" * 60)
        print("\nCheck your Airtable base to see the logged runs and snapshots:")
        print("  - LG Runs table")
        print("  - LG State Snapshots table")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
