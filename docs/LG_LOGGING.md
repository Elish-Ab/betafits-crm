# Universal LangGraph Logging System

This document explains how to use the universal `lg_runs` and `state_snapshots` tables for tracking all LangGraph workflow executions.

## Overview

The universal logging system provides:
- **Run Tracking**: Every workflow execution is tracked with a unique Run ID
- **State Management**: State snapshots captured at each workflow phase
- **Universal Schema**: Consistent logging across all workflows (email, Form 5500, etc.)
- **Observability**: Query runs by workflow, status, environment, trigger source

## Tables

### `lg_runs` - Workflow Execution Tracking

Tracks every workflow run with metadata and step logs.

**Key Fields:**
- `run_id`: Unique identifier (format: `LG-RUN-YYYY-MM-DD-XXXX`)
- `workflow`: Workflow name (e.g., "Email Processing", "Form 5500 Ingestion")
- `triggered_by`: What started the run (e.g., "Gmail API", "CLI", "API Request")
- `environment`: Deployment context ("production", "development", "staging")
- `status`: Current status (started, running, completed, failed, cancelled)
- `step_log`: JSONB array of step executions with timestamps
- `error_details`: JSONB with error information if failed
- `started_at`, `completed_at`, `duration_seconds`: Timing information

### `state_snapshots` - State Management

Captures workflow state at each phase using a universal envelope format.

**Key Fields:**
- `snapshot_id`: Unique snapshot identifier
- `run_id`: Links to `lg_runs.run_id`
- `state_envelope`: Universal JSONB envelope with:
  - `schema_version`: Envelope format version
  - `workflow_id`: Workflow type identifier
  - `run_id`: Run identifier
  - `actor`: Who/what created this snapshot
  - `phase`: Current workflow phase
  - `timestamp`: When snapshot was created
  - `payload`: Actual state data (workflow-specific)
- `phase`: Current workflow phase
- `checkpoint_id`: Optional LangGraph checkpoint ID

## Setup

### 1. Create Tables

Run the database initialization script:

```bash
python -m scripts.init_database
```

This creates both tables with proper indexes and foreign keys.

### 2. Import Utilities

```python
from lib.utils.lg_logging import (
    create_lg_run,
    update_lg_run_status,
    log_lg_step,
    create_state_envelope,
    save_state_snapshot,
    get_lg_run,
    get_state_snapshots,
)
```

## Usage Pattern

### Basic Workflow Integration

```python
# 1. Create run at workflow start
run_id = create_lg_run(
    workflow="Email Processing",
    triggered_by="Gmail API",
    environment="production",
    metadata={"email_id": email_id}
)

# 2. Update status as workflow progresses
update_lg_run_status(run_id, "running")

# 3. Log each step
log_lg_step(run_id, "classify_email", "started")
# ... do work ...
log_lg_step(run_id, "classify_email", "completed", metadata={"result": "sales"})

# 4. Save state snapshots
envelope = create_state_envelope(
    workflow_id="email_processing",
    run_id=run_id,
    actor="classify_node",
    phase="classification",
    payload=current_state
)
save_state_snapshot(run_id, envelope, "classification")

# 5. Mark run complete
update_lg_run_status(run_id, "completed")
```

### Handling Failures

```python
try:
    # Your workflow logic
    pass
except Exception as e:
    update_lg_run_status(
        run_id,
        "failed",
        error_details={
            "error": str(e),
            "error_type": type(e).__name__,
            "step": current_step
        }
    )
    raise
```

### Querying Runs

```python
from lib.utils.lg_logging import get_recent_runs, get_lg_run, get_state_snapshots

# Get recent runs for a workflow
runs = get_recent_runs(workflow="Email Processing", limit=10)

# Get specific run details
run = get_lg_run("LG-RUN-2025-12-31-A1B2C3D4")

# Get all snapshots for a run
snapshots = get_state_snapshots("LG-RUN-2025-12-31-A1B2C3D4")

# Get snapshots for specific phase
extraction_snapshots = get_state_snapshots(
    "LG-RUN-2025-12-31-A1B2C3D4",
    phase="extraction"
)
```

## Integration Examples

See [`examples/lg_logging_integration.py`](../examples/lg_logging_integration.py) for complete examples:
- Email workflow with logging
- Form 5500 workflow with logging
- Error handling patterns

## State Envelope Format

The universal state envelope wraps workflow-specific state:

```json
{
  "schema_version": "1.0",
  "workflow_id": "email_processing",
  "run_id": "LG-RUN-2025-12-31-A1B2C3D4",
  "actor": "classify_email_node",
  "phase": "classification",
  "timestamp": "2025-12-31T10:30:45.123Z",
  "payload": {
    "email_id": "msg_12345",
    "classification": "sales_inquiry",
    "confidence": 0.95
  }
}
```

## Migration from Email-Specific Logging

If you're currently using `crm.validation_logs` (email-specific):

1. **Keep both**: `validation_logs` can coexist with universal tables
2. **Migrate workflows**: Update workflows to use `lg_runs` + `state_snapshots`
3. **Deprecate**: Eventually remove `validation_logs` once migration is complete

Example migration:

```python
# OLD (email-specific)
# Write to crm.validation_logs with email_id FK

# NEW (universal)
run_id = create_lg_run("Email Processing", "Gmail API")
# ... workflow execution ...
save_state_snapshot(run_id, envelope, phase)
```

## Best Practices

1. **Create run at start**: Always create the run record before workflow execution
2. **Log all steps**: Track every significant node execution
3. **Save snapshots**: Capture state at phase boundaries for debugging
4. **Handle errors**: Always update status to 'failed' with error details
5. **Use metadata**: Add context to runs and steps for easier debugging
6. **Phase naming**: Use consistent phase names across workflows
7. **Payload structure**: Keep payloads serializable (no functions, lambdas)

## Workflow Names

Standardize workflow names for consistency:

- "Email Processing"
- "Email Drafting"
- "Form 5500 Ingestion"
- "CRM Sync"
- "Prospect Enrichment"

## Environment Values

- `production`: Live production workflows
- `staging`: Staging environment testing
- `development`: Local development
- `test`: Automated testing

## Querying Examples

```sql
-- Get failed runs from last 24 hours
SELECT * FROM crm.lg_runs
WHERE status = 'failed'
  AND started_at > NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;

-- Get average duration by workflow
SELECT 
  workflow,
  AVG(duration_seconds) as avg_duration,
  COUNT(*) as run_count
FROM crm.lg_runs
WHERE status = 'completed'
GROUP BY workflow;

-- Get state evolution for a run
SELECT 
  phase,
  state_envelope->>'timestamp' as timestamp,
  state_envelope->'payload' as state
FROM crm.state_snapshots
WHERE run_id = 'LG-RUN-2025-12-31-A1B2C3D4'
ORDER BY created_at;

-- Find runs with specific step failures
SELECT * FROM crm.lg_runs
WHERE step_log @> '[{"status": "failed"}]'::jsonb;
```

## Monitoring Dashboard

You can build monitoring dashboards using these tables:

- **Run Success Rate**: `SELECT status, COUNT(*) FROM lg_runs GROUP BY status`
- **Workflow Performance**: Average duration by workflow
- **Error Analysis**: Common failure patterns in step_log
- **Environment Health**: Compare production vs staging metrics

## Testing

Test utilities are provided in `tests/integration/test_lg_logging.py`:

```bash
pytest tests/integration/test_lg_logging.py -v
```

## Support

For questions or issues with the logging system:
1. Check examples in `examples/lg_logging_integration.py`
2. Review this documentation
3. Ask in team chat or open an issue
