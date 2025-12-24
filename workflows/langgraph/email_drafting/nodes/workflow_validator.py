"""Workflow validator node for final validation and logging.

This is the final node of the email drafting pipeline. Responsibilities:
- Validate all pipeline outputs
- Generate validation log entries
- Log warnings and errors
- Return PipelineValidationLog
"""

import logging
import time

from lib.models.io_formats import PipelineValidationLog, ValidationLogEntry
from workflows.langgraph.email_drafting.state import PipelineState

logger = logging.getLogger(__name__)


async def workflow_validator_node(state: PipelineState) -> PipelineState:
    """Validate email drafting pipeline execution and generate audit log.

    Inputs from state:
        - response_draft: DraftedEmail (generated draft)
        - context_retrieval_error: Optional error from context retrieval
        - draft_error: Optional error from drafting
        - drafting_scenario: Input scenario

    Outputs to state:
        - validation_log: PipelineValidationLog (final audit trail)

    Args:
        state: PipelineState with pipeline execution results.

    Returns:
        Updated PipelineState with validation_log field.

    Raises:
        ValueError: If validation fails.
    """
    start_time = time.time()

    # Get inputs
    response_draft_opt = state.get("response_draft")
    drafting_scenario = state.get("drafting_scenario")

    if drafting_scenario is None:
        raise ValueError("drafting_scenario is required in state")

    pipeline_execution_id = state.get(
        "pipeline_execution_id", f"exec_{int(time.time() * 1000)}"
    )

    logger.info(
        f"[Email Drafting Pipeline] workflow_validator_node starting for "
        f"opportunity ID {drafting_scenario.opportunity_id if drafting_scenario else 'N/A'}"
    )

    try:
        # Collect validation entries from pipeline
        validation_entries: list[ValidationLogEntry] = []

        # Check if draft was created successfully
        if response_draft_opt:
            validation_entries.append(
                ValidationLogEntry(
                    stage="email_drafter",
                    status="success",
                    message=f"Email draft created successfully with confidence {response_draft_opt.confidence:.2f}",
                )
            )
        else:
            validation_entries.append(
                ValidationLogEntry(
                    stage="email_drafter",
                    status="error",
                    message="No draft was created",
                )
            )

        # Check for errors in previous stages
        if state.get("context_retrieval_error"):
            validation_entries.append(
                ValidationLogEntry(
                    stage="context_retriever",
                    status="warning",
                    message=f"Context retrieval error: {state.get('context_retrieval_error')}",
                )
            )

        if state.get("draft_error"):
            validation_entries.append(
                ValidationLogEntry(
                    stage="email_drafter",
                    status="error",
                    message=f"Draft error: {state.get('draft_error')}",
                )
            )

        # Check pipeline failure flag
        if state.get("pipeline_failed"):
            validation_entries.append(
                ValidationLogEntry(
                    stage="workflow",
                    status="error",
                    message=f"Pipeline failed: {state.get('error_message', 'Unknown error')}",
                )
            )

        # Add success entry if no errors
        if not any(e.status == "error" for e in validation_entries):
            validation_entries.append(
                ValidationLogEntry(
                    stage="workflow",
                    status="success",
                    message="Email drafting workflow completed successfully",
                )
            )

        # Collect errors and warnings
        errors: list[str] = []
        warnings: list[str] = []

        for entry in validation_entries:
            if entry.status == "error":
                errors.append(f"[{entry.stage}] {entry.message}")
            elif entry.status == "warning":
                warnings.append(f"[{entry.stage}] {entry.message}")

        # Determine final status
        if errors:
            final_status = "failure"
        elif warnings:
            final_status = "partial_success"
        else:
            final_status = "success"

        # Generate summary
        if response_draft_opt:
            summary = (
                f"Email draft created successfully for opportunity {drafting_scenario.opportunity_id}. "
                f"Subject: '{response_draft_opt.subject}'. "
                f"Confidence: {response_draft_opt.confidence:.2f}. "
                f"Status: {response_draft_opt.approval_status}"
            )
        elif state.get("draft_error"):
            summary = f"Failed to create email draft: {state.get('draft_error')}"
        else:
            summary = "Email draft pipeline completed but no draft was generated"

        # Create PipelineValidationLog
        validation_log = PipelineValidationLog(
            email_id=drafting_scenario.opportunity_id if drafting_scenario else "N/A",
            pipeline_execution_id=pipeline_execution_id,
            total_duration_seconds=time.time() - start_time,
            log_entries=validation_entries,
            final_status=final_status,
            errors=errors,
            warnings=warnings,
            summary=summary,
        )

        logger.info(
            f"[Email Drafting Pipeline] Validation complete for "
            f"opportunity {drafting_scenario.opportunity_id if drafting_scenario else 'N/A'} "
            f"in {time.time() - start_time:.2f}s. Status: {final_status}"
        )

        state["validation_log"] = validation_log

        return state

    except ValueError as ve:
        logger.error(f"[Email Drafting Pipeline] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Email Drafting Pipeline] Failed to validate workflow: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to validate workflow: {error}") from error
