"""Workflow validator node for final validation and logging.

This is Node 10 of the 10-node pipeline. Responsibilities:
- Validate all pipeline outputs
- Generate validation log entries
- Log warnings and errors
- Return ResponseValidationLog
"""

import logging
import time

from lib.models.io_formats import EmailSent, PipelineValidationLog, ValidationLogEntry
from workflows.langgraph.email_processing.state import PipelineState

logger = logging.getLogger(__name__)


async def workflow_validator_node(state: PipelineState) -> PipelineState:
    """Validate pipeline outputs and generate validation log.

    Inputs from state:
        - email_sent: EmailSent (email sending confirmation)
        - (all previous node outputs for validation)

    Outputs to state:
        - validation_log: ResponseValidationLog (final validation result)

    Args:
        state: PipelineState with email_sent and previous node outputs.

    Returns:
        Updated PipelineState with validation_log field.

    Raises:
        ValueError: If validation fails (critical).
    """
    start_time = time.time()

    if state.get("should_skip"):
        logger.info(
            "[Node 4] workflow_validator_node skipping validation "
            "as email was marked to skip"
        )
        state["validation_log"] = PipelineValidationLog(
            email_id="N/A",
            pipeline_execution_id=f"exec_{int(time.time() * 1000)}",
            total_duration_seconds=time.time() - start_time,
            log_entries=[
                ValidationLogEntry(
                    stage="workflow",
                    status="info",
                    message="Workflow skipped as email was marked to skip",
                )
            ],
            final_status="skipped",
            errors=[],
            warnings=[],
            summary="Pipeline execution was skipped.",
        )
        return state

    if state.get("is_duplicate"):
        logger.info(
            "[Node 4] workflow_validator_node skipping validation "
            "as email was detected as duplicate"
        )
        state["validation_log"] = PipelineValidationLog(
            email_id="N/A",
            pipeline_execution_id=f"exec_{int(time.time() * 1000)}",
            total_duration_seconds=time.time() - start_time,
            log_entries=[
                ValidationLogEntry(
                    stage="workflow",
                    status="info",
                    message="Workflow skipped as email was detected as duplicate",
                )
            ],
            final_status="skipped",
            errors=[],
            warnings=[],
            summary="Pipeline execution was skipped due to duplicate email.",
        )
        return state

    email_opt = state.get("email")

    if not email_opt:
        raise ValueError("email is required in state")

    email = email_opt

    email_id = email.id

    logger.info(f"[Node 4] workflow_validator_node starting for {email_id}")

    try:
        # Collect validation entries from pipeline
        validation_entries: list[ValidationLogEntry] = []

        # Check for errors in previous stages
        if state.get("kg_update_error"):
            validation_entries.append(
                ValidationLogEntry(
                    stage="kg_rag_updater",
                    status="warning",
                    message=f"KG/RAG update error: {state.get('kg_update_error')}",
                )
            )

        if state.get("context_retrieval_error"):
            validation_entries.append(
                ValidationLogEntry(
                    stage="context_retriever",
                    status="warning",
                    message=f"Context retrieval error: {state.get('context_retrieval_error')}",
                )
            )

        if state.get("should_skip"):
            validation_entries.append(
                ValidationLogEntry(
                    stage="email_classifier",
                    status="info",
                    message="Email was marked as spam and skipped",
                )
            )

        if state.get("is_duplicate"):
            validation_entries.append(
                ValidationLogEntry(
                    stage="email_router",
                    status="info",
                    message="Email was detected as duplicate",
                )
            )

        # Add success entry if no errors
        if not any(e.status == "error" for e in validation_entries):
            validation_entries.append(
                ValidationLogEntry(
                    stage="workflow",
                    status="success",
                    message="Workflow completed successfully",
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

        summary = (
            f"Pipeline completed with status: {final_status}. "
            f"Errors: {len(errors)}, Warnings: {len(warnings)}."
        )

        # Create ResponseValidationLog
        validation_log = PipelineValidationLog(
            email_id=email_id or "",
            pipeline_execution_id=f"exec_{int(time.time() * 1000)}",
            total_duration_seconds=time.time() - start_time,
            log_entries=validation_entries,
            final_status=final_status,
            errors=errors,
            warnings=warnings,
            summary=summary,
        )

        logger.info(
            f"[Node 4] Validation complete for {email_id} "
            f"in {time.time() - start_time:.2f}s"
        )

        state["validation_log"] = validation_log

        return state

    except ValueError as ve:
        logger.error(f"[Node 4] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Node 4] Failed to validate workflow: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to validate workflow: {error}") from error
