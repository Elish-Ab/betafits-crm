from datetime import datetime
from typing import Any, Optional, TypedDict


from lib.models.database_schemas import DraftedEmail
from lib.models.io_formats import (
    ContextBundle,
    EmailDraftingScenario,
    PipelineValidationLog,
)


class PipelineState(TypedDict, total=False):
    drafting_scenario: EmailDraftingScenario

    context_bundle: ContextBundle
    """
    Output of context_retriever_node.
    Retrieved context for drafting: prior emails, KG data, opportunities, history.
    """

    context_retrieval_error: Optional[str]
    """
    If context retrieval failed, error message here.
    Pipeline continues but with reduced context.
    """

    response_draft: DraftedEmail
    """
    Output of response_drafter_node.
    LLM-generated draft email response.
    """

    draft_error: Optional[str]
    """
    If drafting failed, error message here.
    May be critical (blocks further processing).
    """

    validation_log: PipelineValidationLog
    """
    Output of workflow_validator_node (final output).
    Complete pipeline execution log and validation results.
    """

    # ========================================================================
    # Metadata & Control Flow
    # ========================================================================

    pipeline_execution_id: str
    """
    Unique ID for this execution run.
    Generated at pipeline start, used for logging/audit trail.
    """

    pipeline_start_time: datetime
    """
    When pipeline execution started.
    Used to compute total_duration_seconds.
    """

    auto_send_enabled: bool
    """
    Whether auto-send is enabled (from config).
    If false, draft is only saved, not auto-sent.
    """

    user_id: Optional[str]
    """
    Optional user ID if email was processed on behalf of a user.
    Used for audit trail and approval tracking.
    """

    # ========================================================================
    # Error Handling & Flags
    # ========================================================================

    error_message: Optional[str]
    """
    Top-level error message if pipeline failed.
    """

    pipeline_failed: bool
    """
    Whether pipeline encountered a critical failure.
    If true, validation_log.final_status will be "failure".
    """

    skip_approval: bool
    """
    If true, email should be auto-sent without approval.
    Default: false (require manual approval).
    """

    # ========================================================================
    # Additional Context
    # ========================================================================

    config_snapshot: dict[str, Any]
    """
    Snapshot of configuration used during execution.
    Useful for debugging and audit trail.
    """

    model_responses: dict[str, Any]
    """
    Detailed responses from LLM calls (for debugging/audit).
    Keys: classifier, entity_extractor, relation_extractor, response_drafter
    """
