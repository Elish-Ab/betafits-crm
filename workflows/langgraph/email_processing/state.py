"""
LangGraph state definition for the email processing pipeline.

This TypedDict represents the central state machine state that flows through
all 8 nodes. Each node reads from and writes to specific fields.

Note: entity_extractor_node and relation_extractor_node have been removed.
Graphiti's add_episode() handles entity and relation extraction internally.
"""

from datetime import datetime
from typing import Any, List, Optional, TypedDict, Union


from lib.models.database_schemas import Contact, EmailProcessingMode, ReceivedEmail, SentEmail
from lib.models.io_formats import (
    LabeledEmail,
    OpportunitySelectionResult,
    PipelineValidationLog,
)


class PipelineState(TypedDict, total=False):
    """Central state for the 8-node email processing pipeline.

    Fields marked with `total=False` are optional. Each node reads specific
    input fields and writes to specific output fields. Nodes should not modify
    input fields after reading them.

    Node responsibilities (updated):
    1. email_router_node: Raw → EmailParsed (parse, normalize, dedupe)
    2. email_classifier_node: EmailParsed → LabeledEmail (classify)
    3. kg_rag_updater_node: LabeledEmail → KGAndRAGUpdate (Graphiti extracts entities/relations)
    4. context_retriever_node: KGAndRAGUpdate → ContextBundle (retrieve context)
    5. response_drafter_node: ContextBundle → ResponseDraft (draft response)
    6. json_formatter_node: ResponseDraft → ResponseEmailDraft (validate/format)
    7. email_sender_node: ResponseEmailDraft → EmailSent (send or queue)
    8. workflow_validator_node: EmailSent → ResponseValidationLog (validate/log)

    Removed nodes (Graphiti handles internally):
    - entity_extractor_node (deprecated)
    - relation_extractor_node (deprecated)
    """

    # ========================================================================
    # Node 1: email_router_node
    # ========================================================================

    processing_mode: EmailProcessingMode

    are_email_and_contacts_stored: bool

    email: Union[ReceivedEmail, SentEmail]
    """
    Output of email_router_node.
    Parsed, normalized, deduplicated email.
    """

    is_duplicate: bool
    """
    Whether this email was a duplicate of a previously processed email.
    If true, pipeline may short-circuit to validation stage.
    """

    related_contacts: List[Contact]

    are_related_contacts_linked_to_opportunity: bool
    """
    Whether the email sender is linked to the matched opportunity.
    """

    # ========================================================================
    # Node 2: email_classifier_node
    # ========================================================================

    labeled_email: LabeledEmail
    """
    Output of email_classifier_node.
    Email classified as: crm, customer_success, spam
    """

    should_skip: bool
    """
    If true (spam label), skip to workflow_validator_node (short-circuit).
    """

    selected_opportunity_id: str

    matched_opportunity: OpportunitySelectionResult
    """
    Output of opportunity_matcher_node.
    Matched sales opportunity for the email.
    """

    # ========================================================================
    # Node 4: kg_rag_updater_node
    # ========================================================================

    is_kg_updated: bool
    """
    Whether the email episode was added and knowledge graph was successfully updated.
    """

    are_communities_built: bool
    """
    Whether knowledge graph communities were successfully built.
    """

    is_opportunity_index_rag_updated: bool
    """
    Whether the opportunity index in the RAG vector store was successfully updated.
    """

    # ========================================================================
    # Node 8 (formerly Node 10): workflow_validator_node
    # ========================================================================

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
