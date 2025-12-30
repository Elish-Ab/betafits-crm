"""
IOF (Input/Output Format) models for the Betafits email ingestor pipeline.

These Pydantic v2 models define all data structures flowing through the 10-node
LangGraph state machine. All models use strict validation with Field descriptions.
"""

from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from lib.models.database_schemas import BaseEmail, Opportunity


class EmailCategory(str, Enum):
    """Email classification labels."""

    CRM = "crm"
    CUSTOMER_SUCCESS = "customer_success"
    SPAM = "spam"


class ClassificationResult(BaseModel):
    """Structured LLM response model for email classification."""

    model_config = ConfigDict(str_strip_whitespace=True)

    classification: EmailCategory = Field(
        ..., description="Email classification: crm, customer_success, or spam"
    )
    confidence: float = Field(
        ...,
        description="Classification confidence score between 0 and 1",
        ge=0.0,
        le=1.0,
    )
    reasoning: str = Field(
        ..., description="Brief explanation for the classification decision"
    )


class LabeledEmail(BaseModel):
    """IOF-EmailLabel: Classified email with label and confidence.

    Output of email_classifier_node.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    email_id: str = Field(..., description="Email ID from previous stage")
    label: EmailCategory = Field(
        ..., description="Classification: crm, customer_success, spam"
    )
    confidence: float = Field(
        ..., description="Classification confidence score", ge=0.0, le=1.0
    )
    label_metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional classification details"
    )


class OpportunitySelectionResult(BaseModel):
    """LLM response model for opportunity selection."""

    selected_opportunity: Opportunity = Field(
        ..., description="The selected opportunity or None if no match"
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence score (0.0 to 1.0) for the selection",
    )
    reasoning: str = Field(
        ...,
        description="Brief reasoning for the selection or why no match was found",
    )


class EmailDraftingScenario(BaseModel):
    """IOF-EmailDraftingScenario: Scenario details for email drafting.

    Input to response_drafter_node.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    in_reply_to: Optional[str] = Field(
        default=None, description="Message ID of the email to respond to"
    )
    category: Optional[EmailCategory] = Field(
        default=None, description="Category of the email being responded to"
    )
    from_email: str = Field(..., description="Sender email address")
    to_emails: List[str] = Field(..., description="List of recipient email addresses")
    cc_emails: List[str] = Field(
        default_factory=list, description="List of CC recipient email addresses"
    )
    bcc_emails: List[str] = Field(
        default_factory=list, description="List of BCC recipient email addresses"
    )
    opportunity_id: str = Field(..., description="Matched sales opportunity")
    drafting_scenario: str = Field(
        ..., description="Scenario context for drafting the email response"
    )
    drafting_instructions: Optional[str] = Field(
        default=None, description="Special instructions for drafting the response"
    )


class ContextBundle(BaseModel):
    """IOF-ContextBundle: Retrieved context for email response drafting.

    Output of context_retriever_node. Contains RAG-retrieved emails, KG data,
    opportunity history, and contact information.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    in_reply_to_email: Optional[BaseEmail] = Field(
        default=None,
        description="the email in response to which the draft is being created",
    )
    opportunity: Opportunity = Field(..., description="The matched sales opportunity")
    response_tone_guide_emails: List[BaseEmail] = Field(
        default_factory=list,
        description="Example emails illustrating desired response tone",
    )

    def get_prompt_repr(self) -> str:
        """Generate a text representation for prompt inclusion.

        Supports both simple email drafts and response email drafts.
        Conditionally includes the in_reply_to email only if present.
        """
        tone_guide_summaries = []
        for email in self.response_tone_guide_emails:
            tone_guide_summaries.append(email.summary())
        tone_guide_text = (
            "**Response Tone Guide Email**\n".join(tone_guide_summaries)
            if tone_guide_summaries
            else "None"
        )

        # Build the prompt based on whether this is a response or simple email
        prompt_parts = [
            "These are example emails that illustrate the desired tone for your email. "
            "These have nothing to do with the actual email being drafted. "
            "Just use them to understand the tone of how it's optimal to write an email according to Betafits.\n",
            f"## Response Tone Guide Emails:\n{tone_guide_text}\n",
            f"## Opportunity Context:\n{self.opportunity.summary or 'N/A'}\n\n",
        ]

        # Only include in_reply_to email if it exists (for response emails)
        if self.in_reply_to_email:
            prompt_parts.append(
                f"## Email Being Responded To:\n{self.in_reply_to_email.summary()}\n\n"
            )

        return "".join(prompt_parts)


class ResponseDraftStructured(BaseModel):
    """Structured LLM response model for draft email generation.

    Used with OpenRouter structured_completion to ensure deterministic
    JSON output when drafting response emails.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    subject: str = Field(
        ...,
        description="Response email subject line, starting with 'Re:' if appropriate",
    )
    body: str = Field(
        ...,
        description="Response email body with professional tone matching Betafits style",
    )
    to_emails: list[str] = Field(
        ...,
        description="List of recipient email addresses (usually sender of original email)",
    )
    cc_emails: list[str] = Field(
        default_factory=list, description="Optional CC recipients"
    )
    tone: str = Field(
        default="professional",
        description="Tone of email: 'professional', 'friendly', or 'formal'",
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence score (0.0 to 1.0) in draft quality",
    )


class ResponseDraft(BaseModel):
    """IOF-ResponseDraft: LLM-generated draft email response.

    Output of response_drafter_node.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    email_id: str = Field(..., description="ID of email being responded to")
    draft_subject: str = Field(..., description="Proposed subject line")
    draft_body: str = Field(..., description="Proposed email body")
    to_emails: list[str] = Field(..., description="Proposed recipients")
    cc_emails: list[str] = Field(default_factory=list, description="Proposed CC")
    draft_metadata: dict[str, Any] = Field(
        default_factory=dict, description="Metadata about draft"
    )
    model_used: Optional[str] = Field(
        None, description="LLM model used (e.g., gpt-4o-mini)"
    )
    tokens_used: Optional[int] = Field(
        default=None, description="Tokens consumed by LLM"
    )


class ResponseEmailDraft(BaseModel):
    """IOF-ResponseEmailDraft: Formatted and validated email response draft.

    Output of json_formatter_node. Final check before sending.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    email_id: str = Field(..., description="Original email ID")
    subject: str = Field(..., description="Email subject (validated)")
    body: str = Field(..., description="Email body (validated)")
    to_emails: list[str] = Field(...)
    cc_emails: list[str] = Field(default_factory=list)
    bcc_emails: list[str] = Field(default_factory=list)
    should_send: bool = Field(
        default=False,
        description="Whether email should be auto-sent (false = manual review)",
    )
    confidence: float = Field(
        default=1.0,
        description="Confidence in draft quality",
        ge=0.0,
        le=1.0,
    )
    validation_errors: list[str] = Field(default_factory=list)


class EmailSent(BaseModel):
    """IOF-EmailSent: Confirmation that email was sent or queued.

    Output of email_sender_node.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    email_id: str = Field(..., description="Original email ID")
    sent_at: datetime = Field(..., description="When email was sent/queued")
    sent_status: str = Field(
        ...,
        description="Status: sent, queued_for_approval, error, draft_saved",
    )
    sent_message_id: Optional[str] = Field(
        None, description="Gmail message ID of sent email"
    )
    error_message: Optional[str] = Field(
        default=None, description="Error if send failed"
    )


class ValidationLogEntry(BaseModel):
    """Single validation log entry."""

    model_config = ConfigDict(str_strip_whitespace=True)

    stage: str = Field(..., description="Pipeline stage name")
    status: str = Field(..., description="Stage status: success, warning, error")
    message: str = Field(..., description="Log message")
    details: dict[str, Any] = Field(default_factory=dict)


class PipelineValidationLog(BaseModel):
    """IOF-ResponseValidationLog: Complete pipeline validation log.

    Output of workflow_validator_node. Final output in pipeline state.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    email_id: str = Field(..., description="Email ID")
    pipeline_execution_id: str = Field(
        ..., description="Unique execution ID for this email"
    )
    total_duration_seconds: float = Field(
        ..., description="Total time to process email", ge=0.0
    )
    log_entries: list[ValidationLogEntry] = Field(
        default_factory=list, description="Log entries from each stage"
    )
    final_status: str = Field(
        ..., description="Final status: success, partial_success, failure"
    )
    errors: list[str] = Field(default_factory=list, description="Critical errors")
    warnings: list[str] = Field(
        default_factory=list, description="Non-critical warnings"
    )
    summary: str = Field(..., description="Human-readable summary")
