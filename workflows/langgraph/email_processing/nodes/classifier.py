"""Email classifier node for categorizing emails.

This is Node 2 of the 10-node pipeline. Responsibilities:
- Classify email as CRM, Customer Success, or Spam
- Return classification with confidence score
- Set should_skip flag for spam emails
"""

import json
import logging
import time

from lib.config.settings import get_settings
from lib.integrations.openrouter_client import get_openrouter_client
from lib.models.io_formats import ClassificationResult, EmailCategory, LabeledEmail
from lib.prompts.email_chains import (
    CLASSIFIER_EXAMPLES,
    CLASSIFIER_SYSTEM_PROMPT,
)
from workflows.langgraph.email_processing.state import PipelineState

logger = logging.getLogger(__name__)


async def email_classifier_node(state: PipelineState) -> PipelineState:
    """Classify email into CRM, Customer Success, or Spam.

    Inputs from state:
        - parsed_email: EmailParsed from previous node

    Outputs to state:
        - labeled_email: LabeledEmail with classification and confidence
        - should_skip: bool (True if spam, to short-circuit pipeline)

    Args:
        state: PipelineState with parsed_email field.

    Returns:
        Updated PipelineState with labeled_email and should_skip fields.

    Raises:
        ValueError: If classification fails.
    """
    start_time = time.time()

    labeled_email_opt = state.get("labeled_email")
    if labeled_email_opt is not None:
        logger.info(
            f"[Node 2] email_classifier_node skipping for {labeled_email_opt.email_id} "
            f"(already classified as {labeled_email_opt.label.value})"
        )
        return state

    # Get parsed email
    email_opt = state.get("email")
    if email_opt is None:
        raise ValueError("email is required in state")

    email = email_opt

    logger.info(f"[Node 2] email_classifier_node starting for {email.id}")

    try:
        openrouter_client = get_openrouter_client()

        # Build few-shot prompt
        few_shot_examples = "\n".join(
            [
                f"Email: {ex['email']}\nResponse: {ex['response']}"
                for ex in CLASSIFIER_EXAMPLES
            ]
        )

        user_prompt = f"""
Here are some examples of email classifications:
{few_shot_examples}

Now classify this email:

Subject: {email.subject}
Body: {email.body[:1000]}
"""

        # Call LLM with structured completion
        result: ClassificationResult = await openrouter_client.structured_completion(
            messages=[
                {"role": "system", "content": CLASSIFIER_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            response_model=ClassificationResult,
            model=get_settings().openrouter_default_model,
            temperature=0.3,
            max_tokens=200,
        )

        # Extract classification from structured response
        label = result.classification
        confidence = result.confidence

        # Create labeled email
        labeled_email = LabeledEmail(
            email_id=email.id or "",
            label=label,
            confidence=confidence,
            label_metadata={
                "classifier_model": "openrouter",
                "reasoning": result.reasoning,
            },
        )

        # Set skip flag for spam
        should_skip = label == EmailCategory.SPAM

        logger.info(
            f"[Node 2] Classified email {email.id} as {label.value} "
            f"(confidence={confidence:.2f}, skip={should_skip}) "
            f"in {time.time() - start_time:.2f}s"
        )

        state["labeled_email"] = labeled_email
        state["should_skip"] = should_skip

        return state

    except ValueError as ve:
        logger.error(f"[Node 2] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Node 2] Failed to classify email: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to classify email: {error}") from error
