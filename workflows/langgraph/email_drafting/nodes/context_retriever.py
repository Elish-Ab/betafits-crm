import logging
import time

from lib.integrations.supabase.emails_client import EmailDBClient
from lib.integrations.supabase.opportunites_client import OpportuityDBClient
from lib.integrations.supabase.supabase_client import get_supabase_client
from lib.models.io_formats import ContextBundle, EmailCategory
from workflows.langgraph.email_drafting.state import PipelineState

logger = logging.getLogger(__name__)


async def context_retriever_node(state: PipelineState) -> PipelineState:
    """Retrieve context from RAG, KG, and prior emails.

    Inputs from state:
        - drafting_scenraio: EmailDraftingScenario with email and opportunity info

    Outputs to state:
        - context_bundle: ContextBundle with retrieved context

    Args:
        state: PipelineState with parsed_email and kg_rag_update fields.

    Returns:
        Updated PipelineState with context_bundle field.

    Raises:
        ValueError: If retrieval fails critically.
    """
    start_time = time.time()

    context_bundle_opt = state.get("context_bundle")

    if context_bundle_opt is not None:
        logger.info(
            "[Email Drafting Pipeline] context_retriever_node skipping; "
            "context_bundle already exists in state."
        )
        return state

    drafting_scenario_opt = state.get("drafting_scenario")

    if drafting_scenario_opt is None:
        raise ValueError(
            "[Email Drafting Pipeline] drafting_scenario missing in state; cannot retrieve context."
        )

    drafting_scenario = drafting_scenario_opt

    logger.info(
        f"[Email Drafting Pipeline] context_retriever_node starting for opportunity ID "
        f"{drafting_scenario.opportunity_id}"
        f"drafting scenario: {drafting_scenario.drafting_scenario[:50]}..."
    )

    try:
        email_client = await EmailDBClient.create()
        opportunity_client = await OpportuityDBClient.create()

        opportunities = await opportunity_client.fetch_opportunities_by_ids(
            ids=[drafting_scenario.opportunity_id]
        )

        if opportunities:
            opportunity = opportunities[0]
        else:
            logger.warning(
                f"[Email Drafting Pipeline] Opportunity ID "
                f"{drafting_scenario.opportunity_id} not found."
            )
            raise ValueError(
                f"[Email Drafting Pipeline] Opportunity ID "
                f"{drafting_scenario.opportunity_id} not found."
            )

        in_reply_to_email = None
        if drafting_scenario.in_reply_to:
            in_reply_to_email = await email_client.get_received_email(
                drafting_scenario.in_reply_to
            )

            if in_reply_to_email is None:
                in_reply_to_email = await email_client.get_sent_email(
                    drafting_scenario.in_reply_to
                )

                if in_reply_to_email is None:
                    raise ValueError(
                        f"[Email Drafting Pipeline] In-reply-to email ID "
                        f"{drafting_scenario.in_reply_to} not found in DB."
                    )

        # =====================================================================
        # Fetch few-shot examples from Supabase for response style
        # =====================================================================

        email_category = None

        if drafting_scenario.category == EmailCategory.CRM:
            email_category = "Sales / CRM"
        elif drafting_scenario.category == EmailCategory.CUSTOMER_SUCCESS:
            email_category = "Customer Success"

        tone_guide_emails = []
        try:
            tone_guide_emails = await email_client.match_tone_guide_emails(
                query_text=drafting_scenario.drafting_scenario,
                category_filter=email_category,
                match_count=2,
            )

        except Exception as few_shot_error:
            logger.warning(
                f"[Email Drafting Pipeline] Few-shot examples fetch failed (non-critical): {few_shot_error}"
            )

        # =====================================================================
        # Create ContextBundle
        # =====================================================================

        context_bundle = ContextBundle(
            in_reply_to_email=in_reply_to_email,
            opportunity=opportunity,
            response_tone_guide_emails=tone_guide_emails,
        )

        state["context_bundle"] = context_bundle

        return state

    except ValueError as ve:
        logger.error(f"[Email Drafting Pipeline] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Email Drafting Pipeline] Failed to retrieve context: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to retrieve context: {error}") from error
