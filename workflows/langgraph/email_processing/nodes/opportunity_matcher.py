import logging
import time
from typing import Optional
import uuid
from pydantic import BaseModel, Field


from lib.config.settings import get_settings
from lib.integrations.openrouter_client import get_openrouter_client
from lib.integrations.supabase.contacts_client import ContactDBClient
from lib.integrations.supabase.opportunites_client import OpportuityDBClient
from lib.integrations.vector_db_client import rrf
from lib.models.database_schemas import Opportunity, ReceivedEmail
from lib.models.io_formats import OpportunitySelectionResult
from workflows.langgraph.email_processing.state import PipelineState

logger = logging.getLogger(__name__)


def _build_opportunity_selection_prompt(
    email_summary: str,
    opportunities: list[Opportunity],
) -> str:
    """Build a prompt for the LLM to select the best matching opportunity."""
    opp_descriptions = []
    for opp in opportunities:
        opp_descriptions.append(
            f"- ID: {opp.id}\n  Title: {opp.title}\n  Summary: {opp.summary or 'N/A'}"
        )

    opportunities_text = "\n".join(opp_descriptions) if opp_descriptions else "None"

    return f"""You are an expert sales assistant. Given an email and a list of sales opportunities,
determine which opportunity (if any) this email most likely belongs to.

## Email Summary:
{email_summary}

## Available Opportunities:
{opportunities_text}

## Instructions:
1. Analyze the email content and compare it with each opportunity's title and summary.
2. Select the opportunity that best matches the email's context, intent, or subject matter.
3. If no opportunity is a reasonable match, set opportunity_matched = false, and opportunity_selection result to null.
4. Provide a confidence score (0.0-1.0) reflecting how certain you are about the match.
5. Explain your reasoning briefly.

Respond with your selection."""


class OpportunitySelectionReflectionResult(BaseModel):
    opportunity_matched: bool = Field(
        ..., description="Whether an opportunity was matched"
    )
    opportunity_selection_result: Optional[OpportunitySelectionResult] = Field(
        default=None, description="Details of the selected opportunity if matched"
    )


async def opportunity_matcher_node(state: PipelineState) -> PipelineState:
    """Match parsed and labeled emails to sales opportunities."""
    start_time = time.time()

    matched_opportunity_opt = state.get("matched_opportunity")
    if matched_opportunity_opt is not None:
        logger.info(
            f"[Node 3] opportunity_matcher skipping for {matched_opportunity_opt.selected_opportunity.id} "
            f"(already matched)"
        )
        return state

    # Get required inputs

    email_opt = state.get("email")

    if not email_opt:
        raise ValueError("email is required in state")

    email = email_opt

    email_id = email.id

    logger.info(f"[Node 3] opportunity_matcher starting for {email_id} ")

    try:
        opportunity_client = await OpportuityDBClient.create()
        contact_client = await ContactDBClient.create()

        # Use LLM to select the best matching opportunity
        selection_result: Optional[OpportunitySelectionResult] = None

        selected_opportunity_id = state.get("selected_opportunity_id")

        if isinstance(email, ReceivedEmail) or selected_opportunity_id is None:
            contact_related_opportunities = (
                await opportunity_client.fetch_opportunities_by_contact_email(
                    email.from_email
                )
            )
            logger.debug(
                "[Node 3] fetched %d contact-related opportunities",
                len(contact_related_opportunities),
            )

            content_related_opportunities = (
                await opportunity_client.find_similar_opportunities(
                    email.summary(), n_results=3
                )
            )
            logger.debug(
                "[Node 3] fetched %d content-related opportunities (scores=%s)",
                len(content_related_opportunities),
                [score for score, _ in content_related_opportunities],
            )

            top_related_opportunity_ids = rrf(
                ranked_lists=[
                    [oppur.id or "" for oppur in contact_related_opportunities],
                    [oppur.id or "" for score, oppur in content_related_opportunities],
                ],
                k=3,
            )
            logger.debug(
                "[Node 3] rrf produced top IDs: %s", top_related_opportunity_ids
            )

            top_related_opportunities = (
                await opportunity_client.fetch_opportunities_by_ids(
                    top_related_opportunity_ids
                )
            )
            logger.info(
                "[Node 3] selected %d top related opportunities (ids=%s)",
                len(top_related_opportunities),
                [opp.id for opp in top_related_opportunities],
            )

            if len(top_related_opportunities) > 0:
                # Build prompt and call LLM for structured selection
                prompt = _build_opportunity_selection_prompt(
                    email_summary=email.summary(),
                    opportunities=top_related_opportunities,
                )

                openrouter_client = get_openrouter_client()
                reflection_result = await openrouter_client.structured_completion(
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a sales assistant that matches emails to opportunities.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    response_model=OpportunitySelectionReflectionResult,
                    model=get_settings().openrouter_default_model,
                    temperature=0.2,
                )

                if (
                    reflection_result.opportunity_matched
                    and reflection_result.opportunity_selection_result
                ):
                    selection_result = reflection_result.opportunity_selection_result
                else:
                    new_opportunity = Opportunity(
                        id=uuid.uuid4().hex,
                        title=f"New Opportunity from {email.from_email}",
                    )
                    await opportunity_client.insert_opportunity(new_opportunity)
                    selection_result = OpportunitySelectionResult(
                        selected_opportunity=new_opportunity,
                        confidence=0.75,
                        reasoning="No related opportunities found; created a new opportunity.",
                    )

            else:
                new_opportunity = Opportunity(
                    id=uuid.uuid4().hex,
                    title=f"New Opportunity from {email.from_email}",
                )
                await opportunity_client.insert_opportunity(new_opportunity)
                selection_result = OpportunitySelectionResult(
                    selected_opportunity=new_opportunity,
                    confidence=0.75,
                    reasoning="No related opportunities found; created a new opportunity.",
                )

            logger.info(f"[Node 3] Selected opportunity: {selection_result},")

        else:
            opportunities = await opportunity_client.fetch_opportunities_by_ids(
                [selected_opportunity_id]
            )
            if opportunities:
                selection_result = OpportunitySelectionResult(
                    selected_opportunity=opportunities[0],
                    confidence=1.0,
                    reasoning="Opportunity ID was pre-selected in state.",
                )
            else:
                raise ValueError(
                    f"Pre-selected opportunity ID {selected_opportunity_id} not found."
                )

        # Use mapping-style access for TypedDict-like PipelineState
        state["matched_opportunity"] = selection_result

        if not state.get("are_related_contacts_linked_to_opportunity"):
            related_contacts = state.get("related_contacts")
            for contact in related_contacts or []:
                if contact and contact.id and selection_result.selected_opportunity.id:
                    await contact_client.link_contact_to_opportunity(
                        contact_id=contact.id,
                        opportunity_id=selection_result.selected_opportunity.id,
                    )
                    state["are_related_contacts_linked_to_opportunity"] = True
                else:
                    logger.warning(
                        "[Node 3] Cannot link sender to opportunity: missing contact ID or opportunity ID"
                    )
        else:
            logger.info(
                f"[Node 3] Sender to opportunity link skipped (already linked for {email_id})"
            )

        elapsed = time.time() - start_time
        logger.info(f"[Node 3] opportunity_matcher completed in {elapsed:.2f}s")

        return state

    except ValueError as ve:
        logger.error(f"[Node 3] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Node 3] Failed to update KG+RAG: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to update KG+RAG: {error}") from error
