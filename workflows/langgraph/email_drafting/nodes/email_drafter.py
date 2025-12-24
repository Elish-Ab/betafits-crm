import logging
import time
from typing import Optional

from pydantic import SecretStr

from lib.config.settings import get_settings
from lib.integrations.graphiti_client import get_graphiti_client
from lib.integrations.openrouter_client import get_openrouter_client
from lib.integrations.supabase.emails_client import EmailDBClient
from lib.models.database_schemas import DraftedEmail
from lib.models.io_formats import (
    ContextBundle,
    ResponseDraftStructured,
)
from lib.prompts.email_chains import EMAIL_DRAFT_SYSTEM_PROMPT
from workflows.langgraph.email_drafting.state import PipelineState
from langchain.agents import create_agent
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain.messages import HumanMessage

logger = logging.getLogger(__name__)


async def email_drafter_node(state: PipelineState) -> PipelineState:
    """Draft email using LLM and retrieved context.

    Inputs from state:
        - context_bundle: ContextBundle with retrieved context

    Outputs to state:
        - response_draft: ResponseDraft (LLM-generated draft)

    Args:
        state: PipelineState with parsed_email and context_bundle fields.

    Returns:
        Updated PipelineState with response_draft field.

    Raises:
        ValueError: If drafting fails.
    """
    start_time = time.time()

    # Get inputs
    context_bundle_opt = state.get("context_bundle")
    drafting_scenario_opt = state.get("drafting_scenario")

    if not context_bundle_opt:
        raise ValueError("context_bundle is required in state")

    if not drafting_scenario_opt:
        raise ValueError("drafting_scenario is required in state")

    context_bundle: ContextBundle = context_bundle_opt
    drafting_scenario = drafting_scenario_opt

    logger.info(
        f"[Email Drafting Pipeline] context_retriever_node starting for opportunity ID "
        f"{context_bundle.opportunity.id}"
        f"drafting scenario: {drafting_scenario.drafting_scenario[:50]}..."
    )

    try:
        # openrouter_client = get_openrouter_client()
        graphiti_client = get_graphiti_client()
        email_client = await EmailDBClient.create()

        # Determine if this is a response email or simple email draft
        is_response_email = context_bundle.in_reply_to_email is not None

        # Build draft prompt based on email type
        if is_response_email:
            # Response email prompt
            draft_prompt = f"""You are drafting a RESPONSE to an email on behalf of Betafits.

**IMPORTANT**: Study the response style examples provided below. Your response should match Betafits' tone and style.

{context_bundle.get_prompt_repr()}

**🔍 CRITICAL - USE THE get_facts_from_memory TOOL:**
Before drafting, you MUST use the get_facts_from_memory tool to retrieve any additional context you need. This includes:
- Details about the opportunity, contacts, or relationships
- Previous interactions or conversations
- Relevant company information or preferences
- Any facts that would help you write a more personalized and accurate response

Query the knowledge base multiple times if needed to gather comprehensive context. Do NOT draft without first checking if there's relevant information available.

**Drafting Instructions:**
{drafting_scenario.drafting_instructions or "Draft a professional response that addresses all key points."}

**Scenario Context:**
{drafting_scenario.drafting_scenario}

Based on the style examples and context provided, draft a professional response email that:
- Starts with "Re:" in the subject line
- Addresses all points from the original email
- Matches Betafits' communication style
- Includes relevant details and next steps
- Incorporates facts retrieved from the knowledge base"""
        else:
            # Simple/outbound email prompt
            draft_prompt = f"""You are drafting an OUTBOUND email on behalf of Betafits.

**IMPORTANT**: Study the email style examples provided below. Your email should match Betafits' tone and style.

{context_bundle.get_prompt_repr()}

**🔍 CRITICAL - USE THE get_facts_from_memory TOOL:**
Before drafting, you MUST use the get_facts_from_memory tool to retrieve any additional context you need. This includes:
- Details about the opportunity, contacts, or relationships
- Previous interactions or conversations
- Relevant company information or preferences
- Any facts that would help you write a more personalized and accurate email

Query the knowledge base multiple times if needed to gather comprehensive context. Do NOT draft without first checking if there's relevant information available.

**Drafting Instructions:**
{drafting_scenario.drafting_instructions or "Draft a professional email based on the context."}

**Scenario Context:**
{drafting_scenario.drafting_scenario}

Based on the style examples and context provided, draft a professional email that:
- Has an appropriate subject line for the purpose
- Matches Betafits' communication style
- Includes relevant details and next steps
- Maintains a warm, solution-focused tone
- Incorporates facts retrieved from the knowledge base"""

        # # Call LLM with structured completion
        # draft_structured = await openrouter_client.structured_completion(
        #     messages=[
        #         {"role": "system", "content": EMAIL_DRAFT_SYSTEM_PROMPT},
        #         {"role": "user", "content": draft_prompt},
        #     ],
        #     response_model=ResponseDraftStructured,
        #     model=get_settings().openrouter_default_model,
        #     temperature=0.5,
        #     max_tokens=800,
        # )

        draft_structured: Optional[ResponseDraftStructured] = None

        llm = ChatOpenAI(
            model=get_settings().openrouter_default_model,
            api_key=SecretStr(get_settings().openrouter_api_key or ""),
            base_url=get_settings().openrouter_base_url,
            temperature=0.5,
        )

        @tool(args_schema=ResponseDraftStructured)
        def create_draft_email(
            subject: str,
            body: str,
            to_emails: list[str],
            cc_emails: list[str],
            tone: str,
            confidence: float,
        ) -> ResponseDraftStructured:
            """Tool to draft email using structured response."""
            nonlocal draft_structured
            draft_structured = ResponseDraftStructured(
                subject=subject,
                body=body,
                to_emails=to_emails,
                cc_emails=cc_emails,
                tone=tone,
                confidence=confidence,
            )
            return draft_structured

        @tool
        async def get_facts_from_memory(query: str, n_results: int = 10) -> str:
            """Tool to get facts from Graphiti memory."""
            return await graphiti_client.get_facts(
                query, group_id=context_bundle.opportunity.id, max_results=n_results
            )

        response_drafter_agent = create_agent(
            model=llm,
            tools=[create_draft_email, get_facts_from_memory],
            system_prompt=EMAIL_DRAFT_SYSTEM_PROMPT,
        )

        logger.info("[Email Drafting Pipeline] Invoking response drafter agent")
        async for chunk in response_drafter_agent.astream(
            {"messages": [HumanMessage(content=draft_prompt)]}, stream_mode="values"
        ):
            latest_message = chunk["messages"][-1]
            latest_message.pretty_print()

        logger.info("[Email Drafting Pipeline] Response drafter agent completed")

        if not draft_structured:
            raise ValueError("LLM did not return a draft_structured response")

        # Create DraftedEmail from structured response
        response_draft = DraftedEmail(
            from_email=drafting_scenario.from_email,
            to_emails=draft_structured.to_emails,
            subject=draft_structured.subject,
            body=draft_structured.body,
            cc_emails=list(
                set(drafting_scenario.cc_emails + draft_structured.cc_emails)
            ),
            bcc_emails=drafting_scenario.bcc_emails,
            model_used=get_settings().openrouter_default_model,
            metadata={"tone": draft_structured.tone},
            confidence=draft_structured.confidence,
        )

        logger.info(
            f"[Email Drafting Pipeline] Draft generated for opportunity ID "
            f"{context_bundle.opportunity.id} "
            f"drafting scenario: {drafting_scenario.drafting_scenario[:50]}... "
            f"in {time.time() - start_time:.2f}s"
        )

        response_draft.id = await email_client.store_drafted_email(response_draft)

        state["response_draft"] = response_draft

        return state

    except ValueError as ve:
        logger.error(f"[Email Drafting Pipeline] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Email Drafting Pipeline] Failed to draft response: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to draft response: {error}") from error
