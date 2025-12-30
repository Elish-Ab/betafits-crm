"""LangGraph email processing pipeline graph assembly.

This module builds the complete StateGraph that wires together all 8 nodes
of the email processing pipeline. It implements:
- Node-to-node edges with conditional routing
- Short-circuit logic for spam/duplicates
- Graph compilation with invoke/stream methods
- Orchestrator function for end-to-end processing

Pipeline flow (8 nodes):
1. router → classifier (or skip to validator if duplicate)
2. classifier → kg_rag_updater (or skip to validator if spam)
3. kg_rag_updater → context_retriever (Graphiti extracts entities/relations)
4. context_retriever → response_drafter
5. response_drafter → json_formatter
6. json_formatter → email_sender
7. email_sender → validator
8. validator → END
"""

import logging
from typing import Any, Optional, Union
from fastapi import Request
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from lib.models.database_schemas import ReceivedEmail, SentEmail
from workflows.langgraph.email_processing.nodes.opportunity_matcher import (
    opportunity_matcher_node,
)
from workflows.langgraph.email_processing.state import PipelineState
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

logger = logging.getLogger(__name__)


def _should_skip_processing(state: PipelineState) -> str:
    """Conditional edge: check if email should skip to validation.

    Spam emails and duplicates skip enrichment stages and go directly
    to the validator node.

    Args:
        state: Current pipeline state.

    Returns:
        "validator" to skip to validation, otherwise "classifier"
    """
    # Check if email is duplicate (from Node 1)
    if state.get("is_duplicate", False):
        parsed_email = state.get("parsed_email")
        email_id = parsed_email.email_id if parsed_email else "unknown"
        logger.info(f"[Graph] Email {email_id} is duplicate - skipping to validator")
        return "validator"

    # Check if should skip (from Node 2 - spam classification)
    if state.get("should_skip", False):
        parsed_email = state.get("parsed_email")
        email_id = parsed_email.email_id if parsed_email else "unknown"
        logger.info(f"[Graph] Email {email_id} marked as spam - skipping to validator")
        return "validator"

    return "classifier"


def build_email_processing_graph(checkpointer: AsyncSqliteSaver) -> CompiledStateGraph:
    """Build and compile the complete email processing pipeline graph.

    The graph implements an 8-node pipeline with conditional edges:
    1. email_router → (classifier or validator via short-circuit)
    2. classifier → (kg_rag_updater or validator via short-circuit)
    3. kg_rag_updater → context_retriever (Graphiti extracts entities/relations)
    4. context_retriever → response_drafter
    5. response_drafter → json_formatter
    6. json_formatter → email_sender
    7. email_sender → validator
    8. validator → END

    Note: entity_extractor and relation_extractor nodes removed - Graphiti
    handles entity and relation extraction internally via add_episode().

    Returns:
        Compiled StateGraph ready for invocation.

    Raises:
        ValueError: If graph construction fails or checkpointer not initialized.
    """

    # Import nodes here to avoid circular import
    from workflows.langgraph.email_processing.nodes import (
        email_classifier_node,
        email_router_node,
        kg_rag_updater_node,
        workflow_validator_node,
    )

    logger.info("[Graph] Building email processing pipeline graph")

    try:
        # Create StateGraph
        graph = StateGraph(PipelineState)

        # ===================================================================
        # Add all 10 nodes
        # ===================================================================

        graph.add_node("router", email_router_node)
        graph.add_node("classifier", email_classifier_node)
        graph.add_node("opportunity_matcher", opportunity_matcher_node)
        graph.add_node("kg_rag_updater", kg_rag_updater_node)
        graph.add_node("validator", workflow_validator_node)

        # ===================================================================
        # Add edges (connections between nodes)
        # ===================================================================

        # Entry point: Start → router
        graph.set_entry_point("router")

        graph.add_conditional_edges(
            "router",
            _should_skip_processing,
            {
                "classifier": "classifier",
                "validator": "validator",
            },
        )

        graph.add_conditional_edges(
            "classifier",
            lambda state: "validator"
            if state.get("should_skip", False)
            else "opportunity_matcher",
            {
                "opportunity_matcher": "opportunity_matcher",
                "validator": "validator",
            },
        )

        graph.add_edge("opportunity_matcher", "kg_rag_updater")

        graph.add_edge("kg_rag_updater", "validator")

        graph.add_edge("validator", END)

        # ===================================================================
        # Compile graph
        # ===================================================================

        compiled_graph = graph.compile(checkpointer=checkpointer)
        logger.info("[Graph] Email processing pipeline graph compiled successfully")

        return compiled_graph  # type: ignore

    except Exception as error:
        logger.error(f"[Graph] Failed to build graph: {error}")
        raise ValueError(f"Failed to build email processing graph: {error}") from error


async def process_email(
    request: Request,
    email: Union[ReceivedEmail, SentEmail],
    selected_opportunity_id: Optional[str] = None,
) -> dict[str, Any]:
    """Orchestrator function: Process a raw email through the full pipeline.

    This is the main entry point for the email processing pipeline. It:
    1. Takes a raw email dict
    2. Initializes PipelineState
    3. Runs the graph to completion
    4. Returns final state with sent_status and validation_log

    Args:
        raw_email_input: Raw email dict with fields:
            - message_id: str (unique Gmail message ID)
            - from_email: str (sender email)
            - to_emails: list[str] (recipients)
            - cc_emails: list[str] (CC recipients)
            - subject: str (email subject)
            - body: str (email body text)
            - thread_id: str (Gmail thread ID, optional)
            - received_at: str (ISO 8601 timestamp)
            - attachments: list[dict] (attachment metadata, optional)
        selected_opportunity_id: Optional[str] (pre-selected opportunity ID)

    Returns:
        Final state dict with:
            - email_id: str (original email ID)
            - sent_status: str ("sent", "queued_for_approval", etc.)
            - message_id: Optional[str] (Gmail message ID if sent)
            - validation_log: ResponseValidationLog (audit trail)
            - pipeline_duration_seconds: float (total execution time)

    Raises:
        ValueError: If email processing fails critically.
    """
    import time

    start_time = time.time()

    try:
        logger.info(f"[Orchestrator] Starting pipeline for email {email.message_id}")

        # Initialize pipeline state
        initial_state: PipelineState = {
            "email": email,
        }

        if selected_opportunity_id:
            initial_state["selected_opportunity_id"] = selected_opportunity_id

        # Get compiled graph (cached singleton)
        graph = request.app.state.email_processing_graph

        # Run graph to completion
        final_state = await graph.ainvoke(
            input=initial_state,
            config={
                "configurable": {
                    "thread_id": email.message_id,
                }
            },
        )

        # Convert Pydantic models to dicts for proper serialization
        serializable_state = {}
        for key, value in final_state.items():
            if hasattr(value, "model_dump"):
                # Pydantic v2 model
                serializable_state[key] = value.model_dump()
            elif hasattr(value, "dict"):
                # Pydantic v1 model
                serializable_state[key] = value.dict()
            else:
                serializable_state[key] = value

        return serializable_state

        # # Extract final outputs
        # pipeline_result = {
        #     "email_id": email.message_id,
        #     "success": True,
        #     "sent_status": final_state.get("email_sent", {}).get(
        #         "sent_status", "unknown"
        #     ),
        #     "message_id": final_state.get("email_sent", {}).get("sent_message_id"),
        #     "validation_log": final_state.get("validation_log"),
        #     "pipeline_duration_seconds": time.time() - start_time,
        # }

        # logger.info(
        #     f"[Orchestrator] Pipeline completed for {email.message_id} "
        #     f"in {pipeline_result['pipeline_duration_seconds']:.2f}s "
        #     f"(status: {pipeline_result['sent_status']})"
        # )

        # return pipeline_result

    except ValueError as ve:
        logger.error(f"[Orchestrator] Input validation failed: {ve}")
        raise
    except Exception as error:
        duration = time.time() - start_time
        logger.error(f"[Orchestrator] Pipeline failed after {duration:.2f}s: {error}")
        raise ValueError(f"Email processing pipeline failed: {error}") from error


def process_email_sync(
    request: Request, email_input: Union[ReceivedEmail, SentEmail]
) -> dict[str, Any]:
    """Synchronous wrapper for process_email (for non-async contexts).

    Args:
        raw_email_input: Raw email dict (see process_email for schema).

    Returns:
        Final state dict (see process_email for schema).

    Raises:
        ValueError: If email processing fails.

    Note:
        This is a convenience wrapper. For production use async version.
    """
    import asyncio

    try:
        # Get or create event loop
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(process_email(request, email_input))
