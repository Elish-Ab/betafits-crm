import hashlib
import logging
from typing import Any
from fastapi import Request
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from lib.models.io_formats import EmailDraftingScenario
from workflows.langgraph.email_drafting.state import PipelineState
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

logger = logging.getLogger(__name__)


def build_email_drafting_graph(checkpointer: AsyncSqliteSaver) -> CompiledStateGraph:
    """Builds and compiles the email drafting pipeline graph.
    Args:
        checkpointer: AsyncSqliteSaver for state checkpointing.
    Returns:
        CompiledStateGraph ready for execution.
    Raises:
        ValueError: If graph construction fails.
    """

    # Import nodes here to avoid circular import
    from workflows.langgraph.email_drafting.nodes import (
        context_retriever_node,
        email_drafter_node,
        workflow_validator_node,
    )

    logger.info("[Graph] Building email processing pipeline graph")

    try:
        # Create StateGraph
        graph = StateGraph(PipelineState)

        graph.add_node("context_retriever", context_retriever_node)
        graph.add_node("email_drafter", email_drafter_node)
        graph.add_node("validator", workflow_validator_node)

        graph.set_entry_point("context_retriever")

        graph.add_edge("context_retriever", "email_drafter")

        graph.add_edge("email_drafter", "validator")

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


async def draft_email(
    request: Request, drafting_scenario_input: EmailDraftingScenario
) -> dict[str, Any]:
    """Orchestrator function to run the email drafting pipeline end-to-end.
    Args:
        request: FastAPI Request object (for app state access).
        drafting_scenario_input: EmailDraftingScenario input data.
    Returns:
        Final state dict (see PipelineState for schema).
    Raises:
        ValueError: If email processing fails.
    """

    import time

    start_time = time.time()

    try:
        logger.info(
            f"[Orchestrator] Starting pipeline for opportunity ID "
            f"{drafting_scenario_input.opportunity_id}"
            f" drafting scenario: {drafting_scenario_input.drafting_scenario[:50]}..."
        )

        # Initialize pipeline state
        initial_state: PipelineState = {
            "drafting_scenario": drafting_scenario_input,
        }

        # Get compiled graph (cached singleton)
        graph = request.app.state.email_drafting_graph

        # Run graph to completion
        # Generate thread_id by hashing opportunity_id + drafting_scenario
        thread_id = hashlib.sha256(
            f"{drafting_scenario_input.opportunity_id}:{drafting_scenario_input.drafting_scenario}".encode()
        ).hexdigest()

        final_state = await graph.ainvoke(
            input=initial_state,
            config={
                "configurable": {
                    "thread_id": thread_id,
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
        #     "email_id": raw_email.message_id,
        #     "success": True,
        #     "sent_status": final_state.get("email_sent", {}).get(
        #         "sent_status", "unknown"
        #     ),
        #     "message_id": final_state.get("email_sent", {}).get("sent_message_id"),
        #     "validation_log": final_state.get("validation_log"),
        #     "pipeline_duration_seconds": time.time() - start_time,
        # }

        # logger.info(
        #     f"[Orchestrator] Pipeline completed for {raw_email.message_id} "
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
    request: Request, drafting_scenario_input: EmailDraftingScenario
) -> dict[str, Any]:
    """Synchronous wrapper for process_email (for non-async contexts).

    Args:
        request: FastAPI Request object (for app state access).
        drafting_scenario_input: EmailDraftingScenario input data.

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

    return loop.run_until_complete(draft_email(request, drafting_scenario_input))
