import hashlib
import logging
import uuid
from typing import Any
from fastapi import Request
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from lib.config.settings import get_settings
from lib.integrations.supabase.logging_client import LoggingDBClient
from lib.models.database_schemas import (
    LGRun,
    LGRunStatus,
    LGTriggerType,
)
from lib.models.io_formats import EmailDraftingScenario
from workflows.langgraph.email_drafting.state import PipelineState
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

logger = logging.getLogger(__name__)


def _serialize_value(value: Any) -> Any:
    """Convert Pydantic models and other complex types to JSON-serializable format.

    Args:
        value: Value to serialize.

    Returns:
        JSON-serializable version of the value.
    """
    if hasattr(value, "model_dump"):
        # Pydantic v2 model
        return value.model_dump()
    elif hasattr(value, "dict"):
        # Pydantic v1 model
        return value.dict()
    elif isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    return value


def build_email_drafting_graph(checkpointer: AsyncPostgresSaver) -> CompiledStateGraph:
    """Builds and compiles the email drafting pipeline graph.
    Args:
        checkpointer: AsyncPostgresSaver for state checkpointing.
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
        Final state dict with drafting results and validation log.

    Raises:
        ValueError: If email drafting fails.
    """

    import time

    start_time = time.time()
    logging_client = await LoggingDBClient.create()

    # Generate thread_id by hashing opportunity_id + drafting_scenario
    thread_id = hashlib.sha256(
        f"{drafting_scenario_input.opportunity_id}:{drafting_scenario_input.drafting_scenario}".encode()
    ).hexdigest()

    workflow_name = "email_drafting_pipeline"

    lg_run = LGRun(
        thread_id=thread_id,
        workflow_id=uuid.uuid5(uuid.NAMESPACE_DNS, workflow_name),
        workflow_name=workflow_name,
        triggered_by=LGTriggerType.internal_event,
        status=LGRunStatus.running,
        environment=get_settings().get_lg_env_type(),
        input_payload={
            "opportunity_id": drafting_scenario_input.opportunity_id,
            "drafting_scenario": drafting_scenario_input.drafting_scenario,
        },
        input_summary=f"Drafting email for opportunity {drafting_scenario_input.opportunity_id}",
    )

    # Initialize pipeline state
    initial_state: PipelineState = {
        "drafting_scenario": drafting_scenario_input,
    }

    # Get compiled graph (cached singleton)
    graph: CompiledStateGraph = request.app.state.email_drafting_graph

    # Create LGRun record
    run_id = await logging_client.create_lg_run(lg_run)
    initial_state["run_id"] = run_id

    try:
        logger.info(
            f"[Orchestrator] Starting pipeline for opportunity ID "
            f"{drafting_scenario_input.opportunity_id}"
            f" drafting scenario: {drafting_scenario_input.drafting_scenario[:50]}..."
        )

        # Run graph to completion
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

        try:
            lg_run.output_payload = serializable_state
            lg_run.output_summary = (
                f"Email draft generated successfully in {time.time() - start_time:.2f}s"
            )
            lg_run.status = LGRunStatus.completed

            await logging_client.update_lg_run(run_id=run_id, run=lg_run)
        except Exception as log_error:
            logger.error(f"[Orchestrator] Failed to log LGRun results: {log_error}")

        logger.info(
            f"[Orchestrator] Pipeline completed for opportunity {drafting_scenario_input.opportunity_id} "
            f"in {time.time() - start_time:.2f}s"
        )

        return serializable_state

    except ValueError as ve:
        logger.error(f"[Orchestrator] Input validation failed: {ve}")

        try:
            lg_run.status = LGRunStatus.failed
            lg_run.error_message = str(ve)
            await logging_client.update_lg_run(
                run_id=initial_state.get("run_id", "unknown"), run=lg_run
            )
        except Exception as log_error:
            logger.error(f"[Orchestrator] Failed to log LGRun error: {log_error}")

        raise
    except Exception as error:
        duration = time.time() - start_time
        logger.error(f"[Orchestrator] Pipeline failed after {duration:.2f}s: {error}")

        try:
            lg_run.status = LGRunStatus.failed
            lg_run.error_message = str(error)
            await logging_client.update_lg_run(
                run_id=initial_state.get("run_id", "unknown"), run=lg_run
            )
        except Exception as log_error:
            logger.error(f"[Orchestrator] Failed to log LGRun error: {log_error}")

        raise ValueError(f"Email drafting pipeline failed: {error}") from error


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
