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
import uuid
from fastapi import Request
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from lib.config.settings import get_settings
from lib.integrations.supabase.logging_client import LoggingDBClient
from lib.models.database_schemas import (
    LGEnvironment,
    LGRun,
    LGRunStatus,
    LGTriggerType,
    ReceivedEmail,
    SentEmail,
)
from workflows.langgraph.email_processing.nodes.opportunity_matcher import (
    opportunity_matcher_node,
)
from workflows.langgraph.email_processing.state import PipelineState
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

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


def _build_pipeline_result(
    email: Union[ReceivedEmail, SentEmail],
    final_state: PipelineState,
    start_time: float,
) -> dict[str, Any]:
    """Construct comprehensive pipeline result from final state.

    The email processing pipeline ingests emails and updates the knowledge graph
    and database. This builds a result that captures:
    - Email parsing and deduplication
    - Classification results
    - Opportunity matching
    - Contact extraction
    - Knowledge graph and RAG updates
    - Validation and audit trail

    Args:
        email: The original email being processed.
        final_state: The final PipelineState after all nodes complete.
        start_time: Pipeline start time for duration calculation.

    Returns:
        Dictionary containing:
            - Basic info: email_id, message_id, pipeline_id
            - Status: success, final_status
            - Processing: duplicate, classification, opportunity, contacts
            - KG/RAG: knowledge_graph and vector_index updates
            - Database: storage status
            - Validation: errors, warnings, audit log
            - Performance: pipeline_duration_seconds
    """
    import time

    pipeline_duration = time.time() - start_time

    # Determine overall success based on validation_log
    validation_log = final_state.get("validation_log")
    final_status = validation_log.final_status if validation_log else "unknown"
    success = final_status == "success"

    # Identify if pipeline was short-circuited and why
    short_circuit_reason = None
    if final_state.get("is_duplicate"):
        short_circuit_reason = "duplicate_email"
    elif final_state.get("should_skip"):
        short_circuit_reason = "spam_classification"

    # Extract email metadata
    processing_mode = final_state.get("processing_mode", "incoming")
    email_stored = final_state.get("are_email_and_contacts_stored", False)

    # Extract classification results
    labeled_email = final_state.get("labeled_email")
    classification = None
    if labeled_email:
        classification = {
            "label": labeled_email.label,
            "confidence": labeled_email.confidence,
        }

    # Extract matched opportunity
    matched_opp = final_state.get("matched_opportunity")
    opportunity_info = None
    if matched_opp:
        opportunity_info = {
            "id": matched_opp.selected_opportunity.id,
            "title": matched_opp.selected_opportunity.title,
            "matched": matched_opp.reasoning,
            "match_confidence": matched_opp.confidence,
        }

    # Extract related contacts
    related_contacts = final_state.get("related_contacts", [])
    contacts_info = {
        "count": len(related_contacts),
        "linked_to_opportunity": final_state.get(
            "are_related_contacts_linked_to_opportunity", False
        ),
    }

    # Extract KG/RAG update status
    kg_rag_updates = {
        "knowledge_graph": {
            "updated": final_state.get("is_kg_updated", False),
            "communities_built": final_state.get("are_communities_built", False),
        },
        "vector_index": {
            "opportunity_index_updated": final_state.get(
                "is_opportunity_index_rag_updated", False
            ),
        },
    }

    # Extract validation log details
    validation_details = None
    errors = []
    warnings = []
    if validation_log:
        validation_details = {
            "final_status": validation_log.final_status,
            "total_duration_seconds": validation_log.total_duration_seconds,
        }
        errors = validation_log.errors
        warnings = validation_log.warnings

    # Build comprehensive result
    pipeline_result = {
        # ===== Email Identification =====
        "email_id": email.message_id,
        "pipeline_id": final_state.get("pipeline_execution_id", "unknown"),
        "processing_mode": processing_mode,
        # ===== Status =====
        "success": success,
        "final_status": final_status,
        "short_circuit_reason": short_circuit_reason,
        # ===== Email Processing =====
        "email": {
            "is_duplicate": final_state.get("is_duplicate", False),
            "stored": email_stored,
        },
        # ===== Classification =====
        "classification": classification,
        # ===== Opportunity Matching =====
        "matched_opportunity": opportunity_info,
        # ===== Contact Extraction =====
        "contacts": contacts_info,
        # ===== Knowledge Graph & RAG =====
        "kg_rag_updates": kg_rag_updates,
        # ===== Validation & Audit =====
        "validation": validation_details,
        "errors": errors,
        "warnings": warnings,
        "validation_log": _serialize_value(validation_log),
        # ===== Performance =====
        "pipeline_duration_seconds": pipeline_duration,
        "user_id": final_state.get("user_id"),
    }

    return pipeline_result


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
    """Orchestrator function: Process an email through the knowledge graph ingestion pipeline.

    This is the main entry point for email ingestion. The pipeline:
    1. Parses, normalizes, and deduplicates the email
    2. Classifies it (crm, customer_success, spam)
    3. Matches it to a sales opportunity
    4. Extracts entities/relations and updates the knowledge graph via Graphiti
    5. Updates the vector index with opportunity embeddings
    6. Stores email and contacts in the database
    7. Validates the complete process and logs results

    Args:
        request: FastAPI request object (contains graph from app.state).
        email: Union[ReceivedEmail, SentEmail] - the email to process.
        selected_opportunity_id: Optional pre-selected opportunity ID.

    Returns:
        Dictionary with processing results:
            - email_id: Original email message ID
            - pipeline_id: Unique execution ID
            - processing_mode: "incoming" or "outgoing"
            - success: Boolean success flag
            - final_status: "success", "partial_success", or "failure"
            - short_circuit_reason: "duplicate_email" or "spam_classification" if skipped
            - email: {is_duplicate, stored}
            - classification: {label, confidence}
            - matched_opportunity: {id, title, matched, match_confidence}
            - contacts: {count, linked_to_opportunity}
            - kg_rag_updates: Knowledge graph and vector index update status
            - validation: Validation metrics (KG nodes/edges, RAG vectors)
            - errors/warnings: Processing issues
            - validation_log: Complete audit trail
            - pipeline_duration_seconds: Total execution time

    Raises:
        ValueError: If email processing fails critically.
    """
    import time

    start_time = time.time()
    logging_client = await LoggingDBClient.create()

    thread_id = email.message_id
    workflow_name = "email_processing_pipeline"

    lg_run = LGRun(
        thread_id=thread_id,
        workflow_id=uuid.uuid5(uuid.NAMESPACE_DNS, workflow_name),
        workflow_name=workflow_name,
        triggered_by=LGTriggerType.internal_event,
        status=LGRunStatus.running,
        environment=get_settings().get_lg_env_type(),
        input_payload={
            "email": _serialize_value(email),
            "selected_opportunity_id": selected_opportunity_id,
        },
        input_summary=f"Processing email {email.message_id} in thread {thread_id}",
    )

    # Initialize pipeline state
    initial_state: PipelineState = {
        "email": email,
    }

    if selected_opportunity_id:
        initial_state["selected_opportunity_id"] = selected_opportunity_id

    # Get compiled graph (cached singleton)
    graph: CompiledStateGraph = request.app.state.email_processing_graph
    

    initial_state["run_id"] = await logging_client.create_lg_run(lg_run)

    try:
        logger.info(f"[Orchestrator] Starting pipeline for email {email.message_id}")

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

        # Build comprehensive pipeline result
        pipeline_result = _build_pipeline_result(
            email=email,
            final_state=PipelineState(**final_state),
            start_time=start_time,
        )


        try:
            lg_run.output_payload = pipeline_result
            lg_run.output_summary = (
                f"Pipeline completed with status {pipeline_result['final_status']}"
                f" in {pipeline_result['pipeline_duration_seconds']:.2f}s"
            )
            lg_run.status = LGRunStatus.completed
        
            await logging_client.update_lg_run(
                run_id=initial_state["run_id"],
                run=lg_run
            )
        except Exception as log_error:
            logger.error(f"[Orchestrator] Failed to log LGRun results: {log_error}")

        logger.info(
            f"[Orchestrator] Pipeline completed for {email.message_id} "
            f"in {pipeline_result['pipeline_duration_seconds']:.2f}s "
            f"with status {pipeline_result['final_status']}"
        )

        return pipeline_result

    except Exception as error:
        duration = time.time() - start_time
        logger.error(f"[Orchestrator] Pipeline failed after {duration:.2f}s: {error}")

        try:
            lg_run.status = LGRunStatus.failed
            await logging_client.update_lg_run(
                run_id=initial_state["run_id"],
                run=lg_run
            )
        except Exception as log_error:
            logger.error(f"[Orchestrator] Failed to log LGRun results: {log_error}")
        
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
