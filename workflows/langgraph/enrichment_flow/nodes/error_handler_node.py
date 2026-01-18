#  workflows/langgraph/enrichment_flow/nodes/error_handler_node.py
import logging
from workflows.langgraph.enrichment_flow.state import EnrichmentState

logger = logging.getLogger(__name__)

async def error_handler_node(state: EnrichmentState) -> EnrichmentState:
    logger.error("[Enrichment] Error detected: %s", state.get("errors"))
    return state
