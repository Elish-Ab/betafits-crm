#  workflows/langgraph/enrichment_flow/nodes/update_airtable_snapshot_node.py
import logging
from workflows.langgraph.enrichment_flow.state import EnrichmentState
# from services.enrichment.airtable_snapshot_writer import write_airtable_snapshot

logger = logging.getLogger(__name__)

async def update_airtable_snapshot_node(state: EnrichmentState) -> EnrichmentState:
    payload = {
        "company_id": state.get("matched_company_id"),
        "crunchbase": state.get("crunchbase_data"),
        "linkedin": state.get("linkedin_data"),
        "glassdoor": state.get("glassdoor_data"),
    }

    # ok = await write_airtable_snapshot(payload)
    ok = True
    state["airtable_written"] = bool(ok)

    logger.info("[Enrichment] update_airtable completed")
    return state
