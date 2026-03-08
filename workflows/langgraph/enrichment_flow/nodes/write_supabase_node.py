# workflows/langgraph/enrichment_flow/nodes/write_supabase_node.py
import logging
from workflows.langgraph.enrichment_flow.state import EnrichmentState
from services.enrichment.supabase_writer import write_company_enrichment

logger = logging.getLogger(__name__)

async def write_supabase_node(state: EnrichmentState) -> EnrichmentState:
    run_id = state.get("run_id", "unknown")

    payload = {
        "company_id": state.get("matched_company_id"),
        "crunchbase": state.get("crunchbase_data"),
        "linkedin": state.get("linkedin_data"),
        "glassdoor": state.get("glassdoor_data"),
    }

    try:
        ok = await write_company_enrichment(payload, run_id=run_id)
        state["supabase_written"] = bool(ok)
    except Exception as e:
        state["supabase_written"] = False
        state["errors"] = state.get("errors", []) + [f"Supabase write failed: {str(e)}"]
        logger.error(
            f"[Enrichment {run_id}] Supabase write failed: {str(e)}",
            extra={"run_id": run_id},
            exc_info=True,
        )

    logger.info(
        f"[Enrichment {run_id}] write_supabase completed",
        extra={"run_id": run_id, "supabase_written": state.get("supabase_written")},
    )
    return state
