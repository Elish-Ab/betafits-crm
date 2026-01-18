# workflows/langgraph/enrichment_flow/nodes/glassdoor_enrich_node.py
import logging
from typing import Optional
from workflows.langgraph.enrichment_flow.state import EnrichmentState
from services.enrichment.glassdoor_scraper import fetch_glassdoor_profile

logger = logging.getLogger(__name__)


async def glassdoor_enrich_node(state: EnrichmentState) -> EnrichmentState:
    """Enrich company data with Glassdoor profile information.
    
    Extracts glassdoor_url from crunchbase_data (from discovered_companies)
    and fetches Glassdoor profile metrics.
    
    Returns structure:
    {
        "overall_rating": float,
        "benefits_rating": float,
        "total_reviews": int,
        "benefit_reviews": int
    }
    """
    
    run_id = state.get("run_id", "unknown")
    glassdoor_url: Optional[str] = None
    
    try:
        # Extract glassdoor_url from crunchbase_data
        crunchbase_data = state.get("crunchbase_data", {})
        discovered_companies = crunchbase_data.get("discovered_companies", [])
        
        if discovered_companies and len(discovered_companies) > 0:
            # Get the first company's glassdoor URL
            glassdoor_url = discovered_companies[0].get("glassdoor_url")
        
        if not glassdoor_url:
            logger.warning(
                f"[Enrichment {run_id}] No glassdoor_url found in crunchbase_data",
                extra={"run_id": run_id},
            )
            state["glassdoor_data"] = {
                "overall_rating": 0,
                "benefits_rating": 0,
                "total_reviews": 0,
                "benefit_reviews": 0,
            }
            return state
        
        logger.info(
            f"[Enrichment {run_id}] Fetching Glassdoor profile from: {glassdoor_url}",
            extra={"run_id": run_id, "glassdoor_url": glassdoor_url},
        )
        
        # Fetch Glassdoor profile data
        data = await fetch_glassdoor_profile(
            glassdoor_url=glassdoor_url,
            run_id=run_id,
        )
        
        state["glassdoor_data"] = data or {
            "overall_rating": 0,
            "benefits_rating": 0,
            "total_reviews": 0,
            "benefit_reviews": 0,
        }
        
        logger.info(
            f"[Enrichment {run_id}] glassdoor_enrich completed successfully",
            extra={
                "run_id": run_id,
                "glassdoor_data": state["glassdoor_data"],
            },
        )
        
    except Exception as e:
        logger.error(
            f"[Enrichment {run_id}] Error during glassdoor enrichment: {str(e)}",
            extra={"run_id": run_id},
            exc_info=True,
        )
        # Return default empty data on error to allow workflow to continue
        state["glassdoor_data"] = {
            "overall_rating": 0,
            "benefits_rating": 0,
            "total_reviews": 0,
            "benefit_reviews": 0,
        }
    
    return state
