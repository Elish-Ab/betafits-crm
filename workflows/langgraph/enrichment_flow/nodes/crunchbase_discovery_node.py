#  workflows/langgraph/enrichment_flow/nodes/crunchbase_discovery_node.py
import logging
from workflows.langgraph.enrichment_flow.state import EnrichmentState
from services.enrichment.crunchbase_scraper import fetch_crunchbase_profile

logger = logging.getLogger(__name__)

async def crunchbase_discovery_node(state: EnrichmentState) -> EnrichmentState:
    """Discover portfolio companies from Crunchbase.
    
    Currently using hardcoded test data for development/testing.
    Replace with actual fetch_crunchbase_profile() when ready.
    """

    # Hardcoded test data for development - simulates realistic Crunchbase output
    # TODO: Uncomment below to use actual service call when ready
    # data = await fetch_crunchbase_profile(domain)
    data = {
        "discovered_companies": [
            {
                "company_name": "Stripe",
                "linkedin_url": "https://www.linkedin.com/company/stripe/",
                "glassdoor_url": "https://www.glassdoor.co.in/Reviews/Stripe-Bangalore-Reviews-EI_IE671932.0,6_IL.7,16_IM1091.htm",
            },
        ]
    }
    
    state["crunchbase_data"] = data or {}

    logger.info(
        f"[Enrichment] crunchbase_discovery completed - {len(data.get('discovered_companies', []))} companies discovered",
        extra={
            "run_id": state.get("run_id", "unknown"),
            "company_count": len(data.get("discovered_companies", [])),
        },
    )
    return state
