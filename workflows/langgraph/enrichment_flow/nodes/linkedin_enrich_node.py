#  workflows/langgraph/enrichment_flow/nodes/linkedin_enrich_node.py
import logging
import os
from workflows.langgraph.enrichment_flow.state import EnrichmentState
from services.enrichment.linkedin_scraper import fetch_linkedin_profile

logger = logging.getLogger(__name__)

async def linkedin_enrich_node(state: EnrichmentState) -> EnrichmentState:
    """
    Scrape LinkedIn profile for matched company.
    
    Input from state:
    - matched_company_id: dict with "new_ids" and "existing_ids" lists
    - crunchbase_data: dict with discovered_companies list (containing linkedin_url)
    
    Output to state:
    - linkedin_data: dict with extracted employee and job metrics
    """
    run_id: str = state.get("run_id", "")
    matched_result: dict = state.get("matched_company_id", {})
    crunchbase_data: dict = state.get("crunchbase_data", {})
    
    # Get discovered companies from crunchbase data
    discovered_companies: list[dict] = crunchbase_data.get("discovered_companies", [])
    
    if not discovered_companies:
        logger.warning(f"[LinkedIn] No discovered companies in state (run_id: {run_id})")
        state["linkedin_data"] = {}
        return state
    
    # Get LinkedIn URLs from discovered companies
    linkedin_urls = [
        company.get("linkedin_url")
        for company in discovered_companies
        if company.get("linkedin_url")
    ]
    
    if not linkedin_urls:
        logger.warning(
            f"[LinkedIn] No LinkedIn URLs found in discovered companies "
            f"(run_id: {run_id})"
        )
        state["linkedin_data"] = {}
        return state
    
    try:
        # Scrape LinkedIn profile using first available URL
        linkedin_url = linkedin_urls[0]
        logger.info(
            f"[LinkedIn] Scraping LinkedIn profile: {linkedin_url} (run_id: {run_id})"
        )
        
        # Get credentials from environment variables
        linkedin_email = os.getenv("LINKEDIN_EMAIL")
        linkedin_password = os.getenv("LINKEDIN_PASSWORD")
        refresh_cookies = (os.getenv("LINKEDIN_REFRESH_COOKIES") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
        }
        
        linkedin_data = await fetch_linkedin_profile(
            linkedin_url=linkedin_url, 
            run_id=run_id,
            linkedin_email=linkedin_email,
            linkedin_password=linkedin_password,
            refresh_cookies=refresh_cookies,
        )
        state["linkedin_data"] = linkedin_data or {}
        
        logger.info(
            f"[LinkedIn] Extraction completed - "
            f"employees: {linkedin_data.get('total_employees', 0)}, "
            f"us_employees: {linkedin_data.get('us_employees', 0)}, "
            f"open_jobs: {linkedin_data.get('open_jobs', 0)} "
            f"(run_id: {run_id})"
        )
    except Exception as e:
        logger.error(
            f"[LinkedIn] Error scraping LinkedIn profile: {str(e)} (run_id: {run_id})"
        )
        state.setdefault("errors", []).append(
            {"source": "linkedin_enrich_node", "error": str(e)}
        )
        state["linkedin_data"] = {}
    
    return state
