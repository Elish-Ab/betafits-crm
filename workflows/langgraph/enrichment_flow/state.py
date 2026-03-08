#  workflows/langgraph/enrichment_flow/state.py
from typing import TypedDict, Optional

class EnrichmentState(TypedDict, total=False):
    run_id: str

    # Inputs
    company_id: str
    company_domain: str
    trigger_source: str  # e.g., "partner_added", "scheduled_refresh"

    # Scraper outputs
    crunchbase_data: dict
    linkedin_data: dict
    glassdoor_data: dict

    # Combined/normalized output
    enriched_company: dict
    matched_company_id: Optional[str]

    # Persistence results
    supabase_written: bool
    airtable_written: bool

    # Logging/diagnostics
    errors: list[str]
    warnings: list[str]
