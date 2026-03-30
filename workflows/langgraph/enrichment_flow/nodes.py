"""Data Enrichment domain – LangGraph nodes."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from core.state import DataEnrichmentState
from core.storage import airtable_upsert, airtable_get_all, supabase_upsert
from .scrapers import (
    scrape_crunchbase_portfolio,
    scrape_linkedin_company,
    scrape_glassdoor_company,
)


# ---------------------------------------------------------------------------
# Node 1 – Scrape Crunchbase portfolio
# ---------------------------------------------------------------------------

def scrape_crunchbase(state: DataEnrichmentState) -> dict[str, Any]:
    """Scrape the VC partner's Crunchbase portfolio page."""
    if not state.crunchbase_url:
        logger.info("No Crunchbase URL provided – skipping portfolio scrape.")
        return {}

    logger.info("Scraping Crunchbase for vc_partner_id={}", state.vc_partner_id)
    companies = asyncio.run(scrape_crunchbase_portfolio(state.crunchbase_url))
    return {"raw_portfolio_companies": companies}


# ---------------------------------------------------------------------------
# Node 2 – Deduplicate portfolio companies
# ---------------------------------------------------------------------------

def deduplicate_portfolio(state: DataEnrichmentState) -> dict[str, Any]:
    """Remove duplicate companies from the raw portfolio list.

    Crunchbase often lists the same company multiple times (one entry per
    funding round).  We deduplicate on ``crunchbase_url``.
    """
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for company in state.raw_portfolio_companies:
        key = company.get("crunchbase_url", company.get("company_name", ""))
        if key and key not in seen:
            seen.add(key)
            unique.append(company)

    logger.info(
        "Deduplication: {} → {} unique companies",
        len(state.raw_portfolio_companies),
        len(unique),
    )
    return {"deduplicated_companies": unique}


# ---------------------------------------------------------------------------
# Node 3 – Match against CRM
# ---------------------------------------------------------------------------

def match_against_crm(state: DataEnrichmentState) -> dict[str, Any]:
    """Check which portfolio companies already exist in the CRM.

    Matches on company name (case-insensitive).  Returns two lists:
    - ``matched_company_ids``: IDs of existing records to update.
    - ``new_company_ids``: Names of companies that need to be created.
    """
    logger.info(
        "Matching {} companies against CRM", len(state.deduplicated_companies)
    )

    matched: list[str] = []
    new_companies: list[str] = []

    try:
        existing = airtable_get_all("Companies")
        existing_names = {
            r.get("company_name", "").lower(): r.get("record_id", r.get("id", ""))
            for r in existing
        }
    except Exception as exc:
        logger.warning("Could not fetch Companies from Airtable: {}", exc)
        existing_names = {}

    for company in state.deduplicated_companies:
        name = company.get("company_name", "").lower()
        if name in existing_names:
            matched.append(existing_names[name])
        else:
            new_companies.append(company.get("company_name", ""))

    logger.info(
        "CRM match: {} existing, {} new companies",
        len(matched),
        len(new_companies),
    )
    return {
        "matched_company_ids": matched,
        "new_company_ids": new_companies,
    }


# ---------------------------------------------------------------------------
# Node 4 – Create new CRM records
# ---------------------------------------------------------------------------

def create_new_crm_records(state: DataEnrichmentState) -> dict[str, Any]:
    """Insert new company records into Airtable for unmatched portfolio companies."""
    if not state.new_company_ids:
        return {}

    logger.info("Creating {} new company records in Airtable", len(state.new_company_ids))

    records = [
        {
            "company_name": name,
            "source": "crunchbase_portfolio",
            "vc_partner_id": state.vc_partner_id,
            "status": "prospect",
        }
        for name in state.new_company_ids
    ]

    try:
        count = airtable_upsert("Companies", records, key_fields=["company_name"])
        return {"airtable_records_updated": state.airtable_records_updated + count}
    except Exception as exc:
        logger.warning("Airtable record creation skipped: {}", exc)
        return {}


# ---------------------------------------------------------------------------
# Node 5 – Enrich with LinkedIn
# ---------------------------------------------------------------------------

def enrich_linkedin(state: DataEnrichmentState) -> dict[str, Any]:
    """Run the LinkedIn scraper for each portfolio company."""
    logger.info(
        "Running LinkedIn enrichment for {} companies",
        len(state.deduplicated_companies),
    )

    results: list[dict[str, Any]] = []
    for company in state.deduplicated_companies:
        linkedin_url = company.get("linkedin_url")
        if not linkedin_url:
            continue
        try:
            data = asyncio.run(scrape_linkedin_company(linkedin_url))
            data["company_name"] = company.get("company_name")
            results.append(data)
        except Exception as exc:
            logger.warning(
                "LinkedIn enrichment failed for {}: {}", company.get("company_name"), exc
            )

    return {"linkedin_results": results}


# ---------------------------------------------------------------------------
# Node 6 – Enrich with Glassdoor
# ---------------------------------------------------------------------------

def enrich_glassdoor(state: DataEnrichmentState) -> dict[str, Any]:
    """Run the Glassdoor scraper for each portfolio company."""
    logger.info(
        "Running Glassdoor enrichment for {} companies",
        len(state.deduplicated_companies),
    )

    results: list[dict[str, Any]] = []
    for company in state.deduplicated_companies:
        name = company.get("company_name", "")
        if not name:
            continue
        try:
            data = asyncio.run(scrape_glassdoor_company(name))
            results.append(data)
        except Exception as exc:
            logger.warning("Glassdoor enrichment failed for {}: {}", name, exc)

    return {"glassdoor_results": results}


# ---------------------------------------------------------------------------
# Node 7 – Write enriched data back to storage
# ---------------------------------------------------------------------------

def write_enriched_data(state: DataEnrichmentState) -> dict[str, Any]:
    """Merge LinkedIn and Glassdoor results and write them to Airtable and Supabase."""
    logger.info("Writing enriched data to storage")

    # Build a merged record per company
    linkedin_by_name = {
        r.get("company_name", "").lower(): r for r in state.linkedin_results
    }
    glassdoor_by_name = {
        r.get("company_name", "").lower(): r for r in state.glassdoor_results
    }

    merged: list[dict[str, Any]] = []
    for company in state.deduplicated_companies:
        name = company.get("company_name", "")
        key = name.lower()
        record: dict[str, Any] = {"company_name": name}

        li = linkedin_by_name.get(key, {})
        gd = glassdoor_by_name.get(key, {})

        record.update(
            {
                "employee_count": li.get("employee_count"),
                "industry": li.get("industry"),
                "headquarters": li.get("headquarters"),
                "website": li.get("website") or company.get("website"),
                "linkedin_url": li.get("linkedin_url") or company.get("linkedin_url"),
                "is_hiring": li.get("is_hiring"),
                "glassdoor_rating": gd.get("glassdoor_rating"),
                "glassdoor_review_count": gd.get("review_count"),
                "culture_rating": gd.get("culture_rating"),
                "work_life_balance_rating": gd.get("work_life_balance_rating"),
                "compensation_rating": gd.get("compensation_rating"),
            }
        )
        merged.append(record)

    airtable_count = 0
    supabase_count = 0

    try:
        airtable_count = airtable_upsert(
            "Companies", merged, key_fields=["company_name"]
        )
    except Exception as exc:
        logger.warning("Airtable enrichment write skipped: {}", exc)

    try:
        supabase_count = supabase_upsert("enriched_companies", merged)
    except Exception as exc:
        logger.warning("Supabase enrichment write skipped: {}", exc)

    return {
        "airtable_records_updated": state.airtable_records_updated + airtable_count,
        "supabase_records_upserted": state.supabase_records_upserted + supabase_count,
    }
