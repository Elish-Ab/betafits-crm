"""Top-level CRM LangGraph orchestrator.

This module wires the four domain graphs together into a single entry point.
The orchestrator decides which domain graph(s) to invoke based on the trigger
type supplied at runtime.

Trigger types
-------------
- ``email``            → Email Processing → CRM Brain
- ``new_vc_partner``   → Data Enrichment (Crunchbase + LinkedIn + Glassdoor)
- ``quarterly_refresh``→ Data Enrichment (LinkedIn + Glassdoor only)
- ``5500_quarterly``   → Prospect Universe (Form 5500 pipeline)
- ``manual_enrich``    → Data Enrichment with a pre-populated company list
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from core.logging_config import configure_logging
from core.state import (
    CRMBrainState,
    DataEnrichmentState,
    EmailState,
    ProspectUniverseState,
)
from domains.crm_brain import crm_brain_graph
from domains.data_enrichment import data_enrichment_graph
from domains.email_processing import email_processing_graph
from domains.prospect_universe import prospect_universe_graph

configure_logging()


def run_email_pipeline(
    raw_email: str = "",
    email_subject: str = "",
    email_from: str = "",
    email_date: str = "",
    opportunity_id: str = "",
) -> dict[str, Any]:
    """Run the full email → CRM Brain pipeline.

    If ``raw_email`` is provided the email ingestion node is bypassed and the
    supplied content is used directly.
    """
    logger.info("Starting email pipeline")

    # --- Email Processing ---
    email_input = EmailState(
        raw_email=raw_email,
        email_subject=email_subject,
        email_from=email_from,
        email_date=email_date,
        opportunity_id=opportunity_id,
    )
    email_result = email_processing_graph.invoke(email_input.model_dump())
    email_state = EmailState(**email_result)

    logger.info(
        "Email processing complete: opportunity_id={}, sentiment={}",
        email_state.opportunity_id,
        email_state.sentiment,
    )

    # --- CRM Brain ---
    brain_input = CRMBrainState(
        opportunity_id=email_state.opportunity_id,
        interaction_type=email_state.interaction_type,
        interaction_summary=str(email_state.extracted_entities),
        metadata={"sentiment": email_state.sentiment},
    )
    brain_result = crm_brain_graph.invoke(brain_input.model_dump())
    brain_state = CRMBrainState(**brain_result)

    logger.info(
        "CRM Brain complete: stage {} → {}, status={}",
        brain_state.pipeline_stage_before,
        brain_state.pipeline_stage_after,
        brain_state.opportunity_status,
    )

    return {
        "email": email_state.model_dump(),
        "crm_brain": brain_state.model_dump(),
    }


def run_data_enrichment(
    trigger: str = "quarterly_refresh",
    vc_partner_id: str = "",
    crunchbase_url: str = "",
    companies: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run the Data Enrichment pipeline.

    Parameters
    ----------
    trigger:
        One of ``"new_vc_partner"``, ``"quarterly_refresh"``, or
        ``"manual_enrich"``.
    vc_partner_id:
        Airtable record ID of the VC partner (required for ``new_vc_partner``).
    crunchbase_url:
        Crunchbase investor page URL (required for ``new_vc_partner``).
    companies:
        Pre-populated list of company dicts for ``manual_enrich`` or
        ``quarterly_refresh`` triggers.
    """
    logger.info("Starting data enrichment pipeline (trigger={})", trigger)

    enrichment_input = DataEnrichmentState(
        trigger=trigger,
        vc_partner_id=vc_partner_id,
        crunchbase_url=crunchbase_url,
        deduplicated_companies=companies or [],
    )
    result = data_enrichment_graph.invoke(enrichment_input.model_dump())
    enrichment_state = DataEnrichmentState(**result)

    logger.info(
        "Data enrichment complete: {} Airtable records updated, {} Supabase records upserted",
        enrichment_state.airtable_records_updated,
        enrichment_state.supabase_records_upserted,
    )
    return enrichment_state.model_dump()


def run_prospect_universe(
    filing_year: int = 0,
    filing_quarter: str = "",
    source_files: list[str] | None = None,
) -> dict[str, Any]:
    """Run the Prospect Universe (Form 5500) pipeline.

    Parameters
    ----------
    filing_year:
        The year of the Form 5500 filing data (e.g. 2024).
    filing_quarter:
        The quarter label (e.g. ``"Q1"``).
    source_files:
        Optional list of local CSV file paths.  If omitted, the node will
        attempt to download the latest release from the DOL website.
    """
    logger.info(
        "Starting Prospect Universe pipeline (year={}, quarter={})",
        filing_year,
        filing_quarter,
    )

    pu_input = ProspectUniverseState(
        filing_year=filing_year,
        filing_quarter=filing_quarter,
        source_files=source_files or [],
    )
    result = prospect_universe_graph.invoke(pu_input.model_dump())
    pu_state = ProspectUniverseState(**result)

    logger.info(
        "Prospect Universe complete: {} raw records, {} targets, {} new prospects, {} reports",
        pu_state.raw_records_loaded,
        len(pu_state.target_company_ids),
        pu_state.new_prospects_added,
        len(pu_state.report_paths),
    )
    return pu_state.model_dump()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="CRM LangGraph Orchestrator")
    sub = parser.add_subparsers(dest="command")

    # email sub-command
    email_cmd = sub.add_parser("email", help="Run the email pipeline")
    email_cmd.add_argument("--subject", default="")
    email_cmd.add_argument("--from-addr", default="")
    email_cmd.add_argument("--body", default="")
    email_cmd.add_argument("--opportunity-id", default="")

    # enrich sub-command
    enrich_cmd = sub.add_parser("enrich", help="Run the data enrichment pipeline")
    enrich_cmd.add_argument(
        "--trigger",
        choices=["new_vc_partner", "quarterly_refresh", "manual_enrich"],
        default="quarterly_refresh",
    )
    enrich_cmd.add_argument("--vc-partner-id", default="")
    enrich_cmd.add_argument("--crunchbase-url", default="")

    # prospect sub-command
    prospect_cmd = sub.add_parser("prospect", help="Run the Prospect Universe pipeline")
    prospect_cmd.add_argument("--year", type=int, default=0)
    prospect_cmd.add_argument("--quarter", default="")
    prospect_cmd.add_argument("--files", nargs="*", default=[])

    args = parser.parse_args()

    if args.command == "email":
        output = run_email_pipeline(
            raw_email=args.body,
            email_subject=args.subject,
            email_from=args.from_addr,
            opportunity_id=args.opportunity_id,
        )
    elif args.command == "enrich":
        output = run_data_enrichment(
            trigger=args.trigger,
            vc_partner_id=args.vc_partner_id,
            crunchbase_url=args.crunchbase_url,
        )
    elif args.command == "prospect":
        output = run_prospect_universe(
            filing_year=args.year,
            filing_quarter=args.quarter,
            source_files=args.files,
        )
    else:
        parser.print_help()
        raise SystemExit(0)

    print(json.dumps(output, indent=2, default=str))
