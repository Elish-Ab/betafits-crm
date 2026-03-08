"""Company matching node for LangGraph enrichment workflow.

Matches discovered companies with existing CRM records and assigns IDs.
Input: Discovered companies from Crunchbase with metadata.
Output: Matched existing company IDs and newly created company IDs.
"""

import logging
from typing import Optional

from workflows.langgraph.enrichment_flow.state import EnrichmentState
from services.enrichment.company_matcher import (
    match_companies,
    MatchedCompanyResult,
)

logger = logging.getLogger(__name__)


async def company_matcher_node(state: EnrichmentState) -> EnrichmentState:
    """Match discovered companies with CRM records.

    This node takes the list of discovered companies from the Crunchbase
    discovery stage and matches them against existing CRM records. It returns
    IDs for both newly created and existing company matches.

    Args:
        state: Current enrichment workflow state containing discovered companies.

    Returns:
        Updated state with new_ids and existing_ids populated in matched_company_id field.

    Raises:
        ValueError: If required input data is missing.
        Exception: If matching service fails, error is logged and tracked.
    """
    try:
        # Extract discovered companies from state
        crunchbase_data: dict = state.get("crunchbase_data", {})
        run_id: str = state.get("run_id", "unknown")

        # Validate required inputs
        discovered_companies: list[dict] = crunchbase_data.get(
            "discovered_companies", []
        )
        if not discovered_companies:
            logger.warning(
                f"[Enrichment {run_id}] No discovered companies to match",
                extra={"run_id": run_id},
            )
            state["matched_company_id"] = None
            return state

        logger.info(
            f"[Enrichment {run_id}] Starting company matching for {len(discovered_companies)} discovered companies",
            extra={
                "run_id": run_id,
                "discovered_count": len(discovered_companies),
            },
        )

        # Perform matching
        match_result: MatchedCompanyResult = await match_companies(
            discovered_companies=discovered_companies,
            run_id=run_id,
        )

        # Store result in state
        state["matched_company_id"] = {
            "new_ids": match_result.new_ids,
            "existing_ids": match_result.existing_ids,
        }

        logger.info(
            f"[Enrichment {run_id}] Company matching completed",
            extra={
                "run_id": run_id,
                "new_ids_count": len(match_result.new_ids),
                "existing_ids_count": len(match_result.existing_ids),
                "new_ids": match_result.new_ids,
                "existing_ids": match_result.existing_ids,
            },
        )

        return state

    except ValueError as e:
        # Validation error - expected input issue
        error_msg = f"Validation error in company matcher: {str(e)}"
        logger.warning(error_msg, exc_info=True)
        state["errors"] = state.get("errors", []) + [error_msg]
        return state

    except Exception as e:
        # Unexpected error - log and track
        error_msg = f"Unexpected error in company matcher: {str(e)}"
        logger.error(error_msg, exc_info=True)
        state["errors"] = state.get("errors", []) + [error_msg]
        state["warnings"] = state.get("warnings", []) + [
            "Company matching failed, proceeding with enrichment"
        ]
        return state
