"""Company matching service for enrichment workflow.

Provides matching logic to identify discovered companies against existing CRM records.
Handles company deduplication and ID assignment.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from lib.integrations.supabase.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


@dataclass
class MatchedCompanyResult:
    """Result of company matching operation.

    Attributes:
        new_ids: List of IDs for newly created company records.
        existing_ids: List of IDs for matched existing company records.
    """

    new_ids: list[str]
    existing_ids: list[str]


def _companies_table(client):
    if hasattr(client, "_get_table"):
        return client._get_table("companies")
    return client.table("companies")


def _normalize_url(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    normalized = url.strip()
    if not normalized:
        return None
    normalized = normalized.rstrip("/")
    return normalized


async def _select_single_id(query) -> Optional[str]:
    try:
        if hasattr(query, "execute"):
            result = await query.execute()
        elif hasattr(query, "single"):
            result = await query.single()
        else:
            return None

        if hasattr(result, "data") and result.data:
            if isinstance(result.data, list) and result.data and isinstance(result.data[0], dict):
                return result.data[0].get("id")
            if isinstance(result.data, dict):
                return result.data.get("id")

        if isinstance(result, dict):
            return result.get("id")
        return None
    except Exception:
        return None


async def match_companies(
    discovered_companies: list[dict],
    run_id: str,
) -> MatchedCompanyResult:
    """Match discovered companies against existing CRM records.

    Attempts to match each discovered company by:
    1. Company name similarity
    2. LinkedIn URL matching
    3. Glassdoor URL matching

    Creates new records for unmatched companies and returns both
    new and existing company IDs.

    Args:
        discovered_companies: List of company dicts with keys:
            - company_name (str): Name of the company
            - linkedin_url (str, optional): LinkedIn profile URL
            - glassdoor_url (str, optional): Glassdoor profile URL
        run_id: Workflow run ID for logging and tracing.

    Returns:
        MatchedCompanyResult with new_ids and existing_ids lists.

    Raises:
        ValueError: If discovered_companies is empty or invalid.
        Exception: If Supabase connection fails.
    """
    if not discovered_companies:
        raise ValueError("discovered_companies list cannot be empty")

    client = await get_supabase_client()
    new_ids: list[str] = []
    existing_ids: list[str] = []

    logger.info(
        f"[CompanyMatcher {run_id}] Matching {len(discovered_companies)} companies",
        extra={"run_id": run_id, "count": len(discovered_companies)},
    )

    for idx, company_data in enumerate(discovered_companies):
        try:
            company_name: str = company_data.get("company_name", "").strip()
            linkedin_url: Optional[str] = company_data.get("linkedin_url")
            glassdoor_url: Optional[str] = company_data.get("glassdoor_url")

            if not company_name:
                logger.warning(
                    f"[CompanyMatcher {run_id}] Skipping company at index {idx}: missing company_name",
                    extra={"run_id": run_id, "index": idx},
                )
                continue

            logger.debug(
                f"[CompanyMatcher {run_id}] Processing company: {company_name}",
                extra={"run_id": run_id, "company_name": company_name},
            )

            # Try to find existing company by multiple matching strategies
            matched_id: Optional[str] = await _find_existing_company(
                client=client,
                company_name=company_name,
                linkedin_url=linkedin_url,
                glassdoor_url=glassdoor_url,
                run_id=run_id,
            )

            if matched_id:
                logger.info(
                    f"[CompanyMatcher {run_id}] Matched existing company: {company_name} → ID: {matched_id}",
                    extra={"run_id": run_id, "company_name": company_name, "id": matched_id},
                )
                existing_ids.append(matched_id)
            else:
                # Create new company record
                new_id: Optional[str] = await _create_new_company(
                    client=client,
                    company_name=company_name,
                    linkedin_url=linkedin_url,
                    glassdoor_url=glassdoor_url,
                    run_id=run_id,
                )

                if new_id:
                    logger.info(
                        f"[CompanyMatcher {run_id}] Created new company: {company_name} → ID: {new_id}",
                        extra={"run_id": run_id, "company_name": company_name, "id": new_id},
                    )
                    new_ids.append(new_id)
                else:
                    logger.warning(
                        f"[CompanyMatcher {run_id}] Failed to create company: {company_name}",
                        extra={"run_id": run_id, "company_name": company_name},
                    )

        except Exception as e:
            logger.error(
                f"[CompanyMatcher {run_id}] Error processing company at index {idx}: {str(e)}",
                extra={"run_id": run_id, "index": idx},
                exc_info=True,
            )
            continue

    logger.info(
        f"[CompanyMatcher {run_id}] Matching complete: {len(existing_ids)} existing, {len(new_ids)} new",
        extra={"run_id": run_id, "existing_count": len(existing_ids), "new_count": len(new_ids)},
    )

    return MatchedCompanyResult(new_ids=new_ids, existing_ids=existing_ids)


async def _find_existing_company(
    client,
    company_name: str,
    linkedin_url: Optional[str],
    glassdoor_url: Optional[str],
    run_id: str,
) -> Optional[str]:
    """Find existing company by name or URL matching.

    Implements multiple matching strategies with priority:
    1. Direct name match
    2. LinkedIn URL match
    3. Glassdoor URL match

    Args:
        client: Supabase client instance.
        company_name: Company name to search for.
        linkedin_url: LinkedIn profile URL (optional).
        glassdoor_url: Glassdoor profile URL (optional).
        run_id: Workflow run ID for logging.

    Returns:
        Company ID if found, None otherwise.
    """
    linkedin_url = _normalize_url(linkedin_url)
    glassdoor_url = _normalize_url(glassdoor_url)

    try:
        # Strategy 1: Match by exact name
        table = _companies_table(client)

        if hasattr(table, "ilike"):
            query = table.select("id").ilike("name", company_name).limit(1)
        else:
            query = table.select("id").eq("name", company_name).limit(1)

        found_id = await _select_single_id(query)
        if found_id:
            return found_id

    except Exception as e:
        logger.debug(
            f"[CompanyMatcher {run_id}] Name match failed for {company_name}: {str(e)}",
            extra={"run_id": run_id, "company_name": company_name},
        )

    # Strategy 2: Match by LinkedIn URL
    if linkedin_url:
        try:
            table = _companies_table(client)
            query = table.select("id").eq("linkedin_url", linkedin_url).limit(1)
            found_id = await _select_single_id(query)
            if found_id:
                return found_id

            linkedin_url_alt = linkedin_url.rstrip("/")
            if linkedin_url_alt and linkedin_url_alt != linkedin_url:
                query = table.select("id").eq("linkedin_url", linkedin_url_alt).limit(1)
                found_id = await _select_single_id(query)
                if found_id:
                    return found_id
        except Exception as e:
            logger.debug(
                f"[CompanyMatcher {run_id}] LinkedIn URL match failed: {str(e)}",
                extra={"run_id": run_id},
            )

    # Strategy 3: Match by Glassdoor URL
    if glassdoor_url:
        try:
            table = _companies_table(client)
            query = table.select("id").eq("glassdoor_url", glassdoor_url).limit(1)
            found_id = await _select_single_id(query)
            if found_id:
                return found_id

            glassdoor_url_alt = glassdoor_url.rstrip("/")
            if glassdoor_url_alt and glassdoor_url_alt != glassdoor_url:
                query = table.select("id").eq("glassdoor_url", glassdoor_url_alt).limit(1)
                found_id = await _select_single_id(query)
                if found_id:
                    return found_id
        except Exception as e:
            logger.debug(
                f"[CompanyMatcher {run_id}] Glassdoor URL match failed: {str(e)}",
                extra={"run_id": run_id},
            )

    return None


async def _create_new_company(
    client,
    company_name: str,
    linkedin_url: Optional[str],
    glassdoor_url: Optional[str],
    run_id: str,
) -> Optional[str]:
    """Create a new company record in the database.

    Args:
        client: Supabase client instance.
        company_name: Company name.
        linkedin_url: LinkedIn profile URL (optional).
        glassdoor_url: Glassdoor profile URL (optional).
        run_id: Workflow run ID for logging.

    Returns:
        ID of created company record, or None if creation failed.
    """
    try:
        linkedin_url = _normalize_url(linkedin_url)
        glassdoor_url = _normalize_url(glassdoor_url)

        company_data = {
            "name": company_name,
            "linkedin_url": linkedin_url,
            "glassdoor_url": glassdoor_url,
        }

        table = _companies_table(client)
        result = await table.insert(company_data).execute()

        if result and result.data:
            company_id = result.data[0].get("id")
            return company_id

        return None

    except Exception as e:
        logger.error(
            f"[CompanyMatcher {run_id}] Failed to create company {company_name}: {str(e)}",
            extra={"run_id": run_id, "company_name": company_name},
            exc_info=True,
        )
        return None
