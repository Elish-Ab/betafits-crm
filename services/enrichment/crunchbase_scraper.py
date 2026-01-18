"""Crunchbase scraper service for discovering VC portfolio companies.

Fetches portfolio companies from a VC firm's Crunchbase profile or website
and extracts relevant company information including LinkedIn and Glassdoor URLs.
"""

import logging
import re
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredCompany:
    """A discovered company from Crunchbase or web scraping.

    Attributes:
        company_name: Official name of the company.
        linkedin_url: LinkedIn company profile URL (optional).
        glassdoor_url: Glassdoor company profile URL (optional).
    """

    company_name: str
    linkedin_url: Optional[str] = None
    glassdoor_url: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary format for state."""
        return {
            "company_name": self.company_name,
            "linkedin_url": self.linkedin_url,
            "glassdoor_url": self.glassdoor_url,
        }


@dataclass
class CrunchbaseDiscoveryResult:
    """Result of Crunchbase discovery operation.

    Attributes:
        discovered_companies: List of discovered companies.
        total_count: Total number of companies discovered.
        source: Source of discovery (api, scrape, etc.).
    """

    discovered_companies: list[DiscoveredCompany]
    total_count: int
    source: str


async def fetch_crunchbase_profile(
    company_domain: str,
    run_id: str = "unknown",
) -> dict:
    """Discover portfolio companies from a VC firm's Crunchbase profile.

    This function attempts to fetch the Crunchbase profile of a VC firm
    identified by their company domain, and extracts all portfolio companies
    with their LinkedIn and Glassdoor URLs.

    The discovery process:
    1. Tries Crunchbase API (if configured)
    2. Falls back to web scraping if API unavailable
    3. Extracts LinkedIn and Glassdoor URLs for each company
    4. Deduplicates results

    Args:
        company_domain: Company domain (e.g., "sequoia.com").
        run_id: Workflow run ID for logging and tracing.

    Returns:
        Dictionary with structure:
        {
            "discovered_companies": [
                {
                    "company_name": str,
                    "linkedin_url": Optional[str],
                    "glassdoor_url": Optional[str]
                },
                ...
            ]
        }

    Raises:
        ValueError: If company_domain is invalid or empty.
    """
    if not company_domain or not company_domain.strip():
        raise ValueError("company_domain cannot be empty")

    run_id = run_id or "unknown"
    logger.info(
        f"[CrunchbaseDiscovery {run_id}] Starting discovery for domain: {company_domain}",
        extra={"run_id": run_id, "domain": company_domain},
    )

    try:
        # Try API first
        result = await _discover_via_crunchbase_api(
            company_domain=company_domain,
            run_id=run_id,
        )

        if result and result.discovered_companies:
            logger.info(
                f"[CrunchbaseDiscovery {run_id}] Discovered {len(result.discovered_companies)} companies via API",
                extra={
                    "run_id": run_id,
                    "count": len(result.discovered_companies),
                    "source": "crunchbase_api",
                },
            )
            return _format_discovery_output(result.discovered_companies)

        # Fallback to web scraping
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] API returned no results, trying web scrape",
            extra={"run_id": run_id},
        )
        result = await _discover_via_web_scrape(
            company_domain=company_domain,
            run_id=run_id,
        )

        if result and result.discovered_companies:
            logger.info(
                f"[CrunchbaseDiscovery {run_id}] Discovered {len(result.discovered_companies)} companies via web scrape",
                extra={
                    "run_id": run_id,
                    "count": len(result.discovered_companies),
                    "source": "web_scrape",
                },
            )
            return _format_discovery_output(result.discovered_companies)

        # No companies found
        logger.warning(
            f"[CrunchbaseDiscovery {run_id}] No companies discovered for domain: {company_domain}",
            extra={"run_id": run_id, "domain": company_domain},
        )
        return _format_discovery_output([])

    except Exception as e:
        logger.error(
            f"[CrunchbaseDiscovery {run_id}] Unexpected error during discovery: {str(e)}",
            extra={"run_id": run_id, "domain": company_domain},
            exc_info=True,
        )
        # Return empty result on error to allow workflow to continue
        return _format_discovery_output([])


async def _discover_via_crunchbase_api(
    company_domain: str,
    run_id: str,
) -> Optional[CrunchbaseDiscoveryResult]:
    """Discover portfolio companies via Crunchbase API.

    Attempts to use Crunchbase API if configured. Falls back gracefully
    if API is unavailable or not configured.

    Args:
        company_domain: Company domain to look up.
        run_id: Workflow run ID for logging.

    Returns:
        CrunchbaseDiscoveryResult if successful, None if unavailable.
    """
    try:
        # Check if Crunchbase API is configured
        from lib.config import get_settings

        settings = get_settings()
        crunchbase_api_key = getattr(settings, "crunchbase_api_key", None)

        if not crunchbase_api_key:
            logger.debug(
                f"[CrunchbaseDiscovery {run_id}] Crunchbase API not configured",
                extra={"run_id": run_id},
            )
            return None

        # TODO: Implement actual Crunchbase API call
        # This is a placeholder for the actual API implementation
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] Crunchbase API implementation pending",
            extra={"run_id": run_id},
        )
        return None

    except Exception as e:
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] Crunchbase API failed: {str(e)}",
            extra={"run_id": run_id},
        )
        return None


async def _discover_via_web_scrape(
    company_domain: str,
    run_id: str,
) -> Optional[CrunchbaseDiscoveryResult]:
    """Discover portfolio companies via web scraping.

    Attempts to scrape Crunchbase website for the company's portfolio.
    Uses Playwright for handling dynamic content and blocking.

    Args:
        company_domain: Company domain to scrape.
        run_id: Workflow run ID for logging.

    Returns:
        CrunchbaseDiscoveryResult if successful, None otherwise.
    """
    try:
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] Starting web scrape for domain: {company_domain}",
            extra={"run_id": run_id, "domain": company_domain},
        )

        # TODO: Implement actual Playwright-based scraping
        # This is a placeholder for the actual web scraping implementation
        # In real implementation, this would:
        # 1. Use Playwright to navigate Crunchbase
        # 2. Search for the company
        # 3. Extract portfolio company list
        # 4. For each company, fetch LinkedIn and Glassdoor URLs

        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] Web scrape implementation pending",
            extra={"run_id": run_id},
        )

        # Return mock data for development
        return CrunchbaseDiscoveryResult(
            discovered_companies=[],
            total_count=0,
            source="web_scrape",
        )

    except Exception as e:
        logger.error(
            f"[CrunchbaseDiscovery {run_id}] Web scrape failed: {str(e)}",
            extra={"run_id": run_id},
            exc_info=True,
        )
        return None


async def _extract_linkedin_url(
    company_name: str,
    run_id: str,
) -> Optional[str]:
    """Extract LinkedIn URL for a company.

    Searches for a company's LinkedIn profile URL.

    Args:
        company_name: Name of the company.
        run_id: Workflow run ID for logging.

    Returns:
        LinkedIn URL if found, None otherwise.
    """
    try:
        # TODO: Implement LinkedIn URL extraction
        # Could use:
        # 1. LinkedIn API (if available)
        # 2. Google search
        # 3. Web scraping
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] LinkedIn URL extraction for: {company_name}",
            extra={"run_id": run_id, "company_name": company_name},
        )
        return None

    except Exception as e:
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] Failed to extract LinkedIn URL: {str(e)}",
            extra={"run_id": run_id, "company_name": company_name},
        )
        return None


async def _extract_glassdoor_url(
    company_name: str,
    run_id: str,
) -> Optional[str]:
    """Extract Glassdoor URL for a company.

    Searches for a company's Glassdoor profile URL.

    Args:
        company_name: Name of the company.
        run_id: Workflow run ID for logging.

    Returns:
        Glassdoor URL if found, None otherwise.
    """
    try:
        # TODO: Implement Glassdoor URL extraction
        # Could use:
        # 1. Glassdoor API/search
        # 2. Google search
        # 3. Web scraping
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] Glassdoor URL extraction for: {company_name}",
            extra={"run_id": run_id, "company_name": company_name},
        )
        return None

    except Exception as e:
        logger.debug(
            f"[CrunchbaseDiscovery {run_id}] Failed to extract Glassdoor URL: {str(e)}",
            extra={"run_id": run_id, "company_name": company_name},
        )
        return None


def _format_discovery_output(
    discovered_companies: list[DiscoveredCompany],
) -> dict:
    """Format discovery result for state output.

    Args:
        discovered_companies: List of discovered companies.

    Returns:
        Dictionary in the expected format for EnrichmentState.
    """
    return {
        "discovered_companies": [
            company.to_dict() for company in discovered_companies
        ]
    }


def _deduplicate_companies(
    companies: list[DiscoveredCompany],
) -> list[DiscoveredCompany]:
    """Remove duplicate companies from the list.

    Deduplicates based on company name (case-insensitive).

    Args:
        companies: List of discovered companies.

    Returns:
        Deduplicated list of companies.
    """
    seen = set()
    deduplicated = []

    for company in companies:
        company_name_lower = company.company_name.lower().strip()

        if company_name_lower not in seen:
            seen.add(company_name_lower)
            deduplicated.append(company)
        else:
            logger.debug(
                f"Skipping duplicate company: {company.company_name}"
            )

    return deduplicated
