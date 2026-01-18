"""Glassdoor company profile scraper using SeleniumBase.

Extracts ratings and review information from Glassdoor company profiles.
Handles:
  - Overall company rating
  - Benefits rating
  - Total number of reviews
  - Number of benefits reviews
  - Engaged Employer status
"""

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from seleniumbase import SB

logger = logging.getLogger(__name__)

# Configuration paths - cookies file in project root
COOKIES_FILE = Path(__file__).parent.parent.parent / "glassdoor_cookies.json"
TARGET_URL = "https://www.glassdoor.com/index.htm"


@dataclass
class GlassdoorProfileData:
    """Glassdoor company profile data extracted from company page."""

    overall_rating: float = 0.0
    benefits_rating: float = 0.0
    total_reviews: int = 0
    benefit_reviews: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary format for state."""
        data = asdict(self)
        data["overall_rating"] = data.get("overall_rating", 0.0)
        data["benefit_review"] = data.get("benefit_reviews", 0)
        return data


def _parse_compact_number_to_int(value: str) -> int:
    if not value:
        return 0
    text = value.strip()
    text = text.replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)\s*([KMB])?", text, re.IGNORECASE)
    if not m:
        return 0
    number = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    multiplier = 1
    if suffix == "K":
        multiplier = 1_000
    elif suffix == "M":
        multiplier = 1_000_000
    elif suffix == "B":
        multiplier = 1_000_000_000
    return int(number * multiplier)


def _extract_overall_rating(soup: BeautifulSoup) -> float:
    rating_elem = soup.select_one('p[class^="rating-headline-average_rating__"]')
    if rating_elem:
        m = re.search(r"(\d+(?:\.\d+)?)", rating_elem.get_text(strip=True))
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return 0.0

    rating_elem = soup.select_one('span[class^="employer-overview_employerOverviewRating__"]')
    if rating_elem:
        m = re.search(r"(\d+(?:\.\d+)?)", rating_elem.get_text(strip=True))
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return 0.0

    return 0.0


def _extract_tab_count(soup: BeautifulSoup, tab_name_pattern: str) -> int:
    tab_header_elems = soup.select('span[class^="tab-content_HeaderWrapper__"]')
    if not tab_header_elems:
        return 0

    tab_re = re.compile(tab_name_pattern, re.IGNORECASE)
    for span in tab_header_elems:
        context_parts = []
        current = span.parent
        for _ in range(3):
            if not current:
                break
            try:
                context_parts.append(current.get_text(" ", strip=True))
            except Exception:
                pass
            current = getattr(current, "parent", None)
        context_text = " ".join(p for p in context_parts if p)

        if tab_re.search(context_text):
            return _parse_compact_number_to_int(span.get_text(strip=True))

    return 0


def _filter_domain(url: str) -> str:
    """Extract domain from URL, remove www."""
    try:
        domain = urlparse(url).netloc.replace("www.", "")
        return domain
    except Exception:
        return ""


def _extract_glassdoor_id(gdurl: str) -> str:
    """Extract Glassdoor ID from URL (EI_IE format)."""
    if "EI_IE" in gdurl:
        try:
            gd_id = gdurl.split("EI_IE")[1].split(".")[0]
            return gd_id
        except Exception:
            return ""
    return ""


def _load_and_apply_cookies(sb: SB) -> bool:
    """Load cookies from file and apply to browser."""
    if not COOKIES_FILE.exists():
        logger.debug("Cookies file not found")
        return False

    # Navigate to domain first before adding cookies
    sb.uc_open_with_reconnect(TARGET_URL, reconnect_time=2)
    time.sleep(2)

    try:
        with open(COOKIES_FILE, "r") as f:
            cookies = json.load(f)
        for cookie in cookies:
            try:
                sb.driver.add_cookie(cookie)
            except Exception:
                pass
        logger.debug("Cookies loaded and applied")
        return True
    except Exception as e:
        logger.warning(f"Failed to load cookies: {str(e)}")
        return False


def _check_if_logged_in(sb: SB) -> bool:
    """Check if user is logged in by looking for login form absence."""
    try:
        # Check if login form is NOT visible (means logged in)
        login_form = sb.is_element_visible("#inlineUserEmail", timeout=3)
        return not login_form
    except Exception:
        # If element not found, assume logged in
        return True


def _scrape_glassdoor_data_sync(glassdoor_url: str, run_id: str) -> dict:
    """Synchronous function to scrape Glassdoor data using SeleniumBase."""
    try:
        with SB(uc=True, incognito=True, test=True) as sb:
            # Try cookie-based login first
            cookies_loaded = _load_and_apply_cookies(sb)

            if cookies_loaded:
                # Reload page after adding cookies
                sb.uc_open_with_reconnect(TARGET_URL, reconnect_time=3)
                try:
                    sb.uc_gui_handle_captcha()
                except Exception:
                    pass
                time.sleep(3)

                is_logged_in = _check_if_logged_in(sb)
                if not is_logged_in:
                    logger.warning(
                        f"[Glassdoor {run_id}] Cookie login failed, continuing without authentication"
                    )

            # Navigate to Glassdoor profile
            logger.debug(
                f"[Glassdoor {run_id}] Navigating to URL: {glassdoor_url}",
                extra={"run_id": run_id},
            )
            sb.uc_open_with_reconnect(glassdoor_url, reconnect_time=3)
            try:
                sb.uc_gui_handle_captcha()
            except Exception:
                pass

            time.sleep(5)
            page_source = sb.get_page_source()
            soup = BeautifulSoup(page_source, "lxml")

            overall_rating = _extract_overall_rating(soup)

            # Extract benefits rating (try to find it on the page)
            benefits_rating = 0.0
            try:
                # Look for benefits rating in the page
                benefits_elem = soup.select_one('[class*="BenefitsRating"], [class*="benefits-rating"]')
                if benefits_elem:
                    benefits_text = benefits_elem.get_text(strip=True)
                    match = re.search(r"(\d+\.?\d*)", benefits_text)
                    if match:
                        benefits_rating = float(match.group(1))
            except Exception:
                pass

            # Navigate to Reviews page to get review count
            total_reviews_num = _extract_tab_count(soup, r"\breviews\b")

            benefit_reviews = _extract_tab_count(soup, r"\bbenefits\b")

            result = GlassdoorProfileData(
                overall_rating=overall_rating,
                benefits_rating=benefits_rating,
                total_reviews=total_reviews_num,
                benefit_reviews=benefit_reviews,
            )

            logger.info(
                f"[Glassdoor {run_id}] Profile fetch completed successfully",
                extra={
                    "run_id": run_id,
                    "overall_rating": overall_rating,
                    "benefits_rating": benefits_rating,
                    "total_reviews": total_reviews_num,
                    "benefit_reviews": benefit_reviews,
                },
            )

            return result.to_dict()

    except Exception as e:
        logger.error(
            f"[Glassdoor {run_id}] Error during profile scraping: {str(e)}",
            extra={"run_id": run_id, "url": glassdoor_url},
            exc_info=True,
        )
        return GlassdoorProfileData().to_dict()


async def fetch_glassdoor_profile(
    glassdoor_url: str,
    run_id: str = "unknown",
) -> dict:
    """Fetch and scrape Glassdoor company profile.

    Extracts company ratings and review counts from Glassdoor using SeleniumBase.

    Args:
        glassdoor_url: Glassdoor company profile URL
        run_id: Workflow run ID for logging and tracing

    Returns:
        Dictionary with structure:
        {
            "overall_rating": float,
            "benefits_rating": float,
            "total_reviews": int,
            "benefit_reviews": int
        }

    Raises:
        ValueError: If glassdoor_url is invalid or empty
    """
    if not glassdoor_url or not glassdoor_url.strip():
        logger.warning(
            f"[Glassdoor {run_id}] Empty glassdoor_url provided",
            extra={"run_id": run_id},
        )
        return GlassdoorProfileData().to_dict()

    run_id = run_id or "unknown"
    logger.info(
        f"[Glassdoor {run_id}] Starting Glassdoor profile fetch",
        extra={"run_id": run_id, "url": glassdoor_url},
    )

    try:
        # Run the synchronous SeleniumBase code in a thread pool
        # to maintain async compatibility
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, _scrape_glassdoor_data_sync, glassdoor_url, run_id
        )
        return result
    except Exception as e:
        logger.error(
            f"[Glassdoor {run_id}] Unexpected error during Glassdoor fetch: {str(e)}",
            extra={"run_id": run_id, "url": glassdoor_url},
            exc_info=True,
        )
        return GlassdoorProfileData().to_dict()
