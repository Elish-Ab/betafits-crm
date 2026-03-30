"""Data Enrichment domain – Playwright-based web scrapers.

Scrapers are implemented as plain async functions that accept a URL (or
identifier) and return a dictionary of enriched fields.  The LangGraph nodes
call these functions and merge the results into the domain state.

Anti-detection measures:
  - Configurable proxy support
  - Randomised delays between requests
  - Stealth browser context (no automation flags)
  - LinkedIn uses a session cookie rather than username/password
"""

from __future__ import annotations

import asyncio
import random
import re
from typing import Any

from loguru import logger

from config.settings import get_settings


# ---------------------------------------------------------------------------
# Browser context factory
# ---------------------------------------------------------------------------

async def _new_browser_context(playwright_instance, headless: bool = True):
    """Return a stealth Playwright browser context with optional proxy."""
    settings = get_settings()
    launch_kwargs: dict[str, Any] = {"headless": headless}
    if settings.proxy_url:
        launch_kwargs["proxy"] = {"server": settings.proxy_url}

    browser = await playwright_instance.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
        locale="en-US",
        timezone_id="America/New_York",
    )
    # Mask automation signals
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return browser, context


async def _random_delay(min_s: float = 1.5, max_s: float = 4.0) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


# ---------------------------------------------------------------------------
# LinkedIn scraper
# ---------------------------------------------------------------------------

async def scrape_linkedin_company(linkedin_url: str) -> dict[str, Any]:
    """Scrape company data from a LinkedIn company page.

    Uses a session cookie (``LINKEDIN_COOKIE`` env var) to authenticate
    without triggering login-wall detection.
    """
    from playwright.async_api import async_playwright

    settings = get_settings()
    logger.info("Scraping LinkedIn: {}", linkedin_url)

    result: dict[str, Any] = {
        "linkedin_url": linkedin_url,
        "company_name": None,
        "employee_count": None,
        "industry": None,
        "headquarters": None,
        "description": None,
        "website": None,
        "recent_posts_count": 0,
        "is_hiring": False,
        "scrape_error": None,
    }

    try:
        async with async_playwright() as p:
            browser, context = await _new_browser_context(p)

            # Inject LinkedIn session cookie
            if settings.linkedin_cookie:
                await context.add_cookies(
                    [
                        {
                            "name": "li_at",
                            "value": settings.linkedin_cookie,
                            "domain": ".linkedin.com",
                            "path": "/",
                        }
                    ]
                )

            page = await context.new_page()
            await page.goto(linkedin_url, wait_until="domcontentloaded", timeout=30_000)
            await _random_delay()

            # Company name
            try:
                result["company_name"] = await page.locator(
                    "h1.org-top-card-summary__title"
                ).inner_text(timeout=5_000)
            except Exception:
                pass

            # Employee count
            try:
                emp_text = await page.locator(
                    "a[data-control-name='topcard_see_all_employees']"
                ).inner_text(timeout=5_000)
                match = re.search(r"([\d,]+)", emp_text)
                if match:
                    result["employee_count"] = int(match.group(1).replace(",", ""))
            except Exception:
                pass

            # Industry
            try:
                result["industry"] = await page.locator(
                    "dd.org-about-company-module__company-industry"
                ).inner_text(timeout=5_000)
            except Exception:
                pass

            # Headquarters
            try:
                result["headquarters"] = await page.locator(
                    "dd.org-about-company-module__headquarters"
                ).inner_text(timeout=5_000)
            except Exception:
                pass

            # Website
            try:
                result["website"] = await page.locator(
                    "a.org-about-us-company-module__website"
                ).get_attribute("href", timeout=5_000)
            except Exception:
                pass

            # Hiring signal
            try:
                jobs_text = await page.locator(
                    "a[data-control-name='topcard_see_all_jobs']"
                ).inner_text(timeout=5_000)
                result["is_hiring"] = bool(jobs_text)
            except Exception:
                pass

            await browser.close()

    except Exception as exc:
        logger.error("LinkedIn scrape failed for {}: {}", linkedin_url, exc)
        result["scrape_error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Glassdoor scraper
# ---------------------------------------------------------------------------

async def scrape_glassdoor_company(company_name: str) -> dict[str, Any]:
    """Scrape company ratings and reviews from Glassdoor.

    Searches for the company by name and extracts the top-level rating data.
    """
    from playwright.async_api import async_playwright

    logger.info("Scraping Glassdoor for: {}", company_name)

    result: dict[str, Any] = {
        "company_name": company_name,
        "glassdoor_rating": None,
        "ceo_approval": None,
        "recommend_to_friend_pct": None,
        "review_count": None,
        "culture_rating": None,
        "work_life_balance_rating": None,
        "compensation_rating": None,
        "scrape_error": None,
    }

    search_url = (
        f"https://www.glassdoor.com/Search/results.htm"
        f"?keyword={company_name.replace(' ', '+')}&locT=N&locId=1"
    )

    try:
        async with async_playwright() as p:
            browser, context = await _new_browser_context(p)
            page = await context.new_page()
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
            await _random_delay()

            # Click first result
            try:
                first_result = page.locator("a.company-name").first
                await first_result.click(timeout=5_000)
                await page.wait_for_load_state("domcontentloaded")
                await _random_delay()
            except Exception:
                pass

            # Overall rating
            try:
                rating_text = await page.locator(
                    "div[data-test='rating-info'] span.ratingNumber"
                ).inner_text(timeout=5_000)
                result["glassdoor_rating"] = float(rating_text)
            except Exception:
                pass

            # Review count
            try:
                count_text = await page.locator(
                    "div[data-test='rating-info'] span.ratingLink"
                ).inner_text(timeout=5_000)
                match = re.search(r"([\d,]+)", count_text)
                if match:
                    result["review_count"] = int(match.group(1).replace(",", ""))
            except Exception:
                pass

            await browser.close()

    except Exception as exc:
        logger.error("Glassdoor scrape failed for {}: {}", company_name, exc)
        result["scrape_error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Crunchbase scraper
# ---------------------------------------------------------------------------

async def scrape_crunchbase_portfolio(crunchbase_url: str) -> list[dict[str, Any]]:
    """Scrape portfolio companies from a Crunchbase investor page.

    Returns a list of company dictionaries with basic fields.
    Requires a Crunchbase account (free or paid) to be logged in via cookies.
    """
    from playwright.async_api import async_playwright

    logger.info("Scraping Crunchbase portfolio: {}", crunchbase_url)
    companies: list[dict[str, Any]] = []

    try:
        async with async_playwright() as p:
            browser, context = await _new_browser_context(p)
            page = await context.new_page()

            # Navigate to the investments tab
            investments_url = crunchbase_url.rstrip("/") + "/investments"
            await page.goto(investments_url, wait_until="domcontentloaded", timeout=30_000)
            await _random_delay(2.0, 5.0)

            # Scroll to load all rows
            for _ in range(5):
                await page.keyboard.press("End")
                await _random_delay(1.0, 2.0)

            # Extract company rows
            rows = await page.locator("a.cb-overflow-ellipsis[href*='/organization/']").all()
            seen_hrefs: set[str] = set()

            for row in rows:
                try:
                    href = await row.get_attribute("href") or ""
                    name = (await row.inner_text()).strip()
                    if href and href not in seen_hrefs and name:
                        seen_hrefs.add(href)
                        companies.append(
                            {
                                "company_name": name,
                                "crunchbase_url": f"https://www.crunchbase.com{href}",
                                "website": None,
                                "linkedin_url": None,
                            }
                        )
                except Exception:
                    continue

            await browser.close()

    except Exception as exc:
        logger.error("Crunchbase scrape failed for {}: {}", crunchbase_url, exc)

    logger.info(
        "Crunchbase: found {} portfolio companies for {}", len(companies), crunchbase_url
    )
    return companies
