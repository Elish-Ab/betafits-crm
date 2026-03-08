# -*- coding: utf-8 -*-
"""
LinkedIn Company Profile Scraper using Playwright
Extracts employee counts by location and job openings
"""

import logging
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

logger = logging.getLogger(__name__)


def _is_playwright_target_closed_error(exc: Exception) -> bool:
    try:
        msg = str(exc).lower()
    except Exception:
        return False

    return (
        "target page, context or browser has been closed" in msg
        or "has been closed" in msg
        or "connection closed" in msg
    )


def _get_linkedin_storage_state_path(storage_state_path: Optional[str]) -> Optional[str]:
    if storage_state_path:
        return storage_state_path

    env_path = os.getenv("LINKEDIN_STORAGE_STATE_PATH")
    if env_path:
        return env_path

    home_dir = os.path.expanduser("~")
    if not home_dir or home_dir == "~":
        return None

    cache_dir = os.path.join(home_dir, ".cache", "betafits-crm")
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except Exception:
        return None

    return os.path.join(cache_dir, "linkedin_storage_state.json")


def _clear_linkedin_storage_state(storage_state_path: Optional[str], run_id: str) -> None:
    if not storage_state_path:
        return

    try:
        if os.path.exists(storage_state_path):
            os.remove(storage_state_path)
            logger.info(
                f"[LinkedIn] Cleared stored cookies/session at: {storage_state_path} (run_id: {run_id})"
            )
    except Exception as e:
        logger.warning(
            f"[LinkedIn] Failed to clear cookies/session file at {storage_state_path}: {str(e)} (run_id: {run_id})"
        )


async def _is_linkedin_logged_in(page: Page) -> bool:
    try:
        current_url = page.url.lower()
        if "login" in current_url or "signin" in current_url:
            return False

        nav = page.locator('[data-test-id="global-nav"]')
        return await nav.is_visible(timeout=2000)
    except Exception:
        return False


async def _wait_for_linkedin_login(page: Page, run_id: str, timeout_seconds: int = 180) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            if await _is_linkedin_logged_in(page):
                return True
        except Exception as e:
            if _is_playwright_target_closed_error(e):
                return False

        try:
            await page.wait_for_timeout(3000)
        except Exception as e:
            if _is_playwright_target_closed_error(e):
                return False

    logger.warning(
        f"[LinkedIn] Timed out waiting for LinkedIn login/verification to complete (run_id: {run_id})"
    )
    return False


async def _ensure_linkedin_session(
    context: BrowserContext,
    page: Page,
    run_id: str,
    linkedin_email: Optional[str],
    linkedin_password: Optional[str],
    storage_state_path: Optional[str],
    allow_manual_login: bool,
) -> bool:
    try:
        await page.goto("https://www.linkedin.com/feed/", wait_until="load", timeout=30000)
    except Exception:
        pass

    if await _is_linkedin_logged_in(page):
        return True

    if not (linkedin_email and linkedin_password):
        if not allow_manual_login:
            return False

        try:
            await page.goto("https://www.linkedin.com/login", wait_until="load", timeout=30000)
        except Exception:
            pass

        if not await _wait_for_linkedin_login(page, run_id=run_id, timeout_seconds=180):
            return False

        if storage_state_path:
            try:
                await context.storage_state(path=storage_state_path)
                logger.info(
                    f"[LinkedIn] Saved fresh cookies/session to: {storage_state_path} (run_id: {run_id})"
                )
            except Exception as e:
                logger.warning(
                    f"[LinkedIn] Logged in but failed to save cookies/session to {storage_state_path}: {str(e)} (run_id: {run_id})"
                )

        return True

    _ = await _linkedin_login(page, linkedin_email, linkedin_password, run_id)

    if not await _wait_for_linkedin_login(page, run_id=run_id, timeout_seconds=180):
        return False

    if storage_state_path:
        try:
            await context.storage_state(path=storage_state_path)
            logger.info(
                f"[LinkedIn] Saved fresh cookies/session to: {storage_state_path} (run_id: {run_id})"
            )
        except Exception as e:
            logger.warning(
                f"[LinkedIn] Logged in but failed to save cookies/session to {storage_state_path}: {str(e)} (run_id: {run_id})"
            )

    return True


@dataclass
class LinkedInProfileData:
    """LinkedIn company profile data extracted from company page"""
    total_employees: int = 0
    us_employees: int = 0
    hq_employees: int = 0
    other_countries: int = 0
    other_cities: int = 0
    open_jobs: int = 0


async def _linkedin_login(page: Page, email: str, password: str, run_id: str) -> bool:
    """
    Log in to LinkedIn using email and password.
    
    Args:
        page: Playwright page object
        email: LinkedIn email/username
        password: LinkedIn password
        run_id: Run ID for logging
    
    Returns:
        True if login successful, False otherwise
    """
    try:
        logger.info(f"[LinkedIn] Attempting login with email: {email[:20]}... (run_id: {run_id})")
        
        # Navigate to login page
        await page.goto("https://www.linkedin.com/login", wait_until="load", timeout=30000)
        await page.wait_for_timeout(1000)
        
        # Enter email
        try:
            await page.fill('input[name="session_key"]', email, timeout=5000)
        except Exception as e:
            logger.warning(f"[LinkedIn] Could not find email field: {str(e)}")
            return False
        
        # Enter password
        try:
            await page.fill('input[name="session_password"]', password, timeout=5000)
        except Exception as e:
            logger.warning(f"[LinkedIn] Could not find password field: {str(e)}")
            return False
        
        # Click login button
        try:
            login_button = page.locator('button[type="submit"]')
            await login_button.click(timeout=5000)
        except Exception as e:
            logger.warning(f"[LinkedIn] Could not click login button: {str(e)}")
            return False
        
        # Wait for navigation and check for common post-login elements
        await page.wait_for_timeout(3000)
        
        try:
            # Wait for feed or home page to load
            await page.wait_for_selector('[data-test-id="global-nav"]', timeout=10000)
        except Exception:
            pass
        
        # Check if we're still on login page
        current_url = page.url
        if "login" in current_url.lower() or "signin" in current_url.lower():
            logger.warning(f"[LinkedIn] Still on login page after submission - authentication may have failed")
            return False
        
        logger.info(f"[LinkedIn] Login successful (run_id: {run_id})")
        return True
    
    except Exception as e:
        logger.error(f"[LinkedIn] Login error: {str(e)} (run_id: {run_id})")
        return False


async def fetch_linkedin_profile(
    linkedin_url: str,
    run_id: str,
    linkedin_email: Optional[str] = None,
    linkedin_password: Optional[str] = None,
    storage_state_path: Optional[str] = None,
    refresh_cookies: bool = False,
) -> dict:
    """
    Fetch and scrape LinkedIn company profile.
    
    Args:
        linkedin_url: Full LinkedIn company URL (e.g., https://www.linkedin.com/company/anthropic/)
        run_id: Unique identifier for this enrichment run
        linkedin_email: Optional LinkedIn email for authentication (defaults to env var LINKEDIN_EMAIL)
        linkedin_password: Optional LinkedIn password (defaults to env var LINKEDIN_PASSWORD)
    
    Returns:
        Dictionary with extracted data:
        {
            "total_employees": int,
            "us_employees": int,
            "hq_employees": int,
            "other_countries": int,
            "other_cities": int,
            "open_jobs": int
        }
    
    Raises:
        ValueError: If URL is invalid
        Exception: If scraping fails
    """
    if not linkedin_url or not isinstance(linkedin_url, str):
        raise ValueError(f"Invalid LinkedIn URL: {linkedin_url}")
    
    if "linkedin.com/company/" not in linkedin_url.lower():
        raise ValueError(f"URL is not a LinkedIn company profile: {linkedin_url}")
    
    # Get credentials from arguments or environment variables
    if not linkedin_email:
        linkedin_email = os.getenv("LINKEDIN_EMAIL")
    if not linkedin_password:
        linkedin_password = os.getenv("LINKEDIN_PASSWORD")
    
    authenticated = linkedin_email and linkedin_password
    storage_state_path = _get_linkedin_storage_state_path(storage_state_path)

    if refresh_cookies and not storage_state_path:
        logger.warning(
            f"[LinkedIn] Cookie refresh requested but no storage_state_path is available; "
            f"fresh cookies will not be persisted (run_id: {run_id})"
        )
    elif storage_state_path:
        logger.info(
            f"[LinkedIn] storage_state_path resolved to: {storage_state_path} (run_id: {run_id})"
        )
    
    try:
        logger.info(f"[LinkedIn] Starting scrape for: {linkedin_url} (run_id: {run_id}, authenticated: {authenticated})")
        
        # Normalize URL
        linkedin_url = linkedin_url.rstrip("/")
        if not linkedin_url.startswith("http"):
            linkedin_url = "https://" + linkedin_url
        
        # Initialize profile data
        profile_data = LinkedInProfileData()
        
        # Scrape using Playwright
        async with async_playwright() as playwright:
            browser = None
            try:
                logger.info(f"[LinkedIn] Launching Chromium browser (headless=False for debugging)...")
                browser = await playwright.chromium.launch(
                    headless=False,  # Changed to False to see browser window
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--disable-gpu",
                        "--no-sandbox",  # May be needed in Docker/containerized environments
                    ]
                )
                logger.info(f"[LinkedIn] Browser launched successfully - you should see a window open")
            except Exception as e:
                logger.error(
                    f"[LinkedIn] Failed to launch Chromium browser: {str(e)}\n"
                    f"This usually means required system libraries are missing.\n"
                    f"On Linux, run: sudo apt-get install -y libnss3 libxss1 libasound2 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 (run_id: {run_id})"
                )
                raise
            
            if not browser:
                raise RuntimeError("Browser failed to initialize")
            
            try:
                # Create context with realistic user agent
                if refresh_cookies:
                    _clear_linkedin_storage_state(storage_state_path, run_id=run_id)

                context_kwargs = {
                    "user_agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "viewport": {"width": 1920, "height": 1080},
                }

                if storage_state_path and os.path.exists(storage_state_path) and not refresh_cookies:
                    context_kwargs["storage_state"] = storage_state_path
                    logger.info(
                        f"[LinkedIn] Reusing stored cookies/session from: {storage_state_path} (run_id: {run_id})"
                    )

                context = await browser.new_context(**context_kwargs)
                
                page = await context.new_page()
                
                # Set timeout
                page.set_default_timeout(45000)
                page.set_default_navigation_timeout(45000)
                
                # Authenticate if credentials provided
                if refresh_cookies:
                    session_ok = await _ensure_linkedin_session(
                        context=context,
                        page=page,
                        run_id=run_id,
                        linkedin_email=None,
                        linkedin_password=None,
                        storage_state_path=storage_state_path,
                        allow_manual_login=True,
                    )
                    if not session_ok:
                        logger.warning(
                            f"[LinkedIn] Manual cookie refresh/login did not complete - continuing without login (run_id: {run_id})"
                        )
                        authenticated = False
                    else:
                        authenticated = True
                elif authenticated:
                    session_ok = await _ensure_linkedin_session(
                        context=context,
                        page=page,
                        run_id=run_id,
                        linkedin_email=linkedin_email,
                        linkedin_password=linkedin_password,
                        storage_state_path=storage_state_path,
                        allow_manual_login=False,
                    )
                    if not session_ok:
                        logger.warning(
                            f"[LinkedIn] Authentication/session setup failed - continuing without login (run_id: {run_id})"
                        )
                        authenticated = False
                
                try:
                    # Navigate to company page
                    logger.info(f"[LinkedIn] Navigating to: {linkedin_url}")
                    print(f"\n>>> Opening LinkedIn: {linkedin_url}")
                    try:
                        # Use "load" instead of "networkidle" - LinkedIn keeps making requests
                        response = await page.goto(linkedin_url, wait_until="load", timeout=60000)
                        logger.info(f"[LinkedIn] Page loaded with status: {response.status if response else 'None'}")
                    except Exception as nav_error:
                        logger.warning(
                            f"[LinkedIn] Navigation timed out: {str(nav_error)}"
                        )
                        # Continue anyway - page may have loaded enough content
                        try:
                            await page.wait_for_timeout(5000)
                        except Exception as e:
                            if _is_playwright_target_closed_error(e):
                                logger.warning(
                                    f"[LinkedIn] Browser/page closed during navigation fallback wait (run_id: {run_id})"
                                )
                                return asdict(profile_data)
                        response = None
                    
                    if response and response.status == 404:
                        logger.error(f"[LinkedIn] Company page not found (404): {linkedin_url}")
                        return asdict(profile_data)
                    
                    # Wait for key content to appear with longer timeout
                    logger.info("[LinkedIn] Waiting for page content to load...")
                    try:
                        await page.wait_for_selector("[data-test-id='about-us']", timeout=20000)
                        logger.info("[LinkedIn] About section found")
                    except Exception as wait_error:
                        logger.warning(f"[LinkedIn] About section not found (may still have content): {str(wait_error)}")
                        # Page might not have this selector, but continue anyway
                        try:
                            await page.wait_for_timeout(5000)
                        except Exception as e:
                            if _is_playwright_target_closed_error(e):
                                logger.warning(
                                    f"[LinkedIn] Browser/page closed while waiting for content (run_id: {run_id})"
                                )
                                return asdict(profile_data)
                    
                    # Extra wait to ensure all content is loaded
                    try:
                        await page.wait_for_timeout(3000)
                    except Exception as e:
                        if _is_playwright_target_closed_error(e):
                            logger.warning(
                                f"[LinkedIn] Browser/page closed before extraction (run_id: {run_id})"
                            )
                            return asdict(profile_data)
                    logger.info("[LinkedIn] Page ready for extraction")
                    
                    # Navigate to /people page FIRST (this is where employee counts are)
                    logger.info(f"[LinkedIn] Navigating to /people page for employee data extraction...")
                    people_url = linkedin_url.rstrip("/") + "/people"
                    try:
                        await page.goto(people_url, wait_until="load", timeout=30000)
                        try:
                            await page.wait_for_timeout(3000)
                        except Exception as e:
                            if _is_playwright_target_closed_error(e):
                                logger.warning(
                                    f"[LinkedIn] Browser/page closed after /people navigation (run_id: {run_id})"
                                )
                                return asdict(profile_data)
                        logger.info(f"[LinkedIn] Successfully navigated to /people page")
                    except Exception as nav_error:
                        logger.error(f"[LinkedIn] Could not navigate to /people page: {str(nav_error)}")
                        return asdict(profile_data)
                    
                    # Check if redirected to login
                    if "login" in page.url.lower() or "signin" in page.url.lower():
                        logger.error("[LinkedIn] Redirected to login - authentication may have failed")
                        return asdict(profile_data)
                    
                    # Extract employee counts
                    logger.info(f"[LinkedIn] Extracting employee counts from /people page... (run_id: {run_id})")
                    profile_data.total_employees = await _extract_total_employees(
                        page, run_id
                    )
                    logger.info(f"[LinkedIn] Total employees extracted: {profile_data.total_employees} (run_id: {run_id})")
                    
                    # Extract US employees from /people page
                    logger.info(f"[LinkedIn] Extracting US employee count from /people page...")
                    location_data = await _extract_employee_locations(
                        page, profile_data.total_employees, run_id
                    )
                    profile_data.us_employees = location_data.get("us_employees", 0)
                    profile_data.hq_employees = location_data.get("hq_employees", 0)
                    profile_data.other_countries = location_data.get(
                        "other_countries", 0
                    )
                    profile_data.other_cities = location_data.get("other_cities", 0)
                    logger.info(f"[LinkedIn] US employees extracted: {profile_data.us_employees} (run_id: {run_id})")
                    
                    # Navigate back to company page before extracting jobs
                    logger.info(f"[LinkedIn] Navigating back to company page for jobs extraction...")
                    try:
                        await page.goto(linkedin_url, wait_until="load", timeout=30000)
                        await page.wait_for_timeout(2000)
                    except Exception as nav_error:
                        logger.warning(f"[LinkedIn] Could not navigate back to company page: {str(nav_error)}")
                    
                    # Extract open jobs
                    logger.info("[LinkedIn] Extracting open jobs...")
                    profile_data.open_jobs = await _extract_open_jobs(page, run_id)
                    
                    logger.info(
                        f"[LinkedIn] Extraction successful - "
                        f"total_employees: {profile_data.total_employees}, "
                        f"us_employees: {profile_data.us_employees}, "
                        f"open_jobs: {profile_data.open_jobs} (run_id: {run_id})"
                    )
                    
                except Exception as e:
                    if _is_playwright_target_closed_error(e):
                        logger.warning(
                            f"[LinkedIn] Browser/page closed during scrape; returning partial/empty data (run_id: {run_id})"
                        )
                        return asdict(profile_data)
                    logger.error(
                        f"[LinkedIn] Error during page scraping: {str(e)} "
                        f"(run_id: {run_id})"
                    )
                    raise
                finally:
                    try:
                        await page.close()
                    except Exception:
                        pass
                    try:
                        await context.close()
                    except Exception:
                        pass
            finally:
                if browser:
                    try:
                        await browser.close()
                        logger.info("[LinkedIn] Browser closed successfully")
                    except Exception as close_error:
                        logger.warning(f"[LinkedIn] Error closing browser: {str(close_error)}")
        
        return asdict(profile_data)
    
    except Exception as e:
        logger.error(
            f"[LinkedIn] Failed to scrape profile: {str(e)} (run_id: {run_id})"
        )
        raise


async def _extract_total_employees(page: Page, run_id: str) -> int:
    """
    Extract total employee count from company header.
    
    Looks for: "13,734 associated members"
    """
    try:
        logger.info("[LinkedIn] === EXTRACTING TOTAL EMPLOYEES ===")
        
        await page.wait_for_timeout(2000)
        
        # Get page text
        page_text = await page.inner_text("body")
        logger.info(f"[LinkedIn] Got {len(page_text)} characters of page content")
        logger.info(f"{page_text}")
        
        # Look for "13,734 associated members"
        total_match = re.search(r"([\d,]+)\s+associated members", page_text, re.IGNORECASE)
        if total_match:
            total_employees = int(total_match.group(1).replace(",", ""))
            logger.info(f"[LinkedIn] ✓ Found total employees: {total_employees}")
            return total_employees
        
        logger.error(f"[LinkedIn] ✗ Could not extract total employee count (run_id: {run_id})")
        return 0
    
    except Exception as e:
        logger.error(f"[LinkedIn] Error extracting total employees: {str(e)} (run_id: {run_id})")
        return 0


async def _extract_employee_locations(
    page: Page, total_employees: int, run_id: str
) -> dict:
    """
    Extract employee distribution by location from LinkedIn People section.
    
    Note: LinkedIn requires authentication to access the People section with location breakdown.
    This function will extract the data if authenticated, otherwise returns empty data.
    
    Returns:
    {
        "us_employees": int,
        "hq_employees": int,
        "other_countries": int,
        "other_cities": int
    }
    """
    location_data = {
        "us_employees": 0,
        "hq_employees": 0,
        "other_countries": 0,
        "other_cities": 0,
    }
    
    try:
        # Get page text
        page_text = await page.inner_text("body")
        logger.info(f"[LinkedIn] Got {len(page_text)} characters of page content from /people")
        logger.debug(f"[LinkedIn] Page text preview (first 1000 chars): {page_text[:1000]}")
        logger.info(f"{page_text}")
        
        # Look for "6,365 United States" pattern - case insensitive and flexible whitespace
        us_match = re.search(r"([\d,]+)\s+united\s+states", page_text, re.IGNORECASE)
        if us_match:
            count_str = us_match.group(1).replace(",", "")
            us_employees = int(count_str)
            location_data["us_employees"] = us_employees
            logger.info(f"[LinkedIn] ✓ US employees: {us_employees} (matched: '{us_match.group(0)}')")
        else:
            logger.warning(f"[LinkedIn] Could not find US employee count in page text")
        
        logger.info(f"[LinkedIn] Final location data: {location_data}")
        return location_data
    
    except Exception as e:
        logger.debug(f"[LinkedIn] Error extracting locations: {str(e)}")
        return location_data


async def _extract_open_jobs(page: Page, run_id: str) -> int:
    """
    Extract number of open job positions.
    
    Navigates to /jobs page after authentication to get accurate job count.
    """
    try:
        # Get current URL and navigate to jobs page
        current_url = page.url
        jobs_url = current_url.rstrip("/") + "/jobs"
        
        logger.debug(f"[LinkedIn] Navigating to jobs page: {jobs_url}")
        try:
            await page.goto(jobs_url, wait_until="load", timeout=30000)
        except Exception as nav_error:
            logger.debug(
                f"[LinkedIn] Could not navigate to /jobs: {str(nav_error)}"
            )
            return 0
        
        await page.wait_for_timeout(2000)
        
        # Check if redirected to login
        current_page = page.url
        if "login" in current_page.lower() or "signin" in current_page.lower():
            logger.debug("[LinkedIn] Redirected to login - jobs section requires authentication")
            return 0
        
        # Try selectors for job count on jobs page
        job_count_selectors = [
            "text=/\\d+[KM]?\\s+job/i",  # "27 jobs", "1K jobs"
        ]
        
        for selector in job_count_selectors:
            try:
                element = page.locator(selector).first
                if await element.is_visible(timeout=3000):
                    text = await element.text_content()
                    if text:
                        count = _parse_employee_count(text)
                        if count > 0:
                            logger.info(
                                f"[LinkedIn] Found open jobs: {count} from '{text}' (run_id: {run_id})"
                            )
                            return count
            except Exception as selector_error:
                logger.debug(f"[LinkedIn] Job selector failed: {str(selector_error)}")
                continue
        
        # Fallback: Extract page text and search with regex
        logger.debug("[LinkedIn] Job selectors failed, trying HTML extraction")
        try:
            page_text = await page.evaluate("document.body.innerText")
            if page_text:
                # Look for job count patterns
                patterns = [
                    r'([\d,]+)\s+jobs?',  # "27 jobs"
                    r'jobs?\s*\((\d+)\)',  # "jobs (27)"
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        text = match.group(0)
                        count = _parse_employee_count(text)
                        if count > 0:
                            logger.info(
                                f"[LinkedIn] Found open jobs via HTML: {count} from '{text}' (run_id: {run_id})"
                            )
                            return count
        except Exception as fallback_error:
            logger.debug(f"[LinkedIn] Job HTML fallback failed: {str(fallback_error)}")
        
        logger.debug("[LinkedIn] Could not extract open jobs count")
        return 0
    
    except Exception as e:
        logger.error(f"[LinkedIn] Error extracting jobs: {str(e)} (run_id: {run_id})")
        return 0


def _parse_employee_count(text: str) -> int:
    """
    Parse employee count from text.
    
    Handles formats like:
    - "27 employees"
    - "13,734 employees"
    - "1K employees"
    - "51-200 employees"
    - "27 associated members"
    """
    try:
        original_text = text
        # Remove common words
        text = text.lower().replace("employees", "").replace("members", "").replace("view all", "").replace("associated", "").strip()
        
        logger.debug(f"[LinkedIn] Parsing '{original_text}' -> '{text}'")
        
        # Handle ranges like "51-200"
        if "-" in text:
            parts = text.split("-")
            # Use the higher number
            text = parts[-1].strip()
            logger.debug(f"[LinkedIn] Range detected, using: {text}")
        
        # Handle K/M multipliers
        if "k" in text.lower():
            match = re.search(r"(\d+\.?\d*)\s*k", text, re.IGNORECASE)
            if match:
                result = int(float(match.group(1)) * 1000)
                logger.debug(f"[LinkedIn] K multiplier found: {result}")
                return result
        
        if "m" in text.lower():
            match = re.search(r"(\d+\.?\d*)\s*m", text, re.IGNORECASE)
            if match:
                result = int(float(match.group(1)) * 1000000)
                logger.debug(f"[LinkedIn] M multiplier found: {result}")
                return result
        
        # Extract number with commas (e.g., "13,734")
        # Find the largest number by removing all non-digit characters except comma
        match = re.search(r"([\d,]+)", text)
        if match:
            number_str = match.group(1).replace(",", "")
            result = int(number_str)
            logger.debug(f"[LinkedIn] Number parsed: {result}")
            return result
        
        logger.debug(f"[LinkedIn] No number found in '{text}'")
        return 0
    except Exception as e:
        logger.debug(f"Error parsing employee count from '{original_text}': {str(e)}")
        return 0
