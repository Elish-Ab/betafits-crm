# -*- coding: utf-8 -*-
"""
Classify historical emails by sending them to webhook API.

This script reads emails from matt_sent_mails_filtered.mbox and sends
each email's metadata (from, to, subject, body) to the classification
webhook API for processing.

Usage:
    python classify_historical_emails.py
"""

import hashlib
import json
import logging
import mailbox
import re
import sys
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import Message
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import urllib3

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.integrations.supabase.supabase_client import get_supabase_client


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def sanitize_text(text: str) -> str:
    """
    Sanitize text for safe JSON transmission to API.

    - Normalizes Unicode to NFC form (canonical composition)
    - Removes control characters except newlines, tabs, and carriage returns
    - Ensures valid UTF-8 encoding
    - Replaces problematic characters with spaces

    Args:
        text: Raw text string

    Returns:
        Sanitized text safe for JSON encoding
    """
    if not text:
        return ""

    # Normalize Unicode to NFC (canonical composition)
    text = unicodedata.normalize("NFC", text)

    # Remove control characters except \n, \r, \t
    # Control characters are in ranges: 0x00-0x1F and 0x7F-0x9F
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "", text)

    # Replace non-breaking spaces and other problematic whitespace
    text = text.replace("\xa0", " ")  # Non-breaking space
    text = text.replace("\u200b", "")  # Zero-width space
    text = text.replace("\ufeff", "")  # Zero-width no-break space (BOM)

    # Ensure the text can be encoded to UTF-8 (replace any invalid chars)
    text = text.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")

    # Collapse multiple whitespace into single space (but preserve newlines)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)  # Max 2 consecutive newlines

    return text.strip()


def generate_email_id(message: Message) -> str:
    """
    Generate a unique email ID based on message content.

    Uses a hash of message-id, date, from, to, and subject to create
    a deterministic unique identifier.

    Args:
        message: Email message object

    Returns:
        Unique email ID (SHA256 hash prefix)
    """
    # Collect identifying fields
    message_id = message.get("Message-ID", "")
    date = message.get("Date", "")
    from_addr = message.get("From", "")
    to_addr = message.get("To", "")
    subject = message.get("Subject", "")

    # Create composite string
    composite = f"{message_id}|{date}|{from_addr}|{to_addr}|{subject}"

    # Generate hash
    hash_obj = hashlib.sha256(composite.encode("utf-8"))
    email_id = hash_obj.hexdigest()[:32]  # Use first 32 chars

    return email_id


def extract_email_body(message: Message) -> str:
    """
    Extract email body from message, handling multipart messages.

    Args:
        message: Email message object

    Returns:
        Email body as string (plain text preferred, HTML as fallback)
    """
    body = ""

    if message.is_multipart():
        # Iterate through message parts to find text content
        for part in message.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))

            # Skip attachments
            if "attachment" in content_disposition:
                continue

            # Prefer plain text
            if content_type == "text/plain":
                try:
                    payload = part.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        body = payload.decode("utf-8", errors="ignore")
                    else:
                        body = str(payload)
                    break
                except Exception as e:
                    logger.warning(f"Failed to decode text/plain part: {e}")

            # Fallback to HTML if plain text not found
            elif content_type == "text/html" and not body:
                try:
                    payload = part.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        body = payload.decode("utf-8", errors="ignore")
                    else:
                        body = str(payload)
                except Exception as e:
                    logger.warning(f"Failed to decode text/html part: {e}")
    else:
        # Not multipart, get payload directly
        try:
            payload = message.get_payload(decode=True)
            if isinstance(payload, bytes):
                body = payload.decode("utf-8", errors="ignore")
            else:
                body = str(payload) if payload else ""
        except Exception as e:
            logger.warning(f"Failed to decode message payload: {e}")
            body = str(message.get_payload())

    # Sanitize body text to ensure valid UTF-8 and remove problematic characters
    return sanitize_text(body)


def extract_email_data(message: Message, email_id: str) -> Dict[str, str]:
    """
    Extract relevant email data for API submission.

    Args:
        message: Email message object
        email_id: Unique email identifier

    Returns:
        Dictionary with email metadata (id, from, to, subject, body, date)
    """
    # Extract and sanitize all fields to ensure valid UTF-8 encoding
    return {
        "email_id": email_id,
        "from": sanitize_text(message.get("From", "")),
        "to": sanitize_text(message.get("To", "")),
        "subject": sanitize_text(message.get("Subject", "")),
        "date": sanitize_text(message.get("Date", "")),
        "body": extract_email_body(message),
    }


def fetch_all_classified_email_ids() -> Set[str]:
    """
    Fetch all classified email IDs from Supabase at once for fast lookup.

    Uses pagination to fetch all records (Supabase default limit is 1000).

    Returns:
        Set of email IDs that have already been classified
    """
    try:
        supabase_client = get_supabase_client()

        logger.info("Fetching all classified email IDs from Supabase...")

        email_ids: Set[str] = set()
        page_size = 1000
        offset = 0
        total_fetched = 0

        # Fetch all email_ids with pagination
        while True:
            response = (
                supabase_client._client.schema("crm")
                .table("matts_classified_emails")
                .select("email_id")
                .range(offset, offset + page_size - 1)
                .execute()
            )

            # Extract email_ids from current page
            if response.data and isinstance(response.data, list):
                page_count = 0
                for row in response.data:
                    if isinstance(row, dict) and "email_id" in row:
                        email_id = row.get("email_id")
                        if isinstance(email_id, str):
                            email_ids.add(email_id)
                            page_count += 1

                total_fetched += page_count
                logger.info(f"Fetched {page_count} email IDs (total: {total_fetched})")

                # If we got fewer than page_size records, we've reached the end
                if len(response.data) < page_size:
                    break

                # Move to next page
                offset += page_size
            else:
                # No more data
                break

        logger.info(
            f"Found {len(email_ids)} already classified emails (total fetched: {total_fetched})"
        )
        return email_ids

    except Exception as e:
        logger.warning(f"Failed to fetch classified email IDs: {e}")
        logger.warning("Continuing without skip optimization")
        return set()


def send_email_to_api(
    http: urllib3.PoolManager,
    api_url: str,
    email_data: Dict[str, str],
    email_index: int,
) -> bool:
    """
    Send email data to webhook API.

    Args:
        http: urllib3 PoolManager instance
        api_url: API endpoint URL
        email_data: Email metadata dictionary
        email_index: Index of current email (for logging)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Convert data to JSON with explicit UTF-8 encoding
        # ensure_ascii=False allows proper Unicode characters in JSON
        json_data = json.dumps(email_data, ensure_ascii=False)

        # Encode to UTF-8 bytes for transmission
        json_bytes = json_data.encode("utf-8")

        # Send POST request
        response = http.request(
            "POST",
            api_url,
            body=json_bytes,
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=urllib3.Timeout(connect=5.0, read=90.0),
        )

        # Check response status
        if response.status == 200:
            logger.info(
                f"Email {email_index} sent successfully - Subject: {email_data['subject'][:50]}..."
            )
            return True
        else:
            response_text = (
                response.data.decode("utf-8", errors="ignore")
                if isinstance(response.data, bytes)
                else str(response.data)
            )
            logger.error(
                f"Email {email_index} failed - Status: {response.status}, "
                f"Response: {response_text[:200]}"
            )
            return False

    except urllib3.exceptions.TimeoutError as e:
        logger.error(f"Email {email_index} timeout: {e}")
        return False
    except Exception as e:
        logger.error(f"Email {email_index} error: {e}")
        return False


# Thread-safe counter class for tracking progress
class ThreadSafeCounter:
    """Thread-safe counter for tracking concurrent progress."""

    def __init__(self) -> None:
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self._lock = threading.Lock()

    def increment_successful(self) -> None:
        with self._lock:
            self.successful += 1

    def increment_failed(self) -> None:
        with self._lock:
            self.failed += 1

    def increment_skipped(self) -> None:
        with self._lock:
            self.skipped += 1

    def get_counts(self) -> Tuple[int, int, int]:
        with self._lock:
            return self.successful, self.failed, self.skipped


def process_single_email(
    email_task: Tuple[int, Message],
    http: urllib3.PoolManager,
    api_url: str,
    classified_email_ids: Set[str],
    counter: ThreadSafeCounter,
) -> None:
    """
    Process a single email task (for concurrent execution).

    Args:
        email_task: Tuple of (index, message)
        http: urllib3 PoolManager instance
        api_url: API endpoint URL
        classified_email_ids: Set of already classified email IDs
        counter: Thread-safe counter for tracking results
    """
    idx, message = email_task

    try:
        # Generate unique email ID
        email_id = generate_email_id(message)

        # Check if already classified (O(1) lookup in Set)
        if email_id in classified_email_ids:
            logger.info(
                f"Email {idx} (ID: {email_id[:16]}...) already classified, skipping"
            )
            counter.increment_skipped()
            return

        # Extract email data
        email_data = extract_email_data(message, email_id)

        # Send to API
        if send_email_to_api(http, api_url, email_data, idx):
            counter.increment_successful()
        else:
            counter.increment_failed()

    except Exception as e:
        logger.error(f"Email {idx} processing error: {e}")
        counter.increment_failed()


def process_mbox_file(
    mbox_path: str,
    api_url: str,
    max_workers: int = 3,
) -> tuple[int, int, int, int]:
    """
    Process all emails in mbox file and send to API with concurrent execution.

    Args:
        mbox_path: Path to mbox file
        api_url: API endpoint URL
        max_workers: Maximum concurrent workers (default: 3)

    Returns:
        Tuple of (total_emails, successful, failed, skipped)
    """
    # Fetch all classified email IDs upfront for fast O(1) lookups
    classified_email_ids = fetch_all_classified_email_ids()

    # Initialize HTTP client with connection pooling (sized for concurrent use)
    http = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=5.0, read=90.0),
        maxsize=max_workers + 2,  # Extra connections for safety
        retries=urllib3.Retry(
            total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504]
        ),
    )

    # Open mbox file and load all messages into memory
    # (necessary because mbox iteration is not thread-safe)
    logger.info(f"Loading emails from: {mbox_path}")
    mbox = mailbox.mbox(mbox_path)

    # Pre-load all messages with their indices
    email_tasks: List[Tuple[int, Message]] = []
    for idx, message in enumerate(mbox, start=1):
        email_tasks.append((idx, message))

    total_emails = len(email_tasks)
    mbox.close()

    logger.info(f"Loaded {total_emails} emails")
    logger.info(f"API endpoint: {api_url}")
    logger.info(f"Concurrent workers: {max_workers}")
    logger.info("=" * 70)

    # Thread-safe counter
    counter = ThreadSafeCounter()

    # Process emails concurrently
    processed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                process_single_email,
                task,
                http,
                api_url,
                classified_email_ids,
                counter,
            ): task[0]  # Map future to email index
            for task in email_tasks
        }

        # Wait for completion and track progress
        for future in as_completed(futures):
            processed += 1

            # Progress update every 100 emails
            if processed % 100 == 0:
                successful, failed, skipped = counter.get_counts()
                logger.info(
                    f"Progress: {processed}/{total_emails} emails processed "
                    f"({successful} successful, {failed} failed, {skipped} skipped)"
                )

    successful, failed, skipped = counter.get_counts()
    return total_emails, successful, failed, skipped


def main() -> None:
    """Main execution function."""
    script_dir = Path(__file__).parent
    mbox_file = script_dir / "matt_sent_mails_filtered.mbox"
    api_url = "http://localhost:5678/webhook/classify-matts-email"

    # Number of concurrent workers (match n8n's capacity)
    max_workers = 3

    # Check if mbox file exists
    if not mbox_file.exists():
        logger.error(f"Mbox file not found: {mbox_file}")
        return

    logger.info("=" * 70)
    logger.info("Historical Email Classification Script (Concurrent)")
    logger.info(f"Max concurrent workers: {max_workers}")
    logger.info("=" * 70)

    # Process emails
    total, successful, failed, skipped = process_mbox_file(
        str(mbox_file), api_url, max_workers
    )

    # Summary
    logger.info("=" * 70)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total emails processed:  {total}")
    logger.info(f"Already classified:      {skipped}")
    logger.info(f"Successfully sent:       {successful}")
    logger.info(f"Failed:                  {failed}")
    logger.info(
        f"Success rate:            {(successful / (total - skipped) * 100):.1f}%"
        if (total - skipped) > 0
        else "N/A"
    )
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
