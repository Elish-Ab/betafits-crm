# -*- coding: utf-8 -*-
"""
Filter last emails from threads in matt_sent_mails.mbox.

This script processes an mbox file and extracts:
1. Unique emails (emails with unique subjects)
2. Last email from each thread (identified by subject + recipient combination)

Thread identification:
- Same subject + same recipient = same thread
- Handles reply subjects (e.g., "Re: thanks and next steps")
- Extracts only the last email from each thread to avoid duplication

Usage:
    python filter_last_thread_mail.py
"""

import mailbox
import re
from collections import defaultdict
from datetime import datetime
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def normalize_subject(subject: str) -> str:
    """
    Normalize email subject by removing reply/forward prefixes.

    Args:
        subject: Raw email subject line

    Returns:
        Normalized subject without Re:, Fwd:, etc. prefixes
    """
    if not subject:
        return ""

    # Remove Re:, RE:, Fwd:, FW:, etc. prefixes (case-insensitive)
    normalized = re.sub(r"^(re|fwd?|fw):\s*", "", subject.strip(), flags=re.IGNORECASE)

    # Remove multiple prefixes (e.g., "Re: Re: Re: Subject")
    while re.match(r"^(re|fwd?|fw):\s*", normalized, flags=re.IGNORECASE):
        normalized = re.sub(r"^(re|fwd?|fw):\s*", "", normalized, flags=re.IGNORECASE)

    return normalized.strip()


def extract_recipients(message: Message) -> str:
    """
    Extract and normalize recipient email addresses from 'To' field.

    Args:
        message: Email message object

    Returns:
        Comma-separated string of recipient email addresses (sorted)
    """
    to_field = message.get("To", "")

    # Extract email addresses using regex
    email_pattern = r"[\w\.-]+@[\w\.-]+"
    emails = re.findall(email_pattern, to_field)

    # Sort to ensure consistent ordering
    emails_sorted = sorted(set(email.lower() for email in emails))

    return ",".join(emails_sorted)


def get_message_date(message: Message) -> Optional[datetime]:
    """
    Extract datetime from email message.

    Args:
        message: Email message object

    Returns:
        datetime object or None if parsing fails
    """
    date_str = message.get("Date")
    if not date_str:
        return None

    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        return None


def create_thread_key(subject: str, recipients: str) -> str:
    """
    Create a unique thread identifier from subject and recipients.

    Args:
        subject: Normalized email subject
        recipients: Comma-separated recipient emails

    Returns:
        Thread key string
    """
    return f"{subject}||{recipients}"


def process_mbox(input_path: str, output_path: str) -> Tuple[int, int, int]:
    """
    Process mbox file and extract last emails from threads.

    Args:
        input_path: Path to input mbox file
        output_path: Path to output mbox file

    Returns:
        Tuple of (total_emails, threads_found, emails_written)
    """
    # Read all messages and group by thread
    input_mbox = mailbox.mbox(input_path)

    # Dictionary: thread_key -> list of (datetime, message_index, message)
    threads: Dict[str, List[Tuple[Optional[datetime], int, Message]]] = defaultdict(
        list
    )

    print(f"Reading emails from {input_path}...")

    total_emails = 0
    for idx, message in enumerate(input_mbox):
        total_emails += 1

        # Extract subject and recipients
        raw_subject = message.get("Subject", "")
        normalized_subject = normalize_subject(raw_subject)
        recipients = extract_recipients(message)

        # Skip if no subject or recipients
        if not normalized_subject or not recipients:
            print(f"  Skipping email {idx}: Missing subject or recipients")
            continue

        # Create thread key
        thread_key = create_thread_key(normalized_subject, recipients)

        # Get message date
        msg_date = get_message_date(message)

        # Add to thread
        threads[thread_key].append((msg_date, idx, message))

    print(f"Total emails read: {total_emails}")
    print(f"Threads identified: {len(threads)}")

    # Create output mbox and write last email from each thread
    output_mbox = mailbox.mbox(output_path)
    output_mbox.clear()  # Clear any existing content

    emails_written = 0

    print("\nProcessing threads...")

    for thread_key, messages in threads.items():
        # Sort messages by date (None dates go to the beginning)
        messages_sorted = sorted(
            messages, key=lambda x: (x[0] is None, x[0] if x[0] else datetime.min, x[1])
        )

        # Get the last message (most recent)
        last_date, last_idx, last_message = messages_sorted[-1]

        # Add to output mbox
        output_mbox.add(last_message)
        emails_written += 1

        # Log thread info
        subject = last_message.get("Subject", "")
        recipients = last_message.get("To", "")
        date_str = last_date.strftime("%Y-%m-%d %H:%M:%S") if last_date else "Unknown"

        print(f"  Thread {emails_written}: {len(messages)} emails -> keeping last")
        print(f"    Subject: {subject}")
        print(f"    To: {recipients}")
        print(f"    Date: {date_str}")
        print()

    # Flush changes to disk
    output_mbox.flush()
    output_mbox.close()
    input_mbox.close()

    return total_emails, len(threads), emails_written


def main() -> None:
    """Main execution function."""
    script_dir = Path(__file__).parent
    input_file = script_dir / "matt_sent_mails.mbox"
    output_file = script_dir / "matt_sent_mails_filtered.mbox"

    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return

    print("=" * 70)
    print("Email Thread Deduplication Script")
    print("=" * 70)
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print("=" * 70)
    print()

    # Process mbox
    total, threads, written = process_mbox(str(input_file), str(output_file))

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total emails in input:     {total}")
    print(f"Unique threads identified: {threads}")
    print(f"Emails written to output:  {written}")
    print(f"Emails deduplicated:       {total - written}")
    print(f"Reduction:                 {((total - written) / total * 100):.1f}%")
    print("=" * 70)
    print(f"\nFiltered emails saved to: {output_file}")


if __name__ == "__main__":
    main()
