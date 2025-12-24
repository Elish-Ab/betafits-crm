"""Email router node for parsing, normalizing, and deduplicating emails.

This is Node 1 of the 10-node pipeline. Responsibilities:
- Parse raw email input
- Normalize email fields
- Check for duplicates using Message-ID
- Convert EmailRaw → EmailParsed
"""

import logging
import stat
from typing import List

from lib.integrations.supabase.contacts_client import ContactDBClient
from lib.integrations.supabase.emails_client import EmailDBClient
from lib.integrations.supabase.supabase_client import get_supabase_client
from lib.models.database_schemas import (
    Contact,
    ReceivedEmail,
    EmailAttachment,
)
from workflows.langgraph.email_processing.state import PipelineState

logger = logging.getLogger(__name__)


async def email_router_node(state: PipelineState) -> PipelineState:
    """Parse, normalize, and deduplicate incoming emails.

    Inputs from state:
        - raw_email: EmailRaw input from Gmail API

    Outputs to state:
        - is_duplicate: bool (whether this email was a duplicate)

    Args:
        state: PipelineState with raw_email field.

    Returns:
        Updated PipelineState with is_duplicate fields.

    Raises:
        ValueError: If raw_email is invalid or parsing fails.
    """
    import time

    start_time = time.time()

    email_opt = state.get("email")

    if not email_opt:
        raise ValueError("email is required in state")

    email = email_opt

    are_email_and_contacts_stored = state.get("are_email_and_contacts_stored", False)

    if are_email_and_contacts_stored:
        logger.info(
            "[Node 1] email_router_node skipping; "
            "email and contacts already stored."
        )
        return state

    logger.info(f"[Node 1] email_router_node starting for email {email.message_id}")

    try:
        
        # Check if duplicate by Message-ID
        email_db_client = await EmailDBClient.create()
        contact_db_client = await ContactDBClient.create()
        existing_email = await (
            email_db_client.get_received_email(email.message_id)
            if isinstance(email, ReceivedEmail)
            else email_db_client.get_sent_email(email.message_id)
        )

        is_duplicate = existing_email is not None

        related_contacts: List[Contact] = []

        if isinstance(email, ReceivedEmail):
            related_contacts.append(
                Contact(
                    email=email.from_email,
                    name="",
                )
            )
        else:
            for recipient in email.to_emails + email.cc_emails + email.bcc_emails:
                related_contacts.append(
                    Contact(
                        email=recipient,
                        name="",
                    )
                )

        logger.info(
            f"[Node 1] Successfully parsed email {email.message_id} "
            f"(duplicate={is_duplicate}) in {time.time() - start_time:.2f}s"
        )

        # Only store if not a duplicate
        if not is_duplicate:
            await email_db_client.store_received_email(email=email) if isinstance(
                email, ReceivedEmail
            ) else await email_db_client.store_sent_email(email=email)
            for attachment in email.attachments:
                if email.id:
                    attachment = EmailAttachment(
                        email_id=email.id,
                        filename=attachment.filename,
                        mime_type=attachment.mime_type,
                        size_bytes=attachment.size_bytes,
                        sha256=None,
                        storage_path=None,
                    )
                    if isinstance(email, ReceivedEmail):
                        attachment.email_type = "incoming"
                    else:
                        attachment.email_type = "outgoing"

                    await email_db_client.store_email_attachment(attachment)

            for contact in related_contacts:
                try:
                    contact.id = await contact_db_client.upsert_contact(contact)
                    logger.debug(
                        "[Node 1] Upserted contact record for %s",
                        contact.email,
                    )
                except Exception as contact_error:
                    logger.warning(
                        "[Node 1] Contact upsert failed for %s: %s",
                        contact.email,
                        contact_error,
                    )

        # Update state with parsed email and duplicate flag
        state["is_duplicate"] = is_duplicate
        state["related_contacts"] = related_contacts
        state["are_email_and_contacts_stored"] = True

        return state

    except ValueError as ve:
        logger.error(f"[Node 1] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Node 1] Failed to parse email: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to parse email: {error}") from error
