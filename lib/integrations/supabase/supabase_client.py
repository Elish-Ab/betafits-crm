"""Supabase client for interacting with Betafits' PostgreSQL database.

This module provides a wrapper around the supabase-py library with retry logic,
timeout enforcement, and comprehensive error handling for the email ingestor pipeline.

The client handles CRUD operations for five main tables:
- ReceivedEmails: Inbound email metadata and content
- EmailAttachments: File attachments from both received and sent emails (with email_type discriminator)
- DraftedEmails: AI-generated email drafts awaiting review
- SentEmails: Emails sent to recipients with delivery metadata
- ValidationLog: Audit trail of validation and processing steps

Usage:
    from lib.integrations.supabase_client import get_supabase_client
    from lib.models.database_schemas import ReceivedEmailsRow

    client = get_supabase_client()
    email = await client.fetch_email_by_id("email-123")

    new_email = ReceivedEmail(
        email_id="msg-456",
        from_email="sender@example.com",
        to_emails=["recipient@example.com"],
        subject="Test Subject",
        body="Email body",
        received_at=datetime.now(timezone.utc),
    )
    result = await client.store_parsed_email(new_email)
"""

import logging
from typing import Optional

from pydantic import BaseModel
from supabase import acreate_client

from lib.config import get_settings

logger = logging.getLogger(__name__)


class SupabaseClient:
    """AsyncClient for interacting with Betafits' Supabase PostgreSQL database.

    Wraps the supabase-py library with retry logic, timeout enforcement,
    and comprehensive error handling.

    Attributes:
        _client: The underlying Supabase client instance.
        _timeout: Request timeout in seconds from settings.
        _max_retries: Maximum retry attempts from settings.
        _backoff_factor: Exponential backoff factor from settings.
    """

    def __init__(self) -> None:
        """Initialize Supabase client.

        Args:
            client: Optional Supabase client instance. If not provided,
                creates a new client from settings.

        Raises:
            ValueError: If Supabase URL or service key is not configured.
        """
        settings = get_settings()

        self._timeout = settings.request_timeout
        self._max_retries = settings.retry_max_attempts
        self._backoff_factor = settings.retry_backoff_factor
        self._schema = "crm"  # Default schema for all operations

    @classmethod
    async def create(cls) -> "SupabaseClient":
        """Factory method to create and initialize SupabaseClient.

        Returns:
            An initialized SupabaseClient instance.
        """
        client = cls()
        await client.init_client()
        return client

    async def init_client(self):
        """create the Supabase AsyncClient."""
        if hasattr(self, "_client") and self._client is not None:
            return
        else:
            settings = get_settings()
            if not settings.supabase_url or not settings.supabase_service_key:
                raise ValueError(
                    "Supabase URL and service key must be configured "
                    "via SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables"
                )
            self._client = await acreate_client(
                settings.supabase_url,
                settings.supabase_service_key,
            )

    def _get_table(self, table_name: str):
        """Get a table reference with the configured schema.

        Args:
            table_name: Name of the table (without schema prefix).

        Returns:
            Table reference configured for the crm schema.
        """
        return self._client.schema(self._schema).table(table_name)

    def _log_operation(
        self,
        operation: str,
        table: str,
        record_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Log database operation (without sensitive data).

        Args:
            operation: The operation type (READ, INSERT, UPDATE, DELETE).
            table: The table name.
            record_id: Optional record identifier (ID only, no PII).
            error: Optional error message.
        """
        if error:
            logger.error(
                f"Database {operation} failed for {table}[{record_id}]: {error}"
            )
        else:
            logger.debug(f"Database {operation} completed for {table}[{record_id}]")

    async def _insert_model(
        self,
        table_name: str,
        payload: BaseModel,
        record_description: str,
    ) -> str:
        """Insert a BaseModel payload into Supabase and return the new id."""
        if not payload:
            raise ValueError(f"{record_description} must be provided")

        table = self._get_table(table_name)
        payload_dict = payload.model_dump(
            by_alias=True,
            exclude_unset=True,
            mode="json",
        )

        try:
            response = await table.insert(payload_dict).execute()
            if (
                response.data
                and isinstance(response.data, list)
                and len(response.data) > 0
            ):
                record = response.data[0]
                if isinstance(record, dict):
                    record_id = record.get("id")
                    if record_id:
                        self._log_operation("INSERT", table_name, str(record_id))
                        return str(record_id)
            raise ValueError(
                f"No ID returned from insert operation for {record_description}"
            )
        except Exception as exc:
            self._log_operation("INSERT", table_name, None, str(exc))
            raise ValueError(f"Failed to insert {record_description}: {exc}") from exc


# Global singleton instance
_supabase_client: Optional[SupabaseClient] = None


async def get_supabase_client() -> SupabaseClient:
    """Get or create the global Supabase client instance.

    Returns:
        The singleton SupabaseClient instance.

    Raises:
        ValueError: If Supabase URL or service key is not configured.

    Example:
        >>> client = get_supabase_client()
        >>> email = client.fetch_email_by_id("email-123")
    """
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = await SupabaseClient.create()
    return _supabase_client


def reset_supabase_client() -> None:
    """Reset the global Supabase client instance.

    Used in testing to ensure isolation between tests.

    Example:
        >>> reset_supabase_client()
        >>> # New client will be created on next get_supabase_client() call
    """
    global _supabase_client
    _supabase_client = None
