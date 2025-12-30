import logging
from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel

from postgrest.exceptions import APIError
from supabase import acreate_client

from lib.config import get_settings
from lib.integrations.supabase.supabase_client import (
    SupabaseClient,
    get_supabase_client,
)
from lib.integrations.vector_db_client import OpenRouterEmbeddings
from lib.models.database_schemas import (
    DraftedEmail,
    BaseEmail,
    EmailAttachment,
    ReceivedEmail,
    SentEmail,
)
from lib.utils.retry import retry_on_exception

logger = logging.getLogger(__name__)


class EmailDBClient:
    supabase_client: SupabaseClient

    @classmethod
    async def create(cls) -> "EmailDBClient":
        client = cls()
        await client.init_client()
        return client

    async def init_client(self):
        """create the Supabase AsyncClient."""
        if hasattr(self, "supabase_client") and self.supabase_client is not None:
            return
        else:
            self.supabase_client = await get_supabase_client()

    @staticmethod
    def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
        """Convert datetime values in payloads to ISO strings."""
        normalized: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, datetime):
                normalized[key] = value.isoformat()
            elif isinstance(value, dict):
                normalized[key] = EmailDBClient._normalize_payload(value)
            elif isinstance(value, list):
                normalized[key] = [
                    EmailDBClient._normalize_payload(item)
                    if isinstance(item, dict)
                    else item.isoformat()
                    if isinstance(item, datetime)
                    else item
                    for item in value
                ]
            else:
                normalized[key] = value
        return normalized

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def get_received_email(self, message_id: str) -> Optional[ReceivedEmail]:
        """Fetch a received email by its message_id.

        Args:
            message_id: The unique email identifier.

        Returns:
            ReceivedEmail if found, None if not found.

        Raises:
            ValueError: If message_id is empty.
            Exception: If database query fails after retries.
        """
        if not message_id or not isinstance(message_id, str):
            raise ValueError("message_id must be a non-empty string")

        try:
            response = await (
                self.supabase_client._get_table("received_emails")
                .select("*")
                .eq("message_id", message_id)
                .single()
                .execute()
            )
            if response.data and isinstance(response.data, dict):
                self.supabase_client._log_operation(
                    "READ", "received_emails", message_id
                )
                return ReceivedEmail(**response.data)  # type: ignore
            return None
        except APIError as e:
            if e.code == "PGRST116":
                return None
            self.supabase_client._log_operation(
                "READ", "received_emails", message_id, str(e)
            )
            raise
        except Exception as e:
            self.supabase_client._log_operation(
                "READ", "received_emails", message_id, str(e)
            )
            raise

    async def store_received_email(self, email: ReceivedEmail) -> str:
        """Store a received email in the database.

        Args:
            email: The ReceivedEmail model to store.

        Returns:
            The unique Supabase ID of the stored email.

        Raises:
            ValueError: If email is invalid or insertion fails.
        """
        if not email:
            raise ValueError("email must be provided")

        try:
            email_dict = email.model_dump(
                by_alias=True,
                exclude_unset=True,
                mode="json",
            )
            response = await (
                self.supabase_client._get_table("received_emails")
                .insert(email_dict)
                .execute()
            )
            if (
                response.data
                and isinstance(response.data, list)
                and len(response.data) > 0
            ):
                record = response.data[0]
                if isinstance(record, dict):
                    record_id = record.get("id")
                    if record_id:
                        self.supabase_client._log_operation(
                            "INSERT", "received_emails", str(record_id)
                        )
                        return str(record_id)
            raise ValueError("No ID returned from insert operation")
        except Exception as exc:
            self.supabase_client._log_operation(
                "INSERT", "received_emails", None, str(exc)
            )
            raise ValueError(f"Failed to store received email: {exc}") from exc

    async def update_received_email(
        self,
        email_id: str,
        updates: dict[str, Any],
    ) -> bool:
        """Update a received email by email_id.

        Args:
            email_id: The email_id to update.
            updates: Dictionary of fields to update.

        Returns:
            True if a row was affected, False otherwise.

        Raises:
            ValueError: If email_id is empty or update fails.
        """
        if not email_id:
            raise ValueError("email_id must be provided")
        if not updates:
            raise ValueError("updates must be provided")

        payload = self._normalize_payload(updates)
        payload["updated_at"] = datetime.utcnow().isoformat()

        try:
            response = await (
                self.supabase_client._get_table("received_emails")
                .update(payload)
                .eq("email_id", email_id)
                .execute()
            )
            self.supabase_client._log_operation("UPDATE", "received_emails", email_id)
            return bool(response.data)
        except Exception as exc:
            self.supabase_client._log_operation(
                "UPDATE", "received_emails", email_id, str(exc)
            )
            raise ValueError(f"Failed to update received email: {exc}") from exc

    async def delete_received_email(self, email_id: str) -> bool:
        """Delete a received email by email_id.

        Args:
            email_id: The email_id to delete.

        Returns:
            True if a row was deleted, False otherwise.

        Raises:
            ValueError: If email_id is empty or deletion fails.
        """
        if not email_id:
            raise ValueError("email_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("received_emails")
                .delete()
                .eq("email_id", email_id)
                .execute()
            )
            self.supabase_client._log_operation("DELETE", "received_emails", email_id)
            return bool(response.data)
        except Exception as exc:
            self.supabase_client._log_operation(
                "DELETE", "received_emails", email_id, str(exc)
            )
            raise ValueError(f"Failed to delete received email: {exc}") from exc

    async def store_email_attachment(self, attachment: EmailAttachment) -> str:
        """Store an email attachment (received or sent).

        Args:
            attachment: The EmailAttachment model to store (with email_type field).

        Returns:
            The unique Supabase ID of the stored attachment.

        Raises:
            ValueError: If attachment is invalid or insertion fails.
        """
        if not attachment:
            raise ValueError("attachment must be provided")

        try:
            attachment_dict = attachment.model_dump(
                by_alias=True,
                exclude_unset=True,
                mode="json",
            )
            response = await (
                self.supabase_client._get_table("received_email_attachments")
                .insert(attachment_dict)
                .execute()
            )
            if (
                response.data
                and isinstance(response.data, list)
                and len(response.data) > 0
            ):
                record = response.data[0]
                if isinstance(record, dict):
                    record_id = record.get("id")
                    if record_id:
                        self.supabase_client._log_operation(
                            "INSERT", "received_email_attachments", str(record_id)
                        )
                        return str(record_id)
            raise ValueError("No ID returned from insert operation")
        except Exception as exc:
            self.supabase_client._log_operation(
                "INSERT", "received_email_attachments", None, str(exc)
            )
            raise ValueError(f"Failed to store attachment: {exc}") from exc

    async def get_email_attachment(
        self, attachment_id: str
    ) -> Optional[EmailAttachment]:
        """Fetch an email attachment by its ID.

        Args:
            attachment_id: The unique attachment identifier.

        Returns:
            EmailAttachment if found, None otherwise.

        Raises:
            ValueError: If attachment_id is empty.
            Exception: If database query fails.
        """
        if not attachment_id:
            raise ValueError("attachment_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("received_email_attachments")
                .select("*")
                .eq("id", attachment_id)
                .single()
                .execute()
            )
            if response.data and isinstance(response.data, dict):
                self.supabase_client._log_operation(
                    "READ", "received_email_attachments", attachment_id
                )
                return EmailAttachment(**response.data)  # type: ignore
            return None
        except APIError as e:
            if e.code == "PGRST116":
                return None
            self.supabase_client._log_operation(
                "READ", "received_email_attachments", attachment_id, str(e)
            )
            raise
        except Exception as exc:
            self.supabase_client._log_operation(
                "READ", "received_email_attachments", attachment_id, str(exc)
            )
            raise

    async def get_email_attachments_by_email_id(
        self, email_id: str, email_type: str = "received"
    ) -> List[EmailAttachment]:
        """Fetch all attachments for an email (received or sent).

        Args:
            email_id: The email_id to fetch attachments for.
            email_type: The email type filter ("received" or "sent").

        Returns:
            List of EmailAttachment objects, empty list if none found.

        Raises:
            ValueError: If email_id is empty.
            Exception: If database query fails.
        """
        if not email_id:
            raise ValueError("email_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("received_email_attachments")
                .select("*")
                .eq("email_id", email_id)
                .eq("email_type", email_type)
                .execute()
            )
            attachments_data = response.data if isinstance(response.data, list) else []
            attachments = [
                EmailAttachment(**row)  # type: ignore
                for row in attachments_data
                if isinstance(row, dict)
            ]
            self.supabase_client._log_operation(
                "READ", "received_email_attachments", f"{email_id}({len(attachments)})"
            )
            return attachments
        except Exception as exc:
            self.supabase_client._log_operation(
                "READ", "received_email_attachments", email_id, str(exc)
            )
            raise ValueError(f"Failed to fetch attachments: {exc}") from exc

    async def delete_email_attachment(self, attachment_id: str) -> bool:
        """Delete an email attachment by its ID.

        Args:
            attachment_id: The unique attachment identifier.

        Returns:
            True if a row was deleted, False otherwise.

        Raises:
            ValueError: If attachment_id is empty or deletion fails.
        """
        if not attachment_id:
            raise ValueError("attachment_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("received_email_attachments")
                .delete()
                .eq("id", attachment_id)
                .execute()
            )
            self.supabase_client._log_operation(
                "DELETE", "received_email_attachments", attachment_id
            )
            return bool(response.data)
        except Exception as exc:
            self.supabase_client._log_operation(
                "DELETE", "received_email_attachments", attachment_id, str(exc)
            )
            raise ValueError(f"Failed to delete attachment: {exc}") from exc

    async def store_drafted_email(self, draft: DraftedEmail) -> str:
        """Store a drafted email awaiting review.

        Args:
            draft: The drafted email data to store.

        Returns:
            The unique identifier of the drafted email.

        Raises:
            ValueError: If draft is invalid.
            Exception: If database insert fails after retries.

        Example:
            >>> from lib.models.database_schemas import DraftedEmailsRow
            >>> draft = DraftedEmailsRow(
            ...     received_email_id="email-123",
            ...     to_address="recipient@example.com",
            ...     subject="Re: Original Subject",
            ...     body_text="Drafted response",
            ... )
            >>> draft_id = client.store_drafted_email(draft)
        """
        return await self.create_drafted_email(draft)

    async def create_drafted_email(self, draft: DraftedEmail) -> str:
        """Create a drafted email record and return its Supabase ID."""
        if not draft:
            raise ValueError("draft must be provided")
        return await self.supabase_client._insert_model(
            "drafted_emails", draft, "drafted email"
        )

    async def get_drafted_email(self, draft_id: str) -> Optional[DraftedEmail]:
        """Fetch a drafted email row by its primary key."""
        if not draft_id:
            raise ValueError("draft_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("drafted_emails")
                .select("*")
                .eq("id", draft_id)
                .single()
                .execute()
            )
            if response.data and isinstance(response.data, dict):
                self.supabase_client._log_operation("READ", "drafted_emails", draft_id)
                return DraftedEmail(**response.data)  # type: ignore
            return None
        except APIError as e:
            if e.code == "PGRST116":
                return None
            self.supabase_client._log_operation(
                "READ", "drafted_emails", draft_id, str(e)
            )
            raise
        except Exception as exc:
            self.supabase_client._log_operation(
                "READ", "drafted_emails", draft_id, str(exc)
            )
            raise

    async def update_drafted_email(
        self,
        draft_id: str,
        updates: dict[str, Any],
    ) -> bool:
        """Update metadata for a drafted email."""
        if not draft_id or not updates:
            raise ValueError("draft_id and updates must be provided")

        payload = self._normalize_payload(updates)
        payload["updated_at"] = datetime.utcnow().isoformat()

        try:
            response = await (
                self.supabase_client._get_table("drafted_emails")
                .update(payload)
                .eq("id", draft_id)
                .execute()
            )
            self.supabase_client._log_operation("UPDATE", "drafted_emails", draft_id)
            return bool(response.data)
        except Exception as exc:
            self.supabase_client._log_operation(
                "UPDATE", "drafted_emails", draft_id, str(exc)
            )
            raise ValueError(f"Failed to update drafted email: {exc}") from exc

    async def delete_drafted_email(self, draft_id: str) -> bool:
        """Delete drafted email metadata."""
        if not draft_id:
            raise ValueError("draft_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("drafted_emails")
                .delete()
                .eq("id", draft_id)
                .execute()
            )
            self.supabase_client._log_operation("DELETE", "drafted_emails", draft_id)
            return bool(response.data)
        except Exception as exc:
            self.supabase_client._log_operation(
                "DELETE", "drafted_emails", draft_id, str(exc)
            )
            raise ValueError(f"Failed to delete drafted email: {exc}") from exc

    async def store_sent_email(self, email: SentEmail) -> str:
        """Store information about a sent email.

        Args:
            sent_email: The sent email data to store.

        Returns:
            The unique identifier of the sent email record.

        Raises:
            ValueError: If sent_email is invalid.
            Exception: If database insert fails after retries.

        Example:
            >>> from lib.models.database_schemas import SentEmailsRow
            >>> sent = SentEmailsRow(
            ...     drafted_email_id="draft-123",
            ...     to_address="recipient@example.com",
            ...     subject="Re: Subject",
            ...     body_text="Response",
            ...     sent_at=datetime.now(timezone.utc),
            ... )
            >>> sent_id = client.store_sent_email(sent)
        """
        return await self.create_sent_email(email)

    async def create_sent_email(self, sent_email: SentEmail) -> str:
        """Create a sent email record and return its Supabase ID."""
        if not sent_email:
            raise ValueError("sent_email must be provided")
        return await self.supabase_client._insert_model(
            "sent_emails", sent_email, "sent email"
        )

    async def get_sent_email(self, message_id: str) -> Optional[SentEmail]:
        """Fetch a sent email row by its primary key."""
        if not message_id:
            raise ValueError("message_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("sent_emails")
                .select("*")
                .eq("message_id", message_id)
                .single()
                .execute()
            )
            if response.data and isinstance(response.data, dict):
                self.supabase_client._log_operation("READ", "sent_emails", message_id)
                return SentEmail(**response.data)  # type: ignore
            return None
        except APIError as e:
            if e.code == "PGRST116":
                return None
            self.supabase_client._log_operation(
                "READ", "sent_emails", message_id, str(e)
            )
            raise
        except Exception as exc:
            self.supabase_client._log_operation(
                "READ", "sent_emails", message_id, str(exc)
            )
            raise

    async def update_sent_email(
        self,
        sent_id: str,
        updates: dict[str, Any],
    ) -> bool:
        """Update a sent email and return True if any row was modified."""
        if not sent_id or not updates:
            raise ValueError("sent_id and updates must be provided")

        payload = self._normalize_payload(updates)
        try:
            response = await (
                self.supabase_client._get_table("sent_emails")
                .update(payload)
                .eq("id", sent_id)
                .execute()
            )
            self.supabase_client._log_operation("UPDATE", "sent_emails", sent_id)
            return bool(response.data)
        except Exception as exc:
            self.supabase_client._log_operation(
                "UPDATE", "sent_emails", sent_id, str(exc)
            )
            raise ValueError(f"Failed to update sent email: {exc}") from exc

    async def delete_sent_email(self, sent_id: str) -> bool:
        """Delete a sent email entry from Supabase."""
        if not sent_id:
            raise ValueError("sent_id must be provided")

        try:
            response = await (
                self.supabase_client._get_table("sent_emails")
                .delete()
                .eq("id", sent_id)
                .execute()
            )
            self.supabase_client._log_operation("DELETE", "sent_emails", sent_id)
            return bool(response.data)
        except Exception as exc:
            self.supabase_client._log_operation(
                "DELETE", "sent_emails", sent_id, str(exc)
            )
            raise ValueError(f"Failed to delete sent email: {exc}") from exc

    async def match_tone_guide_emails(
        self,
        query_text: str,
        category_filter: Optional[str] = None,
        match_threshold: float = 0.5,
        match_count: int = 5,
    ) -> List[BaseEmail]:
        """Query the tone_guide_emails table using vector similarity search.

        Finds semantically similar tone guide emails for a given query text.
        The results are used as reference examples for email drafting.

        Args:
            query_text: The query text to find similar emails for (typically from
                the context or draft instructions).
            category_filter: Optional filter by category (e.g., 'crm', 'customer_success').
                If None, searches across all categories.
            match_threshold: Minimum similarity score (0.0-1.0) to include in results.
                Default is 0.5.
            match_count: Maximum number of results to return. Default is 5.

        Returns:
            List[BaseEmail]: List of matching tone guide emails mapped to BaseEmail objects.
                Empty list if no matches found or on error.

        Raises:
            No explicit exceptions; errors are logged and empty list returned.
        """
        # 1. Generate the embedding for the input query text
        try:
            openrouter_embedder = OpenRouterEmbeddings()
            query_vector: List[float] = await openrouter_embedder.aembed_query(
                query_text, model=get_settings().openrouter_embedding_model_786
            )
        except Exception as e:
            logger.error(
                "Error generating embedding for tone guide query: %s",
                e,
                exc_info=True,
            )
            return []

        # 2. Perform the vector search using RPC
        try:
            # Call the PostgreSQL function 'match_tone_guide_emails' defined in Supabase.
            # Params must match the arguments defined in the SQL function.
            params = {
                "category_filter": category_filter,
                "query_embedding": query_vector,
                "match_threshold": match_threshold,
                "match_count": match_count,
            }

            # Call the RPC function via Supabase client
            response = await self.supabase_client._client.rpc(
                "match_tone_guide_emails", params
            ).execute()

            # 3. Process and map the results to BaseEmail objects
            data = response.data
            results = []

            for item in data:  # type: ignore
                # Extract similarity score (returned by RPC function)
                similarity = item.pop("similarity", 0.0)
                passes_threshold = item.pop("passes_threshold", True)

                # Map the RPC response fields to BaseEmail fields
                # RPC returns: email_id, subject, body, sender, receiver, received_at, similarity, passes_threshold
                email_parsed = BaseEmail(
                    from_email=item.get("sender", ""),
                    to_emails=[item.get("receiver", "")]
                    if item.get("receiver")
                    else [],
                    subject=item.get("subject", ""),
                    body=item.get("body", ""),
                    metadata={
                        "similarity_score": similarity,
                        "passes_quality_threshold": passes_threshold,
                        "category_filter": category_filter,
                    },
                )
                results.append(email_parsed)

            logger.debug(
                "Found %d tone guide emails matching query (threshold=%.2f)",
                len(results),
                match_threshold,
            )
            return results

        except Exception as e:
            logger.error(
                "Error during tone guide email vector search: %s",
                e,
                exc_info=True,
            )
            return []
