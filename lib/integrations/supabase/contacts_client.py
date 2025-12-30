
import logging


from lib.integrations.supabase.supabase_client import SupabaseClient, get_supabase_client
from lib.models.database_schemas import (
    Contact,
    OpportunityContactLink,
)

logger = logging.getLogger(__name__)


class ContactDBClient:

    supabase_client: SupabaseClient

    @classmethod
    async def create(cls) -> "ContactDBClient":
        client = cls()
        await client.init_client()
        return client

    async def init_client(self):
        """create the Supabase AsyncClient."""
        if hasattr(self, "supabase_client") and self.supabase_client is not None:
            return
        else:
            self.supabase_client = await get_supabase_client()


    async def upsert_contact(self, contact: Contact) -> str:
        """Insert or update a contact record.

        Args:
            contact: Contact information to persist. If `id` is supplied the
                record is updated, otherwise the unique email constraint is used.

        Returns:
            The Supabase contact id.

        Raises:
            ValueError: If contact data is invalid or the upsert fails.
        """
        if not contact:
            raise ValueError("contact must be provided")

        if not contact.email:
            raise ValueError("contact.email is required for upsert")

        payload = contact.model_dump(by_alias=True, exclude_unset=True, mode="json")

        on_conflict = "id" if payload.get("id") else "email"
        if on_conflict == "email":
            payload.pop("id", None)

        try:
            response = await (
                self.supabase_client._get_table("contacts")
                .upsert(payload, on_conflict=on_conflict)
                .execute()
            )

            rows = response.data if isinstance(response.data, list) else []
            if rows:
                row = rows[0]
                if isinstance(row, dict):
                    contact_id = row.get("id")
                    if contact_id:
                        self.supabase_client._log_operation("UPSERT", "contacts", str(contact_id))
                        return str(contact_id)

            if payload.get("id"):
                contact_id = str(payload["id"])
                self.supabase_client._log_operation("UPSERT", "contacts", contact_id)
                return contact_id

            raise ValueError("No ID returned from contact upsert operation")

        except Exception as e:
            self.supabase_client._log_operation("UPSERT", "contacts", payload.get("id"), str(e))
            raise

    async def link_contact_to_opportunity(
        self,
        opportunity_id: str,
        contact_id: str,
    ) -> None:
        """Ensure a contact ↔ opportunity link exists."""

        if not opportunity_id or not isinstance(opportunity_id, str):
            raise ValueError("opportunity_id must be a non-empty string")
        if not contact_id or not isinstance(contact_id, str):
            raise ValueError("contact_id must be a non-empty string")

        table_name = "opportunity_contact"

        try:
            existing = await (
                self.supabase_client._get_table(table_name)
                .select("opportunity_id,contact_id")
                .eq("opportunity_id", opportunity_id)
                .eq("contact_id", contact_id)
                .limit(1)
                .execute()
            )

            if isinstance(existing.data, list) and existing.data:
                self.supabase_client._log_operation(
                    "READ",
                    table_name,
                    f"{opportunity_id}:{contact_id}",
                )
                return

            payload = OpportunityContactLink(
                opportunity_id=opportunity_id,
                contact_id=contact_id,
            ).model_dump(by_alias=True, exclude_unset=True, mode="json")

            await self.supabase_client._get_table(table_name).insert(payload).execute()
            self.supabase_client._log_operation(
                "INSERT",
                table_name,
                f"{opportunity_id}:{contact_id}",
            )

        except Exception as e:
            self.supabase_client._log_operation(
                "INSERT",
                table_name,
                f"{opportunity_id}:{contact_id}",
                str(e),
            )
            raise
