import logging
from datetime import datetime, timezone
from typing import Any, List


from lib.integrations.supabase.supabase_client import SupabaseClient, get_supabase_client
from lib.integrations.vector_db_client import OpenRouterEmbeddings
from lib.models.database_schemas import (
    Opportunity,
)

logger = logging.getLogger(__name__)


class OpportuityDBClient:
    supabase_client: SupabaseClient

    @classmethod
    async def create(cls) -> "OpportuityDBClient":
        client = cls()
        await client.init_client()
        return client

    async def init_client(self):
        """create the Supabase AsyncClient."""
        if hasattr(self, "supabase_client") and self.supabase_client is not None:
            return
        else:
            self.supabase_client = await get_supabase_client()


    async def fetch_opportunities_by_contact_email(
        self, email_address: str
    ) -> List[Opportunity]:
        """
        Fetches opportunities associated with a contact's email address
        by performing chained joins across three tables.

        :param email_address: The email address of the contact to search for.
        :return: A list of Opportunity objects.
        """
        try:
            response = await (
                self.supabase_client._get_table("opportunities")
                .select(
                    "id, title, summary, created_at, contacts!inner(email)"  # Select opportunity fields
                )
                .eq(
                    "contacts.email", email_address
                )  # Filter on the joined 'contacts' email
                .execute()
            )

            # Handle successful response
            data = response.data

            # Map the dictionary data returned by Supabase to your Pydantic model
            opportunities = [Opportunity.model_validate(item) for item in data]

            return opportunities

        except Exception as e:
            print(f"An error occurred while fetching opportunities: {e}")
            return []

    async def fetch_opportunities_by_ids(self, ids: List[str]) -> List[Opportunity]:
        """
        Fetch opportunities from the `opportunities` table by a list of opportunity IDs.

        Args:
            ids: List of opportunity `id` values to fetch.

        Returns:
            A list of validated `Opportunity` objects. Returns an empty list if
            no matching rows are found or on error.
        """
        if not ids:
            return []

        try:
            response = await (
                self.supabase_client._get_table("opportunities").select("*").in_("id", ids).execute()
            )

            data = response.data if isinstance(response.data, list) else []
            opportunities = [
                Opportunity.model_validate(item)
                for item in data
                if isinstance(item, dict)
            ]
            return opportunities

        except Exception as e:
            self.supabase_client._log_operation("READ", "opportunities", None, str(e))
            return []


    async def insert_opportunity(self, opportunity: Opportunity) -> str:
        """Insert a new opportunity into the opportunities table.

        Args:
            opportunity: Opportunity Pydantic model to insert.

        Returns:
            The inserted opportunity `id` as a string.

        Raises:
            ValueError: If insertion fails or no id is returned.
        """
        if not opportunity:
            raise ValueError("opportunity must be provided")

        try:
            opp_dict = opportunity.model_dump(
                by_alias=True,
                exclude_unset=True,
                mode="json",
            )
            response = await self.supabase_client._get_table("opportunities").insert(opp_dict).execute()
            if (
                response.data
                and isinstance(response.data, list)
                and len(response.data) > 0
            ):
                row = response.data[0]
                if isinstance(row, dict):
                    opp_id = row.get("id")
                    if opp_id:
                        self.supabase_client._log_operation("INSERT", "opportunities", str(opp_id))
                        return str(opp_id)
            raise ValueError("No ID returned from insert operation")
        except Exception as e:
            self.supabase_client._log_operation("INSERT", "opportunities", None, str(e))
            raise

    async def upsert_opportunity(self, opportunity: Opportunity) -> str:
        """Insert or update an opportunity using its ID (upsert).

        Args:
            opportunity: Opportunity to upsert. If `id` is provided, the existing
                row is updated; otherwise a new row is created.

        Returns:
            The opportunity `id` after the upsert.

        Raises:
            ValueError: When the payload is invalid or no `id` is returned.
        """
        if not opportunity:
            raise ValueError("opportunity must be provided")

        try:
            opp_dict = opportunity.model_dump(
                by_alias=True,
                exclude_unset=True,
                mode="json",
            )
            opp_id = opp_dict.get("id")
            if not opp_id:
                opp_dict.pop("id", None)

            response = (
                await self.supabase_client._get_table("opportunities")
                .upsert(opp_dict, on_conflict="id")
                .execute()
            )

            data = response.data
            rows = data if isinstance(data, list) else []
            if rows:
                first = rows[0]
                if isinstance(first, dict):
                    final_id = first.get("id") or opp_id
                    if final_id:
                        self.supabase_client._log_operation("UPSERT", "opportunities", str(final_id))
                        return str(final_id)
            if opp_id:
                self.supabase_client._log_operation("UPSERT", "opportunities", str(opp_id))
                return str(opp_id)
            raise ValueError("No ID returned from upsert operation")
        except Exception as e:
            self.supabase_client._log_operation("UPSERT", "opportunities", None, str(e))
            raise

    async def update_opportunity(
        self, opportunity_id: str, updates: dict[str, Any]
    ) -> bool:
        """Update an existing opportunity by ID.

        Args:
            opportunity_id: The UUID of the opportunity to update.
            updates: Partial mapping of fields to update.

        Returns:
            True if the update affected a row, False if no matching row found.

        Raises:
            ValueError: If inputs are invalid.
            Exception: If the database update fails.
        """
        if not opportunity_id or not isinstance(opportunity_id, str):
            raise ValueError("opportunity_id must be a non-empty string")
        if not updates or not isinstance(updates, dict):
            raise ValueError("updates must be a non-empty dict of fields to update")

        try:
            # Ensure updated_at is set
            if "updated_at" not in updates:
                updates["updated_at"] = datetime.now(timezone.utc).isoformat()

            response = await (
                self.supabase_client._get_table("opportunities")
                .update(updates)
                .eq("id", opportunity_id)
                .execute()
            )

            self.supabase_client._log_operation("UPDATE", "opportunities", opportunity_id)
            return bool(response.data)
        except Exception as e:
            self.supabase_client._log_operation("UPDATE", "opportunities", opportunity_id, str(e))
            raise

    async def find_similar_opportunities(
        self,
        query_text: str,
        n_results: int = 5,
    ) -> List[tuple[float, Opportunity]]:
        """
        Performs a vector search to find the top N opportunities semantically similar
        to the input query text.
        """
        # 1. Generate the embedding for the input query text
        try:
            openrouter_embedder = OpenRouterEmbeddings()
            query_vector: List[float] = await openrouter_embedder.aembed_query(
                query_text
            )
        except Exception as e:
            print(f"Error generating embedding for query: {e}")
            return []

        # 2. Perform the Vector Search using RPC
        try:
            # We call the PostgreSQL function 'match_opportunities' defined in Step 1.
            # Params must match the arguments defined in the SQL function.
            params = {
                "query_embedding": query_vector,
                "match_threshold": 0.5,  # Adjust threshold as needed
                "match_count": n_results,
            }

            # NOTE: Make sure to access the root client for RPC calls,
            # not a specific table builder.
            # Assuming 'self.supabase' or similar is your Supabase client instance.
            response = await self.supabase_client._client.rpc("match_opportunities", params).execute()

            # 3. Process and map the results
            data = response.data
            results = []

            for item in data:  # type: ignore
                # The RPC function already calculates 'similarity', so we extract it
                similarity = item.pop("similarity", 0.0)

                # Use Pydantic to validate and map the remaining data
                # Ensure your Opportunity model can handle the fields returned by the RPC
                opportunity = Opportunity.model_validate(item)
                results.append((similarity, opportunity))

            return results

        except Exception as e:
            print(f"An error occurred during vector search: {e}")
            return []