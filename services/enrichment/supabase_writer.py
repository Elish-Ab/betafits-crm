import logging
from typing import Any, Optional

from lib.integrations.supabase.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


def _resolve_company_id(company_id_value: Any) -> Optional[str]:
    if isinstance(company_id_value, str):
        company_id_value = company_id_value.strip()
        return company_id_value or None

    if isinstance(company_id_value, dict):
        existing_ids = company_id_value.get("existing_ids")
        new_ids = company_id_value.get("new_ids")

        for candidates in (existing_ids, new_ids):
            if isinstance(candidates, list) and candidates:
                first = candidates[0]
                if isinstance(first, str) and first.strip():
                    return first.strip()

    return None


async def write_company_enrichment(
    payload: dict[str, Any],
    run_id: Optional[str] = None,
) -> bool:
    company_id = _resolve_company_id(payload.get("company_id"))
    if not company_id:
        logger.warning(
            "[Enrichment] Supabase write skipped: no company_id available",
            extra={"run_id": run_id},
        )
        return False

    client = await get_supabase_client()
    table = (
        client._get_table("companies")
        if hasattr(client, "_get_table")
        else client.table("companies")
    )

    enrichment_payload = {
        "crunchbase": payload.get("crunchbase"),
        "linkedin": payload.get("linkedin"),
        "glassdoor": payload.get("glassdoor"),
    }
    if run_id:
        enrichment_payload["run_id"] = run_id

    existing_metadata: dict[str, Any] = {}
    try:
        response = await table.select("metadata").eq("id", company_id).limit(1).execute()
        if (
            hasattr(response, "data")
            and isinstance(response.data, list)
            and response.data
            and isinstance(response.data[0], dict)
        ):
            metadata_value = response.data[0].get("metadata")
            if isinstance(metadata_value, dict):
                existing_metadata = metadata_value
    except Exception:
        existing_metadata = {}

    merged_metadata: dict[str, Any] = dict(existing_metadata) if isinstance(existing_metadata, dict) else {}
    existing_enrichment = merged_metadata.get("enrichment")
    if not isinstance(existing_enrichment, dict):
        existing_enrichment = {}

    existing_enrichment.update({k: v for k, v in enrichment_payload.items() if v is not None})
    merged_metadata["enrichment"] = existing_enrichment

    try:
        await table.update({"metadata": merged_metadata}).eq("id", company_id).execute()
        if hasattr(client, "_log_operation"):
            client._log_operation("UPDATE", "companies", company_id)
        return True
    except Exception as exc:
        try:
            direct_updates = {k: v for k, v in enrichment_payload.items() if k != "run_id" and v is not None}
            if direct_updates:
                await table.update(direct_updates).eq("id", company_id).execute()
                if hasattr(client, "_log_operation"):
                    client._log_operation("UPDATE", "companies", company_id)
                return True
        except Exception:
            pass

        if hasattr(client, "_log_operation"):
            client._log_operation("UPDATE", "companies", company_id, str(exc))

        logger.error(
            "[Enrichment] Supabase write failed",
            extra={"run_id": run_id, "company_id": company_id},
            exc_info=True,
        )
        return False
