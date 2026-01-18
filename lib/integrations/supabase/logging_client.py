"""Supabase logging client for LangGraph workflow run tracking.

This module provides a specialized client for CRUD operations on the LGRuns table,
which tracks the execution of LangGraph workflows, including status, inputs, outputs,
and error information.

Usage:
    from lib.integrations.supabase.logging_client import get_logging_client
    from lib.models.database_schemas import LGRun, LGRunStatus, LGTriggerType, LGEnvironment
    from uuid import uuid4
    from datetime import datetime, timezone

    client = await get_logging_client()

    # Create a new run
    new_run = LGRun(
        run_code="run_20240101_001",
        workflow_id=uuid4(),
        triggered_by=LGTriggerType.api,
        status=LGRunStatus.running,
        environment=LGEnvironment.prod,
    )
    run_id = await client.create_lg_run(new_run)

    # Retrieve a run
    run = await client.get_lg_run_by_id(run_id)

    # Update run status
    await client.update_lg_run_status(run_id, LGRunStatus.completed)

    # List runs for a workflow
    runs = await client.list_lg_runs_by_workflow(workflow_id)
"""

import logging
from typing import Any, Dict, Optional
from uuid import UUID

from lib.integrations.supabase.supabase_client import (
    SupabaseClient,
    get_supabase_client,
)
from lib.models.database_schemas import LGRun, LGRunStatus

logger = logging.getLogger(__name__)


class LoggingDBClient:
    """Specialized Supabase client for LGRun CRUD operations.

    Manages workflow execution logs and run tracking for LangGraph workflows.

    Attributes:
        _supabase_client: The underlying Supabase client instance.
        _table_name: The database table name for runs.
        _schema: The database schema.
    """

    supabase_client: SupabaseClient

    @classmethod
    async def create(cls) -> "LoggingDBClient":
        client = cls()
        await client.init_client()
        return client

    async def init_client(self):
        """create the Supabase AsyncClient."""
        if hasattr(self, "supabase_client") and self.supabase_client is not None:
            return
        else:
            self.supabase_client = await get_supabase_client()

    async def create_lg_run(self, run: LGRun) -> str:
        """Create a new LGRun record.

        Args:
            run: The LGRun model instance to create.

        Returns:
            The ID of the created run.

        Raises:
            ValueError: If the run is invalid or creation fails.
        """
        if not run:
            raise ValueError("LGRun must be provided")

        table = self.supabase_client._get_table("lg_runs")
        payload_dict = run.model_dump(
            by_alias=True,
            exclude_unset=True,
            exclude_none=False,
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
                        self.supabase_client._log_operation(
                            "INSERT", "lg_runs", str(record_id)
                        )
                        return str(record_id)
            raise ValueError("No ID returned from insert operation")
        except Exception as exc:
            self.supabase_client._log_operation("INSERT", "lg_runs", None, str(exc))
            raise ValueError(f"Failed to create LGRun: {exc}") from exc

    async def get_lg_run_by_id(self, run_id: str) -> Optional[LGRun]:
        """Retrieve an LGRun by ID.

        Args:
            run_id: The ID of the run to retrieve.

        Returns:
            The LGRun instance if found, None otherwise.

        Raises:
            ValueError: If retrieval fails.
        """
        if not run_id:
            raise ValueError("run_id must be provided")

        table = self.supabase_client._get_table("lg_runs")
        try:
            response = await table.select("*").eq("id", run_id).execute()
            if response.data and len(response.data) > 0:
                self.supabase_client._log_operation("SELECT", "lg_runs", run_id)
                return LGRun.model_validate(response.data[0])
            return None
        except Exception as exc:
            self.supabase_client._log_operation("SELECT", "lg_runs", run_id, str(exc))
            raise ValueError(f"Failed to retrieve LGRun: {exc}") from exc

    async def list_lg_runs_by_workflow(
        self,
        workflow_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[LGRun]:
        """List all runs for a specific workflow.

        Args:
            workflow_id: The workflow ID to filter by.
            limit: Maximum number of results to return.
            offset: Number of results to skip for pagination.

        Returns:
            A list of LGRun instances matching the workflow.

        Raises:
            ValueError: If retrieval fails.
        """
        if not workflow_id:
            raise ValueError("workflow_id must be provided")

        table = self.supabase_client._get_table("lg_runs")
        try:
            response = (
                await table.select("*")
                .eq("workflow_id", str(workflow_id))
                .order("created_at", desc=True)
                .range(offset, offset + limit - 1)
                .execute()
            )
            self.supabase_client._log_operation("SELECT", "lg_runs", str(workflow_id))
            return [LGRun.model_validate(row) for row in response.data]
        except Exception as exc:
            self.supabase_client._log_operation("SELECT", "lg_runs", str(workflow_id), str(exc))
            raise ValueError(f"Failed to list LGRuns: {exc}") from exc

    async def list_lg_runs_by_status(
        self,
        status: LGRunStatus,
        limit: int = 100,
        offset: int = 0,
    ) -> list[LGRun]:
        """List all runs with a specific status.

        Args:
            status: The status to filter by.
            limit: Maximum number of results to return.
            offset: Number of results to skip for pagination.

        Returns:
            A list of LGRun instances matching the status.

        Raises:
            ValueError: If retrieval fails.
        """
        if not status:
            raise ValueError("status must be provided")

        table = self.supabase_client._get_table("lg_runs")
        try:
            response = (
                await table.select("*")
                .eq("status", status.value)
                .order("created_at", desc=True)
                .range(offset, offset + limit - 1)
                .execute()
            )
            self.supabase_client._log_operation("SELECT", "lg_runs", None)
            return [LGRun.model_validate(row) for row in response.data]
        except Exception as exc:
            self.supabase_client._log_operation("SELECT", "lg_runs", None, str(exc))
            raise ValueError(f"Failed to list LGRuns by status: {exc}") from exc

    async def update_lg_run(self, run_id: str, run: LGRun) -> LGRun:
        """Update an existing LGRun record.

        Args:
            run_id: The ID of the run to update.
            run: The updated LGRun model instance.

        Returns:
            The updated LGRun instance.

        Raises:
            ValueError: If update fails or run not found.
        """
        if not run_id:
            raise ValueError("run_id must be provided")
        if not run:
            raise ValueError("LGRun must be provided")

        table = self.supabase_client._get_table("lg_runs")
        payload_dict = run.model_dump(
            by_alias=True,
            exclude_unset=False,
            exclude_none=False,
            mode="json",
        )

        try:
            response = await table.update(payload_dict).eq("id", run_id).execute()
            if response.data and len(response.data) > 0:
                self.supabase_client._log_operation("UPDATE", run_id)
                return LGRun.model_validate(response.data[0])
            raise ValueError("Run not found or update returned no data")
        except Exception as exc:
            self.supabase_client._log_operation("UPDATE", run_id, str(exc))
            raise ValueError(f"Failed to update LGRun: {exc}") from exc

    async def update_lg_run_status(
        self,
        run_id: str,
        status: LGRunStatus,
    ) -> None:
        """Update only the status of an LGRun.

        Args:
            run_id: The ID of the run to update.
            status: The new status value.

        Raises:
            ValueError: If update fails.
        """
        if not run_id:
            raise ValueError("run_id must be provided")
        if not status:
            raise ValueError("status must be provided")

        table = self.supabase_client._get_table("lg_runs")
        try:
            await table.update({"status": status.value}).eq("id", run_id).execute()
            self.supabase_client._log_operation("UPDATE", run_id)
        except Exception as exc:
            self.supabase_client._log_operation("UPDATE", run_id, str(exc))
            raise ValueError(f"Failed to update LGRun status: {exc}") from exc

    async def update_lg_run_completion(
        self,
        run_id: str,
        status: LGRunStatus,
        output_summary: Optional[str] = None,
        output_payload: Optional[dict] = None,
        ended_at: Optional[str] = None,
    ) -> None:
        """Update an LGRun with completion details.

        Args:
            run_id: The ID of the run to update.
            status: The final status (completed, failed, cancelled, etc).
            output_summary: Optional summary of the output.
            output_payload: Optional output payload.
            ended_at: Optional end timestamp.

        Raises:
            ValueError: If update fails.
        """
        if not run_id:
            raise ValueError("run_id must be provided")
        if not status:
            raise ValueError("status must be provided")

        table = self.supabase_client._get_table("lg_runs")
        update_dict: Dict[str, Any] = {"status": status.value}

        if output_summary is not None:
            update_dict["output_summary"] = output_summary
        if output_payload is not None:
            update_dict["output_payload"] = output_payload
        if ended_at is not None:
            update_dict["ended_at"] = ended_at

        try:
            await table.update(update_dict).eq("id", run_id).execute()
            self.supabase_client._log_operation("UPDATE", run_id)
        except Exception as exc:
            self.supabase_client._log_operation("UPDATE", run_id, str(exc))
            raise ValueError(f"Failed to update LGRun completion: {exc}") from exc

    async def update_lg_run_error(
        self,
        run_id: str,
        error_message: str,
        error_payload: Optional[dict] = None,
    ) -> None:
        """Update an LGRun with error details.

        Args:
            run_id: The ID of the run to update.
            error_message: Error message.
            error_payload: Optional error payload with details.

        Raises:
            ValueError: If update fails.
        """
        if not run_id:
            raise ValueError("run_id must be provided")
        if not error_message:
            raise ValueError("error_message must be provided")

        table = self.supabase_client._get_table("lg_runs")
        update_dict: Dict[str, Any] = {
            "status": LGRunStatus.failed.value,
            "error_message": error_message,
        }

        if error_payload is not None:
            update_dict["error_payload"] = error_payload

        try:
            await table.update(update_dict).eq("id", run_id).execute()
            self.supabase_client._log_operation("UPDATE", run_id)
        except Exception as exc:
            self.supabase_client._log_operation("UPDATE", run_id, str(exc))
            raise ValueError(f"Failed to update LGRun error: {exc}") from exc

    async def delete_lg_run(self, run_id: str) -> None:
        """Delete an LGRun record.

        Args:
            run_id: The ID of the run to delete.

        Raises:
            ValueError: If deletion fails.
        """
        if not run_id:
            raise ValueError("run_id must be provided")

        table = self.supabase_client._get_table("lg_runs")
        try:
            await table.delete().eq("id", run_id).execute()
            self.supabase_client._log_operation("DELETE", run_id)
        except Exception as exc:
            self.supabase_client._log_operation("DELETE", run_id, str(exc))
            raise ValueError(f"Failed to delete LGRun: {exc}") from exc

    async def get_latest_run(self, workflow_id: UUID) -> Optional[LGRun]:
        """Get the most recent run for a workflow.

        Args:
            workflow_id: The workflow ID to filter by.

        Returns:
            The most recent LGRun instance, or None if no runs exist.

        Raises:
            ValueError: If retrieval fails.
        """
        if not workflow_id:
            raise ValueError("workflow_id must be provided")

        table = self.supabase_client._get_table("lg_runs")
        try:
            response = (
                await table.select("*")
                .eq("workflow_id", str(workflow_id))
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
            if response.data and len(response.data) > 0:
                self.supabase_client._log_operation("SELECT", "lg_runs", str(workflow_id))
                return LGRun.model_validate(response.data[0])
            return None
        except Exception as exc:
            self.supabase_client._log_operation("SELECT", "lg_runs", str(workflow_id), str(exc))
            raise ValueError(f"Failed to get latest LGRun: {exc}") from exc

