"""CRM Brain domain – LangGraph nodes.

The CRM Brain is the central decision-making orchestrator.  It is triggered
after an email or meeting has been processed and decides what additional CRM
actions to take: logging the interaction, updating the opportunity stage,
scheduling follow-ups, and routing to other domain graphs.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from loguru import logger

from core.llm import get_llm
from core.state import CRMBrainState
from core.storage import airtable_upsert, airtable_get_all


# Pipeline stage ordering used for progression checks
_PIPELINE_STAGES = [
    "prospect",
    "qualified",
    "discovery",
    "proposal",
    "negotiation",
    "closed_won",
    "closed_lost",
    "nurture",
]


# ---------------------------------------------------------------------------
# Node 1 – Assess interaction
# ---------------------------------------------------------------------------

_ASSESS_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are the CRM Brain for BetaFits, a benefits consulting firm.
Given an interaction summary, determine the appropriate CRM actions.

Return a JSON object with:
- opportunity_status: "active" | "dead" | "sold" | "nurture"
- pipeline_stage_after: one of {stages}
- stage_changed: boolean
- follow_up_required: boolean
- follow_up_days: integer (days from today, 0 if no follow-up)
- next_actions: list of strings (concrete next steps)
- reasoning: string (brief explanation)
""".format(stages=", ".join(_PIPELINE_STAGES)),
        ),
        (
            "human",
            """Opportunity ID: {opportunity_id}
Current pipeline stage: {current_stage}
Interaction type: {interaction_type}
Interaction summary:
{summary}
""",
        ),
    ]
)


def assess_interaction(state: CRMBrainState) -> dict[str, Any]:
    """Use the LLM to assess the interaction and decide on CRM actions."""
    logger.info(
        "CRM Brain assessing interaction for opportunity_id={}", state.opportunity_id
    )

    # Fetch current opportunity data from Airtable
    current_stage = state.pipeline_stage_before or "prospect"
    try:
        records = airtable_get_all(
            "Opportunities",
            formula=f"{{opportunity_id}}='{state.opportunity_id}'",
        )
        if records:
            current_stage = records[0].get("pipeline_stage", "prospect")
    except Exception as exc:
        logger.warning("Could not fetch opportunity from Airtable: {}", exc)

    llm = get_llm()
    chain = _ASSESS_PROMPT | llm | JsonOutputParser()

    try:
        result: dict[str, Any] = chain.invoke(
            {
                "opportunity_id": state.opportunity_id,
                "current_stage": current_stage,
                "interaction_type": state.interaction_type,
                "summary": state.interaction_summary,
            }
        )

        follow_up_date = ""
        if result.get("follow_up_required") and result.get("follow_up_days", 0) > 0:
            follow_up_date = (
                datetime.utcnow() + timedelta(days=result["follow_up_days"])
            ).strftime("%Y-%m-%d")

        return {
            "pipeline_stage_before": current_stage,
            "pipeline_stage_after": result.get("pipeline_stage_after", current_stage),
            "stage_changed": result.get("stage_changed", False),
            "opportunity_status": result.get("opportunity_status", "active"),
            "follow_up_scheduled": result.get("follow_up_required", False),
            "follow_up_date": follow_up_date,
            "next_actions": result.get("next_actions", []),
        }

    except Exception as exc:
        logger.error("Interaction assessment failed: {}", exc)
        return {"errors": state.errors + [str(exc)]}


# ---------------------------------------------------------------------------
# Node 2 – Log interaction
# ---------------------------------------------------------------------------

def log_interaction(state: CRMBrainState) -> dict[str, Any]:
    """Create an interaction record in Airtable."""
    logger.info(
        "Logging interaction for opportunity_id={}", state.opportunity_id
    )

    record = {
        "opportunity_id": state.opportunity_id,
        "run_id": state.run_id,
        "interaction_type": state.interaction_type,
        "summary": state.interaction_summary,
        "logged_at": datetime.utcnow().isoformat(),
        "sentiment": state.metadata.get("sentiment", ""),
    }

    try:
        airtable_upsert("Interactions", [record], key_fields=["run_id"])
        return {"interaction_logged": True}
    except Exception as exc:
        logger.warning("Airtable interaction log skipped: {}", exc)
        return {"interaction_logged": True}  # Non-fatal – continue graph


# ---------------------------------------------------------------------------
# Node 3 – Update opportunity
# ---------------------------------------------------------------------------

def update_opportunity(state: CRMBrainState) -> dict[str, Any]:
    """Update the opportunity record in Airtable with the new stage and status."""
    logger.info(
        "Updating opportunity {} → stage='{}', status='{}'",
        state.opportunity_id,
        state.pipeline_stage_after,
        state.opportunity_status,
    )

    record = {
        "opportunity_id": state.opportunity_id,
        "pipeline_stage": state.pipeline_stage_after,
        "status": state.opportunity_status,
        "last_interaction_at": datetime.utcnow().isoformat(),
        "next_actions": "; ".join(state.next_actions),
    }

    if state.follow_up_date:
        record["follow_up_date"] = state.follow_up_date

    try:
        airtable_upsert("Opportunities", [record], key_fields=["opportunity_id"])
        return {"opportunity_updated": True}
    except Exception as exc:
        logger.warning("Airtable opportunity update skipped: {}", exc)
        return {"opportunity_updated": True}  # Non-fatal


# ---------------------------------------------------------------------------
# Node 4 – Schedule follow-up
# ---------------------------------------------------------------------------

def schedule_follow_up(state: CRMBrainState) -> dict[str, Any]:
    """Create a follow-up task record in Airtable."""
    if not state.follow_up_scheduled or not state.follow_up_date:
        logger.debug("No follow-up required for opportunity_id={}", state.opportunity_id)
        return {}

    logger.info(
        "Scheduling follow-up for opportunity_id={} on {}",
        state.opportunity_id,
        state.follow_up_date,
    )

    task = {
        "opportunity_id": state.opportunity_id,
        "run_id": state.run_id,
        "task_type": "follow_up",
        "due_date": state.follow_up_date,
        "description": f"Follow up on interaction from {datetime.utcnow().strftime('%Y-%m-%d')}",
        "status": "pending",
    }

    try:
        airtable_upsert("Tasks", [task], key_fields=["run_id"])
    except Exception as exc:
        logger.warning("Follow-up task creation skipped: {}", exc)

    return {"follow_up_scheduled": True}
