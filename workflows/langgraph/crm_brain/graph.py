"""CRM Brain domain – LangGraph graph definition.

Graph topology
--------------
assess_interaction
    │
    ▼
log_interaction
    │
    ▼
update_opportunity
    │
    ▼
schedule_follow_up ──(no follow-up needed)──► END
    │
  (follow-up needed)
    │
    ▼
END
"""

from __future__ import annotations

from langgraph.graph import StateGraph, END

from core.state import CRMBrainState
from .nodes import (
    assess_interaction,
    log_interaction,
    update_opportunity,
    schedule_follow_up,
)


def _route_follow_up(state: CRMBrainState) -> str:
    """Edge condition: only schedule a follow-up if one is required."""
    return "schedule_follow_up" if state.follow_up_scheduled else END


def build_crm_brain_graph() -> StateGraph:
    """Construct and compile the CRM Brain LangGraph."""
    graph = StateGraph(CRMBrainState)

    graph.add_node("assess_interaction", assess_interaction)
    graph.add_node("log_interaction", log_interaction)
    graph.add_node("update_opportunity", update_opportunity)
    graph.add_node("schedule_follow_up", schedule_follow_up)

    graph.set_entry_point("assess_interaction")

    graph.add_edge("assess_interaction", "log_interaction")
    graph.add_edge("log_interaction", "update_opportunity")

    graph.add_conditional_edges(
        "update_opportunity",
        _route_follow_up,
        {"schedule_follow_up": "schedule_follow_up", END: END},
    )

    graph.add_edge("schedule_follow_up", END)

    return graph.compile()


crm_brain_graph = build_crm_brain_graph()
