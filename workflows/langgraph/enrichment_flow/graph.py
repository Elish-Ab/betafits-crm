#  workflows/langgraph/enrichment_flow/graph.py
import logging
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from workflows.langgraph.enrichment_flow.state import EnrichmentState

logger = logging.getLogger(__name__)


def build_enrichment_graph(checkpointer: AsyncSqliteSaver = None) -> CompiledStateGraph:
    from workflows.langgraph.enrichment_flow.nodes.company_matcher_node import (
        company_matcher_node,
    )
    from workflows.langgraph.enrichment_flow.nodes.crunchbase_discovery_node import (
        crunchbase_discovery_node,
    )
    from workflows.langgraph.enrichment_flow.nodes.linkedin_enrich_node import (
        linkedin_enrich_node,
    )
    from workflows.langgraph.enrichment_flow.nodes.glassdoor_enrich_node import (
        glassdoor_enrich_node,
    )
    from workflows.langgraph.enrichment_flow.nodes.write_supabase_node import (
        write_supabase_node,
    )
    from workflows.langgraph.enrichment_flow.nodes.update_airtable_snapshot_node import (
        update_airtable_snapshot_node,
    )
    from workflows.langgraph.enrichment_flow.nodes.error_handler_node import (
        error_handler_node,
    )

    if checkpointer is None:
        checkpointer = AsyncSqliteSaver("enrichment_flow_checkpoints.db")

    graph = StateGraph(EnrichmentState)

    graph.add_node("crunchbase_discovery", crunchbase_discovery_node)
    graph.add_node("company_matcher", company_matcher_node)
    graph.add_node("linkedin_enrich", linkedin_enrich_node)
    graph.add_node("glassdoor_enrich", glassdoor_enrich_node)
    graph.add_node("write_supabase", write_supabase_node)
    graph.add_node("update_airtable", update_airtable_snapshot_node)
    graph.add_node("error_handler", error_handler_node)

    graph.set_entry_point("crunchbase_discovery")
    graph.add_edge("crunchbase_discovery", "company_matcher")
    graph.add_edge("company_matcher", "linkedin_enrich")
    graph.add_edge("linkedin_enrich", "glassdoor_enrich")
    graph.add_edge("glassdoor_enrich", "write_supabase")
    graph.add_edge("write_supabase", "update_airtable")
    graph.add_edge("update_airtable", END)

    # Optional: send errors to handler
    graph.add_edge("error_handler", END)

    return graph.compile(checkpointer=checkpointer)  # type: ignore

# Lazy initialization: graph is created only when accessed at runtime
_graph = None

def graph() -> CompiledStateGraph:
    """Factory function that returns the compiled enrichment graph.
    
    This is called by LangGraph runtime when it needs the graph.
    The actual graph compilation is deferred until this function is called,
    ensuring it happens when an event loop is available.
    """
    global _graph
    if _graph is None:
        _graph = build_enrichment_graph()
    return _graph