"""Data Enrichment domain – LangGraph graph definition.

Graph topology
--------------
                              ┌─────────────────────────────────┐
                              │  trigger == "new_vc_partner"     │
                              │                                  │
scrape_crunchbase ──────────► deduplicate_portfolio              │
                                       │                         │
                                       ▼                         │
                              match_against_crm                  │
                                       │                         │
                                       ▼                         │
                              create_new_crm_records             │
                                       │                         │
                              ┌────────┴────────────────────────►│
                              │  trigger == "quarterly_refresh"  │
                              ▼                                  │
                         enrich_linkedin                         │
                              │                                  │
                              ▼                                  │
                         enrich_glassdoor                        │
                              │                                  │
                              ▼                                  │
                         write_enriched_data                     │
                              │                                  │
                              ▼                                  │
                             END                                 │
"""

from __future__ import annotations

from langgraph.graph import StateGraph, END

from core.state import DataEnrichmentState
from .nodes import (
    scrape_crunchbase,
    deduplicate_portfolio,
    match_against_crm,
    create_new_crm_records,
    enrich_linkedin,
    enrich_glassdoor,
    write_enriched_data,
)


def _route_entry(state: DataEnrichmentState) -> str:
    """Route to Crunchbase scrape for new VC partners, or directly to enrichment."""
    if state.trigger == "new_vc_partner" and state.crunchbase_url:
        return "scrape_crunchbase"
    return "enrich_linkedin"


def build_data_enrichment_graph() -> StateGraph:
    """Construct and compile the Data Enrichment LangGraph."""
    graph = StateGraph(DataEnrichmentState)

    graph.add_node("scrape_crunchbase", scrape_crunchbase)
    graph.add_node("deduplicate_portfolio", deduplicate_portfolio)
    graph.add_node("match_against_crm", match_against_crm)
    graph.add_node("create_new_crm_records", create_new_crm_records)
    graph.add_node("enrich_linkedin", enrich_linkedin)
    graph.add_node("enrich_glassdoor", enrich_glassdoor)
    graph.add_node("write_enriched_data", write_enriched_data)

    # Entry: conditional based on trigger type
    graph.set_conditional_entry_point(
        _route_entry,
        {
            "scrape_crunchbase": "scrape_crunchbase",
            "enrich_linkedin": "enrich_linkedin",
        },
    )

    # VC partner path
    graph.add_edge("scrape_crunchbase", "deduplicate_portfolio")
    graph.add_edge("deduplicate_portfolio", "match_against_crm")
    graph.add_edge("match_against_crm", "create_new_crm_records")
    graph.add_edge("create_new_crm_records", "enrich_linkedin")

    # Shared enrichment path
    graph.add_edge("enrich_linkedin", "enrich_glassdoor")
    graph.add_edge("enrich_glassdoor", "write_enriched_data")
    graph.add_edge("write_enriched_data", END)

    return graph.compile()


data_enrichment_graph = build_data_enrichment_graph()
