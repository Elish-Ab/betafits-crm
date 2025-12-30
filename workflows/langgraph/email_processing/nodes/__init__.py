"""LangGraph nodes for email processing workflow.

10-node pipeline for email ingestion, classification, enrichment, and response generation.

Node Exports:
- Node 1: email_router_node (email_router.py)
- Node 2: email_classifier_node (classifier.py)
- Node 3: entity_extractor_node (entity_extractor.py)
- Node 4: relation_extractor_node (relation_extractor.py)
- Node 5: kg_rag_updater_node (kg_rag_updater.py)
- Node 6: context_retriever_node (context_retriever.py)
- Node 7: response_drafter_node (response_drafter.py)
- Node 8: json_formatter_node (json_formatter.py)
- Node 9: email_sender_node (email_sender.py)
- Node 10: workflow_validator_node (workflow_validator.py)
"""

from workflows.langgraph.email_processing.nodes.classifier import (
    email_classifier_node,
)

from workflows.langgraph.email_processing.nodes.email_router import (
    email_router_node,
)
from workflows.langgraph.email_processing.nodes.kg_rag_updater import (
    kg_rag_updater_node,
)

from workflows.langgraph.email_processing.nodes.workflow_validator import (
    workflow_validator_node,
)

__all__ = [
    "email_router_node",
    "email_classifier_node",
    "kg_rag_updater_node",
    "workflow_validator_node",
]
