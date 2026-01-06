"""Integrations module for external services and APIs.

This module contains client implementations for interacting with external services
like Supabase, Graphiti, OpenRouter, Gmail, and Vector databases.
"""

from lib.integrations.openrouter_client import (
    OpenRouterClient,
    get_openrouter_client,
    reset_openrouter_client,
)
from lib.integrations.supabase.supabase_client import (
    SupabaseClient,
    get_supabase_client,
    reset_supabase_client,
)
from lib.integrations.vector_db_client import (
    VectorDBClient,
    get_vector_db_client,
)

# Graphiti client may not be available if graphiti_core is not installed
try:
    from lib.integrations.graphiti_client import (
        GraphitiClient,
        get_graphiti_client,
        reset_graphiti_client,
    )

    _graphiti_available = True
except ImportError:
    _graphiti_available = False

__all__ = [
    "get_gmail_client",
    "reset_gmail_client",
    "OpenRouterClient",
    "get_openrouter_client",
    "reset_openrouter_client",
    "SupabaseClient",
    "get_supabase_client",
    "reset_supabase_client",
    "VectorDBClient",
    "get_vector_db_client",
]

if _graphiti_available:
    __all__.extend(
        [
            "GraphitiClient",
            "get_graphiti_client",
            "reset_graphiti_client",
        ]
    )
