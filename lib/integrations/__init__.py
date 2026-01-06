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

# Optional imports - may not be available in all environments
_gmail_available = False
_openrouter_available = False
_vector_db_available = False
_graphiti_available = False

try:
    from lib.integrations.gmail_client import (
        GmailClient,
        get_gmail_client,
        reset_gmail_client,
    )
    _gmail_available = True
except ImportError:
    pass

try:
    from lib.integrations.openrouter_client import (
        OpenRouterClient,
        get_openrouter_client,
        reset_openrouter_client,
    )
    _openrouter_available = True
except ImportError:
    pass

try:
    from lib.integrations.vector_db_client import (
        VectorDBClient,
        get_vector_db_client,
    )
    _vector_db_available = True
except ImportError:
    pass

try:
    from lib.integrations.graphiti_client import (
        GraphitiClient,
        get_graphiti_client,
        reset_graphiti_client,
    )
    _graphiti_available = True
except ImportError:
    pass

__all__ = [
    "get_gmail_client",
    "reset_gmail_client",
    "OpenRouterClient",
    "get_openrouter_client",
    "reset_openrouter_client",
    "SupabaseClient",
    "get_supabase_client",
    "reset_supabase_client",
]

if _gmail_available:
    __all__.extend([
        "GmailClient",
        "get_gmail_client",
        "reset_gmail_client",
    ])

if _openrouter_available:
    __all__.extend([
        "OpenRouterClient",
        "get_openrouter_client",
        "reset_openrouter_client",
    ])

if _vector_db_available:
    __all__.extend([
        "VectorDBClient",
        "get_vector_db_client",
    ])

if _graphiti_available:
    __all__.extend([
        "GraphitiClient",
        "get_graphiti_client",
        "reset_graphiti_client",
    ])
