"""Vector database client for Supabase pgvector semantic search using LangChain.

This module provides a wrapper around LangChain's SupabaseVectorStore for
storing and retrieving embeddings with semantic similarity search.

The client handles:
- Upsert embeddings (1536-dimensional vectors from OpenRouter)
- Semantic search with optional metadata filtering
- Batch operations for efficiency
- Embedding generation via OpenRouter

Usage:
    from lib.integrations.vector_db_client import get_vector_db_client

    client = get_vector_db_client()
    results = await client.query_rag(
        query_text="find related emails",
        k=5,
        metadata_filter={"classification": "crm"}
    )
"""

import logging
from turtle import mode
from typing import Any, Dict, List, Optional, cast

from langchain_community.vectorstores import SupabaseVectorStore
from langchain_core.embeddings import Embeddings
from supabase import Client

from lib.config import get_settings
from lib.integrations.openrouter_client import get_openrouter_client
from lib.utils.retry import retry_on_exception

logger = logging.getLogger(__name__)

# Embedding dimension from OpenRouter
EMBEDDING_DIMENSION = 1536


class OpenRouterEmbeddings(Embeddings):
    """LangChain-compatible embeddings wrapper for OpenRouter client.

    Adapts the OpenRouterClient to work with LangChain's Embeddings interface,
    enabling seamless integration with SupabaseVectorStore.

    Attributes:
        client: OpenRouterClient instance for generating embeddings.
    """

    def __init__(self, client=None) -> None:
        """Initialize OpenRouter embeddings wrapper.

        Args:
            client: OpenRouterClient instance. If None, uses global instance.
        """
        self.client = client or get_openrouter_client()

    def embed_documents(self, texts: list[str], model: Optional[str] = None) -> list[list[float]]:
        """Embed documents (batch embedding).

        Args:
            texts: List of text strings to embed.

        Returns:
            List of embedding vectors (each 1536-dimensional).

        Raises:
            ValueError: If embedding fails.
        """
        # This is blocking, so we'll use the underlying HTTP call directly
        try:
            import asyncio

            loop = asyncio.new_event_loop()
            result = loop.run_until_complete(self.client.embed(text_list=texts, model=model))
            return [r["embedding"] for r in result]
        except Exception as error:
            logger.error(f"Failed to embed documents: {error}")
            raise ValueError(f"Failed to embed documents: {error}") from error

    def embed_query(self, text: str, model: Optional[str] = None) -> list[float]:
        """Embed a single query text.

        Args:
            text: Query text to embed.

        Returns:
            Embedding vector (1536-dimensional).

        Raises:
            ValueError: If embedding fails.
        """
        try:
            import asyncio

            loop = asyncio.new_event_loop()
            result = loop.run_until_complete(self.client.embed(text_list=[text], model=model))
            if result and "embedding" in result[0]:
                return result[0]["embedding"]
            raise ValueError("Failed to generate embedding")
        except Exception as error:
            logger.error(f"Failed to embed query: {error}")
            raise ValueError(f"Failed to embed query: {error}") from error

    async def aembed_documents(self, texts: list[str], model: Optional[str] = None) -> list[list[float]]:
        """Embed documents (batch embedding).

        Args:
            texts: List of text strings to embed.

        Returns:
            List of embedding vectors (each 1536-dimensional).

        Raises:
            ValueError: If embedding fails.
        """
        # This is blocking, so we'll use the underlying HTTP call directly
        try:
            result = await self.client.embed(text_list=texts, model=model)
            return [r["embedding"] for r in result]
        except Exception as error:
            logger.error(f"Failed to embed documents: {error}")
            raise ValueError(f"Failed to embed documents: {error}") from error

    async def aembed_query(self, text: str, model: Optional[str] = None) -> list[float]:
        """Embed a single query text.

        Args:
            text: Query text to embed.

        Returns:
            Embedding vector (1536-dimensional).

        Raises:
            ValueError: If embedding fails.
        """
        try:
            result = await self.client.embed(text_list=[text], model=model)
            if result and "embedding" in result[0]:
                return result[0]["embedding"]
            raise ValueError("Failed to generate embedding")
        except Exception as error:
            logger.error(f"Failed to embed query: {error}")
            raise ValueError(f"Failed to embed query: {error}") from error


class VectorDBClient:
    """Client for interacting with Supabase pgvector semantic search via LangChain.

    Wraps LangChain's SupabaseVectorStore with retry logic and comprehensive
    error handling for vector operations.

    Attributes:
        _supabase_client: The underlying Supabase client instance.
        _llm_client: OpenRouter client for generating embeddings.
        _vectorstore: LangChain SupabaseVectorStore instance.
        _table_name: Name of the pgvector table (default: embeddings).
        _query_name: Name of the similarity search RPC function.
    """

    def __init__(
        self,
        supabase_client: Optional[Client] = None,
        table_name: str = "embeddings",
        query_name: str = "similarity_search_with_score",
    ) -> None:
        """Initialize Vector DB client.

        Args:
            supabase_client: Optional Supabase client instance. If not provided,
                uses the global client from get_supabase_client().
            table_name: Name of the pgvector table (default: embeddings).
            query_name: Name of the similarity search RPC function
                (default: similarity_search_with_score).

        Raises:
            ValueError: If Supabase is not configured.
        """

        self._table_name = table_name
        self._query_name = query_name

        # Get OpenRouter client and wrap it for LangChain compatibility
        self._llm_client = get_openrouter_client()
        self._embedding_model = OpenRouterEmbeddings(client=self._llm_client)

        # Initialize LangChain SupabaseVectorStore
        self._vectorstore: Optional[SupabaseVectorStore] = None

    async def _get_vectorstore(self) -> SupabaseVectorStore:
        """Get or initialize SupabaseVectorStore.

        Returns:
            SupabaseVectorStore instance.
        """
        from lib.integrations.supabase.supabase_client import get_supabase_client

        if self._vectorstore is None:
            supabase_client = await get_supabase_client()
            self._vectorstore = SupabaseVectorStore(
                client=supabase_client._client,
                embedding=self._embedding_model,
                table_name=self._table_name,
                query_name=self._query_name,
            )
        return self._vectorstore

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def add_texts(
        self,
        texts: list[str],
        metadatas: Optional[list[dict[str, Any]]] = None,
    ) -> list[str]:
        """Add texts to the vector store with metadata.

        Args:
            texts: List of text strings to store.
            metadatas: Optional list of metadata dictionaries, one per text.

        Returns:
            List of document IDs created.

        Raises:
            ValueError: If texts is empty or metadata mismatch.
        """
        if not texts:
            raise ValueError("texts cannot be empty")

        if metadatas and len(metadatas) != len(texts):
            raise ValueError(
                f"metadatas length ({len(metadatas)}) must match texts length ({len(texts)})"
            )

        try:
            vectorstore = await self._get_vectorstore()
            # LangChain's aadd_texts returns list of IDs
            ids = await vectorstore.aadd_texts(texts=texts, metadatas=metadatas)
            logger.info(f"Added {len(ids)} texts to vector store")
            return ids
        except Exception as error:
            logger.error(f"Failed to add texts to vector store: {error}")
            raise ValueError(f"Failed to add texts: {error}") from error

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def from_texts(
        self,
        texts: list[str],
        metadatas: Optional[list[dict[str, Any]]] = None,
    ) -> SupabaseVectorStore:
        """Create or update vector store from texts.

        Args:
            texts: List of text strings.
            metadatas: Optional list of metadata dictionaries.

        Returns:
            SupabaseVectorStore instance.

        Raises:
            ValueError: If operation fails.
        """
        from lib.integrations.supabase.supabase_client import get_supabase_client

        if not texts:
            raise ValueError("texts cannot be empty")

        try:
            self._vectorstore = SupabaseVectorStore.from_texts(
                texts=texts,
                embedding=self._embedding_model,
                client=(await get_supabase_client())._client,
                table_name=self._table_name,
                query_name=self._query_name,
                metadatas=metadatas,
            )
            logger.info(f"Created vector store with {len(texts)} texts")
            return self._vectorstore
        except Exception as error:
            logger.error(f"Failed to create vector store: {error}")
            raise ValueError(f"Failed to create vector store: {error}") from error

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def query_rag(
        self,
        query_text: str,
        k: int = 5,
        metadata_filter: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Query the RAG system using semantic search via RPC.

        Uses direct RPC call to bypass potential LangChain quirks.

        Args:
            query_text: The query string.
            k: Number of results to return (default: 5).
            metadata_filter: Optional metadata filter
                (e.g., {"classification": "crm"}).

        Returns:
            List of results with id, content, metadata, similarity_score.

        Raises:
            ValueError: If query fails.
        """

        from lib.integrations.supabase.supabase_client import get_supabase_client

        if not query_text:
            raise ValueError("query_text cannot be empty")

        try:
            # Generate embedding for the query
            query_embedding_result = await self._llm_client.embed(
                text_list=[query_text]
            )

            if not query_embedding_result or not query_embedding_result[0]:
                raise ValueError("Failed to generate query embedding")

            query_vector = query_embedding_result[0]["embedding"]

            # Prepare filter parameter
            filter_param = metadata_filter if metadata_filter else {}

            # Call RPC function for semantic search
            supabase_client = await get_supabase_client()

            response = await (
                supabase_client._client.rpc(
                    self._query_name,
                    {
                        "query_embeddings": query_vector,
                        "filter": filter_param,
                    },
                )
                .limit(k)
                .execute()
            )

            # Extract results and ensure type safety
            response_data = response.data if response.data else []
            results: list[dict[str, Any]] = cast(
                list[dict[str, Any]],
                response_data if isinstance(response_data, list) else [],
            )
            logger.info(
                f"Semantic search for '{query_text}' returned {len(results)} results"
            )
            return results
        except Exception as error:
            logger.error(f"Failed to query RAG: {error}")
            raise ValueError(f"Failed to query RAG: {error}") from error

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def similarity_search(
        self,
        query_text: str,
        k: int = 5,
        metadata_filter: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Perform similarity search using LangChain's method.

        Args:
            query_text: The query string.
            k: Number of results to return (default: 5).
            metadata_filter: Optional metadata filter.

        Returns:
            List of results with id, page_content, metadata, similarity_score.

        Raises:
            ValueError: If search fails.
        """
        try:
            vectorstore = await self._get_vectorstore()
            # LangChain's similarity search with score
            results = await vectorstore.asimilarity_search_with_score(
                query=query_text,
                k=k,
                filter=metadata_filter,
            )
            logger.info(f"Similarity search returned {len(results)} results")
            return [
                {
                    "content": doc.page_content,
                    "metadata": doc.metadata,
                    "similarity_score": score,
                }
                for doc, score in results
            ]
        except Exception as error:
            logger.error(f"Failed similarity search: {error}")
            raise ValueError(f"Failed similarity search: {error}") from error

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def delete_by_id(self, ids: list[str]) -> None:
        """Delete documents by their IDs.

        Args:
            ids: List of document IDs to delete.

        Raises:
            ValueError: If deletion fails.
        """
        if not ids:
            raise ValueError("ids cannot be empty")

        try:
            vectorstore = await self._get_vectorstore()
            await vectorstore.adelete(ids=ids)
            logger.info(f"Deleted {len(ids)} documents")
        except Exception as error:
            logger.error(f"Failed to delete documents: {error}")
            raise ValueError(f"Failed to delete documents: {error}") from error


_vector_db_client: Optional[VectorDBClient] = None


def get_vector_db_client() -> VectorDBClient:
    """Get or create global Vector DB client singleton.

    The client is initialized with Supabase and OpenRouter clients,
    maintaining a single SupabaseVectorStore instance.

    Returns:
        VectorDBClient instance.

    Raises:
        ValueError: If Supabase or OpenRouter is not configured.
    """
    global _vector_db_client
    if _vector_db_client is None:
        settings = get_settings()
        _vector_db_client = VectorDBClient(
            table_name=settings.vector_table_name or "embeddings",
        )
    return _vector_db_client


def reset_vector_db_client() -> None:
    """Reset the global Vector DB client singleton.

    Useful for testing or reinitializing with new configuration.
    """
    global _vector_db_client
    _vector_db_client = None


def rrf(ranked_lists: List[List[str]], k: int = 60) -> List[str]:
    """
    Combines multiple ranked lists of strings into a single ranked list using
    Reciprocal Rank Fusion (RRF).

    Args:
        ranked_lists: A list of lists, where each inner list contains strings
                      ranked by a specific method (e.g., vector search, keyword search).
                      Example: [['doc_A', 'doc_B'], ['doc_B', 'doc_C']]
        k: A constant to smooth the rankings (default is 60, as suggested by the original paper).
           A higher k reduces the impact of high rankings in a single list.

    Returns:
        A single list of unique strings sorted by their fused RRF score.
    """

    # Dictionary to store the accumulated RRF score for each unique item
    scores: Dict[str, float] = {}

    # Iterate through each ranked list provided
    for current_list in ranked_lists:
        # Iterate through items in the list with their rank (0-indexed)
        for rank, item in enumerate(current_list):
            # Initialize score if first time seeing this item
            if item not in scores:
                scores[item] = 0.0

            # RRF Formula: score += 1 / (k + rank)
            # We use (rank + 1) because rank is 0-indexed in Python, but the formula
            # typically expects 1-based ranking (1st place, 2nd place, etc.)
            scores[item] += 1.0 / (k + rank + 1)

    # Sort the items based on their final accumulated score in descending order
    # x[0] is the item (string), x[1] is the score
    sorted_items = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Return only the list of strings, discarding the scores
    return [item[0] for item in sorted_items]
