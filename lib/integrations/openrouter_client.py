"""OpenRouter LLM provider integration for chat completions and embeddings.

This module provides a wrapper around the OpenRouter API for:
- Chat completions with multiple LLM models (gpt-4o-mini, gpt-5-turbo, etc)
- Text embeddings for semantic search
- Streaming support for long responses
- Retry logic with exponential backoff
- Timeout enforcement

OpenRouter acts as a routing layer to various LLM providers. This client
abstracts the API details and provides consistent error handling.

Usage:
    from lib.integrations.openrouter_client import get_openrouter_client

    client = get_openrouter_client()

    # Chat completion
    response = await client.chat_completion(
        messages=[{"role": "user", "content": "Hello"}],
        model="openrouter/gpt-4o-mini",
        temperature=0.7,
    )

    # Embeddings
    embeddings = await client.embed(
        text_list=["Sample text 1", "Sample text 2"],
    )
"""

import json
import logging
from typing import Any, Optional, Type, TypeVar

import urllib3
from pydantic import BaseModel
from typing import TypedDict

from lib.config import get_settings
from lib.utils.retry import retry_on_exception

logger = logging.getLogger(__name__)

# Type variable for structured completion response models
T = TypeVar("T", bound=BaseModel)


class Message(TypedDict, total=False):
    """Message structure for chat completion.

    Attributes:
        role: "system", "user", or "assistant".
        content: Text content of the message.
    """

    role: str
    content: str


class ChatCompletionResponse(TypedDict):
    """Response from chat completion API.

    Attributes:
        id: Unique request ID.
        model: Model used for completion.
        content: Generated text response.
        tokens_used: Total tokens consumed (prompt + completion).
        finish_reason: "stop", "length", "error", etc.
    """

    id: str
    model: str
    content: str
    tokens_used: int
    finish_reason: str


class EmbeddingResult(TypedDict):
    """Result from embedding API.

    Attributes:
        text: Original input text.
        embedding: 1536-dimensional embedding vector (for text-embedding-3-small).
    """

    text: str
    embedding: list[float]


class OpenRouterClient:
    """Client for OpenRouter LLM API with retry and timeout logic.

    Wraps OpenRouter HTTP API with comprehensive error handling,
    retry logic, and timeout enforcement.

    Attributes:
        _api_key: OpenRouter API key.
        _base_url: OpenRouter base URL (default: https://openrouter.ai/api/v1).
        _http: urllib3.PoolManager for HTTP requests.
        _timeout: Request timeout in seconds.
        _max_retries: Max retry attempts.
        _backoff_factor: Exponential backoff multiplier.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> None:
        """Initialize OpenRouter client.

        Args:
            api_key: OpenRouter API key. If not provided, loads from settings.
            base_url: OpenRouter base URL. If not provided, loads from settings.
            timeout: Request timeout in seconds. If not provided, loads from settings.

        Raises:
            ValueError: If API key is not configured.
        """
        settings = get_settings()

        self._api_key = api_key or settings.openrouter_api_key
        if not self._api_key:
            raise ValueError(
                "OpenRouter API key must be configured via "
                "OPENROUTER_API_KEY environment variable"
            )

        self._base_url = base_url or settings.openrouter_base_url
        self._timeout = timeout or settings.request_timeout

        # Create HTTP client with connection pooling
        self._http = urllib3.PoolManager(
            timeout=urllib3.Timeout(
                connect=settings.request_connect_timeout,
                read=settings.request_read_timeout,
            ),
            num_pools=settings.request_pool_connections,
            maxsize=settings.request_pool_maxsize,
        )

        self._max_retries = settings.retry_max_attempts
        self._backoff_factor = settings.retry_backoff_factor

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def chat_completion(
        self,
        messages: list[Message],
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        top_p: float = 1.0,
        stream: bool = False,
    ) -> ChatCompletionResponse:
        """Generate chat completion using OpenRouter.

        Args:
            messages: List of message dicts with "role" and "content".
            model: Model identifier (e.g., "openrouter/gpt-4o-mini").
            temperature: Sampling temperature (0.0-2.0, default 0.7).
            max_tokens: Maximum tokens to generate (default 2048).
            top_p: Nucleus sampling parameter (default 1.0).
            stream: Whether to stream the response (default False).

        Returns:
            ChatCompletionResponse with generated content and metadata.

        Raises:
            ValueError: If API response is invalid.
            Exception: On API errors after retries.
        """
        url = f"{self._base_url}/chat/completions"

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "top_p": top_p,
            "stream": stream,
        }

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "User-Agent": "betafits-email-ingestor/1.0",
        }

        try:
            response = self._http.request(
                "POST",
                url,
                body=json.dumps(payload),
                headers=headers,
            )

            if response.status != 200:
                error_text = response.data.decode("utf-8")
                logger.error(
                    f"OpenRouter chat completion failed with status {response.status}: {error_text}"
                )
                raise Exception(f"OpenRouter API error {response.status}: {error_text}")

            response_data = json.loads(response.data.decode("utf-8"))

            # Extract first choice
            if not response_data.get("choices"):
                raise ValueError("No choices in OpenRouter response")

            choice = response_data["choices"][0]
            content = choice.get("message", {}).get("content", "")
            finish_reason = choice.get("finish_reason", "unknown")

            # Calculate tokens (estimate: 1 token ~ 4 chars)
            tokens_used = response_data.get("usage", {}).get("total_tokens", 0)

            result: ChatCompletionResponse = {
                "id": response_data.get("id", ""),
                "model": response_data.get("model", model),
                "content": content,
                "tokens_used": tokens_used,
                "finish_reason": finish_reason,
            }

            logger.info(
                f"Chat completion: model={model}, tokens={tokens_used}, "
                f"finish_reason={finish_reason}"
            )
            return result

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in OpenRouter response: {e}")
            raise
        except Exception as e:
            logger.error(f"Chat completion failed: {e}")
            raise

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def structured_completion(
        self,
        messages: list[Message],
        response_model: Type[T],
        model: str,
        temperature: float = 0.3,
        max_tokens: int = 2048,
    ) -> T:
        """Generate a structured completion with JSON schema enforcement.

        Uses OpenRouter's JSON mode to return responses conforming to a Pydantic model.

        Args:
            messages: List of message dicts with "role" and "content".
            response_model: Pydantic model class defining the expected response structure.
            model: Model identifier (e.g., "openai/gpt-4o-mini").
            temperature: Sampling temperature (0.0-2.0, default 0.3 for structured).
            max_tokens: Maximum tokens to generate (default 2048).

        Returns:
            An instance of response_model populated from the LLM response.

        Raises:
            ValueError: If API response is invalid or doesn't match schema.
            Exception: On API errors after retries.

        Example:
            >>> from pydantic import BaseModel
            >>> class SelectionResult(BaseModel):
            ...     selected_id: str
            ...     confidence: float
            >>> result = await client.structured_completion(
            ...     messages=[{"role": "user", "content": "Select the best match"}],
            ...     response_model=SelectionResult,
            ...     model="openai/gpt-4o-mini",
            ... )
            >>> print(result.selected_id)
        """
        url = f"{self._base_url}/chat/completions"

        # Build JSON schema from Pydantic model
        json_schema = response_model.model_json_schema()

        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": response_model.__name__,
                    "strict": True,
                    "schema": json_schema,
                },
            },
        }

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "User-Agent": "betafits-email-ingestor/1.0",
        }

        try:
            response = self._http.request(
                "POST",
                url,
                body=json.dumps(payload),
                headers=headers,
            )

            if response.status != 200:
                error_text = response.data.decode("utf-8")
                logger.error(
                    f"OpenRouter structured completion failed with status "
                    f"{response.status}: {error_text}"
                )
                raise Exception(f"OpenRouter API error {response.status}: {error_text}")

            response_data = json.loads(response.data.decode("utf-8"))

            if not response_data.get("choices"):
                raise ValueError("No choices in OpenRouter response")

            choice = response_data["choices"][0]
            content = choice.get("message", {}).get("content", "")
            tokens_used = response_data.get("usage", {}).get("total_tokens", 0)

            logger.info(
                f"Structured completion: model={model}, tokens={tokens_used}, "
                f"response_model={response_model.__name__}"
            )

            # Parse JSON content into Pydantic model
            parsed_content = json.loads(content)
            return response_model.model_validate(parsed_content)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in OpenRouter structured response: {e}")
            raise ValueError(f"Failed to parse structured response: {e}") from e
        except Exception as e:
            logger.error(f"Structured completion failed: {e}")
            raise

    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        exceptions=(Exception,),
    )
    async def embed(
        self,
        text_list: list[str],
        model: Optional[str] = None,
    ) -> list[EmbeddingResult]:
        """Generate embeddings for text list using OpenRouter.

        Args:
            text_list: List of text strings to embed.
            model: Model identifier for embeddings
                (default: openrouter/openai/text-embedding-3-small).

        Returns:
            List of EmbeddingResult dicts with text and 1536-dim embedding vectors.

        Raises:
            ValueError: If text_list is empty or API response is invalid.
            Exception: On API errors after retries.
        """
        if not text_list:
            raise ValueError("text_list cannot be empty")

        url = f"{self._base_url}/embeddings"

        payload = {
            "model": model or get_settings().openrouter_embedding_model,
            "input": text_list,
        }

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "User-Agent": "betafits-email-ingestor/1.0",
        }

        try:
            response = self._http.request(
                "POST",
                url,
                body=json.dumps(payload),
                headers=headers,
            )

            if response.status != 200:
                error_text = response.data.decode("utf-8")
                logger.error(
                    f"OpenRouter embeddings failed with status {response.status}: {error_text}"
                )
                raise Exception(f"OpenRouter API error {response.status}: {error_text}")

            response_data = json.loads(response.data.decode("utf-8"))

            # Build results
            results: list[EmbeddingResult] = []
            embeddings = response_data.get("data", [])

            for i, embedding_obj in enumerate(embeddings):
                if i < len(text_list):
                    result: EmbeddingResult = {
                        "text": text_list[i],
                        "embedding": embedding_obj.get("embedding", []),
                    }
                    results.append(result)

            logger.info(f"Generated embeddings for {len(results)} texts using {model}")
            return results

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in OpenRouter embeddings response: {e}")
            raise
        except Exception as e:
            logger.error(f"Embeddings generation failed: {e}")
            raise


# Global client instance (lazy-loaded)
_openrouter_client: Optional[OpenRouterClient] = None


def get_openrouter_client() -> OpenRouterClient:
    """Get or create the global OpenRouterClient instance.

    Uses lazy initialization for efficiency.

    Returns:
        The global OpenRouterClient instance.
    """
    global _openrouter_client
    if _openrouter_client is None:
        _openrouter_client = OpenRouterClient()
    return _openrouter_client


def reset_openrouter_client() -> None:
    """Reset the global OpenRouterClient instance (mainly for testing)."""
    global _openrouter_client
    _openrouter_client = None
