"""Retry decorator and utilities for resilient API calls with exponential backoff.

This module provides a decorator-based approach to adding retry logic with
exponential backoff to functions making external API calls.

Usage:
    @retry_on_exception(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=60.0,
        exceptions=(ConnectionError, TimeoutError),
    )
    def fetch_data(url: str) -> dict:
        # This will be retried up to 3 times on ConnectionError/TimeoutError
        response = requests.get(url)
        return response.json()
"""

import asyncio
import logging
import random
import time
from functools import wraps
from typing import Any, Callable, Type, TypeVar

logger = logging.getLogger(__name__)

# Type variable for generic function wrapping
F = TypeVar("F", bound=Callable[..., Any])


def retry_on_exception(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    initial_delay: float = 1.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
    jitter: bool = True,
) -> Callable[[F], F]:
    """Decorator for retrying functions with exponential backoff.

    Retries the decorated function if it raises one of the specified exceptions.
    Uses exponential backoff with optional jitter to avoid thundering herd.

    Args:
        max_attempts: Maximum number of attempts (default: 3).
        backoff_factor: Multiplier for delay between retries (default: 2.0).
        max_delay: Maximum delay between retries in seconds (default: 60.0).
        initial_delay: Initial delay before first retry in seconds (default: 1.0).
        exceptions: Tuple of exception types to catch and retry on (default: Exception).
        jitter: Add random jitter to delay to avoid thundering herd (default: True).

    Returns:
        Decorated function that retries on specified exceptions.

    Example:
        >>> @retry_on_exception(
        ...     max_attempts=3,
        ...     backoff_factor=2.0,
        ...     exceptions=(ConnectionError, TimeoutError),
        ... )
        ... def fetch_data(url: str) -> dict:
        ...     return requests.get(url).json()
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            delay = initial_delay

            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise

                    # Calculate delay with jitter
                    actual_delay = min(delay, max_delay)
                    if jitter:
                        actual_delay *= random.uniform(0.5, 1.5)

                    logger.warning(
                        f"{func.__name__} attempt {attempt} failed ({e}). "
                        f"Retrying in {actual_delay:.1f}s... "
                        f"(attempt {attempt}/{max_attempts})"
                    )
                    time.sleep(actual_delay)
                    delay *= backoff_factor

        return wrapper  # type: ignore

    return decorator


def retry_on_exception_async(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    initial_delay: float = 1.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
    jitter: bool = True,
) -> Callable[[F], F]:
    """Async version of retry_on_exception decorator.

    Retries the decorated async function if it raises one of the specified exceptions.
    Uses exponential backoff with optional jitter.

    Args:
        max_attempts: Maximum number of attempts (default: 3).
        backoff_factor: Multiplier for delay between retries (default: 2.0).
        max_delay: Maximum delay between retries in seconds (default: 60.0).
        initial_delay: Initial delay before first retry in seconds (default: 1.0).
        exceptions: Tuple of exception types to catch and retry on (default: Exception).
        jitter: Add random jitter to delay to avoid thundering herd (default: True).

    Returns:
        Decorated async function that retries on specified exceptions.

    Example:
        >>> @retry_on_exception_async(
        ...     max_attempts=3,
        ...     backoff_factor=2.0,
        ...     exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
        ... )
        ... async def fetch_data(url: str) -> dict:
        ...     async with aiohttp.ClientSession() as session:
        ...         async with session.get(url) as resp:
        ...             return await resp.json()
    """

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            delay = initial_delay

            while attempt < max_attempts:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise

                    # Calculate delay with jitter
                    actual_delay = min(delay, max_delay)
                    if jitter:
                        actual_delay *= random.uniform(0.5, 1.5)

                    logger.warning(
                        f"{func.__name__} attempt {attempt} failed ({e}). "
                        f"Retrying in {actual_delay:.1f}s... "
                        f"(attempt {attempt}/{max_attempts})"
                    )
                    await asyncio.sleep(actual_delay)
                    delay *= backoff_factor

        return wrapper  # type: ignore

    return decorator


def calculate_backoff_delay(
    attempt: int,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    initial_delay: float = 1.0,
) -> float:
    """Calculate backoff delay for a given attempt number.

    Args:
        attempt: Current attempt number (0-indexed).
        backoff_factor: Multiplier for each attempt.
        max_delay: Maximum delay in seconds.
        initial_delay: Initial delay in seconds.

    Returns:
        Delay in seconds for this attempt.

    Example:
        >>> calculate_backoff_delay(0)
        1.0
        >>> calculate_backoff_delay(1)
        2.0
        >>> calculate_backoff_delay(5)
        60.0  # capped at max_delay
    """
    delay = initial_delay * (backoff_factor**attempt)
    return min(delay, max_delay)
