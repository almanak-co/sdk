"""Retry utilities with exponential backoff for external API calls.

This module provides reusable retry decorators for handling transient failures
in external API calls (price providers, RPC endpoints, etc.).

Key Features:
    - Exponential backoff with configurable base and max delay
    - Jitter to prevent thundering herd problem
    - Support for both sync and async functions
    - Configurable retry conditions based on exception types
    - Logging of retry attempts

Usage:
    # Basic usage with default settings
    @retry_with_backoff()
    async def fetch_price(token: str) -> float:
        ...

    # Custom configuration
    @retry_with_backoff(
        max_retries=5,
        base_delay=2.0,
        max_delay=60.0,
        retryable_exceptions=(TimeoutError, ConnectionError),
    )
    def call_external_api() -> dict:
        ...

    # Using RetryConfig for consistent settings
    config = RetryConfig(max_retries=3, base_delay=1.0, max_delay=32.0)

    @retry_with_backoff(config=config)
    async def another_api_call() -> str:
        ...
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

# Type variables for generic function signatures
P = ParamSpec("P")
T = TypeVar("T")


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class RetryConfig:
    """Configuration for retry behavior with exponential backoff.

    Attributes:
        max_retries: Maximum number of retry attempts (0 = no retries)
        base_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay in seconds (caps exponential growth)
        jitter_factor: Random jitter as fraction of delay (0.0-1.0)
        retryable_exceptions: Tuple of exception types that trigger retry
    """

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 32.0
    jitter_factor: float = 0.5
    retryable_exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: (
            TimeoutError,
            ConnectionError,
            OSError,
        )
    )

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be > 0")
        if self.max_delay <= 0:
            raise ValueError("max_delay must be > 0")
        if not 0 <= self.jitter_factor <= 1:
            raise ValueError("jitter_factor must be between 0 and 1")


# Default configuration - can be customized per use case
DEFAULT_RETRY_CONFIG = RetryConfig()


# =============================================================================
# Backoff Calculation
# =============================================================================


def calculate_backoff_delay(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 32.0,
    jitter_factor: float = 0.5,
) -> float:
    """Calculate backoff delay with exponential growth and jitter.

    Uses formula: min(max_delay, base_delay * 2^attempt) + jitter

    Args:
        attempt: Current retry attempt (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap in seconds
        jitter_factor: Random jitter as fraction of delay (0.0-1.0)

    Returns:
        Delay in seconds with jitter applied
    """
    # Exponential backoff: base_delay * 2^attempt
    delay = base_delay * (2**attempt)

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter: random value between 0 and (delay * jitter_factor)
    jitter = random.uniform(0, delay * jitter_factor)

    return delay + jitter


# =============================================================================
# Retry Decorators
# =============================================================================


def retry_with_backoff(
    config: RetryConfig | None = None,
    *,
    max_retries: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
    jitter_factor: float | None = None,
    retryable_exceptions: tuple[type[Exception], ...] | None = None,
    on_retry: Callable[[int, Exception, float], None] | None = None,
) -> Callable[[Callable[P, T | Awaitable[T]]], Callable[P, T | Awaitable[T]]]:
    """Decorator for retry with exponential backoff.

    Supports both sync and async functions. Auto-detects based on function type.

    Args:
        config: RetryConfig instance (if provided, individual params override it)
        max_retries: Override max retry attempts
        base_delay: Override base delay in seconds
        max_delay: Override maximum delay in seconds
        jitter_factor: Override jitter factor (0.0-1.0)
        retryable_exceptions: Override which exceptions trigger retry
        on_retry: Optional callback called on each retry with (attempt, exception, delay)

    Returns:
        Decorated function with retry behavior

    Example:
        @retry_with_backoff(max_retries=5, base_delay=2.0)
        async def fetch_data():
            response = await client.get("/api/data")
            return response.json()
    """
    # Build effective config from defaults + overrides
    base_config = config or DEFAULT_RETRY_CONFIG
    effective_config = RetryConfig(
        max_retries=max_retries if max_retries is not None else base_config.max_retries,
        base_delay=base_delay if base_delay is not None else base_config.base_delay,
        max_delay=max_delay if max_delay is not None else base_config.max_delay,
        jitter_factor=(jitter_factor if jitter_factor is not None else base_config.jitter_factor),
        retryable_exceptions=(
            retryable_exceptions if retryable_exceptions is not None else base_config.retryable_exceptions
        ),
    )

    def decorator(
        func: Callable[P, T | Awaitable[T]],
    ) -> Callable[P, T | Awaitable[T]]:
        """Decorate function with retry logic."""
        # Check if async
        if asyncio.iscoroutinefunction(func):
            return _wrap_async(func, effective_config, on_retry)
        return _wrap_sync(func, effective_config, on_retry)

    return decorator


def _wrap_async[**P, T](
    func: Callable[P, Awaitable[T]],
    config: RetryConfig,
    on_retry: Callable[[int, Exception, float], None] | None,
) -> Callable[P, Awaitable[T]]:
    """Wrap async function with retry logic."""

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        last_exception: Exception | None = None

        for attempt in range(config.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except config.retryable_exceptions as e:
                last_exception = e

                # Check if we have retries left
                if attempt >= config.max_retries:
                    logger.warning(f"All {config.max_retries + 1} attempts failed for {func.__name__}: {e}")
                    raise

                # Calculate delay
                delay = calculate_backoff_delay(
                    attempt,
                    config.base_delay,
                    config.max_delay,
                    config.jitter_factor,
                )

                # Log retry
                logger.debug(
                    f"Retry {attempt + 1}/{config.max_retries} for {func.__name__} "
                    f"after {delay:.2f}s: {type(e).__name__}: {e}"
                )

                # Callback if provided
                if on_retry:
                    on_retry(attempt, e, delay)

                # Wait before retry
                await asyncio.sleep(delay)

        # Should not reach here, but satisfy type checker
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")

    return wrapper


def _wrap_sync[**P, T](
    func: Callable[P, T],
    config: RetryConfig,
    on_retry: Callable[[int, Exception, float], None] | None,
) -> Callable[P, T]:
    """Wrap sync function with retry logic."""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        last_exception: Exception | None = None

        for attempt in range(config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except config.retryable_exceptions as e:
                last_exception = e

                # Check if we have retries left
                if attempt >= config.max_retries:
                    logger.warning(f"All {config.max_retries + 1} attempts failed for {func.__name__}: {e}")
                    raise

                # Calculate delay
                delay = calculate_backoff_delay(
                    attempt,
                    config.base_delay,
                    config.max_delay,
                    config.jitter_factor,
                )

                # Log retry
                logger.debug(
                    f"Retry {attempt + 1}/{config.max_retries} for {func.__name__} "
                    f"after {delay:.2f}s: {type(e).__name__}: {e}"
                )

                # Callback if provided
                if on_retry:
                    on_retry(attempt, e, delay)

                # Wait before retry
                time.sleep(delay)

        # Should not reach here, but satisfy type checker
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")

    return wrapper


# =============================================================================
# Context Manager for Retry
# =============================================================================


class RetryContext:
    """Context manager for retry operations without decorator.

    Useful when you need more control over the retry loop, or when
    decorating is not practical.

    Usage:
        async with RetryContext(max_retries=3) as ctx:
            while ctx.should_retry():
                try:
                    result = await some_operation()
                    break
                except TimeoutError as e:
                    await ctx.handle_error(e)
    """

    def __init__(
        self,
        config: RetryConfig | None = None,
        *,
        max_retries: int | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
    ) -> None:
        """Initialize retry context.

        Args:
            config: RetryConfig instance
            max_retries: Override max retries
            base_delay: Override base delay
            max_delay: Override max delay
        """
        base = config or DEFAULT_RETRY_CONFIG
        self._max_retries = max_retries if max_retries is not None else base.max_retries
        self._base_delay = base_delay if base_delay is not None else base.base_delay
        self._max_delay = max_delay if max_delay is not None else base.max_delay
        self._jitter_factor = base.jitter_factor
        self._attempt = 0
        self._last_error: Exception | None = None

    async def __aenter__(self) -> RetryContext:
        """Enter async context."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context."""

    def __enter__(self) -> RetryContext:
        """Enter sync context."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit sync context."""

    @property
    def attempt(self) -> int:
        """Current attempt number (0-indexed)."""
        return self._attempt

    @property
    def last_error(self) -> Exception | None:
        """Last error encountered."""
        return self._last_error

    def should_retry(self) -> bool:
        """Check if should attempt (or retry).

        Returns:
            True if attempt count is within limits
        """
        return self._attempt <= self._max_retries

    async def handle_error_async(self, error: Exception) -> None:
        """Handle error and wait for backoff (async version).

        Args:
            error: The exception that occurred

        Raises:
            The error if no retries remaining
        """
        self._last_error = error

        if self._attempt >= self._max_retries:
            raise error

        delay = calculate_backoff_delay(
            self._attempt,
            self._base_delay,
            self._max_delay,
            self._jitter_factor,
        )

        logger.debug(
            f"Retry context: attempt {self._attempt + 1}/{self._max_retries}, "
            f"waiting {delay:.2f}s after: {type(error).__name__}"
        )

        await asyncio.sleep(delay)
        self._attempt += 1

    def handle_error_sync(self, error: Exception) -> None:
        """Handle error and wait for backoff (sync version).

        Args:
            error: The exception that occurred

        Raises:
            The error if no retries remaining
        """
        self._last_error = error

        if self._attempt >= self._max_retries:
            raise error

        delay = calculate_backoff_delay(
            self._attempt,
            self._base_delay,
            self._max_delay,
            self._jitter_factor,
        )

        logger.debug(
            f"Retry context: attempt {self._attempt + 1}/{self._max_retries}, "
            f"waiting {delay:.2f}s after: {type(error).__name__}"
        )

        time.sleep(delay)
        self._attempt += 1


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "RetryConfig",
    "RetryContext",
    "calculate_backoff_delay",
    "retry_with_backoff",
    "DEFAULT_RETRY_CONFIG",
]
