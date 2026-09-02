"""Retry utilities with exponential backoff for external API calls.

.. deprecated:: 2.25.0
    This module is deprecated and scheduled for removal in Almanak SDK 3.0.0.
    It has no callers inside the SDK and is replaced by the explicit-attempt
    :class:`almanak.core.retry.RetryPolicy` (ALM-3197). It is retained only so that the five
    names it exported through ``almanak.framework.utils`` — which ``.syncinclude``
    publishes to the public mirror — do not disappear without a deprecation
    cycle. Importing this module, or resolving one of its names through
    ``almanak.framework.utils``, emits a one-shot :class:`DeprecationWarning`.

    Not to be confused with two live, unrelated things that share names:
    ``TokenBucketRateLimiter.retry_with_backoff``
    (``almanak/framework/backtesting/pnl/providers/rate_limiter.py``) and the
    ``RetryConfig`` classes in ``backtesting/pnl/providers/coingecko.py`` and
    ``intents/state_machine.py``. None of those are affected by this
    deprecation.

The decorators support synchronous and asynchronous functions, exponential
backoff, jitter, configurable exception types, and retry callbacks.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

DEPRECATION_MESSAGE = (
    "almanak.framework.utils.retry is deprecated and will be removed in "
    "Almanak SDK 3.0.0. Use almanak.core.retry.RetryPolicy for explicit "
    "total-attempt budgets (ALM-3197)."
)

_DEPRECATION_EMITTED = False


def _emit_deprecation(stacklevel: int = 3) -> None:
    """Emit the one-shot deprecation warning for this module.

    Called once at module import, so both entry points are covered with a
    single warning: importing ``almanak.framework.utils.retry`` directly, and
    resolving a name through ``almanak.framework.utils.__getattr__`` (which
    imports this module lazily on first access).
    """
    global _DEPRECATION_EMITTED
    if _DEPRECATION_EMITTED:
        return
    _DEPRECATION_EMITTED = True
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=stacklevel)


_emit_deprecation(stacklevel=2)

P = ParamSpec("P")
T = TypeVar("T")


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
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be > 0")
        if self.max_delay <= 0:
            raise ValueError("max_delay must be > 0")
        if not 0 <= self.jitter_factor <= 1:
            raise ValueError("jitter_factor must be between 0 and 1")


DEFAULT_RETRY_CONFIG = RetryConfig()


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
    delay = base_delay * (2**attempt)
    delay = min(delay, max_delay)
    jitter = random.uniform(0, delay * jitter_factor)

    return delay + jitter


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

    """
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
        if asyncio.iscoroutinefunction(func):
            return _wrap_async(func, effective_config, on_retry)
        return _wrap_sync(func, effective_config, on_retry)

    return decorator


def _wrap_async[**P, T](
    func: Callable[P, Awaitable[T]],
    config: RetryConfig,
    on_retry: Callable[[int, Exception, float], None] | None,
) -> Callable[P, Awaitable[T]]:
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        last_exception: Exception | None = None

        for attempt in range(config.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except config.retryable_exceptions as e:
                last_exception = e

                if attempt >= config.max_retries:
                    logger.warning(f"All {config.max_retries + 1} attempts failed for {func.__name__}: {e}")
                    raise

                delay = calculate_backoff_delay(
                    attempt,
                    config.base_delay,
                    config.max_delay,
                    config.jitter_factor,
                )

                logger.debug(
                    f"Retry {attempt + 1}/{config.max_retries} for {func.__name__} "
                    f"after {delay:.2f}s: {type(e).__name__}: {e}"
                )

                if on_retry:
                    on_retry(attempt, e, delay)

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
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        last_exception: Exception | None = None

        for attempt in range(config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except config.retryable_exceptions as e:
                last_exception = e

                if attempt >= config.max_retries:
                    logger.warning(f"All {config.max_retries + 1} attempts failed for {func.__name__}: {e}")
                    raise

                delay = calculate_backoff_delay(
                    attempt,
                    config.base_delay,
                    config.max_delay,
                    config.jitter_factor,
                )

                logger.debug(
                    f"Retry {attempt + 1}/{config.max_retries} for {func.__name__} "
                    f"after {delay:.2f}s: {type(e).__name__}: {e}"
                )

                if on_retry:
                    on_retry(attempt, e, delay)

                time.sleep(delay)

        # Should not reach here, but satisfy type checker
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")

    return wrapper


class RetryContext:
    """Context manager for retry operations without decorator.

    Useful when you need more control over the retry loop, or when
    decorating is not practical.
    """

    def __init__(
        self,
        config: RetryConfig | None = None,
        *,
        max_retries: int | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
    ) -> None:
        base = config or DEFAULT_RETRY_CONFIG
        self._max_retries = max_retries if max_retries is not None else base.max_retries
        self._base_delay = base_delay if base_delay is not None else base.base_delay
        self._max_delay = max_delay if max_delay is not None else base.max_delay
        self._jitter_factor = base.jitter_factor
        self._attempt = 0
        self._last_error: Exception | None = None

    async def __aenter__(self) -> RetryContext:
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context."""

    def __enter__(self) -> RetryContext:
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


__all__ = [
    "RetryConfig",
    "RetryContext",
    "calculate_backoff_delay",
    "retry_with_backoff",
    "DEFAULT_RETRY_CONFIG",
]
