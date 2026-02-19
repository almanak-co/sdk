"""Token bucket rate limiter for API requests.

This module provides a token bucket algorithm implementation for rate limiting
API requests to prevent hitting rate limits (429 errors) proactively.

The token bucket algorithm works by:
1. Having a bucket with a maximum capacity of tokens
2. Tokens are added at a fixed rate (refill_rate tokens per second)
3. Each request consumes one token
4. If no tokens are available, the caller waits until a token is available

Key Features:
    - Proactive rate limiting (prevents 429 errors instead of reacting to them)
    - Configurable rate limit (tokens per minute)
    - Thread-safe and async-friendly
    - Blocking wait when limit reached
    - Statistics tracking for monitoring
    - Adaptive rate reduction on 429 responses (20% reduction)
    - Exponential backoff retry logic for transient failures
    - Request queue for handling burst requests

Example:
    from almanak.framework.backtesting.pnl.providers.rate_limiter import (
        TokenBucketRateLimiter,
    )

    # Create rate limiter for CoinGecko free tier (50 requests per minute)
    limiter = TokenBucketRateLimiter(
        requests_per_minute=50,
        burst_size=10,  # Allow 10 requests in a burst
    )

    # Use before making API requests
    async def fetch_price():
        await limiter.acquire()  # Blocks if rate limit exceeded
        response = await make_api_request()
        return response

    # Or use as context manager
    async with limiter:
        response = await make_api_request()

    # Handle 429 response by reducing rate
    if response.status == 429:
        await limiter.on_rate_limit_response()

    # Use retry with exponential backoff
    result = await limiter.retry_with_backoff(
        make_api_request,
        max_retries=3,
    )
"""

import asyncio
import logging
import random
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class RateLimiterStats:
    """Statistics for rate limiter monitoring.

    Attributes:
        total_requests: Total number of acquire() calls
        total_waits: Number of times caller had to wait for a token
        total_wait_time_seconds: Cumulative time spent waiting
        max_wait_time_seconds: Maximum single wait duration
        tokens_consumed: Total tokens consumed
        created_at: When the rate limiter was created
        rate_limit_reductions: Number of times rate was reduced due to 429 responses
        retry_attempts: Total number of retry attempts
        retry_successes: Number of successful retries
    """

    total_requests: int = 0
    total_waits: int = 0
    total_wait_time_seconds: float = 0.0
    max_wait_time_seconds: float = 0.0
    tokens_consumed: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    rate_limit_reductions: int = 0
    retry_attempts: int = 0
    retry_successes: int = 0

    @property
    def requests_per_second(self) -> float:
        """Calculate average requests per second since creation."""
        elapsed = (datetime.now() - self.created_at).total_seconds()
        if elapsed <= 0:
            return 0.0
        return self.total_requests / elapsed

    @property
    def average_wait_seconds(self) -> float:
        """Calculate average wait time when waiting occurred."""
        if self.total_waits == 0:
            return 0.0
        return self.total_wait_time_seconds / self.total_waits

    @property
    def wait_rate(self) -> float:
        """Calculate percentage of requests that required waiting."""
        if self.total_requests == 0:
            return 0.0
        return (self.total_waits / self.total_requests) * 100

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/metrics."""
        return {
            "total_requests": self.total_requests,
            "total_waits": self.total_waits,
            "total_wait_time_seconds": round(self.total_wait_time_seconds, 3),
            "max_wait_time_seconds": round(self.max_wait_time_seconds, 3),
            "average_wait_seconds": round(self.average_wait_seconds, 3),
            "wait_rate_percent": round(self.wait_rate, 2),
            "requests_per_second": round(self.requests_per_second, 3),
            "tokens_consumed": self.tokens_consumed,
            "created_at": self.created_at.isoformat(),
            "rate_limit_reductions": self.rate_limit_reductions,
            "retry_attempts": self.retry_attempts,
            "retry_successes": self.retry_successes,
        }


class TokenBucketRateLimiter:
    """Token bucket rate limiter for controlling API request rates.

    This implementation uses the token bucket algorithm to limit requests:
    - Tokens are added at a steady rate (based on requests_per_minute)
    - Each acquire() consumes one token
    - If no tokens available, caller blocks until a token is available

    The bucket has a maximum capacity (burst_size) which allows for short
    bursts of traffic while still enforcing the overall rate limit.

    Additional features:
    - Adaptive rate reduction: on_rate_limit_response() reduces rate by 20%
    - Exponential backoff: retry_with_backoff() handles transient failures
    - Request queue: request_queue handles burst requests in order

    Attributes:
        requests_per_minute: Maximum requests allowed per minute
        burst_size: Maximum tokens in the bucket (burst capacity)
        min_interval_seconds: Minimum interval between requests

    Example:
        # CoinGecko free tier: 50 requests per minute
        limiter = TokenBucketRateLimiter(requests_per_minute=50)

        # Before each API call
        await limiter.acquire()
        response = await make_request()

        # Handle 429 response
        if response.status == 429:
            await limiter.on_rate_limit_response()

        # Check stats
        stats = limiter.get_stats()
        print(f"Wait rate: {stats.wait_rate:.1f}%")
    """

    # Rate reduction factor when 429 response is received (20% reduction)
    RATE_REDUCTION_FACTOR = 0.8

    # Minimum rate after reductions (to prevent rate going to zero)
    MIN_REQUESTS_PER_MINUTE = 1

    # Exponential backoff settings
    DEFAULT_BASE_DELAY_SECONDS = 1.0
    DEFAULT_MAX_DELAY_SECONDS = 60.0
    DEFAULT_MAX_RETRIES = 5

    def __init__(
        self,
        requests_per_minute: int = 50,
        burst_size: int | None = None,
    ) -> None:
        """Initialize the token bucket rate limiter.

        Args:
            requests_per_minute: Maximum number of requests allowed per minute.
                Default is 50 (CoinGecko free tier limit).
            burst_size: Maximum number of tokens in the bucket. This allows
                short bursts of traffic. Default is requests_per_minute // 5
                (10 for default 50 req/min).
        """
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")

        self._initial_requests_per_minute = requests_per_minute
        self._requests_per_minute = requests_per_minute

        # Default burst size is 1/5 of requests per minute (at least 1)
        self._burst_size = burst_size if burst_size is not None else max(1, requests_per_minute // 5)

        # Calculate refill rate: tokens per second
        self._refill_rate = requests_per_minute / 60.0

        # Minimum interval between requests (seconds)
        self._min_interval_seconds = 60.0 / requests_per_minute

        # Current token count (starts full)
        self._tokens = float(self._burst_size)

        # Last time we updated the token count
        self._last_refill_time = time.monotonic()

        # Lock for thread safety
        self._lock = asyncio.Lock()

        # Statistics
        self._stats = RateLimiterStats()

        # Request queue for handling burst requests in order
        self._request_queue: asyncio.Queue[asyncio.Event] = asyncio.Queue()
        self._queue_processor_task: asyncio.Task[None] | None = None

        logger.debug(
            "Initialized TokenBucketRateLimiter: %d req/min, burst=%d, interval=%.3fs",
            requests_per_minute,
            self._burst_size,
            self._min_interval_seconds,
        )

    def _refill(self) -> None:
        """Refill tokens based on time elapsed since last refill.

        This is called before checking/consuming tokens to update the
        token count based on how much time has passed.
        """
        now = time.monotonic()
        elapsed = now - self._last_refill_time
        self._last_refill_time = now

        # Add tokens based on elapsed time
        new_tokens = elapsed * self._refill_rate
        self._tokens = min(self._burst_size, self._tokens + new_tokens)

    async def acquire(self) -> None:
        """Acquire a token, blocking if necessary.

        This method will:
        1. Refill tokens based on time elapsed
        2. If a token is available, consume it and return immediately
        3. If no token is available, calculate wait time and sleep
        4. After sleeping, consume a token

        The wait time is calculated to ensure the caller waits exactly
        until a token will be available.

        Raises:
            None: This method always succeeds (may block until a token
                  is available).
        """
        async with self._lock:
            self._stats.total_requests += 1

            # Refill tokens based on elapsed time
            self._refill()

            if self._tokens >= 1.0:
                # Token available, consume it
                self._tokens -= 1.0
                self._stats.tokens_consumed += 1
                return

            # No token available, calculate wait time
            # Time until we have 1 token = (1 - tokens) / refill_rate
            tokens_needed = 1.0 - self._tokens
            wait_time = tokens_needed / self._refill_rate

            self._stats.total_waits += 1
            self._stats.total_wait_time_seconds += wait_time
            self._stats.max_wait_time_seconds = max(self._stats.max_wait_time_seconds, wait_time)

            logger.debug(
                "Rate limiter: waiting %.3fs for token (%.2f tokens available)",
                wait_time,
                self._tokens,
            )

        # Release lock before sleeping (allow other coroutines to check)
        await asyncio.sleep(wait_time)

        # Re-acquire lock to consume token
        async with self._lock:
            self._refill()
            self._tokens -= 1.0
            self._stats.tokens_consumed += 1

    async def acquire_nowait(self) -> bool:
        """Try to acquire a token without blocking.

        Returns:
            True if a token was acquired, False if no token is available.
        """
        async with self._lock:
            self._stats.total_requests += 1
            self._refill()

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                self._stats.tokens_consumed += 1
                return True

            return False

    async def wait_time_seconds(self) -> float:
        """Get the time in seconds until a token will be available.

        Returns:
            Time in seconds to wait, or 0 if a token is already available.
        """
        async with self._lock:
            self._refill()

            if self._tokens >= 1.0:
                return 0.0

            tokens_needed = 1.0 - self._tokens
            return tokens_needed / self._refill_rate

    def get_stats(self) -> RateLimiterStats:
        """Get current statistics.

        Returns:
            Copy of current statistics.
        """
        return RateLimiterStats(
            total_requests=self._stats.total_requests,
            total_waits=self._stats.total_waits,
            total_wait_time_seconds=self._stats.total_wait_time_seconds,
            max_wait_time_seconds=self._stats.max_wait_time_seconds,
            tokens_consumed=self._stats.tokens_consumed,
            created_at=self._stats.created_at,
            rate_limit_reductions=self._stats.rate_limit_reductions,
            retry_attempts=self._stats.retry_attempts,
            retry_successes=self._stats.retry_successes,
        )

    def _update_rate(self, new_rate: int) -> None:
        """Update the rate limit internally.

        Args:
            new_rate: New requests per minute rate.
        """
        self._requests_per_minute = max(self.MIN_REQUESTS_PER_MINUTE, new_rate)
        self._refill_rate = self._requests_per_minute / 60.0
        self._min_interval_seconds = 60.0 / self._requests_per_minute

    async def on_rate_limit_response(self) -> None:
        """Handle a rate limit (429) response by reducing the rate by 20%.

        This method should be called when an API returns a 429 status code.
        It reduces the current rate by 20% (multiplies by 0.8) to prevent
        further rate limit violations.

        The rate will never go below MIN_REQUESTS_PER_MINUTE (1 req/min).

        Example:
            response = await make_api_request()
            if response.status == 429:
                await limiter.on_rate_limit_response()
                # Retry after a delay
                await asyncio.sleep(1)
                response = await make_api_request()
        """
        async with self._lock:
            old_rate = self._requests_per_minute
            new_rate = int(old_rate * self.RATE_REDUCTION_FACTOR)
            self._update_rate(new_rate)
            self._stats.rate_limit_reductions += 1

            logger.warning(
                "Rate limit response received. Reducing rate from %d to %d req/min (reduction #%d)",
                old_rate,
                self._requests_per_minute,
                self._stats.rate_limit_reductions,
            )

    def reset_rate(self) -> None:
        """Reset the rate limit to the initial configured value.

        Call this after a period of successful requests to restore
        the original rate limit.
        """
        self._update_rate(self._initial_requests_per_minute)
        logger.info(
            "Rate limit reset to initial value: %d req/min",
            self._requests_per_minute,
        )

    async def retry_with_backoff(
        self,
        func: Callable[[], Coroutine[Any, Any, T]],
        max_retries: int | None = None,
        base_delay_seconds: float | None = None,
        max_delay_seconds: float | None = None,
        is_rate_limit_error: Callable[[Exception], bool] | None = None,
    ) -> T:
        """Execute a function with exponential backoff retry logic.

        This method handles transient failures (including 429 rate limits)
        by retrying with exponential backoff. On each retry:
        1. Wait with exponential backoff (2^attempt * base_delay, with jitter)
        2. If the error is a rate limit, also call on_rate_limit_response()

        Args:
            func: Async function to execute. Should be a coroutine factory
                (e.g., lambda: make_request()).
            max_retries: Maximum number of retry attempts. Defaults to 5.
            base_delay_seconds: Base delay for exponential backoff. Defaults to 1.0.
            max_delay_seconds: Maximum delay cap. Defaults to 60.0.
            is_rate_limit_error: Optional function to detect if an exception
                is a rate limit error. If None, defaults to checking for
                common rate limit indicators.

        Returns:
            The result of the function call.

        Raises:
            Exception: The last exception if all retries are exhausted.

        Example:
            async def fetch_data():
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 429:
                            raise RateLimitError("Rate limited")
                        return await resp.json()

            result = await limiter.retry_with_backoff(fetch_data)
        """
        max_retries = max_retries if max_retries is not None else self.DEFAULT_MAX_RETRIES
        base_delay = base_delay_seconds if base_delay_seconds is not None else self.DEFAULT_BASE_DELAY_SECONDS
        max_delay = max_delay_seconds if max_delay_seconds is not None else self.DEFAULT_MAX_DELAY_SECONDS

        def default_is_rate_limit(e: Exception) -> bool:
            """Check if exception indicates a rate limit error."""
            error_str = str(e).lower()
            return "429" in error_str or "rate limit" in error_str or "too many requests" in error_str

        is_rate_limit = is_rate_limit_error if is_rate_limit_error is not None else default_is_rate_limit

        last_exception: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                # Acquire a token before making the request
                await self.acquire()

                # Execute the function
                result = await func()

                # Success! Update stats if this was a retry
                if attempt > 0:
                    self._stats.retry_successes += 1
                    logger.info(
                        "Request succeeded on retry attempt %d",
                        attempt,
                    )

                return result

            except Exception as e:
                last_exception = e
                self._stats.retry_attempts += 1

                if attempt >= max_retries:
                    logger.error(
                        "All %d retry attempts exhausted. Last error: %s",
                        max_retries,
                        str(e),
                    )
                    raise

                # Check if this is a rate limit error
                if is_rate_limit(e):
                    await self.on_rate_limit_response()

                # Calculate exponential backoff with jitter
                delay = min(
                    max_delay,
                    base_delay * (2**attempt) + random.uniform(0, 1),
                )

                logger.warning(
                    "Request failed (attempt %d/%d): %s. Retrying in %.2fs",
                    attempt + 1,
                    max_retries + 1,
                    str(e),
                    delay,
                )

                await asyncio.sleep(delay)

        # This should never be reached, but satisfy type checker
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected state in retry_with_backoff")

    def reset_stats(self) -> None:
        """Reset statistics to zero."""
        self._stats = RateLimiterStats()

    @property
    def tokens_available(self) -> float:
        """Get the current number of tokens available (may be stale)."""
        return self._tokens

    @property
    def requests_per_minute(self) -> int:
        """Get the configured requests per minute limit."""
        return self._requests_per_minute

    @property
    def burst_size(self) -> int:
        """Get the burst size (maximum tokens)."""
        return self._burst_size

    @property
    def min_interval_seconds(self) -> float:
        """Get the minimum interval between requests in seconds."""
        return self._min_interval_seconds

    @property
    def initial_requests_per_minute(self) -> int:
        """Get the initial configured requests per minute limit."""
        return self._initial_requests_per_minute

    async def enqueue_request(self) -> None:
        """Enqueue a request and wait for it to be processed.

        This method adds a request to the queue and waits for its turn.
        Requests are processed in FIFO order, each waiting for a token
        before completing.

        This is useful for handling burst requests where multiple
        concurrent requests should be executed in order.

        Example:
            # Multiple concurrent requests will be processed in order
            async def make_requests():
                tasks = [
                    limiter.enqueue_request()
                    for _ in range(10)
                ]
                await asyncio.gather(*tasks)
                # All 10 requests have been rate-limited in order
        """
        event = asyncio.Event()
        await self._request_queue.put(event)

        # Start the queue processor if not running
        if self._queue_processor_task is None or self._queue_processor_task.done():
            self._queue_processor_task = asyncio.create_task(self._process_queue())

        # Wait for our turn
        await event.wait()

    async def _process_queue(self) -> None:
        """Process queued requests in order.

        This runs as a background task, processing requests one at a time.
        Each request waits for a token before signaling completion.
        """
        while True:
            try:
                # Get the next request from the queue (with timeout)
                event = await asyncio.wait_for(
                    self._request_queue.get(),
                    timeout=1.0,
                )

                # Acquire a token for this request
                await self.acquire()

                # Signal that this request can proceed
                event.set()

                logger.debug(
                    "Processed queued request. Queue size: %d",
                    self._request_queue.qsize(),
                )

            except TimeoutError:
                # No requests in queue, check if we should stop
                if self._request_queue.empty():
                    logger.debug("Request queue empty, stopping processor")
                    break

            except Exception as e:
                logger.error("Error processing request queue: %s", str(e))
                # Continue processing to avoid blocking queued requests
                continue

    def get_queue_size(self) -> int:
        """Get the current number of requests in the queue.

        Returns:
            Number of pending requests in the queue.
        """
        return self._request_queue.qsize()

    async def __aenter__(self) -> "TokenBucketRateLimiter":
        """Async context manager entry: acquire a token."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit: nothing to do."""
        pass


# Convenience function to create a rate limiter for CoinGecko
def create_coingecko_rate_limiter(pro_tier: bool = False) -> TokenBucketRateLimiter:
    """Create a rate limiter configured for CoinGecko API.

    Args:
        pro_tier: If True, configures for Pro tier (500 req/min).
                  If False, configures for free tier (50 req/min).

    Returns:
        Configured TokenBucketRateLimiter instance.
    """
    if pro_tier:
        # Pro tier: 500 requests per minute
        return TokenBucketRateLimiter(
            requests_per_minute=500,
            burst_size=50,  # Allow bursts of 50 requests
        )
    else:
        # Free tier: 50 requests per minute (conservatively, actual is ~30)
        return TokenBucketRateLimiter(
            requests_per_minute=50,
            burst_size=10,  # Allow bursts of 10 requests
        )


__all__ = [
    "TokenBucketRateLimiter",
    "RateLimiterStats",
    "create_coingecko_rate_limiter",
]
