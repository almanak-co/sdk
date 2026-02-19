"""Base classes for gateway integrations.

This module provides the foundation for third-party data source integrations:
- BaseIntegration: Abstract base class for all integrations
- RateLimiter: Token bucket rate limiter
- IntegrationRegistry: Singleton registry for discovering integrations

Example:
    class MyIntegration(BaseIntegration):
        name = "my_integration"
        rate_limit_requests = 100  # requests per minute

        async def health_check(self) -> bool:
            # Check if the integration is healthy
            return True
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class IntegrationError(Exception):
    """Base exception for integration errors."""

    def __init__(self, integration: str, message: str, code: str = "UNKNOWN"):
        self.integration = integration
        self.message = message
        self.code = code
        super().__init__(f"[{integration}] {message}")


class IntegrationRateLimitError(IntegrationError):
    """Raised when rate limit is exceeded."""

    def __init__(self, integration: str, retry_after: float):
        self.retry_after = retry_after
        super().__init__(
            integration,
            f"Rate limited, retry after {retry_after:.2f}s",
            code="RATE_LIMITED",
        )


@dataclass
class CacheEntry:
    """Cache entry with TTL tracking."""

    data: Any
    cached_at: datetime
    ttl_seconds: int

    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        age = (datetime.now(UTC) - self.cached_at).total_seconds()
        return age > self.ttl_seconds


@dataclass
class HealthMetrics:
    """Health metrics for integration observability."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limited_requests: int = 0
    cache_hits: int = 0
    total_latency_ms: float = 0.0
    last_error: str | None = None
    last_error_time: datetime | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_latency_ms / self.successful_requests

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for metrics export."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "rate_limited_requests": self.rate_limited_requests,
            "cache_hits": self.cache_hits,
            "success_rate": round(self.success_rate, 2),
            "average_latency_ms": round(self.average_latency_ms, 2),
            "last_error": self.last_error,
            "last_error_time": (self.last_error_time.isoformat() if self.last_error_time else None),
        }


class RateLimiter:
    """Token bucket rate limiter for API calls.

    Implements a token bucket algorithm where tokens are added at a fixed rate
    and consumed on each request. If no tokens are available, requests must wait.

    Attributes:
        requests_per_minute: Maximum requests allowed per minute
        bucket_size: Maximum tokens in bucket (defaults to requests_per_minute)

    Example:
        limiter = RateLimiter(requests_per_minute=60)

        # Before each request:
        await limiter.acquire()  # Blocks if rate limited
    """

    def __init__(
        self,
        requests_per_minute: int,
        bucket_size: int | None = None,
    ):
        """Initialize rate limiter.

        Args:
            requests_per_minute: Maximum requests per minute
            bucket_size: Maximum tokens in bucket (optional)

        Raises:
            ValueError: If requests_per_minute <= 0 or bucket_size <= 0
        """
        if requests_per_minute <= 0:
            raise ValueError(f"requests_per_minute must be > 0, got {requests_per_minute}")

        self.requests_per_minute = requests_per_minute
        self.bucket_size = bucket_size if bucket_size is not None else requests_per_minute

        if self.bucket_size <= 0:
            raise ValueError(f"bucket_size must be > 0, got {self.bucket_size}")

        # Token state
        self._tokens = float(self.bucket_size)
        self._last_refill = time.time()
        self._lock = asyncio.Lock()

        # Calculate refill rate (tokens per second)
        self._refill_rate = requests_per_minute / 60.0

    async def acquire(self, tokens: int = 1) -> float:
        """Acquire tokens, waiting if necessary.

        Uses a loop to ensure atomic check-and-consume: holds the lock while
        checking availability and consuming tokens. If tokens are not available,
        releases the lock, sleeps, then retries. This prevents race conditions
        where another coroutine could take tokens between the check and consume.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            Time waited in seconds (0 if no wait needed)

        Raises:
            ValueError: If tokens <= 0 or tokens > bucket_size
        """
        if tokens <= 0:
            raise ValueError(f"tokens must be > 0, got {tokens}")
        if tokens > self.bucket_size:
            raise ValueError(f"tokens ({tokens}) cannot exceed bucket_size ({self.bucket_size})")

        total_wait_time = 0.0

        while True:
            async with self._lock:
                # Refill tokens based on elapsed time
                now = time.time()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self.bucket_size,
                    self._tokens + elapsed * self._refill_rate,
                )
                self._last_refill = now

                # Check if we have enough tokens
                if self._tokens >= tokens:
                    # Consume tokens atomically with the availability check
                    self._tokens -= tokens
                    return total_wait_time

                # Calculate wait time needed
                tokens_needed = tokens - self._tokens
                wait_time = tokens_needed / self._refill_rate

            # Wait outside the lock, then retry
            await asyncio.sleep(wait_time)
            total_wait_time += wait_time

    def get_wait_time(self, tokens: int = 1) -> float:
        """Get estimated wait time without acquiring.

        Args:
            tokens: Number of tokens needed

        Returns:
            Estimated wait time in seconds
        """
        now = time.time()
        elapsed = now - self._last_refill
        current_tokens = min(
            self.bucket_size,
            self._tokens + elapsed * self._refill_rate,
        )

        if current_tokens >= tokens:
            return 0.0

        tokens_needed = tokens - current_tokens
        return tokens_needed / self._refill_rate


class BaseIntegration(ABC):
    """Abstract base class for gateway integrations.

    Provides common functionality for third-party data source integrations:
    - Rate limiting with configurable limits
    - Response caching with TTL
    - Health metrics tracking
    - HTTP session management

    Subclasses must implement:
    - name: Unique integration name
    - rate_limit_requests: Requests per minute limit
    - health_check(): Health check method

    Example:
        class BinanceIntegration(BaseIntegration):
            name = "binance"
            rate_limit_requests = 1200

            async def health_check(self) -> bool:
                # Try to fetch exchange info
                try:
                    await self._fetch("/api/v3/ping")
                    return True
                except Exception:
                    return False

            async def get_ticker(self, symbol: str) -> dict:
                return await self._cached_fetch(
                    f"/api/v3/ticker/24hr?symbol={symbol}",
                    cache_key=f"ticker:{symbol}",
                    ttl=10,
                )
    """

    # Subclasses must define these
    name: str = ""
    rate_limit_requests: int = 60  # requests per minute
    default_cache_ttl: int = 30  # seconds

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        request_timeout: float = 30.0,
    ):
        """Initialize the integration.

        Args:
            api_key: Optional API key for authenticated endpoints
            base_url: Base URL for API requests (can override default)
            request_timeout: HTTP request timeout in seconds
        """
        self._api_key = api_key
        self._base_url = base_url or ""
        self._request_timeout = request_timeout

        # Rate limiter
        self._rate_limiter = RateLimiter(
            requests_per_minute=self.rate_limit_requests,
        )

        # Cache: key -> CacheEntry
        self._cache: dict[str, CacheEntry] = {}

        # Health metrics
        self._metrics = HealthMetrics()

        # HTTP session (created on first request)
        self._session: aiohttp.ClientSession | None = None

        logger.info(
            "Initialized %s integration",
            self.name,
            extra={
                "rate_limit": self.rate_limit_requests,
                "cache_ttl": self.default_cache_ttl,
            },
        )

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the integration is healthy and responding.

        Returns:
            True if healthy, False otherwise
        """
        pass

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests.

        Override in subclasses to add authentication headers.

        Returns:
            Dictionary of headers
        """
        return {
            "Accept": "application/json",
            "User-Agent": "Almanak-Gateway/1.0",
        }

    def _get_cached(self, cache_key: str) -> Any | None:
        """Get cached data if exists and not expired.

        Args:
            cache_key: Cache key

        Returns:
            Cached data or None if not found/expired
        """
        entry = self._cache.get(cache_key)
        if entry is None or entry.is_expired():
            return None
        self._metrics.cache_hits += 1
        return entry.data

    def _update_cache(self, cache_key: str, data: Any, ttl: int | None = None) -> None:
        """Update cache with data.

        Args:
            cache_key: Cache key
            data: Data to cache
            ttl: TTL in seconds (optional, uses default_cache_ttl). Pass 0 to disable caching.
        """
        self._cache[cache_key] = CacheEntry(
            data=data,
            cached_at=datetime.now(UTC),
            ttl_seconds=ttl if ttl is not None else self.default_cache_ttl,
        )

    async def _fetch(
        self,
        path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> Any:
        """Make an API request with rate limiting.

        Args:
            path: API path (appended to base_url)
            method: HTTP method
            params: Query parameters
            json_data: JSON body for POST requests

        Returns:
            JSON response data

        Raises:
            IntegrationError: On API errors
            IntegrationRateLimitError: When rate limited
        """
        self._metrics.total_requests += 1
        start_time = time.time()

        # Apply rate limiting
        wait_time = await self._rate_limiter.acquire()
        if wait_time > 0:
            logger.debug(
                "Rate limiter wait for %s: %.2fs",
                self.name,
                wait_time,
            )

        url = f"{self._base_url}{path}"

        try:
            session = await self._get_session()
            headers = self._get_headers()

            async with session.request(
                method,
                url,
                params=params,
                json=json_data,
                headers=headers,
            ) as response:
                latency_ms = (time.time() - start_time) * 1000

                # Handle rate limiting from API
                if response.status == 429:
                    self._metrics.rate_limited_requests += 1
                    retry_after = float(response.headers.get("Retry-After", "60"))
                    raise IntegrationRateLimitError(self.name, retry_after)

                # Handle errors
                if response.status >= 400:
                    error_text = await response.text()
                    self._metrics.failed_requests += 1
                    self._metrics.last_error = f"HTTP {response.status}: {error_text}"
                    self._metrics.last_error_time = datetime.now(UTC)
                    raise IntegrationError(
                        self.name,
                        f"HTTP {response.status}: {error_text}",
                        code=f"HTTP_{response.status}",
                    )

                # Parse response
                data = await response.json()

                # Update metrics
                self._metrics.successful_requests += 1
                self._metrics.total_latency_ms += latency_ms

                logger.debug(
                    "%s API call: %s (latency: %.2fms)",
                    self.name,
                    path,
                    latency_ms,
                )

                return data

        except aiohttp.ClientError as e:
            self._metrics.failed_requests += 1
            self._metrics.last_error = str(e)
            self._metrics.last_error_time = datetime.now(UTC)
            raise IntegrationError(self.name, str(e), code="NETWORK_ERROR") from e

        except TimeoutError:
            self._metrics.failed_requests += 1
            self._metrics.last_error = f"Timeout after {self._request_timeout}s"
            self._metrics.last_error_time = datetime.now(UTC)
            raise IntegrationError(
                self.name,
                f"Timeout after {self._request_timeout}s",
                code="TIMEOUT",
            ) from None

    async def _cached_fetch(
        self,
        path: str,
        cache_key: str,
        ttl: int | None = None,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> Any:
        """Fetch data with caching.

        Args:
            path: API path
            cache_key: Cache key for this request
            ttl: Cache TTL in seconds (optional)
            method: HTTP method
            params: Query parameters
            json_data: JSON body

        Returns:
            Response data (from cache or fresh)
        """
        # Check cache first
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Fetch fresh data
        data = await self._fetch(path, method, params, json_data)

        # Update cache
        self._update_cache(cache_key, data, ttl)

        return data

    def get_health_metrics(self) -> dict[str, Any]:
        """Get current health metrics.

        Returns:
            Dictionary of health metrics
        """
        return self._metrics.to_dict()

    def clear_cache(self) -> None:
        """Clear the response cache."""
        self._cache.clear()
        logger.info("Cleared %s cache", self.name)

    async def __aenter__(self) -> "BaseIntegration":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


@dataclass
class IntegrationRegistry:
    """Registry for managing gateway integrations.

    Provides a centralized place to register, discover, and access integrations.
    Implemented as a singleton.

    Example:
        registry = IntegrationRegistry.get_instance()

        # Register integrations
        registry.register(BinanceIntegration())
        registry.register(CoinGeckoIntegration())

        # Get integration by name
        binance = registry.get("binance")

        # Health check all integrations
        health = await registry.health_check_all()
    """

    _instance: "IntegrationRegistry | None" = field(default=None, repr=False)
    _integrations: dict[str, BaseIntegration] = field(default_factory=dict)

    @classmethod
    def get_instance(cls) -> "IntegrationRegistry":
        """Get the singleton registry instance."""
        if cls._instance is None:
            cls._instance = IntegrationRegistry()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the registry (for testing)."""
        cls._instance = None

    def register(self, integration: BaseIntegration) -> None:
        """Register an integration.

        Args:
            integration: Integration instance to register
        """
        if not integration.name:
            raise ValueError("Integration must have a name")

        self._integrations[integration.name] = integration
        logger.info("Registered integration: %s", integration.name)

    def get(self, name: str) -> BaseIntegration | None:
        """Get an integration by name.

        Args:
            name: Integration name

        Returns:
            Integration instance or None if not found
        """
        return self._integrations.get(name)

    def list_integrations(self) -> list[str]:
        """List all registered integration names.

        Returns:
            List of integration names
        """
        return list(self._integrations.keys())

    async def health_check_all(self) -> dict[str, bool]:
        """Run health checks on all integrations.

        Returns:
            Dictionary mapping integration name to health status
        """
        results: dict[str, bool] = {}

        for name, integration in self._integrations.items():
            try:
                results[name] = await integration.health_check()
            except Exception as e:
                logger.warning("Health check failed for %s: %s", name, e)
                results[name] = False

        return results

    async def close_all(self) -> None:
        """Close all integrations."""
        for integration in self._integrations.values():
            await integration.close()
