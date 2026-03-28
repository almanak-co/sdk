"""CoinGecko Price Source implementation.

This module provides a production-ready price source using the CoinGecko API,
with proper caching, rate limiting, and error handling.

Key Features:
    - Response caching with configurable TTL
    - Graceful degradation on timeout (returns stale data with reduced confidence)
    - Exponential backoff with jitter for rate limits (429)
    - Comprehensive logging for observability

Example:
    from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource

    source = CoinGeckoPriceSource(api_key="your-api-key")
    result = await source.get_price("WETH", "USD")
    print(f"Price: {result.price}, Confidence: {result.confidence}")
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp

from almanak.core.constants import STABLECOINS
from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceRateLimited,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key

logger = logging.getLogger(__name__)


# Token ID mappings for Arbitrum tokens
# CoinGecko uses specific IDs for each token
ARBITRUM_TOKEN_IDS: dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDC.E": "usd-coin",
    "ARB": "arbitrum",
    "WBTC": "wrapped-bitcoin",
    "USDT": "tether",
    "DAI": "dai",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "GMX": "gmx",
    "PENDLE": "pendle",
    "RDNT": "radiant-capital",
    "SOL": "solana",
    "JOE": "trader-joe",
    "LDO": "lido-dao",
    "BTC": "bitcoin",
    "STETH": "lido-dao-wrapped-staked-eth",
    "WSTETH": "wrapped-steth",
    "CBETH": "coinbase-wrapped-staked-eth",
    "USDE": "ethena-usde",
    "SUSDE": "ethena-staked-usde",
}

# Token ID mappings for Avalanche tokens
AVALANCHE_TOKEN_IDS: dict[str, str] = {
    "AVAX": "avalanche-2",
    "WAVAX": "avalanche-2",  # Wrapped AVAX uses same price as AVAX
    "USDC": "usd-coin",
    "USDC.E": "usd-coin",
    "USDT": "tether",
    "USDT.E": "tether",
    "DAI": "dai",
    "DAI.E": "dai",
    "WETH": "weth",
    "WETH.E": "weth",
    "WBTC": "wrapped-bitcoin",
    "WBTC.E": "wrapped-bitcoin",
    "JOE": "trader-joe",
    "PNG": "pangolin",
    "QI": "benqi",
    "LINK": "chainlink",
    "AAVE": "aave",
    "BTC.B": "bitcoin",
}

# Token ID mappings for Base tokens
BASE_TOKEN_IDS: dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDBC": "usd-coin",  # Bridged USDC on Base, pegged to $1
    "USDT": "tether",
    "DAI": "dai",
    "CBETH": "coinbase-wrapped-staked-eth",
    "WSTETH": "wrapped-steth",
    "AERO": "aerodrome-finance",
    "BASE": "base-protocol",
    "DEGEN": "degen-base",
    "BRETT": "brett",
}

# Token ID mappings for BSC tokens
BSC_TOKEN_IDS: dict[str, str] = {
    "BNB": "binancecoin",
    "WBNB": "binancecoin",  # Wrapped BNB uses same price as BNB
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "WETH": "weth",  # Bridged ETH on BSC
    "BTCB": "bitcoin",
    "CAKE": "pancakeswap-token",
    "BUSD": "binance-usd",
}

# Token ID mappings for Solana tokens
SOLANA_TOKEN_IDS: dict[str, str] = {
    "SOL": "solana",
    "WSOL": "solana",  # Wrapped SOL uses same price as SOL
    "USDC": "usd-coin",
    "USDT": "tether",
    "JUP": "jupiter-exchange-solana",
    "RAY": "raydium",
    "ORCA": "orca",
    "BONK": "bonk",
    "WIF": "dogwifcoin",
    "JTO": "jito-governance-token",
    "PYTH": "pyth-network",
    "MSOL": "msol",
    "JITOSOL": "jito-staked-sol",
}

# Combined token mappings (chain-agnostic fallback)
# Used when chain-specific mapping not found
MANTLE_TOKEN_IDS: dict[str, str] = {
    "MNT": "mantle",
    "WMNT": "mantle",  # Wrapped MNT uses same price as MNT
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDT": "tether",
}

GLOBAL_TOKEN_IDS: dict[str, str] = {
    **ARBITRUM_TOKEN_IDS,
    **AVALANCHE_TOKEN_IDS,
    **BASE_TOKEN_IDS,
    **BSC_TOKEN_IDS,
    **MANTLE_TOKEN_IDS,
    **SOLANA_TOKEN_IDS,
    # Ethena tokens (available on multiple chains)
    "USDE": "ethena-usde",
    "SUSDE": "ethena-staked-usde",
}


@dataclass
class CacheEntry:
    """Cache entry for price data."""

    result: PriceResult
    cached_at: datetime
    fetch_latency_ms: float = 0.0


@dataclass
class RateLimitState:
    """Tracks rate limit state for exponential backoff."""

    last_429_time: float | None = None
    backoff_seconds: float = 1.0
    consecutive_429s: int = 0
    max_backoff_seconds: float = 10.0

    def record_rate_limit(self) -> None:
        """Record a rate limit hit and increase backoff."""
        self.last_429_time = time.time()
        self.consecutive_429s += 1
        # Exponential backoff: 1s, 2s, 4s, 8s, max 10s (capped to prevent timeouts)
        self.backoff_seconds = min(self.max_backoff_seconds, 2 ** (self.consecutive_429s - 1))

    def record_success(self) -> None:
        """Record successful request, fully reset backoff state."""
        self.consecutive_429s = 0
        self.backoff_seconds = 1.0
        self.last_429_time = None

    def get_wait_time(self) -> float:
        """Get time to wait before next request (with jitter)."""
        if self.last_429_time is None:
            return 0.0
        elapsed = time.time() - self.last_429_time
        remaining = self.backoff_seconds - elapsed
        if remaining <= 0:
            return 0.0
        # Add jitter: 0-25% of remaining time
        jitter = random.uniform(0, 0.25 * remaining)
        return remaining + jitter


@dataclass
class SourceHealthMetrics:
    """Health metrics for observability."""

    total_requests: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    timeouts: int = 0
    rate_limits: int = 0
    errors: int = 0
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
        """Convert to dictionary for logging/metrics."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "cache_hits": self.cache_hits,
            "timeouts": self.timeouts,
            "rate_limits": self.rate_limits,
            "errors": self.errors,
            "success_rate": round(self.success_rate, 2),
            "average_latency_ms": round(self.average_latency_ms, 2),
            "last_error": self.last_error,
            "last_error_time": (self.last_error_time.isoformat() if self.last_error_time else None),
        }


class CoinGeckoPriceSource(BasePriceSource):
    """CoinGecko price source with caching, rate limiting, and graceful degradation.

    This implementation follows the contract defined by BasePriceSource:
    1. On success: Return fresh PriceResult with confidence=1.0
    2. On timeout with cache: Return stale PriceResult with reduced confidence
    3. On timeout without cache: Raise DataSourceUnavailable
    4. On rate limit: Raise DataSourceRateLimited with retry_after

    Attributes:
        api_key: Optional CoinGecko API key (uses pro API if provided)
        cache_ttl: Cache time-to-live in seconds (default 30)
        request_timeout: HTTP request timeout in seconds (default 10)

    Example:
        # Create source with default settings
        source = CoinGeckoPriceSource()

        # Create source with API key and custom TTL
        source = CoinGeckoPriceSource(
            api_key="your-api-key",
            cache_ttl=60,
            request_timeout=15,
        )

        # Fetch price
        result = await source.get_price("WETH", "USD")
        if result.stale:
            logger.warning("Using stale price data")
    """

    # API endpoints
    _FREE_API_BASE = "https://api.coingecko.com/api/v3"
    _PRO_API_BASE = "https://pro-api.coingecko.com/api/v3"

    # Supported tokens on Arbitrum
    _SUPPORTED_TOKENS = list(ARBITRUM_TOKEN_IDS.keys())

    # Cached token registry (lazy-loaded)
    _token_registry: Any = None

    def __init__(
        self,
        api_key: str | None = None,
        cache_ttl: int = 30,
        request_timeout: float = 10.0,
        stale_confidence_multiplier: float = 0.7,
    ) -> None:
        """Initialize the CoinGecko price source.

        Args:
            api_key: Optional CoinGecko API key. If provided, uses pro API.
            cache_ttl: Cache time-to-live in seconds. Default 30.
            request_timeout: HTTP request timeout in seconds. Default 10.
            stale_confidence_multiplier: Confidence multiplier for stale data (0-1).
                Default 0.7 means stale data has 70% of original confidence.
        """
        self._api_key = (_get_gateway_api_key("COINGECKO_API_KEY") or "") if api_key is None else api_key
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._stale_confidence_multiplier = stale_confidence_multiplier

        # Select API base URL based on whether we have an API key (from param or env)
        self._api_base = self._PRO_API_BASE if self._api_key else self._FREE_API_BASE

        # Cache: key -> CacheEntry
        self._cache: dict[str, CacheEntry] = {}

        # Rate limit tracking per endpoint
        self._rate_limit_state = RateLimitState()

        # Health metrics
        self._metrics = SourceHealthMetrics()

        # HTTP session (created on first request)
        self._session: aiohttp.ClientSession | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None

        logger.info(
            "Initialized CoinGeckoPriceSource",
            extra={
                "api_type": "pro" if self._api_key else "free",
                "cache_ttl": cache_ttl,
                "request_timeout": request_timeout,
            },
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session, recreating if event loop changed."""
        current_loop = asyncio.get_running_loop()
        if self._session is not None and not self._session.closed:
            if self._session_loop is not None and self._session_loop is not current_loop:
                try:
                    await self._session.close()
                except Exception:
                    pass
                self._session = None
                self._session_loop = None
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._session_loop = current_loop
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self._session_loop = None

    def _get_cache_key(self, token: str, quote: str) -> str:
        """Generate cache key for token/quote pair."""
        return f"{token.upper()}/{quote.upper()}"

    def _get_cached(self, token: str, quote: str) -> CacheEntry | None:
        """Get cached entry if exists and not expired."""
        cache_key = self._get_cache_key(token, quote)
        entry = self._cache.get(cache_key)
        if entry is None:
            return None

        # Check if expired
        age_seconds = (datetime.now(UTC) - entry.cached_at).total_seconds()
        if age_seconds > self._cache_ttl:
            return None

        return entry

    def _get_stale_cached(self, token: str, quote: str) -> CacheEntry | None:
        """Get cached entry even if expired (for fallback)."""
        cache_key = self._get_cache_key(token, quote)
        return self._cache.get(cache_key)

    def _update_cache(self, token: str, quote: str, result: PriceResult, latency_ms: float) -> None:
        """Update cache with fresh result."""
        cache_key = self._get_cache_key(token, quote)
        self._cache[cache_key] = CacheEntry(
            result=result,
            cached_at=datetime.now(UTC),
            fetch_latency_ms=latency_ms,
        )

    def _resolve_token_id(self, token: str) -> str | None:
        """Resolve token symbol to CoinGecko ID.

        First tries to resolve from the token registry (dynamic, scalable),
        then falls back to hardcoded mappings (for backward compatibility).

        Args:
            token: Token symbol (already uppercased)

        Returns:
            CoinGecko ID if found, None otherwise
        """
        # Try token registry first (dynamic, uses Token.coingecko_id)
        if CoinGeckoPriceSource._token_registry is None:
            try:
                from almanak.framework.data.tokens import get_default_registry

                CoinGeckoPriceSource._token_registry = get_default_registry()
            except Exception:
                # Registry unavailable, will fall back to hardcoded mappings
                CoinGeckoPriceSource._token_registry = False  # Mark as attempted

        if CoinGeckoPriceSource._token_registry:
            token_obj = CoinGeckoPriceSource._token_registry.get(token)
            if token_obj and token_obj.coingecko_id:
                return token_obj.coingecko_id

        # Fall back to hardcoded mappings (backward compatibility)
        return GLOBAL_TOKEN_IDS.get(token)

    async def get_price(self, token: str, quote: str = "USD", *, resolved_token: object | None = None) -> PriceResult:
        """Fetch the current price for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ARB", "USDC")
            quote: Quote currency (default "USD")

        Returns:
            PriceResult with price and metadata

        Raises:
            DataSourceUnavailable: If source is unavailable and no cache exists
            DataSourceRateLimited: If rate limit is exceeded
        """
        self._metrics.total_requests += 1
        token_upper = token.upper()
        quote_upper = quote.upper()

        # Check for cached data first
        cached = self._get_cached(token_upper, quote_upper)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            logger.debug(
                "Cache hit for %s/%s",
                token_upper,
                quote_upper,
                extra={"token": token_upper, "quote": quote_upper, "source": "cache"},
            )
            return cached.result

        # Check rate limit backoff
        wait_time = self._rate_limit_state.get_wait_time()
        if wait_time > 0:
            logger.info(
                "Rate limit backoff: waiting %.2fs before request for %s/%s",
                wait_time,
                token_upper,
                quote_upper,
            )
            await asyncio.sleep(wait_time)

        # Resolve token ID
        token_id = self._resolve_token_id(token_upper)
        if token_id is None:
            # Stablecoin fallback: tokens like FUSDT0, USDbC, etc. may not be
            # listed on CoinGecko but are known USD-pegged stablecoins.
            if token_upper in STABLECOINS and quote_upper == "USD":
                logger.info(f"Token {token_upper} not on CoinGecko, using stablecoin fallback ($1.00)")
                result = PriceResult(
                    price=Decimal("1"),
                    source=f"{self.source_name}/stablecoin_fallback",
                    timestamp=datetime.now(UTC),
                    confidence=0.9,
                    stale=False,
                )
                self._update_cache(token_upper, quote_upper, result, 0.0)
                self._metrics.successful_requests += 1
                return result

            error_msg = f"Unknown token: {token_upper}"
            self._metrics.errors += 1
            self._metrics.last_error = error_msg
            self._metrics.last_error_time = datetime.now(UTC)
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=error_msg,
            )

        # Build API URL
        url = f"{self._api_base}/simple/price"
        params: dict[str, str] = {
            "ids": token_id,
            "vs_currencies": quote_upper.lower(),
        }
        if self._api_key:
            params["x_cg_pro_api_key"] = self._api_key

        start_time = time.time()

        try:
            session = await self._get_session()
            async with session.get(url, params=params) as response:
                latency_ms = (time.time() - start_time) * 1000

                # Handle rate limiting (429)
                if response.status == 429:
                    self._rate_limit_state.record_rate_limit()
                    self._metrics.rate_limits += 1
                    retry_after = self._rate_limit_state.backoff_seconds

                    logger.warning(
                        "Rate limited by CoinGecko for %s/%s, backoff: %.2fs",
                        token_upper,
                        quote_upper,
                        retry_after,
                        extra={
                            "token": token_upper,
                            "quote": quote_upper,
                            "consecutive_429s": self._rate_limit_state.consecutive_429s,
                        },
                    )

                    # Try to return stale data if available
                    stale = self._get_stale_cached(token_upper, quote_upper)
                    if stale is not None:
                        logger.info(
                            "Returning stale data for %s/%s due to rate limit",
                            token_upper,
                            quote_upper,
                        )
                        self._metrics.successful_requests += 1
                        return PriceResult(
                            price=stale.result.price,
                            source=self.source_name,
                            timestamp=stale.result.timestamp,
                            confidence=stale.result.confidence * self._stale_confidence_multiplier,
                            stale=True,
                        )

                    raise DataSourceRateLimited(
                        source=self.source_name,
                        retry_after=retry_after,
                    )

                # Handle other HTTP errors
                if response.status != 200:
                    error_msg = f"HTTP {response.status}: {await response.text()}"
                    self._metrics.errors += 1
                    self._metrics.last_error = error_msg
                    self._metrics.last_error_time = datetime.now(UTC)

                    logger.error(
                        "CoinGecko API error for %s/%s: %s",
                        token_upper,
                        quote_upper,
                        error_msg,
                    )

                    # Try stale data
                    stale = self._get_stale_cached(token_upper, quote_upper)
                    if stale is not None:
                        logger.info(
                            "Returning stale data for %s/%s due to API error",
                            token_upper,
                            quote_upper,
                        )
                        self._metrics.successful_requests += 1
                        return PriceResult(
                            price=stale.result.price,
                            source=self.source_name,
                            timestamp=stale.result.timestamp,
                            confidence=stale.result.confidence * self._stale_confidence_multiplier,
                            stale=True,
                        )

                    raise DataSourceUnavailable(
                        source=self.source_name,
                        reason=error_msg,
                    )

                # Parse successful response
                data = await response.json()

                # Reset rate limit state on success
                self._rate_limit_state.record_success()

                # Extract price from response
                # Response format: {"token_id": {"usd": 1234.56}}
                quote_lower = quote_upper.lower()
                if token_id not in data:
                    error_msg = f"Token {token_id} not in response"
                    self._metrics.errors += 1
                    self._metrics.last_error = error_msg
                    self._metrics.last_error_time = datetime.now(UTC)
                    raise DataSourceUnavailable(
                        source=self.source_name,
                        reason=error_msg,
                    )

                if quote_lower not in data[token_id]:
                    error_msg = f"Quote {quote_upper} not in response for {token_id}"
                    self._metrics.errors += 1
                    self._metrics.last_error = error_msg
                    self._metrics.last_error_time = datetime.now(UTC)
                    raise DataSourceUnavailable(
                        source=self.source_name,
                        reason=error_msg,
                    )

                price = Decimal(str(data[token_id][quote_lower]))

                # Create result
                result = PriceResult(
                    price=price,
                    source=self.source_name,
                    timestamp=datetime.now(UTC),
                    confidence=1.0,
                    stale=False,
                )

                # Update cache
                self._update_cache(token_upper, quote_upper, result, latency_ms)

                # Update metrics
                self._metrics.successful_requests += 1
                self._metrics.total_latency_ms += latency_ms

                logger.debug(
                    "Fetched price for %s/%s: %s (latency: %.2fms)",
                    token_upper,
                    quote_upper,
                    price,
                    latency_ms,
                )

                return result

        except TimeoutError as e:
            self._metrics.timeouts += 1
            latency_ms = (time.time() - start_time) * 1000

            logger.warning(
                "Timeout fetching %s/%s after %.2fms",
                token_upper,
                quote_upper,
                latency_ms,
                extra={
                    "token": token_upper,
                    "quote": quote_upper,
                    "timeout_seconds": self._request_timeout,
                },
            )

            # Try to return stale data
            stale = self._get_stale_cached(token_upper, quote_upper)
            if stale is not None:
                logger.info(
                    "Returning stale data for %s/%s due to timeout",
                    token_upper,
                    quote_upper,
                )
                self._metrics.successful_requests += 1
                return PriceResult(
                    price=stale.result.price,
                    source=self.source_name,
                    timestamp=stale.result.timestamp,
                    confidence=stale.result.confidence * self._stale_confidence_multiplier,
                    stale=True,
                )

            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Timeout after {self._request_timeout}s with no cache",
            ) from e

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            self._metrics.last_error = str(e)
            self._metrics.last_error_time = datetime.now(UTC)

            logger.error(
                "Network error fetching %s/%s: %s",
                token_upper,
                quote_upper,
                str(e),
            )

            # Try to return stale data
            stale = self._get_stale_cached(token_upper, quote_upper)
            if stale is not None:
                logger.info(
                    "Returning stale data for %s/%s due to network error",
                    token_upper,
                    quote_upper,
                )
                self._metrics.successful_requests += 1
                return PriceResult(
                    price=stale.result.price,
                    source=self.source_name,
                    timestamp=stale.result.timestamp,
                    confidence=stale.result.confidence * self._stale_confidence_multiplier,
                    stale=True,
                )

            raise DataSourceUnavailable(
                source=self.source_name,
                reason=str(e),
            ) from e

    @property
    def source_name(self) -> str:
        """Return the unique name of this data source."""
        return "coingecko"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported tokens across all chains."""
        return sorted(GLOBAL_TOKEN_IDS.keys())

    @property
    def cache_ttl_seconds(self) -> int:
        """Return the cache TTL for this source."""
        return self._cache_ttl

    def get_health_metrics(self) -> dict[str, Any]:
        """Get current health metrics for observability."""
        return self._metrics.to_dict()

    async def health_check(self) -> bool:
        """Check if the data source is healthy and responding.

        Returns:
            True if source is healthy, False otherwise
        """
        try:
            await self.get_price("ETH", "USD")
            return True
        except Exception as e:
            logger.warning("Health check failed: %s", str(e))
            return False

    def clear_cache(self) -> None:
        """Clear the price cache."""
        self._cache.clear()
        logger.info("Cleared CoinGecko price cache")

    async def __aenter__(self) -> "CoinGeckoPriceSource":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
