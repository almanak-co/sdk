"""CoinGecko Historical Data Provider for PnL backtesting.

This module provides a concrete implementation of the HistoricalDataProvider
protocol using the CoinGecko API to fetch historical price and OHLCV data.

Key Features:
    - Fetches historical OHLCV data with configurable date ranges
    - Implements rate limiting to respect CoinGecko API limits
    - Caches fetched data to minimize API calls
    - Supports the iterate() method for backtesting engine integration

Example:
    from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider
    from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
    from datetime import datetime

    provider = CoinGeckoDataProvider(api_key="your-api-key")
    config = HistoricalDataConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 6, 1),
        interval_seconds=3600,
        tokens=["WETH", "USDC", "ARB"],
    )

    async for timestamp, market_state in provider.iterate(config):
        eth_price = market_state.get_price("WETH")
        # ... process market state
"""

import asyncio
import logging
import os
import random
import sqlite3
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp

from almanak.framework.backtesting.config import BacktestDataConfig

from ..data_provider import OHLCV, HistoricalDataConfig, MarketState
from .rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)


# Token ID mappings for common tokens
# CoinGecko uses specific IDs for each token
TOKEN_IDS: dict[str, str] = {
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
    "CBETH": "coinbase-wrapped-staked-eth",
    "OP": "optimism",
    "AVAX": "avalanche-2",
    "BNB": "binancecoin",
    "MATIC": "matic-network",
    "AAVE": "aave",
    "CRV": "curve-dao-token",
}


class CoinGeckoRateLimitError(Exception):
    """Raised when CoinGecko rate limit is exceeded after max retries."""

    def __init__(
        self,
        message: str,
        retry_count: int = 0,
        last_backoff: float = 0.0,
    ) -> None:
        """Initialize rate limit error.

        Args:
            message: Error message
            retry_count: Number of retries attempted
            last_backoff: Last backoff duration in seconds
        """
        super().__init__(message)
        self.retry_count = retry_count
        self.last_backoff = last_backoff


@dataclass
class RetryConfig:
    """Configuration for retry behavior with exponential backoff.

    Attributes:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial backoff delay in seconds (default: 1.0)
        max_delay: Maximum backoff delay in seconds (default: 8.0)
        exponential_base: Base for exponential backoff calculation (default: 2)
    """

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 8.0
    exponential_base: int = 2

    def get_delay_for_attempt(self, attempt: int) -> float:
        """Calculate delay for a given retry attempt.

        Args:
            attempt: Retry attempt number (1-indexed)

        Returns:
            Delay in seconds with exponential backoff
        """
        # Exponential backoff: 1s, 2s, 4s, 8s
        delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        return min(delay, self.max_delay)

    @classmethod
    def for_backtest(cls) -> "RetryConfig":
        """Create a retry config tuned for sustained backtest workloads.

        Free-tier CoinGecko allows ~10-30 req/min. Long backtests (14+ days)
        can easily exceed this, so we need more retries with longer backoff
        to ride out rate-limit windows without failing the entire backtest.
        """
        return cls(max_retries=6, base_delay=2.0, max_delay=30.0, exponential_base=2)


@dataclass
class RateLimitState:
    """Tracks rate limit state for exponential backoff."""

    last_429_time: float | None = None
    backoff_seconds: float = 1.0
    consecutive_429s: int = 0
    requests_this_minute: int = 0
    minute_start: float = 0.0
    max_backoff_seconds: float = 30.0

    def record_rate_limit(self) -> None:
        """Record a rate limit hit and increase backoff."""
        self.last_429_time = time.time()
        self.consecutive_429s += 1
        # Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 30s
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

    def record_request(self) -> None:
        """Record a request for rate limiting."""
        current_time = time.time()
        if current_time - self.minute_start >= 60:
            # Reset counter for new minute
            self.minute_start = current_time
            self.requests_this_minute = 0
        self.requests_this_minute += 1


@dataclass
class HistoricalCacheStats:
    """Statistics for historical price cache monitoring.

    Used to track cache efficiency and hit rates during backtests.
    Target: >90% cache hit rate for repeated backtests.

    Attributes:
        total_requests: Total number of cache lookups
        cache_hits: Number of successful cache hits
        cache_misses: Number of cache misses requiring API calls
        cache_entries: Number of entries currently in cache
    """

    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_entries: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate as a percentage.

        Returns:
            Cache hit rate from 0.0 to 100.0
        """
        if self.total_requests == 0:
            return 0.0
        return (self.cache_hits / self.total_requests) * 100.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/metrics."""
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_entries": self.cache_entries,
            "hit_rate_percent": round(self.hit_rate, 2),
        }


@dataclass
class HistoricalCacheEntry:
    """Entry in the historical price cache.

    Attributes:
        price: The cached price
        cached_at: When this entry was cached
    """

    price: Decimal
    cached_at: datetime


class HistoricalPriceCache:
    """Cache for historical prices with (token, date) keys and configurable TTL.

    This cache is designed for aggressive caching of historical price data
    during backtests. Historical data doesn't change, so we use a long TTL
    (default 1 hour) to minimize API calls.

    Key features:
        - Cache key is (token, date_str) for date-level granularity
        - Default TTL of 1 hour (3600 seconds) for historical data
        - Optional SQLite persistence (survives process restarts)
        - Statistics tracking for monitoring cache hit rates
        - Cache warming support for pre-fetching date ranges

    Example:
        cache = HistoricalPriceCache(ttl_seconds=3600)
        cache.set("WETH", datetime(2024, 1, 15), Decimal("2500.00"))
        price = cache.get("WETH", datetime(2024, 1, 15))
        stats = cache.get_stats()
        print(f"Cache hit rate: {stats.hit_rate:.1f}%")

    Persistent mode:
        cache = HistoricalPriceCache(ttl_seconds=0, persistent=True)
        # Data survives across backtest runs — zero API calls on rerun
    """

    _DEFAULT_DB_NAME = "historical_prices.db"

    def __init__(
        self,
        ttl_seconds: int = 3600,
        persistent: bool = False,
        db_path: str | None = None,
    ) -> None:
        """Initialize the historical price cache.

        Args:
            ttl_seconds: Time-to-live for cache entries in seconds.
                        Default 3600 (1 hour). Use 0 to disable TTL
                        (recommended for persistent backtesting cache since
                        historical prices are immutable).
            persistent: If True, back the cache with SQLite for cross-run
                       persistence. Default False (in-memory only).
            db_path: Optional explicit path for the SQLite database.
                    Only used when persistent=True. Default:
                    ~/.almanak/cache/historical_prices.db
        """
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, HistoricalCacheEntry] = {}
        self._stats = HistoricalCacheStats()
        self._persistent = persistent
        self._db: sqlite3.Connection | None = None

        if persistent:
            self._init_db(db_path)

    def _init_db(self, db_path: str | None = None) -> None:
        """Initialize SQLite database for persistent cache.

        Falls back to in-memory-only mode if the database cannot be opened
        (e.g. read-only filesystem, missing HOME in containers).
        """
        try:
            if db_path is None:
                default_dir = Path.home() / ".almanak" / "cache"
                default_dir.mkdir(parents=True, exist_ok=True)
                db_path = str(default_dir / self._DEFAULT_DB_NAME)

            self._db = sqlite3.connect(db_path)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute(
                """
                CREATE TABLE IF NOT EXISTS historical_prices (
                    token TEXT NOT NULL,
                    date_str TEXT NOT NULL,
                    price TEXT NOT NULL,
                    cached_at TEXT NOT NULL,
                    PRIMARY KEY (token, date_str)
                )
                """
            )
            self._db.commit()
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            logger.warning("Could not open persistent cache (%s), falling back to in-memory only", exc)
            self._db = None
            self._persistent = False

    def _make_key(self, token: str, timestamp: datetime) -> str:
        """Create cache key from token and timestamp.

        Uses (token, date_str) format for date-level granularity.

        Args:
            token: Token symbol
            timestamp: Timestamp (date portion used for key)

        Returns:
            Cache key string
        """
        date_str = timestamp.strftime("%Y-%m-%d")
        return f"{token.upper()}:{date_str}"

    def get(self, token: str, timestamp: datetime) -> Decimal | None:
        """Get cached price for a token at a timestamp.

        Args:
            token: Token symbol
            timestamp: Timestamp to look up

        Returns:
            Cached price if found and not expired, None otherwise
        """
        self._stats.total_requests += 1
        key = self._make_key(token, timestamp)

        # Check in-memory first
        entry = self._cache.get(key)
        if entry is not None:
            if self._ttl_seconds > 0:
                age_seconds = (datetime.now(UTC) - entry.cached_at).total_seconds()
                if age_seconds > self._ttl_seconds:
                    del self._cache[key]
                    self._stats.cache_entries = len(self._cache)
                    # Fall through to check persistent store (another run may
                    # have refreshed the key in SQLite)
                else:
                    self._stats.cache_hits += 1
                    return entry.price
            else:
                self._stats.cache_hits += 1
                return entry.price

        # Check persistent store
        if self._persistent and self._db is not None:
            token_upper = token.upper()
            date_str = timestamp.strftime("%Y-%m-%d")
            row = self._db.execute(
                "SELECT price, cached_at FROM historical_prices WHERE token = ? AND date_str = ?",
                (token_upper, date_str),
            ).fetchone()
            if row is not None:
                price = Decimal(row[0])
                cached_at = datetime.fromisoformat(row[1])
                if self._ttl_seconds > 0:
                    age_seconds = (datetime.now(UTC) - cached_at).total_seconds()
                    if age_seconds > self._ttl_seconds:
                        self._stats.cache_misses += 1
                        return None
                # Promote to in-memory cache
                self._cache[key] = HistoricalCacheEntry(price=price, cached_at=cached_at)
                self._stats.cache_entries = len(self._cache)
                self._stats.cache_hits += 1
                return price

        self._stats.cache_misses += 1
        return None

    def set(self, token: str, timestamp: datetime, price: Decimal) -> None:
        """Store a price in the cache.

        Args:
            token: Token symbol
            timestamp: Timestamp for the price
            price: Price to cache
        """
        key = self._make_key(token, timestamp)
        now = datetime.now(UTC)
        self._cache[key] = HistoricalCacheEntry(price=price, cached_at=now)
        self._stats.cache_entries = len(self._cache)

        if self._persistent and self._db is not None:
            token_upper = token.upper()
            date_str = timestamp.strftime("%Y-%m-%d")
            self._db.execute(
                """INSERT OR REPLACE INTO historical_prices (token, date_str, price, cached_at)
                   VALUES (?, ?, ?, ?)""",
                (token_upper, date_str, str(price), now.isoformat()),
            )
            self._db.commit()

    def get_stats(self) -> HistoricalCacheStats:
        """Get current cache statistics.

        Returns:
            Copy of current cache statistics
        """
        return HistoricalCacheStats(
            total_requests=self._stats.total_requests,
            cache_hits=self._stats.cache_hits,
            cache_misses=self._stats.cache_misses,
            cache_entries=len(self._cache),
        )

    def reset_stats(self) -> None:
        """Reset cache statistics (but keep cached data)."""
        self._stats = HistoricalCacheStats(cache_entries=len(self._cache))

    def clear(self) -> None:
        """Clear all cached data and reset statistics."""
        self._cache.clear()
        self._stats = HistoricalCacheStats()
        if self._persistent and self._db is not None:
            self._db.execute("DELETE FROM historical_prices")
            self._db.commit()

    @property
    def ttl_seconds(self) -> int:
        """Get the cache TTL in seconds."""
        return self._ttl_seconds

    @property
    def persistent(self) -> bool:
        """Whether this cache is using SQLite persistence."""
        return self._persistent

    def close(self) -> None:
        """Close the SQLite connection if persistent."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def __len__(self) -> int:
        """Return number of entries in cache."""
        return len(self._cache)


@dataclass
class OHLCVCache:
    """Cache for OHLCV data to minimize API calls."""

    data: dict[str, list[OHLCV]]  # token -> list of OHLCV
    fetched_at: datetime

    def get_price_at(self, token: str, timestamp: datetime) -> Decimal | None:
        """Get interpolated price at a specific timestamp."""
        token_upper = token.upper()
        if token_upper not in self.data:
            return None

        ohlcv_list = self.data[token_upper]
        if not ohlcv_list:
            return None

        # Find the closest OHLCV candle
        for i, candle in enumerate(ohlcv_list):
            if candle.timestamp >= timestamp:
                # Use the close price of the previous candle if available
                if i > 0:
                    return ohlcv_list[i - 1].close
                return candle.open
            if i == len(ohlcv_list) - 1:
                # Past the last candle, use its close
                return candle.close

        return None

    def get_ohlcv_at(self, token: str, timestamp: datetime) -> OHLCV | None:
        """Get OHLCV data at or just before a specific timestamp."""
        token_upper = token.upper()
        if token_upper not in self.data:
            return None

        ohlcv_list = self.data[token_upper]
        if not ohlcv_list:
            return None

        # Find the closest OHLCV candle at or before the timestamp
        result: OHLCV | None = None
        for candle in ohlcv_list:
            if candle.timestamp <= timestamp:
                result = candle
            else:
                break

        return result


class CoinGeckoDataProvider:
    """CoinGecko historical data provider implementation.

    Implements the HistoricalDataProvider protocol to provide historical
    price and OHLCV data from the CoinGecko API for backtesting simulations.

    Attributes:
        api_key: Optional CoinGecko API key (uses pro API if provided)
        request_timeout: HTTP request timeout in seconds
        min_request_interval: Minimum interval between API requests in seconds

    Example:
        provider = CoinGeckoDataProvider(api_key="your-key")

        # Get a single historical price
        price = await provider.get_price("WETH", datetime(2024, 1, 15))

        # Get OHLCV data for a range
        ohlcv = await provider.get_ohlcv(
            "WETH",
            datetime(2024, 1, 1),
            datetime(2024, 1, 31),
            interval_seconds=3600,
        )

        # Iterate for backtesting
        async for ts, market_state in provider.iterate(config):
            price = market_state.get_price("WETH")
    """

    # API endpoints
    _FREE_API_BASE = "https://api.coingecko.com/api/v3"
    _PRO_API_BASE = "https://pro-api.coingecko.com/api/v3"

    # Supported tokens
    _SUPPORTED_TOKENS = list(TOKEN_IDS.keys())

    # Supported chains
    _SUPPORTED_CHAINS = ["arbitrum", "ethereum", "base", "optimism", "avalanche", "bnb", "bsc"]

    # Rate limits (requests per minute)
    # Free tier: ~10-30 calls/min, Pro tier: ~500 calls/min
    _FREE_RATE_LIMIT = 10
    _PRO_RATE_LIMIT = 500

    def __init__(
        self,
        api_key: str = "",
        request_timeout: float = 30.0,
        min_request_interval: float = 1.5,  # Default 1.5s between requests for free tier
        retry_config: RetryConfig | None = None,
        historical_cache_ttl: int = 3600,  # 1 hour default for historical data
        data_config: BacktestDataConfig | None = None,
        persistent_cache: bool = False,
    ) -> None:
        """Initialize the CoinGecko data provider.

        Args:
            api_key: Optional CoinGecko API key. If provided, uses pro API
                     with higher rate limits.
            request_timeout: HTTP request timeout in seconds. Default 30.
            min_request_interval: Minimum interval between API requests in seconds.
                                  Default 1.5 for free tier, set lower for pro tier.
            retry_config: Configuration for retry behavior with exponential backoff.
                          If not provided, uses default config (3 retries, 1-8s backoff).
            historical_cache_ttl: Time-to-live for historical price cache in seconds.
                                  Default 3600 (1 hour). Historical data is immutable,
                                  so longer TTL reduces API calls dramatically.
                                  Use 0 with persistent_cache=True to cache forever.
            data_config: Optional BacktestDataConfig for configuring rate limits.
                         If provided, uses coingecko_rate_limit_per_minute from config.
                         If not provided, uses default rates (10/min free, 500/min pro).
            persistent_cache: If True, back the historical price cache with SQLite
                            at ~/.almanak/cache/historical_prices.db. Cached prices
                            survive across process restarts, eliminating redundant
                            API calls on repeated backtests.
        """
        self._api_key = api_key or os.environ.get("COINGECKO_API_KEY", "")
        self._request_timeout = request_timeout
        self._min_request_interval = min_request_interval
        self._retry_config = retry_config or RetryConfig()
        self._data_config = data_config

        # Select API base URL based on whether we have an API key
        self._api_base = self._PRO_API_BASE if self._api_key else self._FREE_API_BASE

        # Determine rate limit: use config if provided, otherwise use tier-based defaults
        if data_config is not None:
            self._rate_limit = data_config.coingecko_rate_limit_per_minute
            logger.info(
                "Using rate limit from BacktestDataConfig: %d req/min",
                self._rate_limit,
            )
        else:
            # Fall back to tier-based defaults
            self._rate_limit = self._PRO_RATE_LIMIT if self._api_key else self._FREE_RATE_LIMIT
            logger.info(
                "Using default %s tier rate limit: %d req/min",
                "pro" if self._api_key else "free",
                self._rate_limit,
            )

        # If pro tier, reduce default request interval
        if self._api_key and min_request_interval == 1.5:
            self._min_request_interval = 0.2  # 200ms for pro tier

        # Rate limit tracking (reactive - for handling 429 responses)
        self._rate_limit_state = RateLimitState()
        self._last_request_time: float = 0.0

        # Proactive rate limiter (token bucket algorithm)
        # Configured based on config or tier-based defaults
        burst_size = max(1, self._rate_limit // 5)  # 1/5 of rate limit for burst
        self._rate_limiter = TokenBucketRateLimiter(
            requests_per_minute=self._rate_limit,
            burst_size=burst_size,
        )

        # OHLCV cache (per-backtest, for iterate())
        self._cache: OHLCVCache | None = None

        # Historical price cache (persistent across calls, keyed by token+date)
        # Uses 1-hour TTL by default since historical data is immutable
        self._historical_cache = HistoricalPriceCache(
            ttl_seconds=historical_cache_ttl,
            persistent=persistent_cache,
        )

        # HTTP session (created on first request)
        self._session: aiohttp.ClientSession | None = None

        logger.info(
            "Initialized CoinGeckoDataProvider: api_type=%s, rate_limit=%d req/min, "
            "burst_size=%d, request_timeout=%.1fs, max_retries=%d",
            "pro" if self._api_key else "free",
            self._rate_limit,
            burst_size,
            request_timeout,
            self._retry_config.max_retries,
        )

    def __repr__(self) -> str:
        """Return a safe representation without exposing the API key."""
        api_tier = "pro" if self._api_key else "free"
        return (
            f"CoinGeckoDataProvider("
            f"api_tier={api_tier}, "
            f"rate_limit={self._rate_limit}/min, "
            f"timeout={self._request_timeout}s)"
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session and persistent cache."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._historical_cache.close()

    def _resolve_token_id(self, token: str) -> str | None:
        """Resolve token symbol to CoinGecko ID."""
        return TOKEN_IDS.get(token.upper())

    async def _wait_for_rate_limit(self) -> None:
        """Wait if needed to respect rate limits.

        This uses a two-level rate limiting approach:
        1. Proactive: Token bucket rate limiter to prevent hitting limits
        2. Reactive: Exponential backoff if we get a 429 response

        Logs rate limit status at key points for monitoring.
        """
        # First, wait for any 429 backoff from previous requests
        backoff_wait = self._rate_limit_state.get_wait_time()
        if backoff_wait > 0:
            logger.info(
                "Rate limit backoff active: waiting %.2fs (consecutive 429s: %d)",
                backoff_wait,
                self._rate_limit_state.consecutive_429s,
            )
            await asyncio.sleep(backoff_wait)

        # Check token bucket status before acquiring
        wait_time = await self._rate_limiter.wait_time_seconds()
        if wait_time > 0:
            logger.debug(
                "Rate limiter: waiting %.2fs for token (%.2f tokens available, %d req/min limit)",
                wait_time,
                self._rate_limiter.tokens_available,
                self._rate_limiter.requests_per_minute,
            )

        # Then, use token bucket rate limiter to proactively control rate
        # This blocks until a token is available
        await self._rate_limiter.acquire()

        # Also enforce minimum interval between requests (belt and suspenders)
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            sleep_time = self._min_request_interval - elapsed
            await asyncio.sleep(sleep_time)

    async def _make_request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """Make an API request with rate limiting and automatic retry.

        Implements exponential backoff retry logic for 429 (Too Many Requests)
        responses. Retries up to max_retries times with increasing delays:
        1s, 2s, 4s, 8s (configurable via RetryConfig).

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dictionary

        Raises:
            CoinGeckoRateLimitError: If rate limit is exceeded after max retries
            ValueError: If the API returns a non-retryable error
        """
        url = f"{self._api_base}{endpoint}"

        # Add API key header if available
        headers: dict[str, str] = {}
        if self._api_key:
            headers["x-cg-pro-api-key"] = self._api_key

        last_backoff = 0.0
        retry_count = 0

        for attempt in range(self._retry_config.max_retries + 1):
            # Wait for rate limit before each attempt
            await self._wait_for_rate_limit()

            self._last_request_time = time.time()
            self._rate_limit_state.record_request()

            session = await self._get_session()

            try:
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 429:
                        self._rate_limit_state.record_rate_limit()
                        retry_count = attempt + 1

                        # Log rate limit status
                        logger.warning(
                            "CoinGecko rate limit hit (HTTP 429): "
                            "consecutive_429s=%d, current_backoff=%.1fs, "
                            "requests_this_minute=%d, configured_limit=%d/min",
                            self._rate_limit_state.consecutive_429s,
                            self._rate_limit_state.backoff_seconds,
                            self._rate_limit_state.requests_this_minute,
                            self._rate_limit,
                        )

                        # Notify rate limiter of 429 response (reduces rate by 20%)
                        await self._rate_limiter.on_rate_limit_response()

                        # Check if we have retries remaining
                        if attempt < self._retry_config.max_retries:
                            backoff = self._retry_config.get_delay_for_attempt(attempt + 1)
                            last_backoff = backoff
                            logger.warning(
                                "Rate limited by CoinGecko (429), "
                                "retry %d/%d after %.1fs backoff. "
                                "Rate limiter reduced to %d req/min.",
                                retry_count,
                                self._retry_config.max_retries,
                                backoff,
                                self._rate_limiter.requests_per_minute,
                            )
                            await asyncio.sleep(backoff)
                            continue
                        else:
                            # Max retries exceeded
                            logger.error(
                                "CoinGecko rate limit exceeded after %d retries. Final rate: %d req/min",
                                self._retry_config.max_retries,
                                self._rate_limiter.requests_per_minute,
                            )
                            raise CoinGeckoRateLimitError(
                                f"Rate limit exceeded after {self._retry_config.max_retries} retries. "
                                f"Last backoff: {last_backoff:.1f}s",
                                retry_count=retry_count,
                                last_backoff=last_backoff,
                            )

                    if response.status != 200:
                        text = await response.text()
                        raise ValueError(f"CoinGecko API error {response.status}: {text}")

                    self._rate_limit_state.record_success()
                    # Restore proactive rate limiter to original rate after success.
                    # on_rate_limit_response() permanently reduces the rate by 20%
                    # on each 429; without this reset the rate degrades to 1 req/min
                    # over a long session and never recovers.
                    self._rate_limiter.reset_rate()
                    result: dict[str, Any] = await response.json()
                    return result

            except TimeoutError as e:
                raise ValueError(f"Request timed out after {self._request_timeout}s") from e
            except aiohttp.ClientError as e:
                raise ValueError(f"Network error: {str(e)}") from e

        # This should not be reached, but just in case
        raise CoinGeckoRateLimitError(
            "Unexpected: exhausted all retries",
            retry_count=retry_count,
            last_backoff=last_backoff,
        )

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get the price of a token at a specific timestamp.

        Uses the CoinGecko /coins/{id}/history endpoint for historical prices.
        Results are cached by (token, date) with 1-hour TTL to minimize API calls.

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "ARB")
            timestamp: The historical point in time

        Returns:
            Price in USD at the specified timestamp

        Raises:
            ValueError: If price data is not available for the token/timestamp
        """
        token_id = self._resolve_token_id(token)
        if token_id is None:
            raise ValueError(f"Unknown token: {token}")

        # Check historical price cache first (keyed by token+date, 1-hour TTL)
        cached_price = self._historical_cache.get(token, timestamp)
        if cached_price is not None:
            logger.debug(f"Historical cache hit for {token} on {timestamp.strftime('%Y-%m-%d')}")
            return cached_price

        # Check OHLCV cache (if iterate() was called)
        if self._cache is not None:
            ohlcv_cached_price = self._cache.get_price_at(token, timestamp)
            if ohlcv_cached_price is not None:
                # Also store in historical cache for future requests
                self._historical_cache.set(token, timestamp, ohlcv_cached_price)
                return ohlcv_cached_price

        # Format date for CoinGecko API (dd-mm-yyyy)
        date_str = timestamp.strftime("%d-%m-%Y")

        params = {"date": date_str, "localization": "false"}

        data = await self._make_request(f"/coins/{token_id}/history", params)

        if "market_data" not in data:
            raise ValueError(f"No market data available for {token} on {date_str}")

        price_usd = data["market_data"]["current_price"].get("usd")
        if price_usd is None:
            raise ValueError(f"No USD price available for {token} on {date_str}")

        price = Decimal(str(price_usd))

        # Store in historical cache for future requests
        self._historical_cache.set(token, timestamp, price)
        logger.debug(f"Cached historical price for {token} on {timestamp.strftime('%Y-%m-%d')}: ${price}")

        return price

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Uses the CoinGecko /coins/{id}/market_chart/range endpoint.

        Note: CoinGecko's free API has granularity limits:
        - 1-2 days: 5-minute intervals
        - 3-90 days: hourly intervals
        - >90 days: daily intervals

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "ARB")
            start: Start of the time range (inclusive)
            end: End of the time range (inclusive)
            interval_seconds: Candle interval in seconds (default: 3600 = 1 hour)
                              Note: CoinGecko may return different intervals based
                              on the date range.

        Returns:
            List of OHLCV data points, sorted by timestamp ascending

        Raises:
            ValueError: If data is not available for the token/range
        """
        token_id = self._resolve_token_id(token)
        if token_id is None:
            raise ValueError(f"Unknown token: {token}")

        # Convert to Unix timestamps
        start_ts = int(start.timestamp())
        end_ts = int(end.timestamp())

        params = {
            "vs_currency": "usd",
            "from": str(start_ts),
            "to": str(end_ts),
        }

        data = await self._make_request(f"/coins/{token_id}/market_chart/range", params)

        prices = data.get("prices", [])
        if not prices:
            raise ValueError(f"No price data available for {token} in range")

        # CoinGecko returns [timestamp_ms, price] pairs
        # Convert to OHLCV (using same price for O/H/L/C since we only get close prices)
        ohlcv_list: list[OHLCV] = []

        for ts_ms, price in prices:
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
            price_dec = Decimal(str(price))

            ohlcv = OHLCV(
                timestamp=ts,
                open=price_dec,
                high=price_dec,
                low=price_dec,
                close=price_dec,
                volume=None,  # Volume data is separate in CoinGecko API
            )
            ohlcv_list.append(ohlcv)

        # Sort by timestamp ascending
        ohlcv_list.sort(key=lambda x: x.timestamp)

        return ohlcv_list

    async def _prefetch_ohlcv_data(self, config: HistoricalDataConfig) -> OHLCVCache:
        """Prefetch all OHLCV data needed for the backtest.

        This method fetches all historical data upfront to minimize API calls
        during iteration and avoid rate limiting issues.

        Args:
            config: Historical data configuration

        Returns:
            OHLCVCache with all prefetched data
        """
        data: dict[str, list[OHLCV]] = {}

        for token in config.tokens:
            try:
                ohlcv = await self.get_ohlcv(
                    token,
                    config.start_time,
                    config.end_time,
                    config.interval_seconds,
                )
                data[token.upper()] = ohlcv
                logger.info(f"Prefetched {len(ohlcv)} data points for {token}")
            except ValueError as e:
                logger.warning(f"Failed to prefetch data for {token}: {e}")
                data[token.upper()] = []

        return OHLCVCache(data=data, fetched_at=datetime.now(UTC))

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical market states.

        This method prefetches all OHLCV data upfront, then yields market
        state snapshots at regular intervals throughout the configured time range.

        Args:
            config: Configuration specifying time range, interval, and tokens

        Yields:
            Tuples of (timestamp, MarketState) for each time point

        Example:
            async for timestamp, market_state in provider.iterate(config):
                eth_price = market_state.get_price("WETH")
                # Process market state
        """
        logger.info(
            f"Starting iteration from {config.start_time} to {config.end_time} "
            f"with {config.interval_seconds}s interval for tokens: {config.tokens}"
        )

        # Prefetch all OHLCV data to minimize API calls
        self._cache = await self._prefetch_ohlcv_data(config)

        # Generate timestamps at the specified interval
        current_time = config.start_time
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        end_time = config.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        interval = timedelta(seconds=config.interval_seconds)

        while current_time <= end_time:
            # Build prices dict from cache
            prices: dict[str, Decimal] = {}
            ohlcv_data: dict[str, OHLCV] = {}

            for token in config.tokens:
                token_upper = token.upper()

                # Get OHLCV data if requested
                if config.include_ohlcv:
                    candle = self._cache.get_ohlcv_at(token_upper, current_time)
                    if candle is not None:
                        ohlcv_data[token_upper] = candle
                        prices[token_upper] = candle.close

                # If no OHLCV, try to get price directly from cache
                if token_upper not in prices:
                    price = self._cache.get_price_at(token_upper, current_time)
                    if price is not None:
                        prices[token_upper] = price

            # Create MarketState for this timestamp
            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                ohlcv=ohlcv_data if config.include_ohlcv else {},
                chain=config.chains[0] if config.chains else "arbitrum",
                block_number=None,  # Not available from CoinGecko
                gas_price_gwei=None,  # Not available from CoinGecko
            )

            yield (current_time, market_state)

            current_time += interval

        # Log cache hit rate after iteration
        cache_stats = self._historical_cache.get_stats()
        logger.info(
            f"Completed iteration with {config.estimated_data_points} data points. "
            f"Historical cache: {cache_stats.cache_hits}/{cache_stats.total_requests} hits "
            f"({cache_stats.hit_rate:.1f}% hit rate), {cache_stats.cache_entries} entries"
        )

    @property
    def provider_name(self) -> str:
        """Return the unique name of this data provider."""
        return "coingecko"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported token symbols."""
        return self._SUPPORTED_TOKENS.copy()

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        return self._SUPPORTED_CHAINS.copy()

    @property
    def min_timestamp(self) -> datetime | None:
        """Return the earliest timestamp with available data.

        CoinGecko has data going back to each token's launch date.
        For most major tokens, this is several years back.
        """
        # CoinGecko has data going back many years for major tokens
        # Return a reasonable minimum (January 2017)
        return datetime(2017, 1, 1, tzinfo=UTC)

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data.

        For CoinGecko, this is approximately "now" minus a small delay.
        """
        return datetime.now(UTC) - timedelta(minutes=5)

    def get_rate_limiter_stats(self) -> dict:
        """Get statistics from the rate limiter.

        Returns:
            Dictionary with rate limiter statistics including:
            - total_requests: Number of rate limiter acquire() calls
            - total_waits: Number of times waiting was required
            - wait_rate_percent: Percentage of requests that waited
            - average_wait_seconds: Average wait time when waiting occurred
            - tokens_consumed: Total tokens consumed
            - rate_limit_reductions: Number of times rate was reduced due to 429s
            - current_rate_limit: Current rate limit (may be reduced from initial)
            - initial_rate_limit: Initially configured rate limit

        Example:
            stats = provider.get_rate_limiter_stats()
            print(f"Rate limit wait rate: {stats['wait_rate_percent']:.1f}%")
        """
        stats = self._rate_limiter.get_stats().to_dict()
        # Add current and initial rate limits for monitoring
        stats["current_rate_limit"] = self._rate_limiter.requests_per_minute
        stats["initial_rate_limit"] = self._rate_limiter.initial_requests_per_minute
        return stats

    def log_rate_limit_status(self) -> None:
        """Log current rate limit status for monitoring.

        Logs the current state of rate limiting including:
        - Current rate limit (may be reduced from initial if 429s received)
        - Number of tokens available
        - Number of rate limit reductions
        - Requests made and wait statistics

        Example:
            provider.log_rate_limit_status()
            # Output: INFO: CoinGecko rate limit status: 10 req/min (initial: 10),
            #         tokens=8.5, reductions=0, requests=15, waits=2 (13.3%)
        """
        stats = self._rate_limiter.get_stats()
        logger.info(
            "CoinGecko rate limit status: %d req/min (initial: %d), "
            "tokens=%.1f, reductions=%d, requests=%d, waits=%d (%.1f%%)",
            self._rate_limiter.requests_per_minute,
            self._rate_limiter.initial_requests_per_minute,
            self._rate_limiter.tokens_available,
            stats.rate_limit_reductions,
            stats.total_requests,
            stats.total_waits,
            stats.wait_rate,
        )

    def get_historical_cache_stats(self) -> dict[str, Any]:
        """Get statistics from the historical price cache.

        Returns:
            Dictionary with cache statistics including:
            - total_requests: Number of cache lookups
            - cache_hits: Number of successful cache hits
            - cache_misses: Number of cache misses
            - cache_entries: Number of entries in cache
            - hit_rate_percent: Cache hit rate as percentage

        Example:
            stats = provider.get_historical_cache_stats()
            print(f"Cache hit rate: {stats['hit_rate_percent']:.1f}%")
        """
        return self._historical_cache.get_stats().to_dict()

    async def warm_cache(
        self,
        tokens: list[str],
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, int]:
        """Pre-fetch historical prices for a date range to warm the cache.

        This method fetches prices for each token on each date in the range,
        storing them in the historical cache. This dramatically reduces API
        calls during subsequent backtests over the same period.

        Args:
            tokens: List of token symbols to warm cache for
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            Dictionary mapping token to number of prices cached

        Example:
            from datetime import datetime

            provider = CoinGeckoDataProvider()
            cached = await provider.warm_cache(
                tokens=["WETH", "USDC", "ARB"],
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 31),
            )
            print(f"Cached {sum(cached.values())} prices")
            # Subsequent backtests will have >90% cache hit rate
        """
        logger.info(
            f"Warming historical cache for {len(tokens)} tokens "
            f"from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Ensure timestamps have timezone info
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)

        cached_counts: dict[str, int] = {}

        for token in tokens:
            token_upper = token.upper()
            cached_counts[token_upper] = 0

            # Iterate through each day in the range
            current_date = start_date
            while current_date <= end_date:
                # Check if already in cache
                if self._historical_cache.get(token_upper, current_date) is not None:
                    # Already cached, increment count but don't fetch
                    cached_counts[token_upper] += 1
                    current_date += timedelta(days=1)
                    continue

                try:
                    # Fetch and cache the price
                    price = await self.get_price(token_upper, current_date)
                    cached_counts[token_upper] += 1
                    logger.debug(f"Warmed cache: {token_upper} on {current_date.strftime('%Y-%m-%d')}: ${price}")
                except ValueError as e:
                    logger.warning(
                        f"Failed to warm cache for {token_upper} on {current_date.strftime('%Y-%m-%d')}: {e}"
                    )
                except CoinGeckoRateLimitError as e:
                    logger.warning(
                        f"Rate limited while warming cache for {token_upper}: {e}. "
                        f"Cached {cached_counts[token_upper]} prices so far."
                    )
                    break  # Stop warming this token to avoid excessive rate limiting

                current_date += timedelta(days=1)

            logger.info(f"Warmed {cached_counts[token_upper]} prices for {token_upper}")

        total_cached = sum(cached_counts.values())
        cache_stats = self._historical_cache.get_stats()
        logger.info(
            f"Cache warming complete: {total_cached} total prices cached, {cache_stats.cache_entries} entries in cache"
        )

        return cached_counts

    def clear_historical_cache(self) -> None:
        """Clear the historical price cache and reset statistics.

        Use this to force fresh data fetches, e.g., when testing
        or when you suspect stale data.
        """
        self._historical_cache.clear()
        logger.info("Cleared historical price cache")

    async def __aenter__(self) -> "CoinGeckoDataProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "CoinGeckoDataProvider",
    "CoinGeckoRateLimitError",
    "HistoricalCacheStats",
    "HistoricalPriceCache",
    "RetryConfig",
    "TOKEN_IDS",
]
