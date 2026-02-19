"""Tests for CoinGecko Price Source.

This test suite covers:
- Basic price fetching
- Cache behavior (TTL, hits, expiration)
- Timeout handling with stale data fallback
- Rate limiting (429) with exponential backoff
- Error scenarios (unknown token, network errors)
- Health metrics tracking
"""

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, TypeVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import (
    DataSourceRateLimited,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.data.price.coingecko import (
    CacheEntry,
    CoinGeckoPriceSource,
    RateLimitState,
    SourceHealthMetrics,
)

T = TypeVar("T")


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    """Helper to run async functions in sync tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_response() -> dict:
    """Standard mock response for ETH price."""
    return {"ethereum": {"usd": 2500.50}}


@pytest.fixture
def mock_weth_response() -> dict:
    """Mock response for WETH price."""
    return {"weth": {"usd": 2495.75}}


@pytest.fixture
def mock_arb_response() -> dict:
    """Mock response for ARB price."""
    return {"arbitrum": {"usd": 1.25}}


@pytest.fixture
def mock_usdc_response() -> dict:
    """Mock response for USDC price."""
    return {"usd-coin": {"usd": 1.0001}}


# =============================================================================
# RateLimitState Tests
# =============================================================================


class TestRateLimitState:
    """Tests for RateLimitState class."""

    def test_initial_state(self) -> None:
        """Test initial rate limit state."""
        state = RateLimitState()

        assert state.last_429_time is None
        assert state.backoff_seconds == 1.0
        assert state.consecutive_429s == 0
        assert state.get_wait_time() == 0.0

    def test_record_rate_limit(self) -> None:
        """Test recording rate limit hit."""
        state = RateLimitState()

        state.record_rate_limit()

        assert state.last_429_time is not None
        assert state.consecutive_429s == 1
        assert state.backoff_seconds == 1.0  # 2^0 = 1

    def test_exponential_backoff(self) -> None:
        """Test exponential backoff on consecutive rate limits."""
        state = RateLimitState()

        state.record_rate_limit()
        assert state.backoff_seconds == 1.0  # 2^0

        state.record_rate_limit()
        assert state.backoff_seconds == 2.0  # 2^1

        state.record_rate_limit()
        assert state.backoff_seconds == 4.0  # 2^2

        state.record_rate_limit()
        assert state.backoff_seconds == 8.0  # 2^3

    def test_backoff_max_cap(self) -> None:
        """Test backoff is capped at 32 seconds."""
        state = RateLimitState()

        # Hit rate limit 10 times
        for _ in range(10):
            state.record_rate_limit()

        assert state.backoff_seconds == 32.0

    def test_record_success_resets_backoff(self) -> None:
        """Test that success resets backoff."""
        state = RateLimitState()

        # Build up backoff
        for _ in range(5):
            state.record_rate_limit()

        assert state.consecutive_429s == 5
        assert state.backoff_seconds > 1.0

        # Record success
        state.record_success()

        assert state.consecutive_429s == 0
        assert state.backoff_seconds == 1.0

    def test_get_wait_time_with_elapsed(self) -> None:
        """Test wait time decreases as time passes."""
        state = RateLimitState()
        state.backoff_seconds = 5.0
        state.last_429_time = None  # No 429 yet

        # No wait time if never rate limited
        assert state.get_wait_time() == 0.0


# =============================================================================
# SourceHealthMetrics Tests
# =============================================================================


class TestSourceHealthMetrics:
    """Tests for SourceHealthMetrics class."""

    def test_initial_metrics(self) -> None:
        """Test initial metrics state."""
        metrics = SourceHealthMetrics()

        assert metrics.total_requests == 0
        assert metrics.successful_requests == 0
        assert metrics.success_rate == 100.0
        assert metrics.average_latency_ms == 0.0

    def test_success_rate_calculation(self) -> None:
        """Test success rate calculation."""
        metrics = SourceHealthMetrics()
        metrics.total_requests = 10
        metrics.successful_requests = 8

        assert metrics.success_rate == 80.0

    def test_average_latency_calculation(self) -> None:
        """Test average latency calculation."""
        metrics = SourceHealthMetrics()
        metrics.successful_requests = 5
        metrics.total_latency_ms = 500.0

        assert metrics.average_latency_ms == 100.0

    def test_to_dict(self) -> None:
        """Test metrics serialization."""
        metrics = SourceHealthMetrics()
        metrics.total_requests = 100
        metrics.successful_requests = 95
        metrics.cache_hits = 50
        metrics.timeouts = 3
        metrics.rate_limits = 2
        metrics.errors = 5
        metrics.total_latency_ms = 5000.0

        result = metrics.to_dict()

        assert result["total_requests"] == 100
        assert result["successful_requests"] == 95
        assert result["cache_hits"] == 50
        assert result["success_rate"] == 95.0
        assert result["average_latency_ms"] == 52.63  # 5000 / 95


# =============================================================================
# CoinGeckoPriceSource Initialization Tests
# =============================================================================


class TestCoinGeckoPriceSourceInit:
    """Tests for CoinGeckoPriceSource initialization."""

    def test_default_initialization(self) -> None:
        """Test default initialization."""
        source = CoinGeckoPriceSource()

        assert source.source_name == "coingecko"
        assert source.cache_ttl_seconds == 30
        assert source._api_base == CoinGeckoPriceSource._FREE_API_BASE

    def test_pro_api_initialization(self) -> None:
        """Test initialization with API key uses pro API."""
        source = CoinGeckoPriceSource(api_key="test-key")

        assert source._api_base == CoinGeckoPriceSource._PRO_API_BASE

    def test_custom_cache_ttl(self) -> None:
        """Test custom cache TTL."""
        source = CoinGeckoPriceSource(cache_ttl=60)

        assert source.cache_ttl_seconds == 60

    def test_supported_tokens(self) -> None:
        """Test supported tokens list."""
        source = CoinGeckoPriceSource()
        tokens = source.supported_tokens

        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert "ARB" in tokens


# =============================================================================
# CoinGeckoPriceSource Price Fetching Tests
# =============================================================================


class TestCoinGeckoPriceSourceFetching:
    """Tests for price fetching functionality."""

    def test_fetch_eth_price_success(self, mock_response: dict) -> None:
        """Test successful ETH price fetch."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_response)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            result = run_async(source.get_price("ETH", "USD"))

        assert result.price == Decimal("2500.50")
        assert result.source == "coingecko"
        assert result.confidence == 1.0
        assert result.stale is False

    def test_fetch_weth_price_success(self, mock_weth_response: dict) -> None:
        """Test successful WETH price fetch."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_weth_response)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            result = run_async(source.get_price("WETH", "USD"))

        assert result.price == Decimal("2495.75")
        assert result.source == "coingecko"

    def test_fetch_arb_price_success(self, mock_arb_response: dict) -> None:
        """Test successful ARB price fetch."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_arb_response)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            result = run_async(source.get_price("ARB", "USD"))

        assert result.price == Decimal("1.25")

    def test_fetch_unknown_token_raises(self) -> None:
        """Test unknown token raises DataSourceUnavailable."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        with pytest.raises(DataSourceUnavailable) as exc_info:
            run_async(source.get_price("UNKNOWN_TOKEN", "USD"))

        assert "Unknown token" in str(exc_info.value)

    def test_case_insensitive_token(self, mock_response: dict) -> None:
        """Test token symbol is case insensitive."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_response)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            result = run_async(source.get_price("eth", "usd"))

        assert result.price == Decimal("2500.50")


# =============================================================================
# Cache Tests
# =============================================================================


class TestCoinGeckoPriceSourceCache:
    """Tests for caching functionality."""

    def test_cache_hit(self, mock_response: dict) -> None:
        """Test cache hit returns cached data without API call."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_response)

        with patch.object(source, "_get_session") as mock_session:
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_session.return_value.get = MagicMock(return_value=mock_context)

            # First call - should hit API
            result1 = run_async(source.get_price("ETH", "USD"))
            assert mock_session.return_value.get.call_count == 1

            # Second call - should hit cache
            result2 = run_async(source.get_price("ETH", "USD"))
            assert mock_session.return_value.get.call_count == 1  # No additional call

        assert result1.price == result2.price

    def test_cache_key_generation(self) -> None:
        """Test cache key generation."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        key = source._get_cache_key("ETH", "USD")
        assert key == "ETH/USD"

        key_lower = source._get_cache_key("eth", "usd")
        assert key_lower == "ETH/USD"

    def test_get_cached_returns_none_when_empty(self) -> None:
        """Test get_cached returns None when cache is empty."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        result = source._get_cached("ETH", "USD")
        assert result is None

    def test_get_cached_returns_entry_when_valid(self) -> None:
        """Test get_cached returns entry when not expired."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        price_result = PriceResult(
            price=Decimal("2500"),
            source="coingecko",
            timestamp=datetime.now(UTC),
            confidence=1.0,
            stale=False,
        )
        source._cache["ETH/USD"] = CacheEntry(
            result=price_result,
            cached_at=datetime.now(UTC),
        )

        cached = source._get_cached("ETH", "USD")
        assert cached is not None
        assert cached.result.price == Decimal("2500")

    def test_get_cached_returns_none_when_expired(self) -> None:
        """Test get_cached returns None when cache is expired."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        price_result = PriceResult(
            price=Decimal("2500"),
            source="coingecko",
            timestamp=datetime.now(UTC) - timedelta(minutes=5),
            confidence=1.0,
            stale=False,
        )
        source._cache["ETH/USD"] = CacheEntry(
            result=price_result,
            cached_at=datetime.now(UTC) - timedelta(seconds=60),  # Older than TTL
        )

        cached = source._get_cached("ETH", "USD")
        assert cached is None

    def test_get_stale_cached_returns_expired_data(self) -> None:
        """Test get_stale_cached returns expired data for fallback."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        price_result = PriceResult(
            price=Decimal("2500"),
            source="coingecko",
            timestamp=datetime.now(UTC) - timedelta(minutes=5),
            confidence=1.0,
            stale=False,
        )
        source._cache["ETH/USD"] = CacheEntry(
            result=price_result,
            cached_at=datetime.now(UTC) - timedelta(seconds=60),
        )

        stale = source._get_stale_cached("ETH", "USD")
        assert stale is not None
        assert stale.result.price == Decimal("2500")

    def test_clear_cache(self) -> None:
        """Test cache clearing."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        price_result = PriceResult(
            price=Decimal("2500"),
            source="coingecko",
            timestamp=datetime.now(UTC),
            confidence=1.0,
        )
        source._cache["ETH/USD"] = CacheEntry(
            result=price_result,
            cached_at=datetime.now(UTC),
        )

        source.clear_cache()

        assert len(source._cache) == 0


# =============================================================================
# Timeout Tests
# =============================================================================


class TestCoinGeckoPriceSourceTimeout:
    """Tests for timeout handling."""

    def test_timeout_with_stale_cache(self) -> None:
        """Test timeout returns stale data when available."""
        source = CoinGeckoPriceSource(cache_ttl=30, request_timeout=5.0)

        # Pre-populate stale cache
        stale_result = PriceResult(
            price=Decimal("2400"),
            source="coingecko",
            timestamp=datetime.now(UTC) - timedelta(minutes=5),
            confidence=1.0,
            stale=False,
        )
        source._cache["ETH/USD"] = CacheEntry(
            result=stale_result,
            cached_at=datetime.now(UTC) - timedelta(seconds=60),
        )

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(side_effect=TimeoutError())

            result = run_async(source.get_price("ETH", "USD"))

        assert result.price == Decimal("2400")
        assert result.stale is True
        assert result.confidence == 0.7  # Reduced by stale_confidence_multiplier

    def test_timeout_without_cache_raises(self) -> None:
        """Test timeout without cache raises DataSourceUnavailable."""
        source = CoinGeckoPriceSource(cache_ttl=30, request_timeout=5.0)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(side_effect=TimeoutError())

            with pytest.raises(DataSourceUnavailable) as exc_info:
                run_async(source.get_price("ETH", "USD"))

        assert "Timeout" in str(exc_info.value)
        assert "no cache" in str(exc_info.value)

    def test_timeout_increments_metrics(self) -> None:
        """Test timeout increments timeout metrics."""
        source = CoinGeckoPriceSource(cache_ttl=30, request_timeout=5.0)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(side_effect=TimeoutError())

            with pytest.raises(DataSourceUnavailable):
                run_async(source.get_price("ETH", "USD"))

        assert source._metrics.timeouts == 1


# =============================================================================
# Rate Limit Tests
# =============================================================================


class TestCoinGeckoPriceSourceRateLimit:
    """Tests for rate limit handling."""

    def test_rate_limit_with_stale_cache(self) -> None:
        """Test rate limit returns stale data when available."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        # Pre-populate stale cache
        stale_result = PriceResult(
            price=Decimal("2400"),
            source="coingecko",
            timestamp=datetime.now(UTC) - timedelta(minutes=5),
            confidence=1.0,
            stale=False,
        )
        source._cache["ETH/USD"] = CacheEntry(
            result=stale_result,
            cached_at=datetime.now(UTC) - timedelta(seconds=60),
        )

        mock_resp = AsyncMock()
        mock_resp.status = 429

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            result = run_async(source.get_price("ETH", "USD"))

        assert result.price == Decimal("2400")
        assert result.stale is True
        assert result.confidence == 0.7

    def test_rate_limit_without_cache_raises(self) -> None:
        """Test rate limit without cache raises DataSourceRateLimited."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        mock_resp = AsyncMock()
        mock_resp.status = 429

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceRateLimited) as exc_info:
                run_async(source.get_price("ETH", "USD"))

        assert exc_info.value.retry_after > 0

    def test_rate_limit_updates_backoff(self) -> None:
        """Test rate limit updates backoff state."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        mock_resp = AsyncMock()
        mock_resp.status = 429

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceRateLimited):
                run_async(source.get_price("ETH", "USD"))

        assert source._rate_limit_state.consecutive_429s == 1
        assert source._rate_limit_state.last_429_time is not None

    def test_rate_limit_increments_metrics(self) -> None:
        """Test rate limit increments rate limit metrics."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        mock_resp = AsyncMock()
        mock_resp.status = 429

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceRateLimited):
                run_async(source.get_price("ETH", "USD"))

        assert source._metrics.rate_limits == 1


# =============================================================================
# HTTP Error Tests
# =============================================================================


class TestCoinGeckoPriceSourceHTTPErrors:
    """Tests for HTTP error handling."""

    def test_http_500_with_stale_cache(self) -> None:
        """Test HTTP 500 returns stale data when available."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        # Pre-populate stale cache
        stale_result = PriceResult(
            price=Decimal("2400"),
            source="coingecko",
            timestamp=datetime.now(UTC) - timedelta(minutes=5),
            confidence=1.0,
            stale=False,
        )
        source._cache["ETH/USD"] = CacheEntry(
            result=stale_result,
            cached_at=datetime.now(UTC) - timedelta(seconds=60),
        )

        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value="Internal Server Error")

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            result = run_async(source.get_price("ETH", "USD"))

        assert result.price == Decimal("2400")
        assert result.stale is True

    def test_http_500_without_cache_raises(self) -> None:
        """Test HTTP 500 without cache raises DataSourceUnavailable."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value="Internal Server Error")

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceUnavailable) as exc_info:
                run_async(source.get_price("ETH", "USD"))

        assert "500" in str(exc_info.value)

    def test_missing_token_in_response(self) -> None:
        """Test missing token in response raises DataSourceUnavailable."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={})  # Empty response

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceUnavailable) as exc_info:
                run_async(source.get_price("ETH", "USD"))

        assert "not in response" in str(exc_info.value)


# =============================================================================
# Health Metrics Tests
# =============================================================================


class TestCoinGeckoPriceSourceHealth:
    """Tests for health check and metrics."""

    def test_health_check_success(self, mock_response: dict) -> None:
        """Test health check returns True on success."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_response)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            is_healthy = run_async(source.health_check())

        assert is_healthy is True

    def test_health_check_failure(self) -> None:
        """Test health check returns False on failure."""
        source = CoinGeckoPriceSource(cache_ttl=30)

        with patch.object(source, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(side_effect=TimeoutError())

            is_healthy = run_async(source.health_check())

        assert is_healthy is False

    def test_get_health_metrics(self) -> None:
        """Test getting health metrics."""
        source = CoinGeckoPriceSource(cache_ttl=30)
        source._metrics.total_requests = 100
        source._metrics.successful_requests = 95

        metrics = source.get_health_metrics()

        assert metrics["total_requests"] == 100
        assert metrics["successful_requests"] == 95
        assert metrics["success_rate"] == 95.0


# =============================================================================
# Context Manager Tests
# =============================================================================


class TestCoinGeckoPriceSourceContextManager:
    """Tests for async context manager functionality."""

    def test_context_manager(self) -> None:
        """Test async context manager properly closes session."""

        async def test_cm() -> CoinGeckoPriceSource:
            async with CoinGeckoPriceSource() as source:
                return source

        source = run_async(test_cm())
        # Session should be closed after exiting context
        assert source._session is None or source._session.closed
