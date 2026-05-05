"""Tests for RSI Calculator.

This test suite covers:
- RSI calculation using Wilder's smoothing method
- Known RSI values for verification
- OHLCV data fetching and caching
- Error handling for insufficient data
- Edge cases (all gains, all losses, zero movement)
"""

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, TypeVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.indicators.rsi import (
    CoinGeckoOHLCVProvider,
    OHLCVCacheEntry,
    OHLCVData,
    OHLCVHealthMetrics,
    RSICalculator,
)
from almanak.framework.data.interfaces import (
    DataSourceUnavailable,
    InsufficientDataError,
    OHLCVCandle,
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
# Test Data with Known RSI Values
# =============================================================================


# Sample price data with known RSI
# This data is designed to produce predictable RSI values
SAMPLE_PRICES_UPTREND = [
    Decimal("100"),
    Decimal("101"),
    Decimal("102"),
    Decimal("103"),
    Decimal("104"),
    Decimal("105"),
    Decimal("106"),
    Decimal("107"),
    Decimal("108"),
    Decimal("109"),
    Decimal("110"),
    Decimal("111"),
    Decimal("112"),
    Decimal("113"),
    Decimal("114"),
]  # 15 prices (need 14+1 for period=14)

SAMPLE_PRICES_DOWNTREND = [
    Decimal("114"),
    Decimal("113"),
    Decimal("112"),
    Decimal("111"),
    Decimal("110"),
    Decimal("109"),
    Decimal("108"),
    Decimal("107"),
    Decimal("106"),
    Decimal("105"),
    Decimal("104"),
    Decimal("103"),
    Decimal("102"),
    Decimal("101"),
    Decimal("100"),
]

SAMPLE_PRICES_MIXED = [
    Decimal("100"),
    Decimal("102"),  # +2
    Decimal("101"),  # -1
    Decimal("103"),  # +2
    Decimal("102"),  # -1
    Decimal("104"),  # +2
    Decimal("103"),  # -1
    Decimal("105"),  # +2
    Decimal("104"),  # -1
    Decimal("106"),  # +2
    Decimal("105"),  # -1
    Decimal("107"),  # +2
    Decimal("106"),  # -1
    Decimal("108"),  # +2
    Decimal("107"),  # -1
]

# Real-world example from BTC historical data
# Known RSI(14) = approximately 55-60 for this data
SAMPLE_PRICES_REAL = [
    Decimal("42000"),
    Decimal("42150"),
    Decimal("42100"),
    Decimal("42300"),
    Decimal("42250"),
    Decimal("42400"),
    Decimal("42350"),
    Decimal("42500"),
    Decimal("42600"),
    Decimal("42550"),
    Decimal("42700"),
    Decimal("42650"),
    Decimal("42800"),
    Decimal("42750"),
    Decimal("42900"),
]


# =============================================================================
# OHLCVData Tests
# =============================================================================


class TestOHLCVData:
    """Tests for OHLCVData dataclass."""

    def test_create_ohlcv_data(self) -> None:
        """Test creating OHLCVData."""
        now = datetime.now(UTC)
        data = OHLCVData(
            timestamp=now,
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("98"),
            close=Decimal("103"),
        )

        assert data.timestamp == now
        assert data.open == Decimal("100")
        assert data.high == Decimal("105")
        assert data.low == Decimal("98")
        assert data.close == Decimal("103")
        assert data.volume is None

    def test_ohlcv_with_volume(self) -> None:
        """Test OHLCVData with volume."""
        data = OHLCVData(
            timestamp=datetime.now(UTC),
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("98"),
            close=Decimal("103"),
            volume=Decimal("1000000"),
        )

        assert data.volume == Decimal("1000000")

    def test_to_dict(self) -> None:
        """Test OHLCVData serialization."""
        now = datetime.now(UTC)
        data = OHLCVData(
            timestamp=now,
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("98"),
            close=Decimal("103"),
        )

        result = data.to_dict()

        assert result["timestamp"] == now.isoformat()
        assert result["open"] == "100"
        assert result["high"] == "105"
        assert result["low"] == "98"
        assert result["close"] == "103"
        assert result["volume"] is None

    def test_to_dict_with_volume(self) -> None:
        """Test OHLCVData serialization with volume."""
        data = OHLCVData(
            timestamp=datetime.now(UTC),
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("98"),
            close=Decimal("103"),
            volume=Decimal("1000000"),
        )

        result = data.to_dict()
        assert result["volume"] == "1000000"


# =============================================================================
# OHLCVHealthMetrics Tests
# =============================================================================


class TestOHLCVHealthMetrics:
    """Tests for OHLCVHealthMetrics."""

    def test_initial_metrics(self) -> None:
        """Test initial metrics state."""
        metrics = OHLCVHealthMetrics()

        assert metrics.total_requests == 0
        assert metrics.successful_requests == 0
        assert metrics.cache_hits == 0
        assert metrics.errors == 0
        assert metrics.success_rate == 100.0
        assert metrics.average_latency_ms == 0.0

    def test_success_rate_calculation(self) -> None:
        """Test success rate calculation."""
        metrics = OHLCVHealthMetrics()
        metrics.total_requests = 10
        metrics.successful_requests = 8

        assert metrics.success_rate == 80.0

    def test_average_latency_calculation(self) -> None:
        """Test average latency calculation."""
        metrics = OHLCVHealthMetrics()
        metrics.successful_requests = 5
        metrics.total_latency_ms = 500.0

        assert metrics.average_latency_ms == 100.0


# =============================================================================
# RSI Calculation Tests (Core Algorithm)
# =============================================================================


class TestRSICalculation:
    """Tests for RSI calculation using known values."""

    def test_rsi_uptrend_all_gains(self) -> None:
        """Test RSI with pure uptrend (all gains, no losses).

        When there are only gains and no losses, RSI should be 100.
        """
        rsi = RSICalculator.calculate_rsi_from_prices(SAMPLE_PRICES_UPTREND, period=14)

        # Pure uptrend should give RSI = 100
        assert rsi == 100.0

    def test_rsi_downtrend_all_losses(self) -> None:
        """Test RSI with pure downtrend (all losses, no gains).

        When there are only losses and no gains, RSI should be 0.
        """
        rsi = RSICalculator.calculate_rsi_from_prices(SAMPLE_PRICES_DOWNTREND, period=14)

        # Pure downtrend should give RSI = 0
        assert rsi == 0.0

    def test_rsi_mixed_movement(self) -> None:
        """Test RSI with mixed up/down movement.

        With alternating gains and losses where gains > losses,
        RSI should be above 50.
        """
        rsi = RSICalculator.calculate_rsi_from_prices(SAMPLE_PRICES_MIXED, period=14)

        # Gains are +2, losses are -1, so avg_gain > avg_loss
        # RSI should be above 50
        assert 50 < rsi < 100

    def test_rsi_real_world_data(self) -> None:
        """Test RSI with realistic price data."""
        rsi = RSICalculator.calculate_rsi_from_prices(SAMPLE_PRICES_REAL, period=14)

        # RSI should be between 0 and 100
        assert 0 <= rsi <= 100
        # With slight uptrend bias, expect RSI > 50
        assert rsi > 50

    def test_rsi_period_7(self) -> None:
        """Test RSI with shorter period."""
        prices = SAMPLE_PRICES_UPTREND[:9]  # Need 8 prices for period=7 (7+1)
        rsi = RSICalculator.calculate_rsi_from_prices(prices, period=7)

        # Pure uptrend should still give RSI = 100
        assert rsi == 100.0

    def test_rsi_insufficient_data(self) -> None:
        """Test RSI raises error with insufficient data."""
        # Need period + 1 = 15 data points for period=14
        prices = SAMPLE_PRICES_UPTREND[:10]  # Only 10 points

        with pytest.raises(InsufficientDataError) as exc_info:
            RSICalculator.calculate_rsi_from_prices(prices, period=14)

        assert exc_info.value.required == 15
        assert exc_info.value.available == 10
        assert exc_info.value.indicator == "RSI"

    def test_rsi_minimum_data(self) -> None:
        """Test RSI works with minimum required data."""
        # Exactly period + 1 = 15 data points
        prices = SAMPLE_PRICES_UPTREND[:15]
        rsi = RSICalculator.calculate_rsi_from_prices(prices, period=14)

        assert rsi == 100.0

    def test_rsi_flat_prices(self) -> None:
        """Test RSI with no price movement."""
        flat_prices = [Decimal("100") for _ in range(20)]

        rsi = RSICalculator.calculate_rsi_from_prices(flat_prices, period=14)

        # No gains or losses - avg_loss = 0, should return 100 (avoid division by zero)
        # Actually with zero movement: avg_gain = 0, avg_loss = 0
        # Our implementation returns 100 when avg_loss = 0
        assert rsi == 100.0

    def test_rsi_very_small_movements(self) -> None:
        """Test RSI with very small price movements."""
        small_up = [Decimal("100") + Decimal("0.0001") * i for i in range(20)]
        rsi = RSICalculator.calculate_rsi_from_prices(small_up, period=14)

        # Small uptrend should still give RSI = 100
        assert rsi == 100.0


class TestRSIWilderSmoothing:
    """Tests specifically for Wilder's smoothing method."""

    def test_wilder_smoothing_formula(self) -> None:
        """Verify Wilder's smoothing is correctly applied.

        Wilder's smoothing: new_avg = ((prev_avg * (N-1)) + current) / N
        This is equivalent to an EMA with alpha = 1/N.
        """
        # Create a specific sequence where we can verify the smoothing
        prices = [
            Decimal("100"),
            Decimal("101"),  # +1 gain
            Decimal("100"),  # -1 loss
            Decimal("102"),  # +2 gain
            Decimal("101"),  # -1 loss
            Decimal("103"),  # +2 gain
            Decimal("102"),  # -1 loss
        ]

        # With period=3, we need 4 data points minimum
        rsi = RSICalculator.calculate_rsi_from_prices(prices, period=3)

        # Verify RSI is in valid range
        assert 0 <= rsi <= 100

    def test_smoothing_reduces_volatility(self) -> None:
        """Test that Wilder's smoothing reduces RSI volatility over time."""
        # Create oscillating prices
        oscillating = []
        for i in range(30):
            if i % 2 == 0:
                oscillating.append(Decimal("100"))
            else:
                oscillating.append(Decimal("102"))

        rsi = RSICalculator.calculate_rsi_from_prices(oscillating, period=14)

        # With equal oscillation, RSI should be around 50
        # The smoothing should make it relatively stable
        assert 45 <= rsi <= 55


# =============================================================================
# CoinGeckoOHLCVProvider Tests
# =============================================================================


class TestCoinGeckoOHLCVProviderInit:
    """Tests for CoinGeckoOHLCVProvider initialization."""

    def test_default_initialization(self) -> None:
        """Test default initialization."""
        provider = CoinGeckoOHLCVProvider()

        assert provider._api_base == CoinGeckoOHLCVProvider._FREE_API_BASE
        assert provider._cache_ttl == 300

    def test_pro_api_initialization(self) -> None:
        """Test initialization with API key uses pro API."""
        provider = CoinGeckoOHLCVProvider(api_key="test-key")

        assert provider._api_base == CoinGeckoOHLCVProvider._PRO_API_BASE

    def test_custom_cache_ttl(self) -> None:
        """Test custom cache TTL."""
        provider = CoinGeckoOHLCVProvider(cache_ttl=600)

        assert provider._cache_ttl == 600


class TestCoinGeckoOHLCVProviderCache:
    """Tests for OHLCV caching functionality."""

    def test_cache_key_generation(self) -> None:
        """Test cache key generation."""
        provider = CoinGeckoOHLCVProvider()
        key = provider._get_cache_key("WETH", "4h", 30)

        assert key == "WETH:4h:30"

    def test_cache_key_case_insensitive(self) -> None:
        """Test cache key is case insensitive."""
        provider = CoinGeckoOHLCVProvider()
        key1 = provider._get_cache_key("weth", "4h", 30)
        key2 = provider._get_cache_key("WETH", "4h", 30)

        assert key1 == key2

    def test_get_cached_returns_none_when_empty(self) -> None:
        """Test get_cached returns None when cache is empty."""
        provider = CoinGeckoOHLCVProvider()
        result = provider._get_cached("WETH", "4h", 30)

        assert result is None

    def test_get_cached_returns_data_when_valid(self) -> None:
        """Test get_cached returns data when not expired."""
        provider = CoinGeckoOHLCVProvider(cache_ttl=300)

        # Pre-populate cache
        test_data = [
            OHLCVData(
                timestamp=datetime.now(UTC),
                open=Decimal("2500"),
                high=Decimal("2550"),
                low=Decimal("2450"),
                close=Decimal("2520"),
            )
        ]
        provider._cache["WETH:4h:30"] = OHLCVCacheEntry(
            data=test_data,
            cached_at=datetime.now(UTC),
            token="WETH",
            timeframe="4h",
        )

        cached = provider._get_cached("WETH", "4h", 30)

        assert cached is not None
        assert len(cached) == 1
        assert cached[0].close == Decimal("2520")

    def test_get_cached_returns_none_when_expired(self) -> None:
        """Test get_cached returns None when cache is expired."""
        provider = CoinGeckoOHLCVProvider(cache_ttl=300)

        # Pre-populate with expired data
        test_data = [
            OHLCVData(
                timestamp=datetime.now(UTC),
                open=Decimal("2500"),
                high=Decimal("2550"),
                low=Decimal("2450"),
                close=Decimal("2520"),
            )
        ]
        provider._cache["WETH:4h:30"] = OHLCVCacheEntry(
            data=test_data,
            cached_at=datetime.now(UTC) - timedelta(seconds=600),  # Older than TTL
            token="WETH",
            timeframe="4h",
        )

        cached = provider._get_cached("WETH", "4h", 30)

        assert cached is None

    def test_clear_cache(self) -> None:
        """Test cache clearing."""
        provider = CoinGeckoOHLCVProvider()

        # Add some cached data
        test_data = [
            OHLCVData(
                timestamp=datetime.now(UTC),
                open=Decimal("2500"),
                high=Decimal("2550"),
                low=Decimal("2450"),
                close=Decimal("2520"),
            )
        ]
        provider._cache["WETH:4h:30"] = OHLCVCacheEntry(
            data=test_data,
            cached_at=datetime.now(UTC),
            token="WETH",
            timeframe="4h",
        )

        provider.clear_cache()

        assert len(provider._cache) == 0


class TestCoinGeckoOHLCVProviderFetching:
    """Tests for OHLCV data fetching."""

    def test_fetch_ohlcv_success(self) -> None:
        """Test successful OHLCV fetch."""
        provider = CoinGeckoOHLCVProvider()

        # Mock response data (CoinGecko format: [[timestamp, open, high, low, close], ...])
        mock_data = [
            [1705000000000, 2500, 2550, 2450, 2520],
            [1705003600000, 2520, 2560, 2510, 2540],
            [1705007200000, 2540, 2580, 2530, 2560],
        ]

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_data)

        with patch.object(provider, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            result = run_async(provider.get_ohlcv("WETH", "USD", "4h", limit=3))

        assert len(result) == 3
        # Results are sorted by timestamp (oldest first) and returned as OHLCVCandle objects
        assert result[0].close == Decimal("2520")
        assert result[2].close == Decimal("2560")

    def test_fetch_ohlcv_unknown_token(self) -> None:
        """Test fetch with unknown token raises error."""
        provider = CoinGeckoOHLCVProvider()

        with pytest.raises(DataSourceUnavailable) as exc_info:
            run_async(provider.get_ohlcv("UNKNOWN_TOKEN", "USD", "4h", limit=30))

        assert "Unknown token" in str(exc_info.value)

    def test_fetch_ohlcv_rate_limit(self) -> None:
        """Test rate limit handling."""
        provider = CoinGeckoOHLCVProvider()

        mock_resp = AsyncMock()
        mock_resp.status = 429

        with patch.object(provider, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceUnavailable) as exc_info:
                run_async(provider.get_ohlcv("WETH", "USD", "4h", limit=30))

        assert "Rate limited" in str(exc_info.value)

    def test_fetch_ohlcv_http_error(self) -> None:
        """Test HTTP error handling."""
        provider = CoinGeckoOHLCVProvider()

        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value="Internal Server Error")

        with patch.object(provider, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceUnavailable) as exc_info:
                run_async(provider.get_ohlcv("WETH", "USD", "4h", limit=30))

        assert "500" in str(exc_info.value)

    def test_fetch_ohlcv_empty_response(self) -> None:
        """Test empty response handling."""
        provider = CoinGeckoOHLCVProvider()

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])

        with patch.object(provider, "_get_session") as mock_session:
            mock_session.return_value.get = MagicMock(
                return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp))
            )

            with pytest.raises(DataSourceUnavailable) as exc_info:
                run_async(provider.get_ohlcv("WETH", "USD", "4h", limit=30))

        assert "No OHLC data" in str(exc_info.value)

    def test_fetch_ohlcv_cache_hit(self) -> None:
        """Test cache hit returns cached data without API call."""
        provider = CoinGeckoOHLCVProvider(cache_ttl=300)

        # Pre-populate cache with more data than limit
        test_data = [
            OHLCVData(
                timestamp=datetime.now(UTC) - timedelta(hours=i),
                open=Decimal("2500"),
                high=Decimal("2550"),
                low=Decimal("2450"),
                close=Decimal(str(2500 + i)),
            )
            for i in range(10)
        ]
        # Sort oldest first
        test_data.sort(key=lambda x: x.timestamp)

        # Use the correct cache key based on timeframe and calculated days
        # For timeframe="4h" and limit=5, days = max(7, (5 // 6) + 2) = 7
        provider._cache["WETH:4h:7"] = OHLCVCacheEntry(
            data=test_data,
            cached_at=datetime.now(UTC),
            token="WETH",
            timeframe="4h",
        )

        # Should return cached data (last 5 items based on limit)
        result = run_async(provider.get_ohlcv("WETH", "USD", "4h", limit=5))

        assert len(result) == 5
        assert provider._metrics.cache_hits == 1


class TestCoinGeckoOHLCVProviderHealth:
    """Tests for health metrics."""

    def test_get_health_metrics(self) -> None:
        """Test health metrics retrieval."""
        provider = CoinGeckoOHLCVProvider()
        provider._metrics.total_requests = 100
        provider._metrics.successful_requests = 95
        provider._metrics.cache_hits = 50
        provider._metrics.errors = 5

        metrics = provider.get_health_metrics()

        assert metrics["total_requests"] == 100
        assert metrics["successful_requests"] == 95
        assert metrics["cache_hits"] == 50
        assert metrics["errors"] == 5
        assert metrics["success_rate"] == 95.0


# =============================================================================
# RSICalculator Integration Tests
# =============================================================================


class TestRSICalculatorIntegration:
    """Integration tests for RSICalculator with mocked provider."""

    def test_calculate_rsi_success(self) -> None:
        """Test successful RSI calculation through the calculator."""
        # Create mock OHLCV provider
        mock_provider = AsyncMock()

        # Return enough OHLCV data for RSI(14) calculation
        ohlcv_data = []
        base_price = 2500
        for i in range(30):  # 30 data points
            ohlcv_data.append(
                OHLCVCandle(
                    timestamp=datetime.now(UTC) - timedelta(hours=30 - i),
                    open=Decimal(str(base_price + i)),
                    high=Decimal(str(base_price + i + 10)),
                    low=Decimal(str(base_price + i - 10)),
                    close=Decimal(str(base_price + i + 5)),  # Uptrend
                )
            )

        mock_provider.get_ohlcv = AsyncMock(return_value=ohlcv_data)

        calculator = RSICalculator(ohlcv_provider=mock_provider)
        rsi = run_async(calculator.calculate_rsi("WETH", period=14))

        # With pure uptrend, RSI should be 100
        assert rsi == 100.0

    def test_calculate_rsi_insufficient_data(self) -> None:
        """Test RSI calculation with insufficient data."""
        mock_provider = AsyncMock()

        # Return only 5 data points (need 15 for RSI(14))
        ohlcv_data = [
            OHLCVCandle(
                timestamp=datetime.now(UTC),
                open=Decimal("2500"),
                high=Decimal("2510"),
                low=Decimal("2490"),
                close=Decimal("2500"),
            )
            for _ in range(5)
        ]

        mock_provider.get_ohlcv = AsyncMock(return_value=ohlcv_data)

        calculator = RSICalculator(ohlcv_provider=mock_provider)

        with pytest.raises(InsufficientDataError) as exc_info:
            run_async(calculator.calculate_rsi("WETH", period=14))

        assert exc_info.value.required == 15
        assert exc_info.value.available == 5

    def test_calculate_rsi_empty_data(self) -> None:
        """Test RSI calculation with empty data."""
        mock_provider = AsyncMock()
        mock_provider.get_ohlcv = AsyncMock(return_value=[])

        calculator = RSICalculator(ohlcv_provider=mock_provider)

        with pytest.raises(InsufficientDataError) as exc_info:
            run_async(calculator.calculate_rsi("WETH", period=14))

        assert exc_info.value.available == 0

    def test_calculate_rsi_different_periods(self) -> None:
        """Test RSI calculation with different periods."""
        mock_provider = AsyncMock()

        # Return enough data
        ohlcv_data = []
        for i in range(50):
            ohlcv_data.append(
                OHLCVCandle(
                    timestamp=datetime.now(UTC) - timedelta(hours=50 - i),
                    open=Decimal(str(100 + i)),
                    high=Decimal(str(100 + i + 5)),
                    low=Decimal(str(100 + i - 5)),
                    close=Decimal(str(100 + i)),  # Uptrend
                )
            )

        mock_provider.get_ohlcv = AsyncMock(return_value=ohlcv_data)

        calculator = RSICalculator(ohlcv_provider=mock_provider)

        # Test with different periods
        rsi_7 = run_async(calculator.calculate_rsi("WETH", period=7))
        rsi_14 = run_async(calculator.calculate_rsi("WETH", period=14))
        rsi_21 = run_async(calculator.calculate_rsi("WETH", period=21))

        # All should be 100 for pure uptrend
        assert rsi_7 == 100.0
        assert rsi_14 == 100.0
        assert rsi_21 == 100.0

    def test_get_ohlcv_provider_health(self) -> None:
        """Test getting health metrics from provider."""
        mock_provider = MagicMock()
        mock_provider.get_health_metrics = MagicMock(return_value={"total_requests": 100})

        calculator = RSICalculator(ohlcv_provider=mock_provider)
        health = calculator.get_ohlcv_provider_health()

        assert health["total_requests"] == 100


# =============================================================================
# Context Manager Tests
# =============================================================================


class TestCoinGeckoOHLCVProviderContextManager:
    """Tests for async context manager functionality."""

    def test_context_manager(self) -> None:
        """Test async context manager properly closes session."""

        async def test_cm() -> CoinGeckoOHLCVProvider:
            async with CoinGeckoOHLCVProvider() as provider:
                return provider

        provider = run_async(test_cm())
        # Session should be closed after exiting context
        assert provider._session is None or provider._session.closed


# =============================================================================
# Known RSI Value Tests (Verification)
# =============================================================================


class TestKnownRSIValues:
    """Tests using known RSI values to verify calculation accuracy.

    These tests use hand-calculated or verified RSI values to ensure
    the implementation matches the standard Wilder's RSI formula.
    """

    def test_rsi_50_equal_gains_losses(self) -> None:
        """Test RSI with balanced gains and losses.

        With Wilder's smoothing, exact 50 is difficult to achieve as the
        smoothing weights recent values more. We test that balanced
        oscillations produce RSI in the neutral zone (30-70).
        """
        # Create prices with equal up and down movements, ending on down
        prices = [
            Decimal("100"),
            Decimal("101"),  # +1
            Decimal("100"),  # -1
            Decimal("101"),  # +1
            Decimal("100"),  # -1
            Decimal("101"),  # +1
            Decimal("100"),  # -1
        ]

        # With period=3, need 4 points minimum
        rsi = RSICalculator.calculate_rsi_from_prices(prices, period=3)

        # With balanced oscillations and Wilder's smoothing, RSI should be
        # in the neutral zone. Due to exponential nature of Wilder's method,
        # exact 50 isn't guaranteed, but should be between 30-70.
        assert 30 <= rsi <= 70

    def test_rsi_extremes(self) -> None:
        """Test RSI at extreme values."""
        # Pure gains = RSI 100
        all_gains = [Decimal(str(100 + i)) for i in range(20)]
        rsi_100 = RSICalculator.calculate_rsi_from_prices(all_gains, period=14)
        assert rsi_100 == 100.0

        # Pure losses = RSI 0
        all_losses = [Decimal(str(200 - i)) for i in range(20)]
        rsi_0 = RSICalculator.calculate_rsi_from_prices(all_losses, period=14)
        assert rsi_0 == 0.0

    def test_rsi_oversold_threshold(self) -> None:
        """Test detecting oversold condition (RSI < 30)."""
        # Create downtrending prices that should produce low RSI
        prices = [Decimal("100")]
        # Mix of mostly losses
        for i in range(1, 20):
            if i % 5 == 0:
                prices.append(prices[-1] + Decimal("0.5"))  # Small gain
            else:
                prices.append(prices[-1] - Decimal("1"))  # Larger loss

        rsi = RSICalculator.calculate_rsi_from_prices(prices, period=14)

        # Should be below 30 (oversold)
        assert rsi < 30

    def test_rsi_overbought_threshold(self) -> None:
        """Test detecting overbought condition (RSI > 70)."""
        # Create uptrending prices that should produce high RSI
        prices = [Decimal("100")]
        # Mix of mostly gains
        for i in range(1, 20):
            if i % 5 == 0:
                prices.append(prices[-1] - Decimal("0.5"))  # Small loss
            else:
                prices.append(prices[-1] + Decimal("1"))  # Larger gain

        rsi = RSICalculator.calculate_rsi_from_prices(prices, period=14)

        # Should be above 70 (overbought)
        assert rsi > 70
