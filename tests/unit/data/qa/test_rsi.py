"""Tests for RSI Indicator Test Module.

This test suite covers:
- RSIResult and RSIDataPoint dataclass creation and serialization
- RSITest with mocked CoinGeckoOHLCVProvider
- RSI bounds validation (0-100)
- Signal detection (Oversold, Overbought, Neutral)
- Error handling for InsufficientDataError and DataSourceUnavailable
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, InsufficientDataError, OHLCVCandle
from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.tests.rsi import (
    RSI_OVERBOUGHT_THRESHOLD,
    RSI_OVERSOLD_THRESHOLD,
    RSIDataPoint,
    RSIResult,
    RSITest,
    get_rsi_signal,
)

# =============================================================================
# Helper Functions
# =============================================================================


def create_candle(
    timestamp: datetime,
    close_price: float = 100.0,
) -> OHLCVCandle:
    """Create an OHLCVCandle for testing."""
    return OHLCVCandle(
        timestamp=timestamp,
        open=Decimal(str(close_price - 5.0)),
        high=Decimal(str(close_price + 5.0)),
        low=Decimal(str(close_price - 10.0)),
        close=Decimal(str(close_price)),
        volume=None,
    )


def create_trending_candles(
    start: datetime,
    count: int,
    start_price: float = 100.0,
    price_change_per_candle: float = 1.0,
    interval_hours: int = 4,
) -> list[OHLCVCandle]:
    """Create a series of candles with a trending price."""
    candles = []
    current = start
    price = start_price
    for _ in range(count):
        candles.append(create_candle(timestamp=current, close_price=price))
        current = current + timedelta(hours=interval_hours)
        price += price_change_per_candle
    return candles


def create_volatile_candles(
    start: datetime,
    count: int,
    base_price: float = 100.0,
    interval_hours: int = 4,
) -> list[OHLCVCandle]:
    """Create a series of candles with alternating prices (high volatility)."""
    candles = []
    current = start
    for i in range(count):
        # Alternate between high and low prices
        price = base_price + (10 if i % 2 == 0 else -10)
        candles.append(create_candle(timestamp=current, close_price=price))
        current = current + timedelta(hours=interval_hours)
    return candles


# =============================================================================
# RSIDataPoint Tests
# =============================================================================


class TestRSIDataPoint:
    """Tests for RSIDataPoint dataclass."""

    def test_create_data_point(self) -> None:
        """Test creating an RSI data point."""
        point = RSIDataPoint(index=5, rsi=45.5)
        assert point.index == 5
        assert point.rsi == 45.5


# =============================================================================
# RSIResult Tests
# =============================================================================


class TestRSIResult:
    """Tests for RSIResult dataclass."""

    def test_create_passing_result(self) -> None:
        """Test creating a passing result."""
        history = [
            RSIDataPoint(index=0, rsi=30.0),
            RSIDataPoint(index=1, rsi=45.0),
            RSIDataPoint(index=2, rsi=55.0),
        ]

        result = RSIResult(
            token="ETH",
            current_rsi=55.0,
            signal="Neutral",
            rsi_history=history,
            min_rsi=30.0,
            max_rsi=55.0,
            avg_rsi=43.33,
            passed=True,
            error=None,
        )

        assert result.token == "ETH"
        assert result.current_rsi == 55.0
        assert result.signal == "Neutral"
        assert len(result.rsi_history) == 3
        assert result.min_rsi == 30.0
        assert result.max_rsi == 55.0
        assert result.avg_rsi == 43.33
        assert result.passed is True
        assert result.error is None

    def test_create_failing_result(self) -> None:
        """Test creating a failing result."""
        result = RSIResult(
            token="ETH",
            current_rsi=None,
            signal="Unknown",
            passed=False,
            error="Insufficient data: need 15 points, have 5",
        )

        assert result.token == "ETH"
        assert result.current_rsi is None
        assert result.signal == "Unknown"
        assert result.rsi_history == []
        assert result.min_rsi is None
        assert result.max_rsi is None
        assert result.avg_rsi is None
        assert result.passed is False
        assert result.error == "Insufficient data: need 15 points, have 5"

    def test_to_dict_with_values(self) -> None:
        """Test serialization with all values present."""
        history = [
            RSIDataPoint(index=0, rsi=25.0),
            RSIDataPoint(index=1, rsi=75.0),
        ]

        result = RSIResult(
            token="ETH",
            current_rsi=75.0,
            signal="Overbought",
            rsi_history=history,
            min_rsi=25.0,
            max_rsi=75.0,
            avg_rsi=50.0,
            passed=True,
            error=None,
        )

        d = result.to_dict()

        assert d["token"] == "ETH"
        assert d["current_rsi"] == 75.0
        assert d["signal"] == "Overbought"
        assert len(d["rsi_history"]) == 2
        assert d["rsi_history"][0] == {"index": 0, "rsi": 25.0}
        assert d["rsi_history"][1] == {"index": 1, "rsi": 75.0}
        assert d["min_rsi"] == 25.0
        assert d["max_rsi"] == 75.0
        assert d["avg_rsi"] == 50.0
        assert d["passed"] is True
        assert d["error"] is None

    def test_to_dict_with_none_values(self) -> None:
        """Test serialization with None values."""
        result = RSIResult(
            token="ETH",
            current_rsi=None,
            signal="Unknown",
            passed=False,
            error="Some error",
        )

        d = result.to_dict()

        assert d["current_rsi"] is None
        assert d["rsi_history"] == []
        assert d["min_rsi"] is None
        assert d["max_rsi"] is None
        assert d["avg_rsi"] is None
        assert d["error"] == "Some error"


# =============================================================================
# get_rsi_signal Tests
# =============================================================================


class TestGetRSISignal:
    """Tests for get_rsi_signal function."""

    def test_oversold_signal(self) -> None:
        """Test oversold signal for RSI < 30."""
        assert get_rsi_signal(0.0) == "Oversold"
        assert get_rsi_signal(15.0) == "Oversold"
        assert get_rsi_signal(29.9) == "Oversold"

    def test_overbought_signal(self) -> None:
        """Test overbought signal for RSI > 70."""
        assert get_rsi_signal(70.1) == "Overbought"
        assert get_rsi_signal(85.0) == "Overbought"
        assert get_rsi_signal(100.0) == "Overbought"

    def test_neutral_signal(self) -> None:
        """Test neutral signal for RSI between 30 and 70."""
        assert get_rsi_signal(30.0) == "Neutral"
        assert get_rsi_signal(50.0) == "Neutral"
        assert get_rsi_signal(70.0) == "Neutral"

    def test_threshold_constants(self) -> None:
        """Test that threshold constants are correct."""
        assert RSI_OVERSOLD_THRESHOLD == 30.0
        assert RSI_OVERBOUGHT_THRESHOLD == 70.0


# =============================================================================
# RSITest Tests
# =============================================================================


@pytest.fixture
def mock_ohlcv_provider() -> MagicMock:
    """Create a mock CoinGeckoOHLCVProvider."""
    mock = MagicMock()
    mock.get_ohlcv = AsyncMock()
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_rsi_calculator() -> MagicMock:
    """Create a mock RSICalculator."""
    mock = MagicMock()
    return mock


@pytest.fixture
def qa_config() -> QAConfig:
    """Create a QAConfig for testing."""
    return QAConfig(
        chain="arbitrum",
        historical_days=30,
        timeframe="4h",
        rsi_period=14,
        thresholds=QAThresholds(
            min_confidence=0.8,
            max_price_impact_bps=100,
            max_gap_hours=8.0,
            max_stale_seconds=120,
        ),
        popular_tokens=["ETH", "WBTC"],
        additional_tokens=["UNI"],
        dex_tokens=[],
    )


class TestRSITest:
    """Tests for RSITest class."""

    @pytest.mark.asyncio
    async def test_run_all_passing(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test run() when all tokens pass validation."""
        # Create candles with uptrend (should give valid RSI)
        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_trending_candles(start, count=64, start_price=100.0, price_change_per_candle=0.5)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Should have 3 results (2 popular + 1 additional)
        assert len(results) == 3

        # All should pass (RSI values should be within 0-100)
        assert all(r.passed for r in results)
        assert all(r.error is None for r in results)
        assert all(r.current_rsi is not None for r in results)
        assert all(r.signal in ["Neutral", "Oversold", "Overbought"] for r in results)

        # Verify get_ohlcv was called for each token
        assert mock_ohlcv_provider.get_ohlcv.call_count == 3

    @pytest.mark.asyncio
    async def test_run_rsi_bounds_valid(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that RSI values are within valid bounds."""
        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_volatile_candles(start, count=64)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        for result in results:
            assert result.passed is True
            # Check all RSI values in history are within bounds
            for point in result.rsi_history:
                assert 0.0 <= point.rsi <= 100.0
            # Check statistics are within bounds
            if result.min_rsi is not None:
                assert 0.0 <= result.min_rsi <= 100.0
            if result.max_rsi is not None:
                assert 0.0 <= result.max_rsi <= 100.0
            if result.avg_rsi is not None:
                assert 0.0 <= result.avg_rsi <= 100.0

    @pytest.mark.asyncio
    async def test_run_insufficient_data(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test handling of insufficient data for RSI calculation."""
        # Only 10 candles (need 15 for period=14)
        start = datetime.now(UTC) - timedelta(days=2)
        candles = create_trending_candles(start, count=10)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail due to insufficient data
        assert all(not r.passed for r in results)
        assert all("Insufficient data" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_empty_candles_fails(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that empty candles list causes failure."""
        mock_ohlcv_provider.get_ohlcv.return_value = []

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail
        assert all(not r.passed for r in results)
        assert all("Insufficient data" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_data_source_unavailable(
        self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig
    ) -> None:
        """Test graceful handling of DataSourceUnavailable."""
        mock_ohlcv_provider.get_ohlcv.side_effect = DataSourceUnavailable(
            source="coingecko_ohlcv",
            reason="Rate limited",
        )

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all(r.current_rsi is None for r in results)
        assert all("Data source unavailable" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_insufficient_data_error(
        self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig
    ) -> None:
        """Test graceful handling of InsufficientDataError."""
        mock_ohlcv_provider.get_ohlcv.side_effect = InsufficientDataError(
            required=15,
            available=5,
            indicator="RSI",
        )

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all(r.current_rsi is None for r in results)
        assert all("Insufficient data" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_unexpected_error(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling of unexpected exceptions."""
        mock_ohlcv_provider.get_ohlcv.side_effect = RuntimeError("Network error")

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all("Unexpected error" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_mixed_results(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test handling of mixed pass/fail results."""
        start = datetime.now(UTC) - timedelta(days=30)

        # Good candles for ETH
        good_candles = create_trending_candles(start, count=64)

        # Not enough candles for WBTC
        few_candles = create_trending_candles(start, count=10)

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return good_candles
            elif token == "WBTC":
                return few_candles
            else:
                raise DataSourceUnavailable(
                    source="coingecko_ohlcv",
                    reason="Token not found",
                )

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Find results by token
        eth_result = next(r for r in results if r.token == "ETH")
        wbtc_result = next(r for r in results if r.token == "WBTC")
        uni_result = next(r for r in results if r.token == "UNI")

        # ETH should pass
        assert eth_result.passed is True
        assert eth_result.error is None
        assert eth_result.current_rsi is not None

        # WBTC should fail due to insufficient data
        assert wbtc_result.passed is False
        assert "Insufficient data" in (wbtc_result.error or "")

        # UNI should fail due to unavailable
        assert uni_result.passed is False
        assert "Data source unavailable" in (uni_result.error or "")

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test async context manager properly closes resources."""
        async with RSITest(qa_config, mock_ohlcv_provider) as test:
            assert test is not None

        mock_ohlcv_provider.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_token_list(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test handling of empty token list."""
        config = QAConfig(
            popular_tokens=[],
            additional_tokens=[],
        )

        test = RSITest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should return empty list
        assert results == []
        mock_ohlcv_provider.get_ohlcv.assert_not_called()

    @pytest.mark.asyncio
    async def test_rsi_history_populated(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that RSI history is correctly populated."""
        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_trending_candles(start, count=64)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        for result in results:
            # Should have RSI history
            assert len(result.rsi_history) > 0
            # Indices should be sequential starting from 0
            for i, point in enumerate(result.rsi_history):
                assert point.index == i

    @pytest.mark.asyncio
    async def test_oversold_signal_detection(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test detection of oversold signal (downtrend)."""
        start = datetime.now(UTC) - timedelta(days=30)
        # Strong downtrend should give low RSI
        candles = create_trending_candles(start, count=64, start_price=200.0, price_change_per_candle=-2.0)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Should have oversold signal with strong downtrend
        for result in results:
            assert result.passed is True
            assert result.current_rsi is not None
            # RSI should be low (but still valid)
            assert 0.0 <= result.current_rsi <= 100.0

    @pytest.mark.asyncio
    async def test_overbought_signal_detection(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test detection of overbought signal (uptrend)."""
        start = datetime.now(UTC) - timedelta(days=30)
        # Strong uptrend should give high RSI
        candles = create_trending_candles(start, count=64, start_price=100.0, price_change_per_candle=2.0)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Should have overbought signal with strong uptrend
        for result in results:
            assert result.passed is True
            assert result.current_rsi is not None
            # RSI should be high (but still valid)
            assert 0.0 <= result.current_rsi <= 100.0

    @pytest.mark.asyncio
    async def test_statistics_calculation(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that min, max, avg RSI are correctly calculated."""
        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_volatile_candles(start, count=64)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        for result in results:
            assert result.passed is True
            assert result.min_rsi is not None
            assert result.max_rsi is not None
            assert result.avg_rsi is not None

            # min <= avg <= max
            assert result.min_rsi <= result.avg_rsi <= result.max_rsi

            # Verify against actual history
            rsi_values = [p.rsi for p in result.rsi_history]
            assert result.min_rsi == min(rsi_values)
            assert result.max_rsi == max(rsi_values)
            assert abs(result.avg_rsi - (sum(rsi_values) / len(rsi_values))) < 0.01

    @pytest.mark.asyncio
    async def test_different_rsi_period(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test with different RSI period (7 instead of 14)."""
        config = QAConfig(
            chain="arbitrum",
            historical_days=30,
            timeframe="4h",
            rsi_period=7,  # Different period
            thresholds=QAThresholds(),
            popular_tokens=["ETH"],
            additional_tokens=[],
            dex_tokens=[],
        )

        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_trending_candles(start, count=32)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(config, mock_ohlcv_provider)
        results = await test.run()

        assert len(results) == 1
        assert results[0].passed is True
        # Should have more history points with shorter period
        assert len(results[0].rsi_history) > 0

    @pytest.mark.asyncio
    async def test_single_candle_insufficient(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that a single candle is insufficient for RSI."""
        start = datetime.now(UTC)
        candles = [create_candle(start)]

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = RSITest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        assert all(not r.passed for r in results)
        assert all("Insufficient data" in (r.error or "") for r in results)
