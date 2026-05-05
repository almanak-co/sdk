"""Tests for CEX Historical Price Test Module.

This test suite covers:
- CEXHistoricalResult dataclass creation and serialization
- CEXHistoricalTest with mocked CoinGeckoOHLCVProvider
- Gap detection and validation logic
- Error handling for DataSourceUnavailable
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.tests.cex_historical import (
    TIMEFRAME_SECONDS,
    CEXHistoricalResult,
    CEXHistoricalTest,
)

# =============================================================================
# Helper Functions
# =============================================================================


def create_candle(
    timestamp: datetime,
    open_price: float = 100.0,
    high_price: float = 105.0,
    low_price: float = 95.0,
    close_price: float = 102.0,
) -> OHLCVCandle:
    """Create an OHLCVCandle for testing."""
    return OHLCVCandle(
        timestamp=timestamp,
        open=Decimal(str(open_price)),
        high=Decimal(str(high_price)),
        low=Decimal(str(low_price)),
        close=Decimal(str(close_price)),
        volume=None,
    )


def create_candle_series(
    start: datetime,
    count: int,
    interval_hours: int = 4,
) -> list[OHLCVCandle]:
    """Create a series of candles with consistent intervals."""
    candles = []
    current = start
    for i in range(count):
        candles.append(
            create_candle(
                timestamp=current,
                open_price=100.0 + i,
                high_price=105.0 + i,
                low_price=95.0 + i,
                close_price=102.0 + i,
            )
        )
        current = current + timedelta(hours=interval_hours)
    return candles


# =============================================================================
# CEXHistoricalResult Tests
# =============================================================================


class TestCEXHistoricalResult:
    """Tests for CEXHistoricalResult dataclass."""

    def test_create_passing_result(self) -> None:
        """Test creating a passing result."""
        ts = datetime.now(UTC)
        candles = [create_candle(ts)]

        result = CEXHistoricalResult(
            token="ETH",
            candles=candles,
            total_candles=100,
            expected_candles=100,
            missing_count=0,
            max_gap_hours=4.0,
            price_range=(Decimal("95.0"), Decimal("105.0")),
            passed=True,
            error=None,
        )

        assert result.token == "ETH"
        assert result.total_candles == 100
        assert result.expected_candles == 100
        assert result.missing_count == 0
        assert result.max_gap_hours == 4.0
        assert result.price_range == (Decimal("95.0"), Decimal("105.0"))
        assert result.passed is True
        assert result.error is None

    def test_create_failing_result(self) -> None:
        """Test creating a failing result."""
        result = CEXHistoricalResult(
            token="ETH",
            candles=[],
            total_candles=0,
            expected_candles=180,
            missing_count=180,
            max_gap_hours=0.0,
            price_range=None,
            passed=False,
            error="Data source unavailable: Timeout",
        )

        assert result.token == "ETH"
        assert result.candles == []
        assert result.total_candles == 0
        assert result.expected_candles == 180
        assert result.missing_count == 180
        assert result.price_range is None
        assert result.passed is False
        assert result.error == "Data source unavailable: Timeout"

    def test_to_dict_with_values(self) -> None:
        """Test serialization with all values present."""
        ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)
        candle = create_candle(ts)

        result = CEXHistoricalResult(
            token="ETH",
            candles=[candle],
            total_candles=100,
            expected_candles=100,
            missing_count=5,
            max_gap_hours=6.5,
            price_range=(Decimal("95.0"), Decimal("105.0")),
            passed=True,
            error=None,
        )

        d = result.to_dict()

        assert d["token"] == "ETH"
        assert len(d["candles"]) == 1
        assert d["total_candles"] == 100
        assert d["expected_candles"] == 100
        assert d["missing_count"] == 5
        assert d["max_gap_hours"] == 6.5
        assert d["price_range"] == ("95.0", "105.0")
        assert d["passed"] is True
        assert d["error"] is None

    def test_to_dict_with_none_values(self) -> None:
        """Test serialization with None values."""
        result = CEXHistoricalResult(
            token="ETH",
            candles=[],
            total_candles=0,
            expected_candles=180,
            missing_count=180,
            max_gap_hours=0.0,
            price_range=None,
            passed=False,
            error="Some error",
        )

        d = result.to_dict()

        assert d["candles"] == []
        assert d["price_range"] is None


# =============================================================================
# TIMEFRAME_SECONDS Tests
# =============================================================================


class TestTimeframeSeconds:
    """Tests for TIMEFRAME_SECONDS constant."""

    def test_timeframe_values(self) -> None:
        """Test that all timeframes have correct second values."""
        assert TIMEFRAME_SECONDS["1m"] == 60
        assert TIMEFRAME_SECONDS["5m"] == 300
        assert TIMEFRAME_SECONDS["15m"] == 900
        assert TIMEFRAME_SECONDS["1h"] == 3600
        assert TIMEFRAME_SECONDS["4h"] == 14400
        assert TIMEFRAME_SECONDS["1d"] == 86400


# =============================================================================
# CEXHistoricalTest Tests
# =============================================================================


@pytest.fixture
def mock_ohlcv_provider() -> MagicMock:
    """Create a mock CoinGeckoOHLCVProvider."""
    mock = MagicMock()
    mock.get_ohlcv = AsyncMock()
    mock.close = AsyncMock()
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


class TestCEXHistoricalTest:
    """Tests for CEXHistoricalTest class."""

    @pytest.mark.asyncio
    async def test_run_all_passing(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test run() when all tokens pass validation."""
        # Create candles with consistent 4-hour intervals
        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_candle_series(start, count=180, interval_hours=4)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Should have 3 results (2 popular + 1 additional)
        assert len(results) == 3

        # All should pass (max_gap_hours is 4.0 which is less than threshold 8.0)
        assert all(r.passed for r in results)
        assert all(r.error is None for r in results)

        # Verify get_ohlcv was called for each token
        assert mock_ohlcv_provider.get_ohlcv.call_count == 3

    @pytest.mark.asyncio
    async def test_run_gap_exceeds_threshold_fails(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that gaps exceeding threshold cause failure."""
        start = datetime.now(UTC) - timedelta(days=30)

        # Create candles with a large gap (12 hours between candle 5 and 6)
        candles = create_candle_series(start, count=5, interval_hours=4)
        # Add a big gap
        gap_candle = create_candle(candles[-1].timestamp + timedelta(hours=12))
        candles.append(gap_candle)
        # Continue normal series
        candles.extend(
            create_candle_series(
                gap_candle.timestamp + timedelta(hours=4),
                count=5,
                interval_hours=4,
            )
        )

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail due to gap (12 hours > 8 hours threshold)
        assert all(not r.passed for r in results)
        assert all("Max gap" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_empty_candles_fails(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that empty candles list causes failure."""
        mock_ohlcv_provider.get_ohlcv.return_value = []

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail
        assert all(not r.passed for r in results)
        assert all("No OHLCV data returned" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_data_source_unavailable(
        self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig
    ) -> None:
        """Test graceful handling of DataSourceUnavailable."""
        mock_ohlcv_provider.get_ohlcv.side_effect = DataSourceUnavailable(
            source="coingecko_ohlcv",
            reason="Rate limited",
        )

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all(r.total_candles == 0 for r in results)
        assert all("Data source unavailable" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_unexpected_error(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling of unexpected exceptions."""
        mock_ohlcv_provider.get_ohlcv.side_effect = RuntimeError("Network error")

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with error message
        assert all(not r.passed for r in results)
        assert all("Unexpected error" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_mixed_results(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test handling of mixed pass/fail results."""
        start = datetime.now(UTC) - timedelta(days=30)

        # Good candles for ETH
        good_candles = create_candle_series(start, count=180, interval_hours=4)

        # Candles with large gap for WBTC
        gap_candles = create_candle_series(start, count=5, interval_hours=4)
        gap_candle = create_candle(gap_candles[-1].timestamp + timedelta(hours=12))
        gap_candles.append(gap_candle)

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return good_candles
            elif token == "WBTC":
                return gap_candles
            else:
                raise DataSourceUnavailable(
                    source="coingecko_ohlcv",
                    reason="Token not found",
                )

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Find results by token
        eth_result = next(r for r in results if r.token == "ETH")
        wbtc_result = next(r for r in results if r.token == "WBTC")
        uni_result = next(r for r in results if r.token == "UNI")

        # ETH should pass
        assert eth_result.passed is True
        assert eth_result.error is None

        # WBTC should fail due to gap
        assert wbtc_result.passed is False
        assert "Max gap" in (wbtc_result.error or "")

        # UNI should fail due to unavailable
        assert uni_result.passed is False
        assert "Data source unavailable" in (uni_result.error or "")

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test async context manager properly closes resources."""
        async with CEXHistoricalTest(qa_config, mock_ohlcv_provider) as test:
            assert test is not None

        mock_ohlcv_provider.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_token_list(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test handling of empty token list."""
        config = QAConfig(
            popular_tokens=[],
            additional_tokens=[],
        )

        test = CEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should return empty list
        assert results == []
        mock_ohlcv_provider.get_ohlcv.assert_not_called()

    @pytest.mark.asyncio
    async def test_price_range_calculation(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that price range is correctly calculated."""
        start = datetime.now(UTC) - timedelta(days=30)
        # Create candles with known min/max values
        candles = [
            create_candle(start, low_price=90.0, high_price=100.0),
            create_candle(start + timedelta(hours=4), low_price=85.0, high_price=110.0),
            create_candle(start + timedelta(hours=8), low_price=95.0, high_price=105.0),
        ]

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Check price range for first token
        eth_result = results[0]
        assert eth_result.price_range is not None
        assert eth_result.price_range[0] == Decimal("85.0")  # min low
        assert eth_result.price_range[1] == Decimal("110.0")  # max high

    @pytest.mark.asyncio
    async def test_gap_detection_at_threshold(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test gap detection at exactly the threshold (8 hours)."""
        start = datetime.now(UTC) - timedelta(days=30)

        # Create candles with gap exactly at threshold (8 hours)
        candles = create_candle_series(start, count=5, interval_hours=4)
        # Add candle exactly 8 hours after (should pass since <= threshold)
        next_candle = create_candle(candles[-1].timestamp + timedelta(hours=8))
        candles.append(next_candle)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Should pass since gap is exactly at threshold
        assert all(r.passed for r in results)

    @pytest.mark.asyncio
    async def test_gap_detection_just_above_threshold(
        self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig
    ) -> None:
        """Test gap detection just above the threshold."""
        start = datetime.now(UTC) - timedelta(days=30)

        # Create candles with gap just above threshold (8.5 hours)
        candles = create_candle_series(start, count=5, interval_hours=4)
        # Add candle 8.5 hours after (should fail since > threshold)
        next_candle = create_candle(candles[-1].timestamp + timedelta(hours=8, minutes=30))
        candles.append(next_candle)

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Should fail since gap exceeds threshold
        assert all(not r.passed for r in results)
        assert all("Max gap" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_missing_count_calculation(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that missing candle count is correctly calculated."""
        start = datetime.now(UTC) - timedelta(days=30)

        # Create candles with known gap (16 hours = 3 missing 4-hour candles)
        candles = [
            create_candle(start),
            create_candle(start + timedelta(hours=16)),  # Gap of 16 hours (3 missing candles)
        ]

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = CEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Check missing count
        eth_result = results[0]
        # 16 hours / 4 hours per candle - 1 = 3 missing candles
        assert eth_result.missing_count == 3
        assert eth_result.max_gap_hours == 16.0

    @pytest.mark.asyncio
    async def test_different_timeframe(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test with different timeframe (1h instead of 4h)."""
        config = QAConfig(
            chain="arbitrum",
            historical_days=7,  # 1 week
            timeframe="1h",
            rsi_period=14,
            thresholds=QAThresholds(max_gap_hours=4.0),
            popular_tokens=["ETH"],
            additional_tokens=[],
            dex_tokens=[],
        )

        start = datetime.now(UTC) - timedelta(days=7)
        candles = create_candle_series(start, count=168, interval_hours=1)  # 7 days * 24 hours

        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = CEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        assert len(results) == 1
        assert results[0].passed is True
        # Expected candles for 7 days at 1h interval = 7 * 24 = 168
        assert results[0].expected_candles == 168
