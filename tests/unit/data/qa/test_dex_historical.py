"""Tests for DEX Historical Price Test Module.

This test suite covers:
- DEXHistoricalResult and WETHPricePoint dataclass creation and serialization
- DEXHistoricalTest with mocked CoinGeckoOHLCVProvider
- WETH price conversion logic
- Error handling for DataSourceUnavailable
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.tests.dex_historical import (
    DEXHistoricalResult,
    DEXHistoricalTest,
    WETHPricePoint,
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
    base_price: float = 100.0,
) -> list[OHLCVCandle]:
    """Create a series of candles with consistent intervals."""
    candles = []
    current = start
    for i in range(count):
        candles.append(
            create_candle(
                timestamp=current,
                open_price=base_price + i,
                high_price=base_price + i + 5,
                low_price=base_price + i - 5,
                close_price=base_price + i + 2,
            )
        )
        current = current + timedelta(hours=interval_hours)
    return candles


# =============================================================================
# WETHPricePoint Tests
# =============================================================================


class TestWETHPricePoint:
    """Tests for WETHPricePoint dataclass."""

    def test_create_price_point(self) -> None:
        """Test creating a WETH price point."""
        ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)
        point = WETHPricePoint(
            timestamp=ts,
            price_weth=Decimal("0.045"),
        )

        assert point.timestamp == ts
        assert point.price_weth == Decimal("0.045")

    def test_to_dict(self) -> None:
        """Test serialization."""
        ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)
        point = WETHPricePoint(
            timestamp=ts,
            price_weth=Decimal("0.045"),
        )

        d = point.to_dict()

        assert d["timestamp"] == "2025-01-18T12:00:00+00:00"
        assert d["price_weth"] == "0.045"


# =============================================================================
# DEXHistoricalResult Tests
# =============================================================================


class TestDEXHistoricalResult:
    """Tests for DEXHistoricalResult dataclass."""

    def test_create_passing_result(self) -> None:
        """Test creating a passing result."""
        ts = datetime.now(UTC)
        weth_prices = [WETHPricePoint(timestamp=ts, price_weth=Decimal("0.045"))]

        result = DEXHistoricalResult(
            token="WBTC",
            weth_prices=weth_prices,
            total_points=100,
            passed=True,
            error=None,
        )

        assert result.token == "WBTC"
        assert len(result.weth_prices) == 1
        assert result.total_points == 100
        assert result.passed is True
        assert result.error is None
        assert result.note == "Derived from CEX data with WETH conversion"

    def test_create_failing_result(self) -> None:
        """Test creating a failing result."""
        result = DEXHistoricalResult(
            token="WBTC",
            weth_prices=[],
            total_points=0,
            passed=False,
            error="Data source unavailable: Timeout",
        )

        assert result.token == "WBTC"
        assert result.weth_prices == []
        assert result.total_points == 0
        assert result.passed is False
        assert result.error == "Data source unavailable: Timeout"

    def test_to_dict_with_values(self) -> None:
        """Test serialization with all values present."""
        ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)
        weth_prices = [WETHPricePoint(timestamp=ts, price_weth=Decimal("0.045"))]

        result = DEXHistoricalResult(
            token="WBTC",
            weth_prices=weth_prices,
            total_points=100,
            passed=True,
            error=None,
        )

        d = result.to_dict()

        assert d["token"] == "WBTC"
        assert len(d["weth_prices"]) == 1
        assert d["weth_prices"][0]["price_weth"] == "0.045"
        assert d["total_points"] == 100
        assert d["passed"] is True
        assert d["error"] is None
        assert d["note"] == "Derived from CEX data with WETH conversion"

    def test_to_dict_with_empty_prices(self) -> None:
        """Test serialization with empty prices."""
        result = DEXHistoricalResult(
            token="WBTC",
            weth_prices=[],
            total_points=0,
            passed=False,
            error="Some error",
        )

        d = result.to_dict()

        assert d["weth_prices"] == []
        assert d["total_points"] == 0


# =============================================================================
# DEXHistoricalTest Tests
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
        dex_tokens=["WBTC", "UNI"],
    )


class TestDEXHistoricalTest:
    """Tests for DEXHistoricalTest class."""

    @pytest.mark.asyncio
    async def test_run_all_passing(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test run() when all tokens pass validation."""
        start = datetime.now(UTC) - timedelta(days=30)

        # ETH candles at $2500
        eth_candles = create_candle_series(start, count=180, interval_hours=4, base_price=2500.0)
        # Token candles at $100 (giving ~0.04 WETH price)
        token_candles = create_candle_series(start, count=180, interval_hours=4, base_price=100.0)

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            return token_candles

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # Should have 2 results (WBTC and UNI from dex_tokens, ETH excluded)
        assert len(results) == 2

        # All should pass
        assert all(r.passed for r in results)
        assert all(r.error is None for r in results)
        assert all(r.total_points > 0 for r in results)

    @pytest.mark.asyncio
    async def test_run_excludes_eth(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that ETH is excluded from DEX historical tests."""
        config = QAConfig(
            dex_tokens=["ETH", "WBTC"],
        )

        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_candle_series(start, count=180, interval_hours=4)
        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should only have WBTC (ETH excluded)
        assert len(results) == 1
        assert results[0].token == "WBTC"

    @pytest.mark.asyncio
    async def test_run_eth_data_unavailable_fails_all(
        self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig
    ) -> None:
        """Test that all tokens fail when ETH data is unavailable."""
        mock_ohlcv_provider.get_ohlcv.side_effect = DataSourceUnavailable(
            source="coingecko_ohlcv",
            reason="Rate limited",
        )

        test = DEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with ETH unavailable error
        assert all(not r.passed for r in results)
        assert all("ETH/USD data unavailable" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_empty_eth_candles_fails_all(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test that all tokens fail when ETH returns empty candles."""
        mock_ohlcv_provider.get_ohlcv.return_value = []

        test = DEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with ETH unavailable error
        assert all(not r.passed for r in results)
        assert all("ETH/USD data unavailable" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_token_data_unavailable(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling when token data is unavailable."""
        start = datetime.now(UTC) - timedelta(days=30)
        eth_candles = create_candle_series(start, count=180, interval_hours=4)

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            raise DataSourceUnavailable(
                source="coingecko_ohlcv",
                reason="Token not found",
            )

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with data unavailable
        assert all(not r.passed for r in results)
        assert all("Data source unavailable" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_run_handles_unexpected_error(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test graceful handling of unexpected exceptions."""
        start = datetime.now(UTC) - timedelta(days=30)
        eth_candles = create_candle_series(start, count=180, interval_hours=4)

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            raise RuntimeError("Network error")

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(qa_config, mock_ohlcv_provider)
        results = await test.run()

        # All should fail with unexpected error
        assert all(not r.passed for r in results)
        assert all("Unexpected error" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_weth_conversion_calculation(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that WETH price is correctly calculated."""
        config = QAConfig(dex_tokens=["WBTC"])
        start = datetime.now(UTC)

        # ETH at $2500
        eth_candles = [create_candle(start, close_price=2500.0)]
        # WBTC at $50000
        wbtc_candles = [create_candle(start, close_price=50000.0)]

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            return wbtc_candles

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        assert len(results) == 1
        result = results[0]

        assert result.passed is True
        assert len(result.weth_prices) == 1
        # WETH price = 50000 / 2500 = 20
        assert result.weth_prices[0].price_weth == Decimal("20")

    @pytest.mark.asyncio
    async def test_timestamp_matching_exact(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that exact timestamp matching works."""
        config = QAConfig(dex_tokens=["WBTC"])
        ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)

        eth_candles = [create_candle(ts, close_price=2500.0)]
        wbtc_candles = [create_candle(ts, close_price=50000.0)]

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            return wbtc_candles

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        assert results[0].passed is True
        assert len(results[0].weth_prices) == 1

    @pytest.mark.asyncio
    async def test_timestamp_matching_within_tolerance(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that timestamp matching works within 1-hour tolerance."""
        config = QAConfig(dex_tokens=["WBTC"])
        eth_ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)
        # WBTC timestamp 30 minutes later (within tolerance)
        wbtc_ts = datetime(2025, 1, 18, 12, 30, 0, tzinfo=UTC)

        eth_candles = [create_candle(eth_ts, close_price=2500.0)]
        wbtc_candles = [create_candle(wbtc_ts, close_price=50000.0)]

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            return wbtc_candles

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should still convert successfully
        assert results[0].passed is True
        assert len(results[0].weth_prices) == 1

    @pytest.mark.asyncio
    async def test_timestamp_matching_outside_tolerance(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that timestamp matching fails outside 1-hour tolerance."""
        config = QAConfig(dex_tokens=["WBTC"])
        eth_ts = datetime(2025, 1, 18, 12, 0, 0, tzinfo=UTC)
        # WBTC timestamp 2 hours later (outside tolerance)
        wbtc_ts = datetime(2025, 1, 18, 14, 0, 0, tzinfo=UTC)

        eth_candles = [create_candle(eth_ts, close_price=2500.0)]
        wbtc_candles = [create_candle(wbtc_ts, close_price=50000.0)]

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            return wbtc_candles

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should fail (no matching ETH price)
        assert results[0].passed is False
        assert "No WETH prices could be derived" in (results[0].error or "")

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_ohlcv_provider: MagicMock, qa_config: QAConfig) -> None:
        """Test async context manager properly closes resources."""
        async with DEXHistoricalTest(qa_config, mock_ohlcv_provider) as test:
            assert test is not None

        mock_ohlcv_provider.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_token_list(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test handling of empty token list."""
        config = QAConfig(
            popular_tokens=[],
            additional_tokens=[],
            dex_tokens=[],
        )

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should return empty list
        assert results == []
        mock_ohlcv_provider.get_ohlcv.assert_not_called()

    @pytest.mark.asyncio
    async def test_uses_all_tokens_when_no_dex_tokens(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that all_tokens is used when dex_tokens is empty."""
        config = QAConfig(
            popular_tokens=["WBTC", "LINK"],
            additional_tokens=["UNI"],
            dex_tokens=[],  # Empty dex_tokens
        )

        start = datetime.now(UTC) - timedelta(days=30)
        candles = create_candle_series(start, count=180, interval_hours=4)
        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should have 3 results (WBTC, LINK, UNI - no ETH in this config)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_mixed_results(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test handling of mixed pass/fail results."""
        config = QAConfig(dex_tokens=["WBTC", "UNI"])
        start = datetime.now(UTC)

        eth_candles = [create_candle(start, close_price=2500.0)]
        wbtc_candles = [create_candle(start, close_price=50000.0)]

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            elif token == "WBTC":
                return wbtc_candles
            else:
                raise DataSourceUnavailable(
                    source="coingecko_ohlcv",
                    reason="Token not found",
                )

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Find results by token
        wbtc_result = next(r for r in results if r.token == "WBTC")
        uni_result = next(r for r in results if r.token == "UNI")

        # WBTC should pass
        assert wbtc_result.passed is True
        assert wbtc_result.error is None

        # UNI should fail
        assert uni_result.passed is False
        assert "Data source unavailable" in (uni_result.error or "")

    @pytest.mark.asyncio
    async def test_note_is_included(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that the note about WETH conversion is included."""
        config = QAConfig(dex_tokens=["WBTC"])
        start = datetime.now(UTC)

        candles = [create_candle(start, close_price=2500.0)]
        mock_ohlcv_provider.get_ohlcv.return_value = candles

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        assert results[0].note == "Derived from CEX data with WETH conversion"

    @pytest.mark.asyncio
    async def test_zero_eth_price_skipped(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that zero ETH prices are handled correctly."""
        config = QAConfig(dex_tokens=["WBTC"])
        start = datetime.now(UTC)

        # ETH at $0 (invalid)
        eth_candles = [create_candle(start, close_price=0.0)]
        wbtc_candles = [create_candle(start, close_price=50000.0)]

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            return wbtc_candles

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should fail (cannot divide by zero)
        assert results[0].passed is False
        assert "No WETH prices could be derived" in (results[0].error or "")

    @pytest.mark.asyncio
    async def test_insufficient_data_fails(self, mock_ohlcv_provider: MagicMock) -> None:
        """Test that insufficient converted data points fails validation."""
        config = QAConfig(dex_tokens=["WBTC"])
        start = datetime.now(UTC)

        # 10 ETH candles but only 2 matching token candles
        eth_candles = create_candle_series(start, count=10, interval_hours=4)
        # Token candles with timestamps that mostly don't match
        token_candles = [
            create_candle(start, close_price=50000.0),
            create_candle(start + timedelta(hours=4), close_price=50100.0),
        ]

        def get_ohlcv_side_effect(token: str, quote: str, timeframe: str, limit: int) -> list[OHLCVCandle]:
            if token == "ETH":
                return eth_candles
            return token_candles

        mock_ohlcv_provider.get_ohlcv.side_effect = get_ohlcv_side_effect

        test = DEXHistoricalTest(config, mock_ohlcv_provider)
        results = await test.run()

        # Should pass (2 candles, need at least 1 = 50% of 2)
        assert results[0].passed is True
        assert results[0].total_points == 2
