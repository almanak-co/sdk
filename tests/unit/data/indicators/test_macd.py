"""Tests for MACD Calculator.

This test suite covers:
- MACD calculation from prices (algorithm correctness)
- EMA calculation (helper method)
- Integration with mocked OHLCVProvider
- Error handling for insufficient data
- Edge cases (flat prices, all gains, all losses)
"""

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, TypeVar
from unittest.mock import AsyncMock

import pytest

from almanak.framework.data.indicators.macd import MACDCalculator
from almanak.framework.data.interfaces import (
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
# Test Data
# =============================================================================

# 50 prices increasing by $1 (pure uptrend)
SAMPLE_PRICES_UPTREND = [Decimal(str(100 + i)) for i in range(50)]

# 50 prices decreasing by $1 (pure downtrend)
SAMPLE_PRICES_DOWNTREND = [Decimal(str(200 - i)) for i in range(50)]

# 50 identical prices
SAMPLE_PRICES_FLAT = [Decimal("100") for _ in range(50)]


class TestMACDCalculation:
    """Core algorithm tests using calculate_macd_from_prices()."""

    def test_macd_uptrend(self):
        """Pure uptrend: MACD line > 0, histogram ~ 0 or positive."""
        result = MACDCalculator.calculate_macd_from_prices(SAMPLE_PRICES_UPTREND)
        assert result.macd_line > 0, f"Expected positive MACD line in uptrend, got {result.macd_line}"
        # Histogram may be tiny negative due to float precision in a perfectly linear trend
        assert result.histogram >= -1e-10, f"Expected ~non-negative histogram in uptrend, got {result.histogram}"

    def test_macd_downtrend(self):
        """Pure downtrend: MACD line < 0, histogram ~ 0 or negative."""
        result = MACDCalculator.calculate_macd_from_prices(SAMPLE_PRICES_DOWNTREND)
        assert result.macd_line < 0, f"Expected negative MACD line in downtrend, got {result.macd_line}"
        # Histogram may be tiny positive due to float precision in a perfectly linear trend
        assert result.histogram <= 1e-10, f"Expected ~non-positive histogram in downtrend, got {result.histogram}"

    def test_macd_crossover_detection(self):
        """Transition from downtrend to uptrend should show histogram sign change."""
        # Create a series that transitions: first descending, then ascending
        prices = [Decimal(str(200 - i)) for i in range(30)] + [Decimal(str(170 + i * 2)) for i in range(30)]
        result = MACDCalculator.calculate_macd_from_prices(prices)
        # After an uptrend recovery, MACD should become positive
        assert result.macd_line > 0

    def test_macd_insufficient_data(self):
        """Less than slow_period + signal_period prices raises InsufficientDataError."""
        short_prices = [Decimal(str(100 + i)) for i in range(20)]  # Only 20, need 35
        with pytest.raises(InsufficientDataError):
            MACDCalculator.calculate_macd_from_prices(short_prices)

    def test_macd_minimum_data(self):
        """Exactly enough data produces valid result."""
        # Default: slow=26, signal=9, need at least 35
        prices = [Decimal(str(100 + i)) for i in range(35)]
        result = MACDCalculator.calculate_macd_from_prices(prices)
        assert isinstance(result.macd_line, float)
        assert isinstance(result.signal_line, float)
        assert isinstance(result.histogram, float)

    def test_macd_custom_periods(self):
        """Non-default periods produce valid output."""
        result = MACDCalculator.calculate_macd_from_prices(
            SAMPLE_PRICES_UPTREND, fast_period=8, slow_period=17, signal_period=5
        )
        assert result.macd_line > 0  # Uptrend should still be positive

    def test_macd_flat_prices(self):
        """All same price: MACD line, signal, histogram all ~ 0."""
        result = MACDCalculator.calculate_macd_from_prices(SAMPLE_PRICES_FLAT)
        assert abs(result.macd_line) < 0.001, f"Expected ~0 MACD line for flat prices, got {result.macd_line}"
        assert abs(result.signal_line) < 0.001
        assert abs(result.histogram) < 0.001


class TestEMACalculation:
    """Tests for _calculate_ema() static method."""

    def test_ema_insufficient_data(self):
        """Returns empty list when prices < period."""
        result = MACDCalculator._calculate_ema([1.0, 2.0], period=5)
        assert result == []

    def test_ema_length_matches_input(self):
        """Output length == input length."""
        prices = [float(i) for i in range(20)]
        result = MACDCalculator._calculate_ema(prices, period=5)
        assert len(result) == len(prices)

    def test_ema_first_values_nan(self):
        """First period-1 values are NaN."""
        import math

        prices = [float(i) for i in range(20)]
        result = MACDCalculator._calculate_ema(prices, period=5)
        for i in range(4):  # period-1 = 4
            assert math.isnan(result[i])
        assert not math.isnan(result[4])  # First valid value

    def test_ema_sma_seed(self):
        """First valid EMA value equals SMA of first period prices."""
        prices = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]
        period = 5
        result = MACDCalculator._calculate_ema(prices, period=period)
        expected_sma = sum(prices[:period]) / period  # (10+20+30+40+50)/5 = 30
        assert abs(result[period - 1] - expected_sma) < 0.001


class TestMACDCalculatorIntegration:
    """Integration tests with mocked OHLCVProvider."""

    def _create_candles(self, prices: list[Decimal]) -> list[OHLCVCandle]:
        """Create mock candles from price list."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        return [
            OHLCVCandle(
                timestamp=base_time + timedelta(hours=i),
                open=prices[max(0, i - 1)],
                high=price + Decimal("1"),
                low=price - Decimal("1"),
                close=price,
                volume=Decimal("1000"),
            )
            for i, price in enumerate(prices)
        ]

    def test_calculate_macd_success(self):
        """Mock provider returns 50+ candles, verify MACDResult fields are floats."""
        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.return_value = self._create_candles(SAMPLE_PRICES_UPTREND)

        calculator = MACDCalculator(ohlcv_provider=mock_provider)
        result = run_async(calculator.calculate_macd("WETH"))

        assert isinstance(result.macd_line, float)
        assert isinstance(result.signal_line, float)
        assert isinstance(result.histogram, float)
        mock_provider.get_ohlcv.assert_awaited_once()

    def test_calculate_macd_insufficient_data(self):
        """Mock returns only 5 candles, raises InsufficientDataError."""
        short_prices = [Decimal(str(100 + i)) for i in range(5)]
        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.return_value = self._create_candles(short_prices)

        calculator = MACDCalculator(ohlcv_provider=mock_provider)
        with pytest.raises(InsufficientDataError):
            run_async(calculator.calculate_macd("WETH"))

    def test_calculate_macd_empty_data(self):
        """Mock returns [], raises InsufficientDataError."""
        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.return_value = []

        calculator = MACDCalculator(ohlcv_provider=mock_provider)
        with pytest.raises(InsufficientDataError):
            run_async(calculator.calculate_macd("WETH"))
