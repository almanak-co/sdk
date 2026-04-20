"""Unit tests for ADXCalculator.

VIB-349: Verify ADX/+DI/-DI calculations match expected behavior
for known price patterns.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.indicators.adx import ADXCalculator
from almanak.framework.data.interfaces import InsufficientDataError, OHLCVCandle

_BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _make_candle(high: float, low: float, close: float, ts_offset: int = 0) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=_BASE_TIME + timedelta(hours=ts_offset),
        open=Decimal(str((high + low) / 2)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=Decimal("1000"),
    )


def _trending_up_candles(n: int = 30) -> list[OHLCVCandle]:
    """Generate candles with a strong uptrend."""
    candles = []
    base = 100.0
    for i in range(n):
        base += 2.0  # Consistent upward movement
        candles.append(_make_candle(base + 1.0, base - 0.5, base, ts_offset=i))
    return candles


def _trending_down_candles(n: int = 30) -> list[OHLCVCandle]:
    """Generate candles with a strong downtrend."""
    candles = []
    base = 200.0
    for i in range(n):
        base -= 2.0
        candles.append(_make_candle(base + 0.5, base - 1.0, base, ts_offset=i))
    return candles


def _sideways_candles(n: int = 30) -> list[OHLCVCandle]:
    """Generate candles with no clear trend."""
    candles = []
    base = 100.0
    for i in range(n):
        offset = 1.0 if i % 2 == 0 else -1.0
        candles.append(_make_candle(base + 1.5, base - 1.5, base + offset, ts_offset=i))
    return candles


class TestADXFromCandles:
    """Test ADXCalculator.calculate_adx_from_candles static method."""

    def test_uptrend_has_positive_di_above_negative(self):
        candles = _trending_up_candles(30)
        result = ADXCalculator.calculate_adx_from_candles(candles, period=14)
        assert result.plus_di > result.minus_di, "+DI should be above -DI in uptrend"

    def test_downtrend_has_negative_di_above_positive(self):
        candles = _trending_down_candles(30)
        result = ADXCalculator.calculate_adx_from_candles(candles, period=14)
        assert result.minus_di > result.plus_di, "-DI should be above +DI in downtrend"

    def test_strong_trend_has_high_adx(self):
        candles = _trending_up_candles(40)
        result = ADXCalculator.calculate_adx_from_candles(candles, period=14)
        assert result.adx > 20, f"ADX should be > 20 in strong trend, got {result.adx}"

    def test_sideways_has_lower_adx(self):
        candles = _sideways_candles(40)
        result = ADXCalculator.calculate_adx_from_candles(candles, period=14)
        # Sideways markets typically have lower ADX
        assert result.adx < 50, f"ADX should be < 50 in sideways market, got {result.adx}"

    def test_adx_in_valid_range(self):
        candles = _trending_up_candles(30)
        result = ADXCalculator.calculate_adx_from_candles(candles, period=14)
        assert 0 <= result.adx <= 100, f"ADX must be 0-100, got {result.adx}"
        assert result.plus_di >= 0, f"+DI must be non-negative, got {result.plus_di}"
        assert result.minus_di >= 0, f"-DI must be non-negative, got {result.minus_di}"

    def test_insufficient_data_raises(self):
        candles = _trending_up_candles(10)  # Less than period * 2
        with pytest.raises(InsufficientDataError):
            ADXCalculator.calculate_adx_from_candles(candles, period=14)

    def test_to_dict(self):
        candles = _trending_up_candles(30)
        result = ADXCalculator.calculate_adx_from_candles(candles, period=14)
        d = result.to_dict()
        assert "adx" in d
        assert "plus_di" in d
        assert "minus_di" in d

    def test_custom_period(self):
        candles = _trending_up_candles(40)
        result_7 = ADXCalculator.calculate_adx_from_candles(candles, period=7)
        result_14 = ADXCalculator.calculate_adx_from_candles(candles, period=14)
        # Both should produce valid results
        assert 0 <= result_7.adx <= 100
        assert 0 <= result_14.adx <= 100
        # Shorter period should be more responsive — verify uptrend detected by both
        assert result_7.plus_di > result_7.minus_di, "Period 7 should detect uptrend"
        assert result_14.plus_di > result_14.minus_di, "Period 14 should detect uptrend"
