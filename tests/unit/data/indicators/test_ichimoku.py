"""Unit tests for IchimokuCalculator.

VIB-349: Verify Ichimoku Cloud calculations for known price patterns.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.indicators.ichimoku import IchimokuCalculator
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


def _trending_up_candles(n: int = 60) -> list[OHLCVCandle]:
    """Generate candles trending upward."""
    candles = []
    for i in range(n):
        base = 100.0 + i * 1.5
        candles.append(_make_candle(base + 2, base - 1, base + 0.5, ts_offset=i))
    return candles


def _flat_candles(n: int = 60, price: float = 100.0) -> list[OHLCVCandle]:
    """Generate flat candles at a fixed price."""
    return [_make_candle(price + 2, price - 2, price, ts_offset=i) for i in range(n)]


class TestIchimokuFromCandles:
    """Test IchimokuCalculator.calculate_ichimoku_from_candles static method."""

    def test_default_periods(self):
        """Test with default Ichimoku periods (9/26/52)."""
        candles = _trending_up_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(candles)
        assert result.tenkan_sen > 0
        assert result.kijun_sen > 0
        assert result.senkou_span_a > 0
        assert result.senkou_span_b > 0
        assert result.current_price > 0

    def test_tenkan_kijun_relationship_in_uptrend(self):
        """In an uptrend, Tenkan (9-period) should be above Kijun (26-period)."""
        candles = _trending_up_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(candles)
        assert result.tenkan_sen > result.kijun_sen, (
            f"Tenkan ({result.tenkan_sen:.2f}) should be above Kijun ({result.kijun_sen:.2f}) in uptrend"
        )

    def test_senkou_span_a_is_average_of_tenkan_kijun(self):
        """Senkou Span A = (Tenkan + Kijun) / 2."""
        candles = _trending_up_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(candles)
        expected = (result.tenkan_sen + result.kijun_sen) / 2.0
        assert abs(result.senkou_span_a - expected) < 0.001

    def test_flat_market_tenkan_equals_kijun(self):
        """In a perfectly flat market, Tenkan and Kijun should be equal."""
        candles = _flat_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(candles)
        assert abs(result.tenkan_sen - result.kijun_sen) < 0.01

    def test_chikou_span_equals_current_price(self):
        """Chikou span is the current close price."""
        candles = _trending_up_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(candles)
        assert result.chikou_span == result.current_price

    def test_current_price_matches_last_close(self):
        """Current price should match the last candle's close."""
        candles = _trending_up_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(candles)
        assert result.current_price == float(candles[-1].close)

    def test_insufficient_data_raises(self):
        candles = _trending_up_candles(30)  # Less than 52
        with pytest.raises(InsufficientDataError):
            IchimokuCalculator.calculate_ichimoku_from_candles(candles, senkou_b_period=52)

    def test_custom_periods(self):
        """Test with custom Ichimoku periods."""
        candles = _trending_up_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(
            candles, tenkan_period=7, kijun_period=22, senkou_b_period=44
        )
        assert result.tenkan_sen > 0
        assert result.kijun_sen > 0

    def test_midpoint_calculation(self):
        """Verify _midpoint returns (highest + lowest) / 2."""
        candles = [
            _make_candle(110, 90, 100),
            _make_candle(120, 95, 105),
            _make_candle(115, 85, 100),
        ]
        midpoint = IchimokuCalculator._midpoint(candles)
        # Highest high = 120, lowest low = 85
        assert midpoint == (120 + 85) / 2.0

    def test_to_dict(self):
        candles = _trending_up_candles(60)
        result = IchimokuCalculator.calculate_ichimoku_from_candles(candles)
        d = result.to_dict()
        assert "tenkan_sen" in d
        assert "kijun_sen" in d
        assert "senkou_span_a" in d
        assert "senkou_span_b" in d
        assert "chikou_span" in d
        assert "current_price" in d
