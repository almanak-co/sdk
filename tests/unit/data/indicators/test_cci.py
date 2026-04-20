"""Unit tests for CCICalculator.

VIB-349: Verify CCI calculations for known price patterns.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.indicators.cci import CCICalculator
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


def _flat_candles(n: int = 20, price: float = 100.0) -> list[OHLCVCandle]:
    """Generate flat candles at a fixed price."""
    return [_make_candle(price + 0.5, price - 0.5, price, ts_offset=i) for i in range(n)]


def _rising_candles(n: int = 25) -> list[OHLCVCandle]:
    """Generate candles with prices rising above the mean."""
    candles = []
    for i in range(n):
        price = 100.0 + i * 2.0
        candles.append(_make_candle(price + 1, price - 1, price, ts_offset=i))
    return candles


def _falling_candles(n: int = 25) -> list[OHLCVCandle]:
    """Generate candles with prices falling below the mean."""
    candles = []
    for i in range(n):
        price = 200.0 - i * 2.0
        candles.append(_make_candle(price + 1, price - 1, price, ts_offset=i))
    return candles


class TestCCIFromCandles:
    """Test CCICalculator.calculate_cci_from_candles static method."""

    def test_flat_market_cci_near_zero(self):
        """CCI should be near zero in a flat market."""
        candles = _flat_candles(20)
        cci = CCICalculator.calculate_cci_from_candles(candles, period=20)
        assert abs(cci) < 10, f"CCI should be near 0 in flat market, got {cci}"

    def test_rising_market_positive_cci(self):
        """CCI should be positive when price is above the mean."""
        candles = _rising_candles(25)
        cci = CCICalculator.calculate_cci_from_candles(candles, period=20)
        assert cci > 0, f"CCI should be positive in rising market, got {cci}"

    def test_falling_market_negative_cci(self):
        """CCI should be negative when price is below the mean."""
        candles = _falling_candles(25)
        cci = CCICalculator.calculate_cci_from_candles(candles, period=20)
        assert cci < 0, f"CCI should be negative in falling market, got {cci}"

    def test_strong_trend_extreme_cci(self):
        """Strong trends should produce CCI values beyond +/- 100."""
        candles = _rising_candles(30)
        cci = CCICalculator.calculate_cci_from_candles(candles, period=20)
        assert cci > 100, f"CCI should be > 100 in strong uptrend, got {cci}"

    def test_zero_mean_deviation_returns_zero(self):
        """When all typical prices are identical, CCI should be 0."""
        # All candles with same H/L/C
        candles = [_make_candle(101, 99, 100, ts_offset=i) for i in range(20)]
        cci = CCICalculator.calculate_cci_from_candles(candles, period=20)
        assert cci == 0.0

    def test_insufficient_data_raises(self):
        candles = _flat_candles(10)
        with pytest.raises(InsufficientDataError):
            CCICalculator.calculate_cci_from_candles(candles, period=20)

    def test_custom_period(self):
        candles = _rising_candles(25)
        cci_10 = CCICalculator.calculate_cci_from_candles(candles, period=10)
        cci_20 = CCICalculator.calculate_cci_from_candles(candles, period=20)
        # Both should produce valid positive values (rising market)
        assert cci_10 > 0
        assert cci_20 > 0

    def test_returns_float(self):
        candles = _rising_candles(25)
        cci = CCICalculator.calculate_cci_from_candles(candles, period=20)
        assert isinstance(cci, float)
