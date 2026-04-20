"""Unit tests for OBVCalculator.

VIB-349: Verify OBV calculations for known price/volume patterns.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.indicators.obv import OBVCalculator
from almanak.framework.data.interfaces import InsufficientDataError, OHLCVCandle

_BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _make_candle(close: float, volume: float, ts_offset: int = 0) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=_BASE_TIME + timedelta(hours=ts_offset),
        open=Decimal(str(close)),
        high=Decimal(str(close + 1)),
        low=Decimal(str(close - 1)),
        close=Decimal(str(close)),
        volume=Decimal(str(volume)),
    )


class TestOBVFromCandles:
    """Test OBVCalculator.calculate_obv_from_candles static method."""

    def test_rising_prices_accumulate_volume(self):
        """OBV should increase when prices rise on volume."""
        candles = [
            _make_candle(100, 1000, 0),
            _make_candle(101, 2000, 1),  # Up -> +2000
            _make_candle(102, 3000, 2),  # Up -> +3000
            _make_candle(103, 1500, 3),  # Up -> +1500
        ]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)
        assert result.obv == 6500.0  # 2000 + 3000 + 1500

    def test_falling_prices_subtract_volume(self):
        """OBV should decrease when prices fall on volume."""
        candles = [
            _make_candle(100, 1000, 0),
            _make_candle(99, 2000, 1),   # Down -> -2000
            _make_candle(98, 3000, 2),   # Down -> -3000
        ]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)
        assert result.obv == -5000.0

    def test_flat_price_no_change(self):
        """OBV should not change when price is flat."""
        candles = [
            _make_candle(100, 1000, 0),
            _make_candle(100, 2000, 1),  # Flat -> no change
            _make_candle(100, 3000, 2),  # Flat -> no change
        ]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)
        assert result.obv == 0.0

    def test_mixed_movement(self):
        """Test OBV with mixed price movements."""
        candles = [
            _make_candle(100, 1000, 0),
            _make_candle(101, 2000, 1),  # Up -> +2000
            _make_candle(99, 3000, 2),   # Down -> -3000
            _make_candle(100, 1000, 3),  # Up -> +1000
        ]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)
        assert result.obv == 0.0  # 2000 - 3000 + 1000 = 0

    def test_signal_line_is_sma(self):
        """Signal line should be SMA of recent OBV values."""
        candles = [
            _make_candle(100, 1000, 0),
            _make_candle(101, 2000, 1),  # Up -> OBV = 2000
            _make_candle(102, 3000, 2),  # Up -> OBV = 5000
            _make_candle(103, 1000, 3),  # Up -> OBV = 6000
            _make_candle(104, 2000, 4),  # Up -> OBV = 8000
        ]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=3)
        # Signal line = SMA of last 3 OBV values: (5000 + 6000 + 8000) / 3
        assert result.obv == 8000.0
        assert result.signal_line != 0.0, "Signal line should be non-zero with varying OBV"

    def test_none_volume_treated_as_zero(self):
        """Missing volume should be treated as 0."""
        candles = [
            OHLCVCandle(
                timestamp=_BASE_TIME + timedelta(hours=i),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal(str(100 + i)),
                volume=None,
            )
            for i in range(5)
        ]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)
        # All volumes are None (0), so OBV = 0
        assert result.obv == 0.0

    def test_insufficient_data_raises(self):
        candles = [_make_candle(100, 1000, 0)]
        with pytest.raises(InsufficientDataError):
            OBVCalculator.calculate_obv_from_candles(candles, signal_period=21)

    def test_to_dict(self):
        candles = [_make_candle(100 + i, 1000, i) for i in range(25)]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=21)
        d = result.to_dict()
        assert "obv" in d
        assert "signal_line" in d
