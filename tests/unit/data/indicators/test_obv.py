"""Unit tests for OBVCalculator.

VIB-349: Verify OBV calculations for known price/volume patterns.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.indicators.obv import OBVCalculator
from almanak.framework.data.interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    VolumeUnavailableError,
)

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

    def test_unmeasured_volume_refuses_rather_than_substituting_zero(self):
        """ALM-3148 §8.2: `volume is None` must refuse, never become 0.0.

        This replaces ``test_none_volume_treated_as_zero``, which asserted the
        defect: it pinned ``obv == 0.0`` for a series that measured no volume at
        all, and a passing suite therefore read as evidence the behaviour was
        intended. ``0.0`` is not "no volume was measured" — it is the
        structurally valid claim "volume was measured and there was none", i.e.
        no accumulation pressure. A strategy cannot tell the two apart from the
        return value, which is the whole reason `Empty != Zero` is a rule here.

        Latent while only volume-less sources could reach it; the venue-native
        default makes it reachable for every perp strategy, so it is fixed in
        the same change that makes it reachable.
        """
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
        with pytest.raises(VolumeUnavailableError) as excinfo:
            OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)

        # The refusal has to be actionable: name the indicator and the way out.
        message = str(excinfo.value)
        assert "OBV" in message
        assert "ohlcv_source" in message
        assert excinfo.value.observed == 4

    def test_the_refusal_is_catchable_as_the_documented_valueerror(self):
        """Every indicator on `MarketSnapshot` documents `Raises: ValueError` for
        "not available", and strategies guard on exactly that
        (`strategies/internal/tests/obv_divergence/strategy.py`). A refusal that
        is not a `ValueError` escapes those handlers and raises out of
        `decide()` -- a harder failure than the fabricated 0.0 it replaced, and
        one the strategist has no documented way to catch.
        """
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
        # Caught as ValueError, not as VolumeUnavailableError: this asserts the
        # contract a strategy actually writes, not the class we happen to raise.
        with pytest.raises(ValueError) as excinfo:
            OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)
        assert isinstance(excinfo.value, VolumeUnavailableError)

    def test_partially_unmeasured_volume_also_refuses(self):
        """One unmeasured candle poisons the accumulation, so it refuses too.

        OBV is a running sum: a single substituted zero silently omits that
        bar's contribution from every subsequent value. There is no partial
        answer to give.
        """
        candles = [_make_candle(100 + i, 1000, i) for i in range(5)]
        candles[3] = OHLCVCandle(
            timestamp=candles[3].timestamp,
            open=candles[3].open,
            high=candles[3].high,
            low=candles[3].low,
            close=candles[3].close,
            volume=None,
        )
        with pytest.raises(VolumeUnavailableError):
            OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)

    def test_measured_zero_volume_is_still_computed(self):
        """A measured zero is real data and must NOT be refused.

        The liveness control for the guard above: if the guard fired on
        ``Decimal("0")`` as well, it would be rejecting the very distinction it
        exists to preserve, and every quiet-market series would fail.
        """
        candles = [
            OHLCVCandle(
                timestamp=_BASE_TIME + timedelta(hours=i),
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal(str(100 + i)),
                volume=Decimal("0"),
            )
            for i in range(5)
        ]
        result = OBVCalculator.calculate_obv_from_candles(candles, signal_period=2)
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
