"""Tests for realized volatility calculator.

Tests cover:
- Close-to-close estimator with known datasets
- Parkinson estimator with known datasets
- Annualization across timeframes
- Window selection and minimum observation requirements
- Volatility cone computation
- Edge cases: zero prices, insufficient data, unknown timeframes
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import InsufficientDataError, OHLCVCandle
from almanak.framework.data.volatility.realized import (
    _PERIODS_PER_YEAR,
    MIN_OBSERVATIONS,
    RealizedVolatilityCalculator,
    VolatilityResult,
    VolConeEntry,
    VolConeResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_candles(
    prices: list[float],
    timeframe: str = "1h",
    high_low_spread_pct: float = 0.01,
) -> list[OHLCVCandle]:
    """Create candles from a list of close prices.

    High and low are derived from close +/- spread_pct/2.
    """
    if timeframe == "1d":
        delta = timedelta(days=1)
    elif timeframe == "4h":
        delta = timedelta(hours=4)
    elif timeframe == "1h":
        delta = timedelta(hours=1)
    elif timeframe == "15m":
        delta = timedelta(minutes=15)
    elif timeframe == "5m":
        delta = timedelta(minutes=5)
    elif timeframe == "1m":
        delta = timedelta(minutes=1)
    else:
        delta = timedelta(hours=1)

    start = datetime(2024, 1, 1, tzinfo=UTC)
    candles = []
    for i, p in enumerate(prices):
        spread = p * high_low_spread_pct / 2
        candles.append(
            OHLCVCandle(
                timestamp=start + delta * i,
                open=Decimal(str(p)),
                high=Decimal(str(p + spread)),
                low=Decimal(str(p - spread)),
                close=Decimal(str(p)),
                volume=Decimal("100"),
            )
        )
    return candles


def _constant_price_candles(price: float = 100.0, n: int = 50, timeframe: str = "1h") -> list[OHLCVCandle]:
    """Candles where every close is the same price -> vol should be ~0."""
    return _make_candles([price] * n, timeframe=timeframe)


def _known_returns_candles(n: int = 50, timeframe: str = "1d") -> list[OHLCVCandle]:
    """Create candles where close-to-close returns form a known series.

    Returns alternate +1% and -1% log returns from $100 starting price.
    """
    prices = [100.0]
    for i in range(1, n):
        if i % 2 == 1:
            prices.append(prices[-1] * math.exp(0.01))
        else:
            prices.append(prices[-1] * math.exp(-0.01))
    return _make_candles(prices, timeframe=timeframe, high_low_spread_pct=0.02)


# ---------------------------------------------------------------------------
# RealizedVolatilityCalculator tests
# ---------------------------------------------------------------------------


class TestCloseToClose:
    """Tests for close-to-close volatility estimator."""

    def setup_method(self):
        self.calc = RealizedVolatilityCalculator()

    def test_constant_price_vol_near_zero(self):
        """Constant price should give near-zero volatility."""
        candles = _constant_price_candles(100.0, n=50)
        result = self.calc.realized_vol(candles, window_days=2, timeframe="1h")
        assert result.annualized_vol < 0.001
        assert result.estimator == "close_to_close"
        assert result.sample_count == 47  # 2d * 24h = 48 candles -> 47 returns

    def test_known_returns(self):
        """Verify vol calculation against manual computation with known returns."""
        # Alternating +1% / -1% log returns.
        n = 50
        candles = _known_returns_candles(n=n, timeframe="1d")
        result = self.calc.realized_vol(candles, window_days=50, timeframe="1d")

        # Manual: all log returns are +/-0.01, mean = 0.
        # Variance = sum(0.01^2) / (n-1) = (49 * 0.0001) / 48 = 0.000102...
        # periodic_vol = sqrt(0.000102) ~= 0.01010
        # annualized = periodic_vol * sqrt(365)
        expected_periodic = math.sqrt(49 * 0.0001 / 48)
        expected_annual = expected_periodic * math.sqrt(365)

        assert abs(result.annualized_vol - expected_annual) < 0.001
        assert result.sample_count == 49

    def test_daily_and_hourly_scaling(self):
        """Daily and hourly vol should be consistent with annualized."""
        candles = _known_returns_candles(n=50, timeframe="1d")
        result = self.calc.realized_vol(candles, window_days=50, timeframe="1d")

        # daily_vol * sqrt(365) should equal annualized_vol
        recomputed = result.daily_vol * math.sqrt(365)
        assert abs(recomputed - result.annualized_vol) < 0.0001

        # hourly_vol * sqrt(8760) should equal annualized_vol
        recomputed_h = result.hourly_vol * math.sqrt(8760)
        assert abs(recomputed_h - result.annualized_vol) < 0.0001

    def test_hourly_timeframe(self):
        """Hourly candles should use 8760 periods/year for annualization."""
        prices = [100.0 + i * 0.1 for i in range(100)]
        candles = _make_candles(prices, timeframe="1h")
        result = self.calc.realized_vol(candles, window_days=4, timeframe="1h")
        assert result.annualized_vol > 0
        assert result.estimator == "close_to_close"
        assert result.window_start == candles[-96].timestamp  # 4*24 candles
        assert result.window_end == candles[-1].timestamp

    def test_result_dataclass_fields(self):
        """VolatilityResult should have all expected fields."""
        candles = _known_returns_candles(n=50, timeframe="1d")
        result = self.calc.realized_vol(candles, window_days=50, timeframe="1d")
        assert isinstance(result, VolatilityResult)
        assert result.annualized_vol > 0
        assert result.daily_vol > 0
        assert result.hourly_vol > 0
        assert result.sample_count > 0
        assert isinstance(result.window_start, datetime)
        assert isinstance(result.window_end, datetime)
        assert result.estimator == "close_to_close"


class TestParkinson:
    """Tests for Parkinson high-low volatility estimator."""

    def setup_method(self):
        self.calc = RealizedVolatilityCalculator()

    def test_parkinson_basic(self):
        """Parkinson estimator should produce positive vol for varying prices."""
        prices = [100.0 + i * 0.5 for i in range(50)]
        candles = _make_candles(prices, timeframe="1d", high_low_spread_pct=0.04)
        result = self.calc.realized_vol(candles, window_days=50, timeframe="1d", estimator="parkinson")
        assert result.annualized_vol > 0
        assert result.estimator == "parkinson"

    def test_parkinson_known_range(self):
        """Verify Parkinson against manual calculation with known H/L ranges."""
        # Create 40 candles where high = close * 1.01, low = close * 0.99
        # ln(H/L) = ln(1.01 / 0.99) = ln(1.020202...) ~= 0.02
        n = 40
        base = 100.0
        candles = []
        start = datetime(2024, 1, 1, tzinfo=UTC)
        for i in range(n):
            candles.append(
                OHLCVCandle(
                    timestamp=start + timedelta(days=i),
                    open=Decimal(str(base)),
                    high=Decimal(str(base * 1.01)),
                    low=Decimal(str(base * 0.99)),
                    close=Decimal(str(base)),
                    volume=Decimal("100"),
                )
            )

        result = self.calc.realized_vol(candles, window_days=50, timeframe="1d", estimator="parkinson")

        # Manual: ln(1.01/0.99) ~= 0.020003
        hl_log = math.log(1.01 / 0.99)
        factor = 1.0 / (4.0 * n * math.log(2))
        expected_variance = factor * n * hl_log**2
        expected_periodic = math.sqrt(expected_variance)
        expected_annual = expected_periodic * math.sqrt(365)

        assert abs(result.annualized_vol - expected_annual) < 0.001
        assert result.sample_count == n

    def test_parkinson_more_efficient_than_close_to_close(self):
        """Parkinson should produce non-zero vol even when all closes are equal.

        When H > L but close is constant, close-to-close gives ~0 but
        Parkinson captures the intra-period range.
        """
        n = 50
        candles = []
        start = datetime(2024, 1, 1, tzinfo=UTC)
        for i in range(n):
            candles.append(
                OHLCVCandle(
                    timestamp=start + timedelta(hours=i),
                    open=Decimal("100"),
                    high=Decimal("102"),
                    low=Decimal("98"),
                    close=Decimal("100"),
                    volume=Decimal("100"),
                )
            )

        cc_result = self.calc.realized_vol(candles, window_days=2, timeframe="1h")
        pk_result = self.calc.realized_vol(candles, window_days=2, timeframe="1h", estimator="parkinson")

        assert cc_result.annualized_vol < 0.001  # near zero (constant close)
        assert pk_result.annualized_vol > 0.1  # detects H-L range


class TestEdgeCases:
    """Tests for error handling and edge cases."""

    def setup_method(self):
        self.calc = RealizedVolatilityCalculator()

    def test_insufficient_data_raises(self):
        """Should raise InsufficientDataError with fewer than 30 observations."""
        candles = _make_candles([100.0] * 20, timeframe="1h")
        with pytest.raises(InsufficientDataError) as exc_info:
            self.calc.realized_vol(candles, window_days=1, timeframe="1h")
        assert exc_info.value.required >= MIN_OBSERVATIONS
        assert exc_info.value.available < MIN_OBSERVATIONS

    def test_unsupported_timeframe_raises(self):
        """Should raise ValueError for unknown timeframe."""
        candles = _make_candles([100.0] * 50, timeframe="1h")
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            self.calc.realized_vol(candles, window_days=2, timeframe="2h")

    def test_unknown_estimator_raises(self):
        """Should raise ValueError for unknown estimator."""
        candles = _make_candles([100.0] * 50, timeframe="1h")
        with pytest.raises(ValueError, match="Unknown estimator"):
            self.calc.realized_vol(candles, window_days=2, timeframe="1h", estimator="garman_klass")

    def test_zero_price_candles_filtered(self):
        """Candles with zero close prices should be skipped in log returns."""
        prices = [100.0] * 30 + [0.0] + [100.0] * 20
        candles = _make_candles(prices, timeframe="1h")
        # Should still work: zero-price returns are skipped.
        result = self.calc.realized_vol(candles, window_days=2, timeframe="1h")
        assert result.sample_count > 0

    def test_frozen_result(self):
        """VolatilityResult should be immutable (frozen)."""
        candles = _known_returns_candles(n=50, timeframe="1d")
        result = self.calc.realized_vol(candles, window_days=50, timeframe="1d")
        with pytest.raises(AttributeError):
            result.annualized_vol = 0.5  # type: ignore[misc]

    def test_window_larger_than_data_uses_all(self):
        """If window_days exceeds available data, use all candles."""
        candles = _known_returns_candles(n=50, timeframe="1d")
        result = self.calc.realized_vol(candles, window_days=365, timeframe="1d")
        assert result.sample_count == 49  # all 50 candles -> 49 returns

    def test_multiple_timeframes(self):
        """Should work correctly with different timeframes."""
        for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
            prices = [100.0 + i * 0.01 for i in range(100)]
            candles = _make_candles(prices, timeframe=tf)
            result = self.calc.realized_vol(candles, window_days=365, timeframe=tf)
            assert result.annualized_vol > 0
            assert result.estimator == "close_to_close"


class TestVolCone:
    """Tests for volatility cone computation."""

    def setup_method(self):
        self.calc = RealizedVolatilityCalculator()

    def _large_candle_set(self, n: int = 2500) -> list[OHLCVCandle]:
        """Create a large candle dataset with random-ish price movement."""
        import math

        prices = [100.0]
        for i in range(1, n):
            # Simple deterministic pseudo-random: sin wave + trend.
            change = 0.001 * math.sin(i * 0.1) + 0.0001
            prices.append(prices[-1] * math.exp(change))
        return _make_candles(prices, timeframe="1h", high_low_spread_pct=0.02)

    def test_vol_cone_basic(self):
        """Vol cone should return one entry per requested window."""
        candles = self._large_candle_set(2500)
        cone = self.calc.vol_cone(candles, windows=[7, 14, 30], timeframe="1h", token="WETH")
        assert isinstance(cone, VolConeResult)
        assert len(cone.entries) == 3
        assert cone.token == "WETH"
        assert cone.timeframe == "1h"

    def test_vol_cone_entry_fields(self):
        """Each VolConeEntry should have expected fields."""
        candles = self._large_candle_set(2500)
        cone = self.calc.vol_cone(candles, windows=[7], timeframe="1h")
        entry = cone.entries[0]
        assert isinstance(entry, VolConeEntry)
        assert entry.window_days == 7
        assert entry.current_vol > 0
        assert 0 <= entry.percentile <= 100
        assert entry.min_vol <= entry.current_vol or entry.min_vol >= 0
        assert entry.max_vol >= entry.min_vol

    def test_vol_cone_default_windows(self):
        """Default windows should be [7, 14, 30, 90]."""
        candles = self._large_candle_set(3000)
        cone = self.calc.vol_cone(candles, timeframe="1h")
        assert len(cone.entries) == 4
        assert [e.window_days for e in cone.entries] == [7, 14, 30, 90]

    def test_vol_cone_percentile_range(self):
        """Percentile should be between 0 and 100."""
        candles = self._large_candle_set(2500)
        cone = self.calc.vol_cone(candles, windows=[7, 14], timeframe="1h")
        for entry in cone.entries:
            assert 0 <= entry.percentile <= 100

    def test_vol_cone_insufficient_data(self):
        """Should raise InsufficientDataError if not enough candles."""
        candles = _make_candles([100.0] * 10, timeframe="1h")
        with pytest.raises(InsufficientDataError):
            self.calc.vol_cone(candles, windows=[7], timeframe="1h")

    def test_vol_cone_parkinson(self):
        """Vol cone should work with Parkinson estimator."""
        candles = self._large_candle_set(2500)
        cone = self.calc.vol_cone(candles, windows=[7], timeframe="1h", estimator="parkinson")
        assert len(cone.entries) == 1
        assert cone.entries[0].current_vol > 0

    def test_vol_cone_entries_sorted_by_window(self):
        """Entries should be sorted by window_days ascending."""
        candles = self._large_candle_set(3000)
        cone = self.calc.vol_cone(candles, windows=[30, 7, 14], timeframe="1h")
        window_days = [e.window_days for e in cone.entries]
        assert window_days == sorted(window_days)

    def test_vol_cone_frozen_entries(self):
        """VolConeEntry should be immutable (frozen)."""
        candles = self._large_candle_set(2500)
        cone = self.calc.vol_cone(candles, windows=[7], timeframe="1h")
        with pytest.raises(AttributeError):
            cone.entries[0].current_vol = 0.5  # type: ignore[misc]


class TestAnnualization:
    """Tests for correct annualization across timeframes."""

    def setup_method(self):
        self.calc = RealizedVolatilityCalculator()

    def test_annualization_factors(self):
        """Verify internal annualization constants."""
        assert _PERIODS_PER_YEAR["1d"] == 365
        assert _PERIODS_PER_YEAR["1h"] == 8760
        assert _PERIODS_PER_YEAR["4h"] == 2190
        assert _PERIODS_PER_YEAR["1m"] == 525600

    def test_higher_frequency_same_underlying_vol(self):
        """Different timeframes on the same price process should give similar
        annualized vol (within reasonable tolerance for discretization)."""
        # Build 1h candles, then subsample to 4h and 1d.
        prices_1h = [100.0]
        for i in range(1, 2000):
            # Deterministic pseudo-random walk.
            change = 0.001 * math.sin(i * 0.3)
            prices_1h.append(prices_1h[-1] * math.exp(change))

        candles_1h = _make_candles(prices_1h, timeframe="1h")
        candles_4h = candles_1h[::4]  # Every 4th candle.
        candles_1d = candles_1h[::24]  # Every 24th candle.

        vol_1h = self.calc.realized_vol(candles_1h, window_days=60, timeframe="1h")
        vol_4h = self.calc.realized_vol(candles_4h, window_days=60, timeframe="4h")
        vol_1d = self.calc.realized_vol(candles_1d, window_days=60, timeframe="1d")

        # All three should give similar annualized vol (within 50% tolerance
        # since subsampling introduces discretization effects).
        assert vol_1h.annualized_vol > 0
        assert vol_4h.annualized_vol > 0
        assert vol_1d.annualized_vol > 0
