"""Realized volatility estimators and volatility cone analysis.

Provides close-to-close (log-return standard deviation) and Parkinson
(high-low range) estimators with proper annualization. All calculations
are pure math on OHLCV data -- no external API calls needed.

Example:
    from almanak.framework.data.volatility.realized import (
        RealizedVolatilityCalculator,
    )

    calc = RealizedVolatilityCalculator()
    result = calc.realized_vol(candles, window_days=30, timeframe="1h")
    print(result.annualized_vol)  # e.g. 0.65 (65%)

    cone = calc.vol_cone(candles_90d, windows=[7, 14, 30, 90], timeframe="1h")
    for entry in cone.entries:
        print(f"{entry.window_days}d: {entry.current_vol:.2%} (p{entry.percentile:.0f})")
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime

from almanak.framework.data.interfaces import InsufficientDataError, OHLCVCandle

logger = logging.getLogger(__name__)

# Minimum observations required for valid volatility estimation.
MIN_OBSERVATIONS = 30

# Annualization factors: periods per year for each timeframe.
_PERIODS_PER_YEAR: dict[str, int] = {
    "1m": 525_600,  # 365 * 24 * 60
    "5m": 105_120,  # 365 * 24 * 12
    "15m": 35_040,  # 365 * 24 * 4
    "1h": 8_760,  # 365 * 24
    "4h": 2_190,  # 365 * 6
    "1d": 365,
}

# Hours per candle for each timeframe (used for window_days -> candle count).
_HOURS_PER_CANDLE: dict[str, float] = {
    "1m": 1 / 60,
    "5m": 5 / 60,
    "15m": 0.25,
    "1h": 1.0,
    "4h": 4.0,
    "1d": 24.0,
}


@dataclass(frozen=True)
class VolatilityResult:
    """Realized volatility calculation result.

    All vol figures are expressed as decimals (e.g. 0.65 = 65%).

    Attributes:
        annualized_vol: Annualized volatility (sqrt(periods_per_year) * periodic_vol).
        daily_vol: Per-day volatility.
        hourly_vol: Per-hour volatility.
        sample_count: Number of observations (return periods) used.
        window_start: Timestamp of the earliest candle in the window.
        window_end: Timestamp of the latest candle in the window.
        estimator: Estimator name ("close_to_close" or "parkinson").
    """

    annualized_vol: float
    daily_vol: float
    hourly_vol: float
    sample_count: int
    window_start: datetime
    window_end: datetime
    estimator: str


@dataclass(frozen=True)
class VolConeEntry:
    """Single entry in a volatility cone.

    Attributes:
        window_days: Lookback window in days.
        current_vol: Current realized vol for this window (annualized).
        percentile: Where current vol sits in the historical distribution (0-100).
        min_vol: Minimum historical vol observed at this window length.
        max_vol: Maximum historical vol observed at this window length.
        median_vol: Median historical vol at this window length.
    """

    window_days: int
    current_vol: float
    percentile: float
    min_vol: float
    max_vol: float
    median_vol: float


@dataclass(frozen=True)
class VolConeResult:
    """Volatility cone: current vol vs historical distribution at multiple windows.

    Attributes:
        entries: List of VolConeEntry for each requested window.
        token: Token symbol used.
        timeframe: Candle timeframe used.
    """

    entries: list[VolConeEntry]
    token: str
    timeframe: str


class RealizedVolatilityCalculator:
    """Computes realized volatility from OHLCV candle data.

    Supports two estimators:
    - **close_to_close**: Standard deviation of log returns. Simple and
      widely used, but only uses closing prices.
    - **parkinson**: High-low range estimator. More efficient (lower variance)
      for the same sample size because it uses intra-period range information.

    All volatilities are annualized using sqrt(periods_per_year).
    """

    def realized_vol(
        self,
        candles: list[OHLCVCandle],
        window_days: int = 30,
        timeframe: str = "1h",
        estimator: str = "close_to_close",
    ) -> VolatilityResult:
        """Calculate realized volatility over a lookback window.

        Args:
            candles: OHLCV candles sorted ascending by timestamp. Should cover
                at least ``window_days`` of data at the given ``timeframe``.
            window_days: Lookback window in calendar days.
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d).
            estimator: "close_to_close" (default) or "parkinson".

        Returns:
            VolatilityResult with annualized, daily, and hourly vol.

        Raises:
            InsufficientDataError: If fewer than 30 observations in the window.
            ValueError: If timeframe is unsupported or estimator unknown.
        """
        if timeframe not in _PERIODS_PER_YEAR:
            raise ValueError(f"Unsupported timeframe '{timeframe}'. Supported: {sorted(_PERIODS_PER_YEAR.keys())}")
        if estimator not in ("close_to_close", "parkinson"):
            raise ValueError(f"Unknown estimator '{estimator}'. Use 'close_to_close' or 'parkinson'.")

        # Select candles within the lookback window.
        window_candles = self._select_window(candles, window_days, timeframe)

        if estimator == "close_to_close":
            periodic_vol, sample_count = self._close_to_close_vol(window_candles)
        else:
            periodic_vol, sample_count = self._parkinson_vol(window_candles)

        periods_per_year = _PERIODS_PER_YEAR[timeframe]
        annualized_vol = periodic_vol * math.sqrt(periods_per_year)

        # Scale to daily and hourly.
        periods_per_day = periods_per_year / 365.0
        periods_per_hour = periods_per_year / 8760.0
        daily_vol = periodic_vol * math.sqrt(periods_per_day)
        hourly_vol = periodic_vol * math.sqrt(periods_per_hour)

        return VolatilityResult(
            annualized_vol=annualized_vol,
            daily_vol=daily_vol,
            hourly_vol=hourly_vol,
            sample_count=sample_count,
            window_start=window_candles[0].timestamp,
            window_end=window_candles[-1].timestamp,
            estimator=estimator,
        )

    def vol_cone(
        self,
        candles: list[OHLCVCandle],
        windows: list[int] | None = None,
        timeframe: str = "1h",
        estimator: str = "close_to_close",
        token: str = "",
    ) -> VolConeResult:
        """Compute volatility cone: current vol vs historical percentile.

        For each window length, calculates the current realized vol and
        compares it to the distribution of rolling volatilities computed
        over the full candle history.

        Args:
            candles: Full OHLCV history sorted ascending. Should be
                significantly longer than the largest window for meaningful
                percentile estimation.
            windows: Lookback windows in days. Default [7, 14, 30, 90].
            timeframe: Candle timeframe.
            estimator: "close_to_close" or "parkinson".
            token: Token symbol for labeling.

        Returns:
            VolConeResult with one VolConeEntry per window.

        Raises:
            InsufficientDataError: If not enough data for the smallest window.
            ValueError: If timeframe is unsupported.
        """
        if windows is None:
            windows = [7, 14, 30, 90]

        if timeframe not in _PERIODS_PER_YEAR:
            raise ValueError(f"Unsupported timeframe '{timeframe}'. Supported: {sorted(_PERIODS_PER_YEAR.keys())}")

        entries: list[VolConeEntry] = []

        for window_days in sorted(windows):
            candles_per_window = self._window_to_candles(window_days, timeframe)

            # Need at least MIN_OBSERVATIONS for the current window.
            if len(candles) < max(candles_per_window, MIN_OBSERVATIONS):
                raise InsufficientDataError(
                    required=max(candles_per_window, MIN_OBSERVATIONS),
                    available=len(candles),
                    indicator=f"vol_cone({window_days}d)",
                )

            # Current vol: last `candles_per_window` candles.
            current_window = candles[-candles_per_window:]
            if estimator == "close_to_close":
                current_periodic, _ = self._close_to_close_vol(current_window)
            else:
                current_periodic, _ = self._parkinson_vol(current_window)

            periods_per_year = _PERIODS_PER_YEAR[timeframe]
            current_annualized = current_periodic * math.sqrt(periods_per_year)

            # Rolling historical vols across the full history.
            rolling_vols = self._rolling_annualized_vols(candles, candles_per_window, timeframe, estimator)

            if not rolling_vols:
                # Only one window fits the data -- no percentile possible.
                entries.append(
                    VolConeEntry(
                        window_days=window_days,
                        current_vol=current_annualized,
                        percentile=50.0,
                        min_vol=current_annualized,
                        max_vol=current_annualized,
                        median_vol=current_annualized,
                    )
                )
                continue

            sorted_vols = sorted(rolling_vols)
            percentile = self._percentile_rank(sorted_vols, current_annualized)
            median_vol = sorted_vols[len(sorted_vols) // 2]

            entries.append(
                VolConeEntry(
                    window_days=window_days,
                    current_vol=current_annualized,
                    percentile=percentile,
                    min_vol=sorted_vols[0],
                    max_vol=sorted_vols[-1],
                    median_vol=median_vol,
                )
            )

        return VolConeResult(entries=entries, token=token, timeframe=timeframe)

    # -----------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------

    def _select_window(
        self,
        candles: list[OHLCVCandle],
        window_days: int,
        timeframe: str,
    ) -> list[OHLCVCandle]:
        """Select the most recent ``window_days`` of candles."""
        candles_needed = self._window_to_candles(window_days, timeframe)
        # Use the last N candles (or all if fewer available).
        window = candles[-candles_needed:] if len(candles) > candles_needed else list(candles)

        if len(window) < MIN_OBSERVATIONS:
            raise InsufficientDataError(
                required=MIN_OBSERVATIONS,
                available=len(window),
                indicator=f"realized_vol({window_days}d, {timeframe})",
            )

        return window

    def _window_to_candles(self, window_days: int, timeframe: str) -> int:
        """Convert a window in days to an approximate number of candles."""
        hours_per_candle = _HOURS_PER_CANDLE[timeframe]
        total_hours = window_days * 24
        return max(int(total_hours / hours_per_candle), MIN_OBSERVATIONS)

    def _close_to_close_vol(self, candles: list[OHLCVCandle]) -> tuple[float, int]:
        """Compute per-period volatility using close-to-close log returns.

        Returns:
            (periodic_vol, sample_count) where sample_count = len(log_returns).
        """
        log_returns: list[float] = []
        for i in range(1, len(candles)):
            prev_close = float(candles[i - 1].close)
            curr_close = float(candles[i].close)
            if prev_close > 0 and curr_close > 0:
                log_returns.append(math.log(curr_close / prev_close))

        if len(log_returns) < MIN_OBSERVATIONS:
            raise InsufficientDataError(
                required=MIN_OBSERVATIONS,
                available=len(log_returns),
                indicator="close_to_close_vol",
            )

        n = len(log_returns)
        mean = sum(log_returns) / n
        variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
        periodic_vol = math.sqrt(variance)

        return periodic_vol, n

    def _parkinson_vol(self, candles: list[OHLCVCandle]) -> tuple[float, int]:
        """Compute per-period volatility using Parkinson high-low estimator.

        Parkinson (1980): sigma^2 = (1 / 4*n*ln(2)) * sum(ln(H/L)^2)

        More efficient than close-to-close because it uses range information.

        Returns:
            (periodic_vol, sample_count) where sample_count = number of candles used.
        """
        hl_log_sq: list[float] = []
        for candle in candles:
            high = float(candle.high)
            low = float(candle.low)
            if high > 0 and low > 0 and high >= low:
                hl_log_sq.append(math.log(high / low) ** 2)

        if len(hl_log_sq) < MIN_OBSERVATIONS:
            raise InsufficientDataError(
                required=MIN_OBSERVATIONS,
                available=len(hl_log_sq),
                indicator="parkinson_vol",
            )

        n = len(hl_log_sq)
        factor = 1.0 / (4.0 * n * math.log(2))
        variance = factor * sum(hl_log_sq)
        periodic_vol = math.sqrt(variance)

        return periodic_vol, n

    def _rolling_annualized_vols(
        self,
        candles: list[OHLCVCandle],
        window_size: int,
        timeframe: str,
        estimator: str,
    ) -> list[float]:
        """Compute rolling annualized vols across the full candle history.

        Steps through the candles with step = window_size (non-overlapping) to
        build a distribution of historical realized vols.
        """
        periods_per_year = _PERIODS_PER_YEAR[timeframe]
        vols: list[float] = []

        # Use overlapping windows with step = window_size // 4 for denser distribution.
        step = max(window_size // 4, 1)
        for start in range(0, len(candles) - window_size + 1, step):
            window = candles[start : start + window_size]
            try:
                if estimator == "close_to_close":
                    periodic_vol, _ = self._close_to_close_vol(window)
                else:
                    periodic_vol, _ = self._parkinson_vol(window)
                vols.append(periodic_vol * math.sqrt(periods_per_year))
            except InsufficientDataError:
                continue

        return vols

    @staticmethod
    def _percentile_rank(sorted_values: list[float], value: float) -> float:
        """Compute percentile rank of ``value`` within sorted list (0-100)."""
        n = len(sorted_values)
        if n == 0:
            return 50.0
        count_below = sum(1 for v in sorted_values if v < value)
        count_equal = sum(1 for v in sorted_values if v == value)
        # Interpolated percentile rank.
        rank = (count_below + 0.5 * count_equal) / n * 100.0
        return min(max(rank, 0.0), 100.0)
