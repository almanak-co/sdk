"""Indicator Engine for PnL Backtester.

Computes technical indicators from rolling price history and populates
them on MarketSnapshot so that strategies using market.rsi(), market.macd(),
market.bollinger_bands(), and market.atr() work identically in live and
backtest modes.

The engine uses existing static calculator methods from the indicator modules,
avoiding any code duplication. It maintains a rolling price buffer per token
and computes only the indicators declared by the strategy (or defaults).

Usage:
    from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine

    engine = BacktestIndicatorEngine(required_indicators=["rsi", "macd", "bollinger_bands", "atr"])

    # Each tick: append price, then populate snapshot
    engine.append_price("WETH", Decimal("3500.00"))
    engine.populate_snapshot(snapshot, strategy_config)
"""

import logging
import re
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from almanak.framework.data.indicators.adx import ADXCalculator
from almanak.framework.data.indicators.atr import ATRCalculator
from almanak.framework.data.indicators.bollinger_bands import BollingerBandsCalculator
from almanak.framework.data.indicators.cci import CCICalculator
from almanak.framework.data.indicators.ichimoku import IchimokuCalculator
from almanak.framework.data.indicators.macd import MACDCalculator
from almanak.framework.data.indicators.moving_averages import MovingAverageCalculator
from almanak.framework.data.indicators.rsi import RSICalculator
from almanak.framework.data.indicators.stochastic import StochasticCalculator
from almanak.framework.data.interfaces import InsufficientDataError, OHLCVCandle
from almanak.framework.market import (
    ADXData,
    ATRData,
    BollingerBandsData,
    CCIData,
    IchimokuData,
    MACDData,
    MAData,
    MarketSnapshot,
    RSIData,
    StochasticData,
)
from almanak.framework.market.models import IndicatorProvider

logger = logging.getLogger(__name__)

# Default indicators PRE-POPULATED each tick when the strategy doesn't declare
# required_indicators — the legacy fast path, kept at its original size so the
# hold-only perf gate holds. The ALM-2951 additions (sma/stochastic/adx/cci/
# ichimoku) are served lazily by the on-demand snapshot providers instead:
# computed only when a strategy actually reads them. Declaring them in
# required_indicators opts into eager pre-population. OBV is deliberately
# absent everywhere: it needs volume history the close-only series lacks.
DEFAULT_INDICATORS = frozenset({"rsi", "macd", "bollinger_bands", "atr", "ema"})

# Supported indicator names for validation (pre-population opt-in set)
SUPPORTED_INDICATORS = frozenset(
    {"rsi", "macd", "bollinger_bands", "atr", "ema", "sma", "stochastic", "adx", "cci", "ichimoku"}
)

# interval_seconds -> canonical timeframe label for pre-populated indicators.
_TIMEFRAME_LABELS = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h", 14400: "4h", 86400: "1d"}


def timeframe_label(interval_seconds: int) -> str:
    """Canonical timeframe label for a tick interval (fallback: ``"{n}s"``)."""
    return _TIMEFRAME_LABELS.get(int(interval_seconds), f"{int(interval_seconds)}s")


_SECONDS_BY_LABEL = {label: seconds for seconds, label in _TIMEFRAME_LABELS.items()}


_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_TIMEFRAME_RE = re.compile(r"^(\d+)([smhd])$")


def _timeframe_seconds(timeframe: str) -> int:
    """Seconds for any ``<n><unit>`` timeframe (e.g. "2h", "45m", "3d")."""
    label = timeframe.strip().lower()
    if label in _SECONDS_BY_LABEL:
        return _SECONDS_BY_LABEL[label]
    match = _TIMEFRAME_RE.match(label)
    if match:
        return int(match.group(1)) * _UNIT_SECONDS[match.group(2)]
    raise ValueError(f"unknown timeframe {timeframe!r}")


# Maximum price history to keep per token (covers all standard indicator periods)
DEFAULT_MAX_HISTORY = 200


class BacktestIndicatorEngine:
    """Computes indicators from rolling price history for the PnL backtester.

    This engine bridges the gap between the backtester (which only has close prices)
    and MarketSnapshot's indicator methods (which expect pre-populated data).

    Supports: RSI, MACD, Bollinger Bands, ATR (close-only approximation).

    Attributes:
        required_indicators: Set of indicator names to compute each tick.
        max_history: Maximum number of prices to retain per token.
    """

    def __init__(
        self,
        required_indicators: set[str] | frozenset[str] | None = None,
        max_history: int = DEFAULT_MAX_HISTORY,
    ) -> None:
        if required_indicators is None:
            self._required = DEFAULT_INDICATORS
        else:
            self._required = frozenset(required_indicators)
        if max_history < 1:
            raise ValueError("max_history must be >= 1")
        self._max_history = max_history
        # Constructed capacity: the base the granularity retention scale
        # multiplies (ALM-2957 review round) — kept so rescaling is idempotent.
        self._base_max_history = max_history
        # token -> rolling deque of close prices (oldest first)
        self._price_buffers: dict[str, deque[Decimal]] = {}
        # Measured resolution of the UNDERLYING price data (ALM-2957). When
        # coarser than the tick interval (e.g. daily CG data under hourly
        # ticks), the tick buffer is an upsampled flat-within-period plane:
        # indicators at the tick timeframe are degenerate (RSI pins ~0/100)
        # and must refuse, not serve. None = data matches the tick grid.
        self._data_granularity_seconds: int | None = None
        self._tick_interval_seconds: int | None = None

        unknown = self._required - SUPPORTED_INDICATORS
        if unknown:
            logger.warning(
                "BacktestIndicatorEngine: unknown indicators requested (will be skipped): %s. Supported: %s",
                ", ".join(sorted(unknown)),
                ", ".join(sorted(SUPPORTED_INDICATORS)),
            )

    def append_price(self, token: str, price: Decimal) -> None:
        """Append a close price to the rolling buffer for a token."""
        if token not in self._price_buffers:
            self._price_buffers[token] = deque(maxlen=self._max_history)
        self._price_buffers[token].append(price)

    def get_buffer_size(self, token: str) -> int:
        """Return number of prices buffered for a token."""
        return len(self._price_buffers.get(token, []))

    def populate_snapshot(
        self,
        snapshot: MarketSnapshot,
        config: dict | None = None,
        active_tokens: set[str] | None = None,
        timeframe: str | None = None,
    ) -> None:
        """Compute indicators and set them on the snapshot.

        Args:
            snapshot: The MarketSnapshot to populate with indicator data.
            config: Optional strategy config dict used to read indicator parameters
                    (e.g., rsi_period, macd_fast, bb_period, atr_period).
                    Falls back to standard defaults.
            active_tokens: If provided, only compute indicators for these tokens.
                    Prevents stale indicators from being set for tokens missing from
                    the current tick's market data.
        """
        config = config or {}

        if self._degenerate_at_tick():
            # The tick buffer is upsampled coarser data (ALM-2957): eager
            # tick-timeframe values would be confidently degenerate. Skip —
            # reads route to the on-demand providers, which refuse finer-
            # than-data timeframes and get RECORDED in the decision-input
            # ledger, instead of silently serving saturated values.
            return

        for token, prices in self._price_buffers.items():
            # Skip tokens not present in the current tick to avoid stale indicators
            if active_tokens is not None and token not in active_tokens:
                continue

            price_list = list(prices)

            if "rsi" in self._required:
                self._populate_rsi(snapshot, token, price_list, config, timeframe)

            if "macd" in self._required:
                self._populate_macd(snapshot, token, price_list, config, timeframe)

            if "bollinger_bands" in self._required:
                self._populate_bollinger(snapshot, token, price_list, config, timeframe)

            if "atr" in self._required:
                self._populate_atr(snapshot, token, price_list, config, timeframe)

            if "ema" in self._required:
                self._populate_ema(snapshot, token, price_list, config, timeframe)

            if "sma" in self._required:
                self._populate_sma(snapshot, token, price_list, config, timeframe)

            # Candle-shape indicators share ONE candle list per (token, tick),
            # trimmed to the largest window any of them needs — building
            # per-indicator over the full 200-price buffer regressed the
            # 1-year perf gate (ALM-2951 review).
            candle_indicators = self._required & {"stochastic", "adx", "cci", "ichimoku"}
            if candle_indicators:
                window = self._candle_window(config, candle_indicators)
                candles = self._close_candles(price_list[-window:])

                if "stochastic" in candle_indicators:
                    self._populate_stochastic(snapshot, token, candles, config, timeframe)

                if "adx" in candle_indicators:
                    self._populate_adx(snapshot, token, candles, config, timeframe)

                if "cci" in candle_indicators:
                    self._populate_cci(snapshot, token, candles, config, timeframe)

                if "ichimoku" in candle_indicators:
                    self._populate_ichimoku(snapshot, token, candles, config, timeframe)

    def _populate_rsi(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute RSI and set on snapshot. Silently skips if insufficient data."""
        period = int(config.get("rsi_period", 14))
        try:
            rsi_value = RSICalculator.calculate_rsi_from_prices(prices, period)
            snapshot.set_rsi(
                token,
                RSIData(
                    value=Decimal(str(round(rsi_value, 4))),
                    period=period,
                ),
                timeframe=timeframe,
            )
        except InsufficientDataError:
            pass  # Not enough data yet -- indicator will raise ValueError if strategy calls it

    def _populate_macd(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute MACD and set on snapshot. Silently skips if insufficient data."""
        fast = int(config.get("macd_fast", 12))
        slow = int(config.get("macd_slow", 26))
        signal = int(config.get("macd_signal", 9))
        try:
            result = MACDCalculator.calculate_macd_from_prices(prices, fast, slow, signal)
            snapshot.set_macd(
                token,
                MACDData(
                    macd_line=Decimal(str(round(result.macd_line, 6))),
                    signal_line=Decimal(str(round(result.signal_line, 6))),
                    histogram=Decimal(str(round(result.histogram, 6))),
                    fast_period=fast,
                    slow_period=slow,
                    signal_period=signal,
                ),
                timeframe=timeframe,
            )
        except InsufficientDataError:
            pass

    def _populate_bollinger(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute Bollinger Bands and set on snapshot. Silently skips if insufficient data."""
        period = int(config.get("bb_period", 20))
        std_dev = float(config.get("bb_std_dev", 2.0))
        try:
            result = BollingerBandsCalculator.calculate_bollinger_from_prices(prices, period, std_dev)
            snapshot.set_bollinger_bands(
                token,
                BollingerBandsData(
                    upper_band=Decimal(str(round(result.upper_band, 6))),
                    middle_band=Decimal(str(round(result.middle_band, 6))),
                    lower_band=Decimal(str(round(result.lower_band, 6))),
                    bandwidth=Decimal(str(round(result.bandwidth, 6))),
                    percent_b=Decimal(str(round(result.percent_b, 6))),
                    period=period,
                    std_dev=std_dev,
                ),
                timeframe=timeframe,
            )
        except InsufficientDataError:
            pass

    def _populate_atr(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute ATR from close prices and set on snapshot.

        Uses close-only ATR approximation (TR ≈ |close[i] - close[i-1]|) since
        the backtester only has close prices. This is appropriate for backtesting
        and Monte Carlo simulation where OHLCV data is unavailable.

        Silently skips if insufficient data.
        """
        period = int(config.get("atr_period", 14))
        if period < 1:
            logger.warning("BacktestIndicatorEngine: atr_period must be >= 1, skipping ATR population")
            return
        try:
            atr_value = ATRCalculator.calculate_atr_from_prices(prices, period)
            # Compute ATR as percentage of current price for value_percent
            current_price = float(prices[-1])
            atr_pct = (atr_value / current_price * 100) if current_price > 0 else 0.0
            snapshot.set_atr(
                token,
                ATRData(
                    value=Decimal(str(round(atr_value, 6))),
                    value_percent=Decimal(str(round(atr_pct, 4))),
                    period=period,
                ),
                timeframe=timeframe,
            )
        except InsufficientDataError:
            pass

    @staticmethod
    def _ema_periods_from_config(config: dict) -> list[int]:
        """Collect the EMA periods a strategy needs from its config.

        ta_swap-style strategies declare ``ema_fast_period`` / ``ema_slow_period``;
        a single-EMA strategy may use ``ema_period``; ``ema_periods`` accepts an
        explicit list. Deduped, positive-only, order-stable. An EMA query for a
        period not pre-populated here still raises (honest miss), matching the
        other indicators.
        """
        periods: list[int] = []
        for key in ("ema_period", "ema_fast_period", "ema_slow_period"):
            value = config.get(key)
            if value is not None:
                periods.append(int(value))
        extra = config.get("ema_periods")
        if isinstance(extra, list | tuple):
            periods.extend(int(p) for p in extra)
        if not periods:
            # No EMA period declared: populate EMA(12) so a bare
            # ``market.ema(token)`` (snapshot default period=12) resolves.
            periods.append(12)
        seen: set[int] = set()
        unique: list[int] = []
        for period in periods:
            if period >= 1 and period not in seen:
                seen.add(period)
                unique.append(period)
        return unique

    def _populate_ema(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute EMA(s) and set on snapshot for each configured period.

        The backtest snapshot has no live indicator provider, so a strategy
        calling ``market.ema(token, period=N)`` only resolves if EMA(N) was
        pre-populated here. Periods come from the strategy config
        (:meth:`_ema_periods_from_config`). Silently skips a period with
        insufficient history (the strategy's read then raises, same as RSI/BB).
        """
        current_price = prices[-1] if prices else Decimal("0")
        for period in self._ema_periods_from_config(config):
            try:
                ema_value = MovingAverageCalculator.calculate_ema_from_prices(prices, period)
            except InsufficientDataError:
                continue
            snapshot.set_ma(
                token,
                MAData(
                    value=Decimal(str(round(ema_value, 6))),
                    ma_type="EMA",
                    period=period,
                    current_price=current_price,
                ),
                ma_type="EMA",
                period=period,
                timeframe=timeframe,
            )

    @staticmethod
    def _candle_window(config: dict, candle_indicators: set[str] | frozenset[str]) -> int:
        """Largest lookback any enabled candle-shape indicator needs (+1 slack)."""
        window = 1
        if "stochastic" in candle_indicators:
            window = max(window, int(config.get("stochastic_k_period", 14)) + int(config.get("stochastic_d_period", 3)))
        if "adx" in candle_indicators:
            window = max(window, int(config.get("adx_period", 14)) * 2 + 1)
        if "cci" in candle_indicators:
            window = max(window, int(config.get("cci_period", 20)) + 1)
        if "ichimoku" in candle_indicators:
            window = max(
                window,
                int(config.get("ichimoku_tenkan_period", 9)),
                int(config.get("ichimoku_kijun_period", 26)),
                int(config.get("ichimoku_senkou_b_period", 52)) + 1,
            )
        return window

    @staticmethod
    def _close_candles(prices: list[Decimal]) -> list[OHLCVCandle]:
        """Close-only candles (o=h=l=c, no volume) for candle-based calculators.

        The backtest series carries closes only, so candle-shape indicators
        (stochastic/ADX/CCI/ichimoku) run as documented close-derived
        approximations — same convention as the close-only ATR.
        """
        # Monotonic synthetic timestamps: the calculators read values, not
        # times, but identical timestamps would violate the candle contract.
        base = datetime(2000, 1, 1, tzinfo=UTC)
        return [
            OHLCVCandle(
                timestamp=base + timedelta(seconds=i), open=price, high=price, low=price, close=price, volume=None
            )
            for i, price in enumerate(prices)
        ]

    def _populate_sma(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute SMA and set on snapshot. Silently skips if insufficient data."""
        period = int(config.get("sma_period", 20))
        try:
            sma_value = MovingAverageCalculator.calculate_sma_from_prices(prices, period)
        except InsufficientDataError:
            return
        snapshot.set_ma(
            token,
            MAData(
                value=Decimal(str(round(sma_value, 6))),
                ma_type="SMA",
                period=period,
                current_price=prices[-1] if prices else Decimal("0"),
            ),
            ma_type="SMA",
            period=period,
            timeframe=timeframe,
        )

    def _populate_stochastic(
        self,
        snapshot: MarketSnapshot,
        token: str,
        candles: list[OHLCVCandle],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute Stochastic (close-derived) and set on snapshot."""
        k_period = int(config.get("stochastic_k_period", 14))
        d_period = int(config.get("stochastic_d_period", 3))
        try:
            result = StochasticCalculator.calculate_stochastic_from_candles(candles, k_period, d_period)
        except InsufficientDataError:
            return
        snapshot.set_stochastic(
            token,
            StochasticData(
                k_value=Decimal(str(round(result.k_value, 4))),
                d_value=Decimal(str(round(result.d_value, 4))),
                k_period=k_period,
                d_period=d_period,
            ),
            timeframe=timeframe,
        )

    def _populate_adx(
        self,
        snapshot: MarketSnapshot,
        token: str,
        candles: list[OHLCVCandle],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute ADX (close-derived) and set on snapshot."""
        period = int(config.get("adx_period", 14))
        try:
            result = ADXCalculator.calculate_adx_from_candles(candles, period)
        except InsufficientDataError:
            return
        snapshot.set_adx(
            token,
            ADXData(
                adx=Decimal(str(round(result.adx, 4))),
                plus_di=Decimal(str(round(result.plus_di, 4))),
                minus_di=Decimal(str(round(result.minus_di, 4))),
                period=period,
            ),
            timeframe=timeframe,
        )

    def _populate_cci(
        self,
        snapshot: MarketSnapshot,
        token: str,
        candles: list[OHLCVCandle],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute CCI (close-derived typical price) and set on snapshot."""
        period = int(config.get("cci_period", 20))
        try:
            cci_value = CCICalculator.calculate_cci_from_candles(candles, period)
        except InsufficientDataError:
            return
        snapshot.set_cci(
            token,
            CCIData(
                value=Decimal(str(round(cci_value, 4))),
                period=period,
            ),
            timeframe=timeframe,
        )

    def _populate_ichimoku(
        self,
        snapshot: MarketSnapshot,
        token: str,
        candles: list[OHLCVCandle],
        config: dict,
        timeframe: str | None = None,
    ) -> None:
        """Compute Ichimoku (close-derived midpoints) and set on snapshot."""
        tenkan = int(config.get("ichimoku_tenkan_period", 9))
        kijun = int(config.get("ichimoku_kijun_period", 26))
        senkou_b = int(config.get("ichimoku_senkou_b_period", 52))
        try:
            result = IchimokuCalculator.calculate_ichimoku_from_candles(candles, tenkan, kijun, senkou_b)
        except InsufficientDataError:
            return
        snapshot.set_ichimoku(
            token,
            IchimokuData(
                tenkan_sen=Decimal(str(round(result.tenkan_sen, 6))),
                kijun_sen=Decimal(str(round(result.kijun_sen, 6))),
                senkou_span_a=Decimal(str(round(result.senkou_span_a, 6))),
                senkou_span_b=Decimal(str(round(result.senkou_span_b, 6))),
                current_price=candles[-1].close if candles else Decimal("0"),
                tenkan_period=tenkan,
                kijun_period=kijun,
                senkou_b_period=senkou_b,
            ),
            timeframe=timeframe,
        )

    def enrich_price_data(
        self,
        snapshot: MarketSnapshot,
        tick_interval_seconds: int,
        active_tokens: set[str] | None = None,
    ) -> None:
        """Fill ``price_data``'s 24h fields from the run's own price series.

        The bare snapshot serves ``price_24h_ago=0`` / ``change_24h_pct=0``
        when nothing enriches it — a momentum read then sees a permanently
        flat market (ALM-2951 SILENT-WRONG). The backtest has the real
        series, so serve real values; ticks without 24h of history yet are
        left unenriched (the honest warm-up).
        """
        from almanak.framework.market import PriceData

        window = max(1, int(86400 // max(1, int(tick_interval_seconds))))
        for token, buffer in self._price_buffers.items():
            if active_tokens is not None and token not in active_tokens:
                continue
            prices = list(buffer)
            if len(prices) <= window:
                continue
            current = prices[-1]
            day_ago = prices[-window - 1]
            day_slice = prices[-window - 1 :]
            change_pct = ((current - day_ago) / day_ago * 100) if day_ago > 0 else Decimal("0")
            snapshot.set_price_data(
                token,
                PriceData(
                    price=current,
                    price_24h_ago=day_ago,
                    change_24h_pct=Decimal(str(round(change_pct, 4))),
                    high_24h=max(day_slice),
                    low_24h=min(day_slice),
                    source="backtest_price_series",
                ),
            )

    def min_warmup_ticks(self, config: dict | None = None) -> int:
        """Return the minimum number of ticks required before all indicators can compute.

        This is determined by the indicator with the largest period requirement.
        For example, MACD(26, 12, 9) needs 26+9-1 = 34 data points, while RSI(14) needs 15.
        """
        config = config or {}
        required = 0
        if "rsi" in self._required:
            # RSI needs period + 1 data points
            required = max(required, int(config.get("rsi_period", 14)) + 1)
        if "macd" in self._required:
            slow = int(config.get("macd_slow", 26))
            signal = int(config.get("macd_signal", 9))
            required = max(required, slow + signal - 1)
        if "bollinger_bands" in self._required:
            required = max(required, int(config.get("bb_period", 20)))
        if "atr" in self._required:
            # ATR needs period + 1 data points
            required = max(required, int(config.get("atr_period", 14)) + 1)
        if "ema" in self._required:
            # EMA(n) needs n data points; warm up for the largest configured
            # period so a slow EMA (e.g. ema_slow_period=55) is not treated as
            # past warm-up before _populate_ema can compute it.
            ema_periods = self._ema_periods_from_config(config)
            if ema_periods:
                required = max(required, max(ema_periods))
        if "sma" in self._required:
            required = max(required, int(config.get("sma_period", 20)))
        if "stochastic" in self._required:
            required = max(
                required,
                int(config.get("stochastic_k_period", 14)) + int(config.get("stochastic_d_period", 3)) - 1,
            )
        if "adx" in self._required:
            required = max(required, int(config.get("adx_period", 14)) * 2)
        if "cci" in self._required:
            required = max(required, int(config.get("cci_period", 20)))
        if "ichimoku" in self._required:
            required = max(
                required,
                int(config.get("ichimoku_tenkan_period", 9)),
                int(config.get("ichimoku_kijun_period", 26)),
                int(config.get("ichimoku_senkou_b_period", 52)),
            )
        return required

    def is_warming_up(self, token: str, config: dict | None = None) -> bool:
        """Check if the engine is still in warm-up for a given token.

        During warm-up, not enough data points have accumulated for indicators
        to compute. Strategy calls to market.rsi() etc. will raise ValueError,
        which is expected and should not be logged as an error.
        """
        return self.get_buffer_size(token) < self.min_warmup_ticks(config)

    def reset(self) -> None:
        """Clear all price buffers. Useful between backtest runs."""
        self._price_buffers.clear()

    # ------------------------------------------------------------------
    # On-demand snapshot providers (ALM-2951)
    # ------------------------------------------------------------------

    #: Retention scale ceiling when data is coarser than the tick grid: the
    #: buffer holds tick-resolution samples, so serving DEFAULT_MAX_HISTORY
    #: bars at the NATIVE timeframe needs ratio× more of them. 96 covers a
    #: daily-over-15-minute grid at ~19k Decimals per token — cheap; anything
    #: coarser is capped rather than unbounded.
    _MAX_GRANULARITY_RETENTION_SCALE = 96

    def set_data_granularity(self, granularity_seconds: int | None, tick_interval_seconds: int) -> None:
        """Record the measured resolution of the underlying price data.

        Called once per run after prefetch (ALM-2957). When the data is
        coarser than the tick grid, ``_series_for`` refuses timeframes finer
        than the data and eager population is skipped — a refusal the
        decision-input ledger records beats a confidently served value
        computed from flat upsampled ticks.

        Retention is scaled by the coarseness ratio (review round, #3311):
        the buffers hold TICK samples, so with e.g. daily data under hourly
        ticks the default 200-sample window resamples to only ~8 native bars
        and a 1d RSI(14) could never warm up — the very fallback the refusal
        message directs callers to. Existing buffers are rebuilt with the
        scaled capacity.
        """
        self._data_granularity_seconds = granularity_seconds
        self._tick_interval_seconds = tick_interval_seconds
        if (
            granularity_seconds is not None
            and tick_interval_seconds > 0
            and granularity_seconds > tick_interval_seconds
        ):
            ratio = min(
                -(-granularity_seconds // tick_interval_seconds),  # ceil div
                self._MAX_GRANULARITY_RETENTION_SCALE,
            )
            # Scale from the CONSTRUCTED capacity (idempotent — a second call
            # must not compound the scale).
            scaled = self._base_max_history * ratio
            if scaled > self._max_history:
                self._max_history = scaled
                self._price_buffers = {
                    token: deque(buffer, maxlen=scaled) for token, buffer in self._price_buffers.items()
                }

    def _degenerate_at_tick(self) -> bool:
        """True when the tick buffer is an upsampled coarser-data plane."""
        return (
            self._data_granularity_seconds is not None
            and self._tick_interval_seconds is not None
            and self._data_granularity_seconds > self._tick_interval_seconds
        )

    def _series_for(self, token: str, timeframe: str | None, tick_interval_seconds: int) -> list[Decimal]:
        """Close series for ``timeframe``, resampled from the tick series.

        ``None`` or the tick timeframe returns the raw buffer. A timeframe
        that is a whole multiple of the tick interval is resampled (buckets
        aligned to end at the current tick, bucket close = last close).
        Anything else raises — never silently serve tick bars for a
        different requested timeframe, and never serve a timeframe FINER
        than the underlying data's measured resolution (ALM-2957: daily CG
        points under hourly ticks pinned RSI at ~0/100 for months).
        """
        prices = list(self._price_buffers.get(token, []))
        if not prices:
            raise InsufficientDataError(required=1, available=0, indicator="price history")
        requested = _timeframe_seconds(timeframe) if timeframe else tick_interval_seconds
        native = self._data_granularity_seconds
        if native is not None and native > tick_interval_seconds and requested < native:
            raise ValueError(
                f"underlying price data has {timeframe_label(native)} resolution; a "
                f"{timeframe_label(requested)} indicator would be computed from flat upsampled "
                f"ticks and saturate (ALM-2957) — request {timeframe_label(native)} or coarser"
            )
        if requested == tick_interval_seconds:
            return prices
        if requested % tick_interval_seconds != 0:
            raise ValueError(
                f"timeframe {timeframe!r} is not derivable from the backtest tick interval "
                f"({timeframe_label(tick_interval_seconds)}); use the tick timeframe or a whole multiple"
            )
        step = requested // tick_interval_seconds
        # Bucket closes, aligned so the newest bucket ends at the current tick.
        return prices[::-1][::step][::-1]

    def snapshot_providers(
        self,
        config: dict | None,
        tick_interval_seconds: int,
    ) -> tuple:
        """(rsi_provider, IndicatorProvider) computing on demand from the buffers.

        Serves any period, and any timeframe derivable from the tick interval
        (whole multiples are resampled; others raise). OBV raises: it needs
        volume history, which the close-only backtest series does not carry.
        """
        _ = config

        def _rsi(token: str, period: int = 14, timeframe: str | None = None) -> RSIData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            value = RSICalculator.calculate_rsi_from_prices(prices, int(period))
            return RSIData(value=Decimal(str(round(value, 4))), period=int(period))

        def _macd(token, fast_period=12, slow_period=26, signal_period=9, timeframe=None) -> MACDData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            result = MACDCalculator.calculate_macd_from_prices(
                prices, int(fast_period), int(slow_period), int(signal_period)
            )
            return MACDData(
                macd_line=Decimal(str(round(result.macd_line, 6))),
                signal_line=Decimal(str(round(result.signal_line, 6))),
                histogram=Decimal(str(round(result.histogram, 6))),
                fast_period=int(fast_period),
                slow_period=int(slow_period),
                signal_period=int(signal_period),
            )

        def _bollinger(token, period=20, std_dev=2.0, timeframe=None) -> BollingerBandsData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            result = BollingerBandsCalculator.calculate_bollinger_from_prices(prices, int(period), float(std_dev))
            return BollingerBandsData(
                upper_band=Decimal(str(round(result.upper_band, 6))),
                middle_band=Decimal(str(round(result.middle_band, 6))),
                lower_band=Decimal(str(round(result.lower_band, 6))),
                bandwidth=Decimal(str(round(result.bandwidth, 6))),
                percent_b=Decimal(str(round(result.percent_b, 6))),
                period=int(period),
                std_dev=float(std_dev),
            )

        def _stochastic(token, k_period=14, d_period=3, timeframe=None) -> StochasticData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            result = StochasticCalculator.calculate_stochastic_from_candles(
                self._close_candles(prices), int(k_period), int(d_period)
            )
            return StochasticData(
                k_value=Decimal(str(round(result.k_value, 4))),
                d_value=Decimal(str(round(result.d_value, 4))),
                k_period=int(k_period),
                d_period=int(d_period),
            )

        def _atr(token, period=14, timeframe=None) -> ATRData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            value = ATRCalculator.calculate_atr_from_prices(prices, int(period))
            current = float(prices[-1])
            pct = (value / current * 100) if current > 0 else 0.0
            return ATRData(
                value=Decimal(str(round(value, 6))),
                value_percent=Decimal(str(round(pct, 4))),
                period=int(period),
            )

        def _sma(token, period=20, timeframe=None) -> MAData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            value = MovingAverageCalculator.calculate_sma_from_prices(prices, int(period))
            return MAData(
                value=Decimal(str(round(value, 6))),
                ma_type="SMA",
                period=int(period),
                current_price=prices[-1],
            )

        def _ema(token, period=12, timeframe=None) -> MAData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            value = MovingAverageCalculator.calculate_ema_from_prices(prices, int(period))
            return MAData(
                value=Decimal(str(round(value, 6))),
                ma_type="EMA",
                period=int(period),
                current_price=prices[-1],
            )

        def _adx(token, period=14, timeframe=None) -> ADXData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            result = ADXCalculator.calculate_adx_from_candles(self._close_candles(prices), int(period))
            return ADXData(
                adx=Decimal(str(round(result.adx, 4))),
                plus_di=Decimal(str(round(result.plus_di, 4))),
                minus_di=Decimal(str(round(result.minus_di, 4))),
                period=int(period),
            )

        def _obv(token, signal_period=21, timeframe=None):
            raise ValueError("OBV requires volume history; the backtest price series is close-only (ALM-2951)")

        def _cci(token, period=20, timeframe=None) -> CCIData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            value = CCICalculator.calculate_cci_from_candles(self._close_candles(prices), int(period))
            return CCIData(value=Decimal(str(round(value, 4))), period=int(period))

        def _ichimoku(token, tenkan_period=9, kijun_period=26, senkou_b_period=52, timeframe=None) -> IchimokuData:
            prices = self._series_for(token, timeframe, tick_interval_seconds)
            result = IchimokuCalculator.calculate_ichimoku_from_candles(
                self._close_candles(prices), int(tenkan_period), int(kijun_period), int(senkou_b_period)
            )
            return IchimokuData(
                tenkan_sen=Decimal(str(round(result.tenkan_sen, 6))),
                kijun_sen=Decimal(str(round(result.kijun_sen, 6))),
                senkou_span_a=Decimal(str(round(result.senkou_span_a, 6))),
                senkou_span_b=Decimal(str(round(result.senkou_span_b, 6))),
                current_price=prices[-1],
                tenkan_period=int(tenkan_period),
                kijun_period=int(kijun_period),
                senkou_b_period=int(senkou_b_period),
            )

        provider = IndicatorProvider(
            macd=_macd,
            bollinger=_bollinger,
            stochastic=_stochastic,
            atr=_atr,
            sma=_sma,
            ema=_ema,
            adx=_adx,
            obv=_obv,
            cci=_cci,
            ichimoku=_ichimoku,
        )
        return _rsi, provider
