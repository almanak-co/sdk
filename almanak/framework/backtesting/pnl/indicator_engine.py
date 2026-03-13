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
from collections import deque
from decimal import Decimal

from almanak.framework.data.indicators.atr import ATRCalculator
from almanak.framework.data.indicators.bollinger_bands import BollingerBandsCalculator
from almanak.framework.data.indicators.macd import MACDCalculator
from almanak.framework.data.indicators.rsi import RSICalculator
from almanak.framework.data.interfaces import InsufficientDataError
from almanak.framework.strategies.intent_strategy import (
    ATRData,
    BollingerBandsData,
    MACDData,
    MarketSnapshot,
    RSIData,
)

logger = logging.getLogger(__name__)

# Default indicators to compute when strategy doesn't declare required_indicators
DEFAULT_INDICATORS = frozenset({"rsi", "macd", "bollinger_bands", "atr"})

# Supported indicator names for validation
SUPPORTED_INDICATORS = frozenset({"rsi", "macd", "bollinger_bands", "atr"})

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
        # token -> rolling deque of close prices (oldest first)
        self._price_buffers: dict[str, deque[Decimal]] = {}

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

        for token, prices in self._price_buffers.items():
            # Skip tokens not present in the current tick to avoid stale indicators
            if active_tokens is not None and token not in active_tokens:
                continue

            price_list = list(prices)

            if "rsi" in self._required:
                self._populate_rsi(snapshot, token, price_list, config)

            if "macd" in self._required:
                self._populate_macd(snapshot, token, price_list, config)

            if "bollinger_bands" in self._required:
                self._populate_bollinger(snapshot, token, price_list, config)

            if "atr" in self._required:
                self._populate_atr(snapshot, token, price_list, config)

    def _populate_rsi(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
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
            )
        except InsufficientDataError:
            pass  # Not enough data yet -- indicator will raise ValueError if strategy calls it

    def _populate_macd(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
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
            )
        except InsufficientDataError:
            pass

    def _populate_bollinger(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
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
            )
        except InsufficientDataError:
            pass

    def _populate_atr(
        self,
        snapshot: MarketSnapshot,
        token: str,
        prices: list[Decimal],
        config: dict,
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
            )
        except InsufficientDataError:
            pass

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
