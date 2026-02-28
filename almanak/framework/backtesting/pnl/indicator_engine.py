"""Indicator Engine for PnL Backtester.

Computes technical indicators from rolling price history and populates
them on MarketSnapshot so that strategies using market.rsi(), market.macd(),
and market.bollinger_bands() work identically in live and backtest modes.

The engine uses existing static calculator methods from the indicator modules,
avoiding any code duplication. It maintains a rolling price buffer per token
and computes only the indicators declared by the strategy (or defaults).

Usage:
    from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine

    engine = BacktestIndicatorEngine(required_indicators=["rsi", "macd", "bollinger_bands"])

    # Each tick: append price, then populate snapshot
    engine.append_price("WETH", Decimal("3500.00"))
    engine.populate_snapshot(snapshot, strategy_config)
"""

import logging
from collections import deque
from decimal import Decimal

from almanak.framework.data.indicators.bollinger_bands import BollingerBandsCalculator
from almanak.framework.data.indicators.macd import MACDCalculator
from almanak.framework.data.indicators.rsi import RSICalculator
from almanak.framework.data.interfaces import InsufficientDataError
from almanak.framework.strategies.intent_strategy import (
    BollingerBandsData,
    MACDData,
    MarketSnapshot,
    RSIData,
)

logger = logging.getLogger(__name__)

# Default indicators to compute when strategy doesn't declare required_indicators
DEFAULT_INDICATORS = frozenset({"rsi", "macd", "bollinger_bands"})

# Maximum price history to keep per token (covers all standard indicator periods)
DEFAULT_MAX_HISTORY = 200


class BacktestIndicatorEngine:
    """Computes indicators from rolling price history for the PnL backtester.

    This engine bridges the gap between the backtester (which only has close prices)
    and MarketSnapshot's indicator methods (which expect pre-populated data).

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

        unknown = self._required - {"rsi", "macd", "bollinger_bands"}
        if unknown:
            logger.warning(
                "BacktestIndicatorEngine: unknown indicators requested (will be skipped): %s. "
                "Supported: rsi, macd, bollinger_bands",
                ", ".join(sorted(unknown)),
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
                    (e.g., rsi_period, macd_fast, bb_period). Falls back to standard defaults.
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

    def reset(self) -> None:
        """Clear all price buffers. Useful between backtest runs."""
        self._price_buffers.clear()
