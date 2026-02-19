"""Triple Signal Momentum Strategy (CCStratBT).

THESIS
------
Single-indicator strategies suffer from high false-signal rates. This strategy
combines three orthogonal technical indicators into a consensus-based system:

1. RSI (mean reversion) -- Identifies oversold/overbought extremes
2. MACD (trend following) -- Confirms momentum direction via crossovers
3. Bollinger Bands (volatility) -- Detects price at band extremes (%B)

A trade fires only when at least `min_signals_to_trade` (default: 2) of the 3
indicators agree on direction.

BACKTESTING
-----------
The strategy maintains an internal price buffer and computes all indicators
from raw prices, making it compatible with both live execution (via gateway)
and the PnL backtester (which only provides prices, not indicators).

PARAMETERS (all tunable via config.json)
-----------------------------------------
RSI:   rsi_period, rsi_oversold, rsi_overbought
MACD:  macd_fast, macd_slow, macd_signal
BB:    bb_period, bb_std_dev, bb_buy_threshold, bb_sell_threshold
Trade: trade_size_usd, max_slippage_pct, min_signals_to_trade, cooldown_ticks
"""

import logging
import math
from collections import deque
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


# =============================================================================
# Pure indicator functions (no external dependencies)
# =============================================================================

def compute_rsi(prices: list[float], period: int) -> float | None:
    """Compute RSI using Wilder's smoothing from a list of close prices."""
    if len(prices) < period + 1:
        return None

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]

    # Initial average using simple mean
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Wilder's smoothing
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_ema(prices: list[float], period: int) -> list[float]:
    """Compute EMA series from prices."""
    if len(prices) < period:
        return []
    multiplier = 2.0 / (period + 1)
    ema_values = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema_values.append((price - ema_values[-1]) * multiplier + ema_values[-1])
    return ema_values


def compute_macd(
    prices: list[float], fast: int, slow: int, signal: int
) -> tuple[float, float, float] | None:
    """Compute MACD line, signal line, and histogram."""
    if len(prices) < slow + signal:
        return None

    fast_ema = compute_ema(prices, fast)
    slow_ema = compute_ema(prices, slow)

    # Align: fast_ema starts at index (fast-1), slow_ema starts at index (slow-1)
    # Difference offset
    offset = slow - fast
    if offset >= len(fast_ema):
        return None

    macd_line_series = [
        fast_ema[i + offset] - slow_ema[i] for i in range(len(slow_ema))
    ]

    if len(macd_line_series) < signal:
        return None

    signal_ema = compute_ema(macd_line_series, signal)
    if not signal_ema:
        return None

    macd_val = macd_line_series[-1]
    signal_val = signal_ema[-1]
    histogram = macd_val - signal_val
    return macd_val, signal_val, histogram


def compute_bollinger_bands(
    prices: list[float], period: int, std_dev_mult: float
) -> tuple[float, float, float, float] | None:
    """Compute Bollinger Bands: (upper, middle, lower, percent_b)."""
    if len(prices) < period:
        return None

    window = prices[-period:]
    middle = sum(window) / period
    variance = sum((p - middle) ** 2 for p in window) / period
    std_dev = math.sqrt(variance)

    upper = middle + std_dev_mult * std_dev
    lower = middle - std_dev_mult * std_dev
    band_width = upper - lower
    current_price = prices[-1]

    if band_width == 0:
        percent_b = 0.5
    else:
        percent_b = (current_price - lower) / band_width

    return upper, middle, lower, percent_b


# =============================================================================
# Strategy
# =============================================================================

@almanak_strategy(
    name="cc_triple_signal",
    description="Triple Signal Momentum: RSI + MACD + Bollinger Bands consensus trading",
    version="1.0.0",
    author="CCStrat",
    tags=["momentum", "mean-reversion", "multi-indicator", "rsi", "macd", "bollinger", "enso"],
    supported_chains=["arbitrum", "ethereum", "base"],
    supported_protocols=["enso"],
    intent_types=["SWAP", "HOLD"],
)
class TripleSignalStrategy(IntentStrategy):
    """Multi-indicator consensus strategy combining RSI, MACD, and Bollinger Bands.

    Maintains an internal price buffer and computes indicators from raw prices,
    making it compatible with both live execution and backtesting.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config_dict = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "get"):
            config_dict = self.config

        # Trade parameters
        self.trade_size_usd = Decimal(str(config_dict.get("trade_size_usd", "500")))
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 1.0))
        self.base_token = config_dict.get("base_token", "WETH")
        self.quote_token = config_dict.get("quote_token", "USDC")

        # RSI parameters
        self.rsi_period = int(config_dict.get("rsi_period", 14))
        self.rsi_oversold = float(config_dict.get("rsi_oversold", 35))
        self.rsi_overbought = float(config_dict.get("rsi_overbought", 65))

        # MACD parameters
        self.macd_fast = int(config_dict.get("macd_fast", 12))
        self.macd_slow = int(config_dict.get("macd_slow", 26))
        self.macd_signal = int(config_dict.get("macd_signal", 9))

        # Bollinger Bands parameters
        self.bb_period = int(config_dict.get("bb_period", 20))
        self.bb_std_dev = float(config_dict.get("bb_std_dev", 2.0))
        self.bb_buy_threshold = float(config_dict.get("bb_buy_threshold", 0.2))
        self.bb_sell_threshold = float(config_dict.get("bb_sell_threshold", 0.8))

        # Consensus parameters
        self.min_signals_to_trade = int(config_dict.get("min_signals_to_trade", 2))
        self.cooldown_ticks = int(config_dict.get("cooldown_ticks", 3))

        # Internal state
        self._position = "flat"  # flat | long
        self._prev_macd_histogram: float | None = None
        self._ticks_since_trade = 999  # Start high so first trade can fire
        self._trades_executed = 0
        self._entry_price = Decimal("0")

        # Price history buffer for indicator calculations
        # Need at least max(rsi_period+1, macd_slow+macd_signal, bb_period) prices
        max_needed = max(self.rsi_period + 1, self.macd_slow + self.macd_signal, self.bb_period)
        self._price_buffer: deque[float] = deque(maxlen=max_needed + 10)

        logger.info(
            f"TripleSignalStrategy initialized: "
            f"RSI({self.rsi_period}, {self.rsi_oversold}/{self.rsi_overbought}), "
            f"MACD({self.macd_fast},{self.macd_slow},{self.macd_signal}), "
            f"BB({self.bb_period}, {self.bb_std_dev}x), "
            f"consensus>={self.min_signals_to_trade}, cooldown={self.cooldown_ticks}, "
            f"buffer_size={max_needed + 10}"
        )

    # =========================================================================
    # SIGNAL GENERATORS (from internal price buffer)
    # =========================================================================

    def _get_rsi_signal(self) -> int:
        """Get RSI signal: +1 buy, -1 sell, 0 neutral."""
        prices = list(self._price_buffer)
        rsi_val = compute_rsi(prices, self.rsi_period)
        if rsi_val is None:
            return 0

        if rsi_val < self.rsi_oversold:
            return 1  # Oversold -> buy
        elif rsi_val > self.rsi_overbought:
            return -1  # Overbought -> sell
        return 0

    def _get_macd_signal(self) -> int:
        """Get MACD signal based on histogram crossover: +1 buy, -1 sell, 0 neutral."""
        prices = list(self._price_buffer)
        result = compute_macd(prices, self.macd_fast, self.macd_slow, self.macd_signal)
        if result is None:
            return 0

        _, _, histogram = result
        prev = self._prev_macd_histogram
        self._prev_macd_histogram = histogram

        if prev is None:
            return 1 if histogram > 0 else (-1 if histogram < 0 else 0)

        # Crossover detection
        if prev <= 0 and histogram > 0:
            return 1  # Bullish crossover
        elif prev >= 0 and histogram < 0:
            return -1  # Bearish crossover

        # Sustained direction (weaker but still counted)
        if histogram > 0:
            return 1
        elif histogram < 0:
            return -1
        return 0

    def _get_bb_signal(self) -> int:
        """Get Bollinger Bands signal based on %B: +1 buy, -1 sell, 0 neutral."""
        prices = list(self._price_buffer)
        result = compute_bollinger_bands(prices, self.bb_period, self.bb_std_dev)
        if result is None:
            return 0

        _, _, _, pct_b = result
        if pct_b < self.bb_buy_threshold:
            return 1  # Price near lower band -> buy
        elif pct_b > self.bb_sell_threshold:
            return -1  # Price near upper band -> sell
        return 0

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide based on multi-indicator consensus.

        Adds the current price to the internal buffer, computes all three
        indicators from the buffer, then trades on consensus.
        """
        try:
            # Get current price and add to buffer
            try:
                price = float(market.price(self.base_token))
                self._price_buffer.append(price)
            except (ValueError, AttributeError):
                return Intent.hold(reason="Price unavailable")

            self._ticks_since_trade += 1

            # Need minimum data before generating signals
            min_needed = max(self.rsi_period + 1, self.macd_slow + self.macd_signal, self.bb_period)
            if len(self._price_buffer) < min_needed:
                return Intent.hold(
                    reason=f"Buffering prices: {len(self._price_buffer)}/{min_needed}"
                )

            # Collect signals
            rsi_sig = self._get_rsi_signal()
            macd_sig = self._get_macd_signal()
            bb_sig = self._get_bb_signal()

            signals = [rsi_sig, macd_sig, bb_sig]
            signal_names = ["RSI", "MACD", "BB"]

            buy_count = sum(1 for s in signals if s > 0)
            sell_count = sum(1 for s in signals if s < 0)

            sig_str = ", ".join(
                f"{name}={'+' if s > 0 else ('-' if s < 0 else '0')}"
                for name, s in zip(signal_names, signals)
            )

            logger.debug(
                f"Signals: [{sig_str}] buy={buy_count} sell={sell_count} "
                f"pos={self._position} cooldown={self._ticks_since_trade}/{self.cooldown_ticks}"
            )

            # Cooldown check
            if self._ticks_since_trade < self.cooldown_ticks:
                return Intent.hold(
                    reason=f"Cooldown: {self._ticks_since_trade}/{self.cooldown_ticks} ticks"
                )

            # BUY LOGIC: consensus bullish + currently flat
            if buy_count >= self.min_signals_to_trade and self._position == "flat":
                firing = [name for name, s in zip(signal_names, signals) if s > 0]
                logger.info(
                    f"BUY CONSENSUS ({buy_count}/3): {', '.join(firing)} | "
                    f"Buying {format_usd(self.trade_size_usd)} {self.base_token} @ ${price:,.2f}"
                )
                self._position = "long"
                self._ticks_since_trade = 0
                self._entry_price = Decimal(str(price))
                return self._create_buy_intent()

            # SELL LOGIC: consensus bearish + currently long
            if sell_count >= self.min_signals_to_trade and self._position == "long":
                firing = [name for name, s in zip(signal_names, signals) if s < 0]
                logger.info(
                    f"SELL CONSENSUS ({sell_count}/3): {', '.join(firing)} | "
                    f"Selling {self.base_token} @ ${price:,.2f} (entry: ${self._entry_price:,.2f})"
                )
                self._position = "flat"
                self._ticks_since_trade = 0
                return self._create_sell_intent()

            # No consensus
            return Intent.hold(
                reason=f"No consensus [{sig_str}] pos={self._position}"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_buy_intent(self) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        amount_usd = self.trade_size_usd.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=amount_usd,
            max_slippage=max_slippage,
            protocol="enso",
        )

    def _create_sell_intent(self) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        amount_usd = self.trade_size_usd.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount_usd=amount_usd,
            max_slippage=max_slippage,
            protocol="enso",
        )

    # =========================================================================
    # STATUS / TEARDOWN
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "cc_triple_signal",
            "chain": self.chain,
            "config": {
                "rsi": f"({self.rsi_period}, {self.rsi_oversold}/{self.rsi_overbought})",
                "macd": f"({self.macd_fast},{self.macd_slow},{self.macd_signal})",
                "bb": f"({self.bb_period}, {self.bb_std_dev}x)",
                "trade_size": str(self.trade_size_usd),
                "consensus": self.min_signals_to_trade,
            },
            "state": {
                "position": self._position,
                "trades_executed": self._trades_executed,
                "entry_price": str(self._entry_price),
                "ticks_since_trade": self._ticks_since_trade,
                "buffer_size": len(self._price_buffer),
            },
        }

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._position == "long":
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="triple_signal_token_0",
                    chain=self.chain,
                    protocol="enso",
                    value_usd=self.trade_size_usd,
                    details={
                        "asset": self.base_token,
                        "entry_price": str(self._entry_price),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "cc_triple_signal"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._position == "long":
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
            intents.append(
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=slippage,
                    protocol="enso",
                )
            )
        return intents

    def to_dict(self) -> dict[str, Any]:
        metadata = self.get_metadata()
        config_dict = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "to_dict"):
            config_dict = self.config.to_dict()
        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }
