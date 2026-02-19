"""MACD Momentum Trader - Trend Following via MACD Crossovers.

A quant-oriented strategy that uses MACD (Moving Average Convergence Divergence)
crossover signals to trade WETH/USDC on Base via the Enso DEX aggregator.

THESIS:
-------
Unlike RSI which is a mean-reversion indicator, MACD is a trend-following
momentum indicator. When the MACD line crosses above the signal line, it
suggests bullish momentum is building. When it crosses below, bearish momentum.

This strategy follows trends rather than fading them - the opposite philosophy
of RSI-based strategies.

SIGNALS:
--------
- Bullish crossover (MACD > Signal, Histogram > 0): BUY base token
- Bearish crossover (MACD < Signal, Histogram < 0): SELL base token
- No crossover / weak signal: HOLD

"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_macd_momentum",
    description="MACD crossover momentum trading via Enso on Base",
    version="1.0.0",
    author="QuantUser",
    tags=["momentum", "macd", "enso", "trend-following", "base"],
    supported_chains=["base", "arbitrum", "ethereum"],
    supported_protocols=["enso"],
    intent_types=["SWAP", "HOLD"],
)
class MACDMomentumStrategy(IntentStrategy):
    """Trend-following strategy using MACD crossover signals.

    Uses MACD histogram direction and magnitude to determine entry/exit:
    - Positive histogram with increasing magnitude = buy signal
    - Negative histogram with increasing magnitude = sell signal
    - Weak histogram = no trade (avoid chop)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config_dict = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "get"):
            config_dict = self.config

        self.trade_size_usd = Decimal(str(config_dict.get("trade_size_usd", "100")))
        self.macd_fast = int(config_dict.get("macd_fast", 12))
        self.macd_slow = int(config_dict.get("macd_slow", 26))
        self.macd_signal = int(config_dict.get("macd_signal", 9))
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 1.0))
        self.base_token = config_dict.get("base_token", "WETH")
        self.quote_token = config_dict.get("quote_token", "USDC")
        self.force_action = config_dict.get("force_action", None)

        # Track previous histogram for crossover detection
        self._prev_histogram = None
        self._position = "flat"  # flat, long
        self._trades_executed = 0
        self._entry_price = Decimal("0")

        logger.info(
            f"MACDMomentumStrategy initialized: "
            f"MACD({self.macd_fast},{self.macd_slow},{self.macd_signal}), "
            f"trade_size={format_usd(self.trade_size_usd)}, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide based on MACD crossover signals.

        Uses histogram sign change as the primary signal:
        - Histogram flips positive = bullish crossover -> BUY
        - Histogram flips negative = bearish crossover -> SELL (if long)
        """
        try:
            # Handle forced actions for testing
            if self.force_action:
                logger.info(f"Force action: {self.force_action}")
                if self.force_action == "buy":
                    self._position = "long"
                    try:
                        self._entry_price = Decimal(str(market.price(self.base_token)))
                    except (ValueError, AttributeError):
                        self._entry_price = Decimal("0")
                    return self._create_buy_intent()
                elif self.force_action == "sell":
                    self._position = "flat"
                    return self._create_sell_intent()

            # Get MACD data
            try:
                macd_data = market.macd(
                    self.base_token,
                    fast_period=self.macd_fast,
                    slow_period=self.macd_slow,
                    signal_period=self.macd_signal,
                )
                histogram = macd_data.histogram
                macd_line = macd_data.macd_line
                signal_line = macd_data.signal_line
            except (ValueError, AttributeError):
                logger.warning(f"MACD unavailable for {self.base_token}, holding")
                return Intent.hold(reason="MACD data unavailable")

            logger.debug(
                f"MACD: line={macd_line:.4f}, signal={signal_line:.4f}, "
                f"histogram={histogram:.4f}, position={self._position}"
            )

            # Detect crossover (histogram sign change)
            prev = self._prev_histogram
            self._prev_histogram = histogram

            # Need at least 2 readings for crossover detection
            if prev is None:
                return Intent.hold(reason="Collecting initial MACD reading")

            # Bullish crossover: histogram flips from negative to positive
            bullish_crossover = prev <= 0 and histogram > 0
            # Bearish crossover: histogram flips from positive to negative
            bearish_crossover = prev >= 0 and histogram < 0

            if bullish_crossover and self._position == "flat":
                logger.info(
                    f"BULLISH CROSSOVER: histogram {prev:.4f} -> {histogram:.4f} | "
                    f"Buying {format_usd(self.trade_size_usd)} of {self.base_token}"
                )
                self._position = "long"
                try:
                    self._entry_price = Decimal(str(market.price(self.base_token)))
                except (ValueError, AttributeError):
                    self._entry_price = Decimal("0")
                return self._create_buy_intent()

            elif bearish_crossover and self._position == "long":
                logger.info(
                    f"BEARISH CROSSOVER: histogram {prev:.4f} -> {histogram:.4f} | "
                    f"Selling {self.base_token} position"
                )
                self._position = "flat"
                return self._create_sell_intent()

            # No crossover - hold
            trend = "bullish" if histogram > 0 else "bearish"
            return Intent.hold(
                reason=f"MACD {trend} (histogram={histogram:.4f}), "
                f"position={self._position}, waiting for crossover"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _create_buy_intent(self) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="enso",
        )

    def _create_sell_intent(self) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="enso",
        )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_macd_momentum",
            "chain": self.chain,
            "config": {
                "macd_params": f"({self.macd_fast},{self.macd_slow},{self.macd_signal})",
                "trade_size_usd": str(self.trade_size_usd),
                "pair": f"{self.base_token}/{self.quote_token}",
            },
            "state": {
                "position": self._position,
                "prev_histogram": self._prev_histogram,
                "trades_executed": self._trades_executed,
                "entry_price": str(self._entry_price),
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
                    position_id="macd_momentum_token_0",
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
            strategy_id=getattr(self, "strategy_id", "demo_macd_momentum"),
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
        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }
