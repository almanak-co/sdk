"""Multi-Signal Accumulator - Double Confirmation DCA Strategy.

A quant-oriented strategy that requires MULTIPLE technical indicators to
confirm before entering a trade. This reduces false signals and implements
a disciplined dollar-cost averaging approach.

THESIS:
-------
Single-indicator strategies suffer from high false-positive rates. By requiring
TWO independent indicators to agree, we filter out noise and only trade on
high-conviction signals.

We combine:
1. RSI (mean-reversion) - identifies oversold/overbought conditions
2. Bollinger Bands (volatility) - identifies price extremes

ENTRY RULES (must satisfy BOTH):
- RSI < oversold_threshold (e.g., 35)
- Price below lower Bollinger Band (percent_b < 0)
-> BUY a fixed USD amount of base token

EXIT RULES (must satisfy BOTH):
- RSI > overbought_threshold (e.g., 65)
- Price above upper Bollinger Band (percent_b > 1)
-> SELL accumulated position

POSITION MANAGEMENT:
--------------------
- Accumulates position across multiple buy signals (DCA)
- Tracks average entry price and total accumulated amount
- Has a maximum accumulated position size (risk cap)
- Each buy is a fixed USD amount
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
    name="demo_multi_signal_accumulator",
    description="Multi-indicator accumulation strategy with RSI + Bollinger Band confirmation",
    version="1.0.0",
    author="QuantUser",
    tags=["multi-signal", "accumulator", "rsi", "bollinger", "dca", "quant"],
    supported_chains=["arbitrum", "ethereum", "base"],
    supported_protocols=["enso"],
    intent_types=["SWAP", "HOLD"],
)
class MultiSignalAccumulatorStrategy(IntentStrategy):
    """Accumulation strategy requiring double confirmation from RSI + Bollinger Bands.

    Only buys when both indicators agree on oversold conditions.
    Only sells when both agree on overbought conditions.
    Tracks accumulated position with average cost basis.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config_dict = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "get"):
            config_dict = self.config

        # Trading params
        self.trade_size_usd = Decimal(str(config_dict.get("trade_size_usd", "100")))
        self.max_accumulated_usd = Decimal(str(config_dict.get("max_accumulated_usd", "1000")))

        # RSI params
        self.rsi_oversold = int(config_dict.get("rsi_oversold", 35))
        self.rsi_overbought = int(config_dict.get("rsi_overbought", 65))

        # Bollinger Band params
        self.bb_period = int(config_dict.get("bb_period", 20))
        self.bb_std_dev = float(config_dict.get("bb_std_dev", 2.0))

        # Execution
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 1.0))
        self.base_token = config_dict.get("base_token", "WETH")
        self.quote_token = config_dict.get("quote_token", "USDC")
        self.force_action = config_dict.get("force_action", None)

        # Position tracking
        self._accumulated_usd = Decimal("0")
        self._buy_count = 0
        self._sell_count = 0
        self._avg_entry_price = Decimal("0")
        self._total_token_amount = Decimal("0")

        logger.info(
            f"MultiSignalAccumulatorStrategy initialized: "
            f"RSI({self.rsi_oversold}/{self.rsi_overbought}), "
            f"BB({self.bb_period}, {self.bb_std_dev}), "
            f"trade_size={format_usd(self.trade_size_usd)}, "
            f"max_position={format_usd(self.max_accumulated_usd)}, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide based on double confirmation from RSI + Bollinger Bands.

        Only trades when BOTH indicators align on extreme conditions.
        """
        try:
            # Handle forced actions for testing
            if self.force_action:
                logger.info(f"Force action: {self.force_action}")
                if self.force_action == "buy":
                    return self._create_buy_intent()
                elif self.force_action == "sell":
                    if self._accumulated_usd > 0:
                        return self._create_sell_intent(amount_usd=self._accumulated_usd)
                    return Intent.hold(reason="No position to sell")

            # Get indicators
            rsi_value = None
            percent_b = None
            bandwidth = None

            try:
                rsi_data = market.rsi(self.base_token)
                rsi_value = float(rsi_data.value)
            except (ValueError, AttributeError):
                logger.warning(f"RSI unavailable for {self.base_token}")

            try:
                bb = market.bollinger_bands(
                    self.base_token,
                    period=self.bb_period,
                    std_dev=self.bb_std_dev,
                )
                percent_b = bb.percent_b
                bandwidth = bb.bandwidth
            except (ValueError, AttributeError):
                logger.warning(f"Bollinger Bands unavailable for {self.base_token}")

            # Need both indicators to make a decision
            if rsi_value is None or percent_b is None:
                return Intent.hold(
                    reason=f"Incomplete data: RSI={'N/A' if rsi_value is None else f'{rsi_value:.1f}'}, "
                    f"%B={'N/A' if percent_b is None else f'{percent_b:.3f}'}"
                )

            logger.debug(
                f"Signals: RSI={rsi_value:.1f}, %B={percent_b:.3f}, "
                f"BW={bandwidth:.4f}, accumulated={format_usd(self._accumulated_usd)}"
            )

            # BUY signal: RSI oversold AND price below lower BB
            rsi_oversold = rsi_value < self.rsi_oversold
            below_lower_band = percent_b < 0.0

            # SELL signal: RSI overbought AND price above upper BB
            rsi_overbought = rsi_value > self.rsi_overbought
            above_upper_band = percent_b > 1.0

            if rsi_oversold and below_lower_band:
                # Check position cap
                if self._accumulated_usd >= self.max_accumulated_usd:
                    return Intent.hold(
                        reason=f"DOUBLE BUY SIGNAL but max position reached: "
                        f"{format_usd(self._accumulated_usd)} >= {format_usd(self.max_accumulated_usd)}"
                    )

                logger.info(
                    f"DOUBLE BUY SIGNAL: RSI={rsi_value:.1f} < {self.rsi_oversold} AND "
                    f"%B={percent_b:.3f} < 0 | "
                    f"Accumulating {format_usd(self.trade_size_usd)} "
                    f"(total: {format_usd(self._accumulated_usd + self.trade_size_usd)})"
                )

                # Update position tracking
                try:
                    current_price = Decimal(str(market.price(self.base_token)))
                    new_token_amount = self.trade_size_usd / current_price
                    self._total_token_amount += new_token_amount

                    # Calculate weighted average entry price
                    total_cost = self._accumulated_usd + self.trade_size_usd
                    if self._total_token_amount > 0:
                        self._avg_entry_price = total_cost / self._total_token_amount
                except (ValueError, AttributeError, ZeroDivisionError):
                    pass

                self._accumulated_usd += self.trade_size_usd
                self._buy_count += 1
                return self._create_buy_intent()

            elif rsi_overbought and above_upper_band and self._accumulated_usd > 0:
                sell_amount = self._accumulated_usd
                logger.info(
                    f"DOUBLE SELL SIGNAL: RSI={rsi_value:.1f} > {self.rsi_overbought} AND "
                    f"%B={percent_b:.3f} > 1 | "
                    f"Selling accumulated position ({format_usd(sell_amount)})"
                )
                self._accumulated_usd = Decimal("0")
                self._total_token_amount = Decimal("0")
                self._avg_entry_price = Decimal("0")
                self._sell_count += 1
                return self._create_sell_intent(amount_usd=sell_amount)

            # Log partial signals for debugging
            partial_signals = []
            if rsi_oversold:
                partial_signals.append(f"RSI oversold ({rsi_value:.1f})")
            if below_lower_band:
                partial_signals.append(f"below lower BB (%B={percent_b:.3f})")
            if rsi_overbought:
                partial_signals.append(f"RSI overbought ({rsi_value:.1f})")
            if above_upper_band:
                partial_signals.append(f"above upper BB (%B={percent_b:.3f})")

            if partial_signals:
                reason = f"Partial signal only: {', '.join(partial_signals)} - need double confirmation"
            else:
                reason = f"Neutral: RSI={rsi_value:.1f}, %B={percent_b:.3f}"

            return Intent.hold(reason=reason)

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _create_buy_intent(self) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="enso",
        )

    def _create_sell_intent(self, amount_usd: Decimal | None = None) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        sell_amount = amount_usd if amount_usd is not None else self.trade_size_usd
        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount_usd=sell_amount,
            max_slippage=max_slippage,
            protocol="enso",
        )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_multi_signal_accumulator",
            "chain": self.chain,
            "config": {
                "rsi_thresholds": f"{self.rsi_oversold}/{self.rsi_overbought}",
                "bb_params": f"({self.bb_period}, {self.bb_std_dev})",
                "trade_size_usd": str(self.trade_size_usd),
                "max_accumulated_usd": str(self.max_accumulated_usd),
                "pair": f"{self.base_token}/{self.quote_token}",
            },
            "state": {
                "accumulated_usd": str(self._accumulated_usd),
                "avg_entry_price": str(self._avg_entry_price),
                "total_token_amount": str(self._total_token_amount),
                "buy_count": self._buy_count,
                "sell_count": self._sell_count,
            },
        }

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._accumulated_usd > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="multi_signal_acc_token_0",
                    chain=self.chain,
                    protocol="enso",
                    value_usd=self._accumulated_usd,
                    details={
                        "asset": self.base_token,
                        "avg_entry_price": str(self._avg_entry_price),
                        "buy_count": self._buy_count,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_multi_signal_accumulator"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._accumulated_usd > 0:
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
