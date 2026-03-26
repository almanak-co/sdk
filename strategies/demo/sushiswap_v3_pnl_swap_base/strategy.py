"""SushiSwap V3 RSI Swap Strategy -- PnL Backtesting on Base.

This strategy exercises the PnL backtester with SushiSwap V3 swap intents
on Base. SushiSwap V3 has a paper trade demo on BSC but no PnL backtest
on any chain.

PURPOSE:
--------
1. First PnL backtest for SushiSwap V3 on any chain.
2. Tests PnL backtest engine with SushiSwap V3 compilation on Base.
3. Validates historical price feed integration for WETH/USDC on Base.

USAGE:
------
    almanak strat backtest pnl -s demo_sushiswap_v3_pnl_swap_base \\
        --start 2024-01-01 --end 2024-06-01 --interval 1h

STRATEGY LOGIC:
---------------
Each tick:
  1. Read RSI(WETH, 14-period)
  2. If RSI < 30 (oversold) and have USDC -> buy WETH
  3. If RSI > 70 (overbought) and have WETH -> sell WETH
  4. Otherwise -> hold
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_sushiswap_v3_pnl_swap_base",
    description="SushiSwap V3 RSI swap for PnL backtesting on Base -- buy/sell WETH based on RSI",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "backtesting", "swap", "sushiswap-v3", "base", "pnl"],
    supported_chains=["base"],
    supported_protocols=["sushiswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class SushiSwapV3PnLSwapBaseStrategy(IntentStrategy):
    """RSI-based swap strategy using SushiSwap V3 on Base.

    Configuration (config.json):
        base_token: Token to trade (default: "WETH")
        quote_token: Quote token (default: "USDC")
        trade_size_usd: USD amount per trade (default: 100)
        rsi_period: RSI lookback period (default: 14)
        rsi_oversold: RSI buy threshold (default: 30)
        rsi_overbought: RSI sell threshold (default: 70)
        max_slippage_bps: Max slippage in basis points (default: 100)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.base_token = str(self.get_config("base_token", "WETH"))
        self.quote_token = str(self.get_config("quote_token", "USDC"))
        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "100")))
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        # Internal state
        self._tick_count = 0
        self._total_buys = 0
        self._total_sells = 0
        self._base_held = Decimal("0")
        self._current_timestamp: datetime | None = None

        logger.info(
            f"SushiSwapV3PnLSwapBase initialized: "
            f"{self.base_token}/{self.quote_token}, "
            f"trade_size={format_usd(self.trade_size_usd)}, "
            f"RSI({self.rsi_period}) buy<{self.rsi_oversold} sell>{self.rsi_overbought}, "
            f"slippage={self.max_slippage_bps}bps"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-gated swap: buy oversold, sell overbought."""
        self._current_timestamp = getattr(market, "timestamp", None)
        self._tick_count += 1

        # Get RSI
        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period)
            rsi_value = rsi.value
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"RSI data unavailable: {e}")
            return Intent.hold(reason=f"RSI data unavailable: {e}")

        # BUY: RSI oversold -> buy base token with quote
        if rsi_value < self.rsi_oversold:
            try:
                quote_balance = market.balance(self.quote_token)
                if quote_balance.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Insufficient {self.quote_token} "
                        f"({format_usd(quote_balance.balance_usd)} < {format_usd(self.trade_size_usd)})"
                    )
            except (ValueError, KeyError):
                return Intent.hold(reason=f"Cannot check {self.quote_token} balance")

            logger.info(
                f"[tick {self._tick_count}] BUY {self.base_token}: "
                f"RSI={rsi_value:.1f} < {self.rsi_oversold}, "
                f"spending {format_usd(self.trade_size_usd)}"
            )
            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=self.max_slippage,
                protocol="sushiswap_v3",
                chain=self.chain,
            )

        # SELL: RSI overbought -> sell base token for quote
        if rsi_value > self.rsi_overbought:
            try:
                base_price = market.price(self.base_token)
            except (ValueError, KeyError) as e:
                return Intent.hold(reason=f"Price unavailable for {self.base_token}: {e}")

            if base_price <= 0:
                return Intent.hold(reason=f"Invalid price for {self.base_token}: {base_price}")

            # Calculate base amount from USD first, then check balance
            base_amount = (self.trade_size_usd / base_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            if base_amount <= 0:
                return Intent.hold(
                    reason=f"Computed sell amount is zero for {self.base_token} at price {base_price}"
                )

            try:
                base_balance = market.balance(self.base_token)
                if base_balance.balance < base_amount:
                    return Intent.hold(
                        reason=f"Insufficient {self.base_token} "
                        f"(have {base_balance.balance} < need {base_amount})"
                    )
            except (ValueError, KeyError):
                return Intent.hold(reason=f"Cannot check {self.base_token} balance")

            logger.info(
                f"[tick {self._tick_count}] SELL {self.base_token}: "
                f"RSI={rsi_value:.1f} > {self.rsi_overbought}, "
                f"selling {base_amount} {self.base_token}"
            )
            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount=base_amount,
                max_slippage=self.max_slippage,
                protocol="sushiswap_v3",
                chain=self.chain,
            )

        return Intent.hold(
            reason=f"RSI={rsi_value:.1f} in neutral zone "
            f"[{self.rsi_oversold}, {self.rsi_overbought}]"
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track buy/sell counts."""
        if not success:
            logger.warning(f"Swap failed: {getattr(intent, 'intent_type', 'unknown')}")
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type and intent_type.value == "SWAP":
            from_token = getattr(intent, "from_token", "")
            if from_token == self.quote_token:
                self._total_buys += 1
                out = self._extract_amount_out(result)
                if out:
                    self._base_held += out
                ts = getattr(result, "timestamp", None) or self._current_timestamp or datetime.now(UTC)
                add_event(
                    TimelineEvent(
                        timestamp=ts,
                        event_type=TimelineEventType.TRADE,
                        description=f"BUY {self.base_token} (trade #{self._total_buys})",
                        strategy_id=self.strategy_id,
                        details={"action": "buy", "total_buys": self._total_buys},
                    )
                )
            else:
                self._total_sells += 1
                in_amt = self._extract_amount_in(result)
                if in_amt:
                    self._base_held = max(Decimal("0"), self._base_held - in_amt)
                ts = getattr(result, "timestamp", None) or self._current_timestamp or datetime.now(UTC)
                add_event(
                    TimelineEvent(
                        timestamp=ts,
                        event_type=TimelineEventType.TRADE,
                        description=f"SELL {self.base_token} (trade #{self._total_sells})",
                        strategy_id=self.strategy_id,
                        details={"action": "sell", "total_sells": self._total_sells},
                    )
                )

    @staticmethod
    def _extract_amount_out(result: Any) -> Decimal | None:
        """Extract output amount from live execution or PnL backtest result."""
        if not result:
            return None
        if hasattr(result, "swap_amounts") and result.swap_amounts:
            return result.swap_amounts.amount_out_decimal
        if hasattr(result, "actual_amount_out") and result.actual_amount_out:
            return Decimal(str(result.actual_amount_out))
        return None

    @staticmethod
    def _extract_amount_in(result: Any) -> Decimal | None:
        """Extract input amount from live execution or PnL backtest result."""
        if not result:
            return None
        if hasattr(result, "swap_amounts") and result.swap_amounts:
            return result.swap_amounts.amount_in_decimal
        if hasattr(result, "actual_amount_in") and result.actual_amount_in:
            return Decimal(str(result.actual_amount_in))
        return None

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_sushiswap_v3_pnl_swap_base",
            "chain": self.chain,
            "base_token": self.base_token,
            "quote_token": self.quote_token,
            "tick_count": self._tick_count,
            "total_buys": self._total_buys,
            "total_sells": self._total_sells,
            "base_held": str(self._base_held),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "tick_count": self._tick_count,
            "total_buys": self._total_buys,
            "total_sells": self._total_sells,
            "base_held": str(self._base_held),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "tick_count" in state:
            self._tick_count = int(state["tick_count"])
        if "total_buys" in state:
            self._total_buys = int(state["total_buys"])
        if "total_sells" in state:
            self._total_sells = int(state["total_sells"])
        if "base_held" in state:
            self._base_held = Decimal(str(state["base_held"]))
        logger.info(
            f"Restored state: buys={self._total_buys}, sells={self._total_sells}, "
            f"base_held={self._base_held}"
        )

    # Teardown: convert remaining base token back to quote
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._base_held > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"sushiswap-{self.base_token}",
                    chain=self.chain,
                    protocol="sushiswap_v3",
                    value_usd=Decimal("0"),
                    details={
                        "token": self.base_token,
                        "amount": str(self._base_held),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=self._current_timestamp or datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._base_held > 0:
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
            intents.append(
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount=self._base_held,
                    max_slippage=slippage,
                    protocol="sushiswap_v3",
                    chain=self.chain,
                )
            )
        return intents
