"""
===============================================================================
DEMO: Uniswap V3 Paper Trade -- RSI-Based Swap on Optimism
===============================================================================

Paper trading vehicle for exercising the paper trading engine on Optimism.
This strategy is intentionally simple: buy WETH when RSI is oversold, sell
when overbought, hold otherwise. Exercises the Uniswap V3 swap path on
Optimism via paper trading.

PURPOSE:
--------
1. Validate the paper trading pipeline on Optimism (first Optimism paper trade):
   - Anvil fork management on Optimism (OP gas pricing model)
   - Uniswap V3 swap execution on L2
   - PnL journal entries and equity curve generation
   - Multi-iteration execution lifecycle
2. Exercise Uniswap V3 SWAP intents on Optimism via paper trading.

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \\
        -s demo_uniswap_paper_trade_optimism \\
        --chain optimism \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/uniswap_paper_trade_optimism \\
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. Read RSI(ETH, 14)
  2. If RSI < 40 (oversold) and have USDC -> buy WETH
  3. If RSI > 70 (overbought) and have WETH -> sell WETH
  4. Otherwise -> hold
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_uniswap_paper_trade_optimism",
    description="Paper trading demo -- RSI swap on Uniswap V3 (Optimism)",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "swap", "uniswap", "optimism", "backtesting"],
    supported_chains=["optimism"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="optimism",
    quote_asset="USD",
)
class UniswapPaperTradeOptimismStrategy(IntentStrategy):
    """RSI-gated Uniswap V3 swap strategy for paper trading on Optimism.

    Configuration (config.json):
        trade_size_usd: Trade size in USD (default: 3)
        rsi_period: RSI period (default: 14)
        rsi_oversold: RSI buy threshold (default: 40)
        rsi_overbought: RSI sell threshold (default: 70)
        max_slippage_bps: Max slippage in bps (default: 100)
        base_token: Token to trade (default: WETH)
        quote_token: Quote token (default: USDC)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "3")))
        self.rsi_period = int(self.get_config("rsi_period", 14))
        self.rsi_oversold = Decimal(str(self.get_config("rsi_oversold", "40")))
        self.rsi_overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        self._consecutive_holds = 0
        self._total_buys = 0
        self._total_sells = 0
        self._holding_base = False
        # Neutral-rearm latch: only act on a signal transition, re-arm via neutral.
        self._last_signal = "neutral"

        logger.info(
            f"UniswapPaperTradeOptimism initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"RSI({self.rsi_period}) range=[{self.rsi_oversold}, {self.rsi_overbought}], "
            f"pair={self.base_token}/{self.quote_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """RSI-gated swap decision for paper trading."""
        try:
            rsi = market.rsi(self.base_token, period=self.rsi_period)
            rsi_value = rsi.value
            logger.info(f"RSI({self.rsi_period}) = {rsi_value:.1f}")
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"RSI data unavailable: {e}")
            return Intent.hold(reason=f"RSI data unavailable: {e}")

        try:
            base_price = market.price(self.base_token)
        except (ValueError, KeyError, AttributeError) as e:
            logger.warning(f"Could not get price for {self.base_token}: {e}")
            return Intent.hold(reason=f"Price data unavailable for {self.base_token}")

        if base_price <= 0:
            return Intent.hold(reason=f"Invalid price for {self.base_token}: {base_price}")

        try:
            quote_balance = market.balance(self.quote_token)
            base_balance = market.balance(self.base_token)
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get balances: {e}")
            return Intent.hold(reason="Balance data unavailable")

        # Neutral re-arm: act only on a transition into a signal zone, not every
        # tick RSI stays extreme. Re-arm when RSI returns to neutral; the buy/sell
        # latch is set in on_intent_executed on a SUCCESSFUL swap, so a held-back
        # (insufficient balance) or failed swap never locks out the next attempt.
        current_signal = (
            "buy" if rsi_value <= self.rsi_oversold else "sell" if rsi_value >= self.rsi_overbought else "neutral"
        )
        if current_signal == "neutral":
            self._last_signal = "neutral"
            self._consecutive_holds += 1
            return Intent.hold(
                reason=f"RSI={rsi_value:.1f} in neutral zone "
                f"[{self.rsi_oversold}-{self.rsi_overbought}] "
                f"(hold #{self._consecutive_holds})"
            )
        if current_signal == self._last_signal:
            return Intent.hold(reason=f"RSI={rsi_value:.1f} still {current_signal}; awaiting neutral reset")

        # BUY: RSI oversold
        if current_signal == "buy":
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(reason=f"Oversold (RSI={rsi_value:.1f}) but insufficient {self.quote_token}")

            # Reset the hold counter on an actionable signal. The trade counter,
            # holding flag, and timeline event are NOT set here -- they are
            # reconciled in on_intent_executed on a SUCCESSFUL swap, so a failed
            # or held-back swap can't persist a phantom holding_base (which
            # teardown acts on) or inflate the counters across a restart.
            self._consecutive_holds = 0

            logger.info(
                f"BUY SIGNAL: RSI={rsi_value:.1f} < {self.rsi_oversold} | "
                f"Buying {format_usd(self.trade_size_usd)} of {self.base_token}"
            )

            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                protocol="uniswap_v3",
            )

        # SELL: RSI overbought
        if current_signal == "sell":
            min_base_to_sell = self.trade_size_usd / base_price
            if base_balance.balance < min_base_to_sell:
                return Intent.hold(reason=f"Overbought (RSI={rsi_value:.1f}) but insufficient {self.base_token}")

            self._consecutive_holds = 0

            logger.info(
                f"SELL SIGNAL: RSI={rsi_value:.1f} > {self.rsi_overbought} | "
                f"Selling {format_usd(self.trade_size_usd)} of {self.base_token}"
            )

            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount_usd=self.trade_size_usd,
                max_slippage=Decimal(str(self.max_slippage_bps)) / Decimal("10000"),
                protocol="uniswap_v3",
            )

        return Intent.hold(reason=f"RSI={rsi_value:.1f}; no actionable signal")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Reconcile execution-derived state ONLY on a successful swap.

        The holding flag, trade counters, neutral-rearm latch, and timeline
        event are all set here -- never optimistically in decide(). A failed or
        held-back swap therefore can't leave a phantom holding_base (which
        teardown would act on with an ``amount="all"`` sell) or inflate the
        buy/sell counters, including across a restart via persisted state.
        """
        if not success:
            return
        intent_type = getattr(intent, "intent_type", None)
        if not intent_type or intent_type.value != "SWAP":
            return
        amount_usd = getattr(intent, "amount_usd", None) or self.trade_size_usd
        if getattr(intent, "to_token", None) == self.base_token:
            self._holding_base = True
            self._total_buys += 1
            self._last_signal = "buy"
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    deployment_id=getattr(self, "deployment_id", "demo_uniswap_paper_trade_optimism"),
                    description=f"BUY {format_usd(amount_usd)} {self.base_token} executed",
                )
            )
        elif getattr(intent, "from_token", None) == self.base_token:
            self._holding_base = False
            self._total_sells += 1
            self._last_signal = "sell"
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    deployment_id=getattr(self, "deployment_id", "demo_uniswap_paper_trade_optimism"),
                    description=f"SELL {format_usd(amount_usd)} {self.base_token} executed",
                )
            )

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "last_signal": self._last_signal,
            "consecutive_holds": self._consecutive_holds,
            "total_buys": self._total_buys,
            "total_sells": self._total_sells,
            "holding_base": self._holding_base,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if state:
            self._last_signal = state.get("last_signal", "neutral")
            self._consecutive_holds = int(state.get("consecutive_holds", 0))
            self._total_buys = int(state.get("total_buys", 0))
            self._total_sells = int(state.get("total_sells", 0))
            self._holding_base = bool(state.get("holding_base", False))

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_uniswap_paper_trade_optimism",
            "chain": self.chain,
            "total_buys": self._total_buys,
            "total_sells": self._total_sells,
            "consecutive_holds": self._consecutive_holds,
        }

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []
        if self._holding_base:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="uniswap_paper_opt_token_0",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=self.trade_size_usd,
                    details={
                        "asset": self.base_token,
                        "total_buys": self._total_buys,
                        "total_sells": self._total_sells,
                    },
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "demo_uniswap_paper_trade_optimism"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        if not self._holding_base:
            return []

        from almanak.framework.teardown import TeardownMode

        max_slippage = (
            Decimal("0.03") if mode == TeardownMode.HARD else Decimal(str(self.max_slippage_bps)) / Decimal("10000")
        )

        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="uniswap_v3",
            )
        ]
