"""
===============================================================================
Camelot Swap Demo — BUY + SELL Lifecycle on Arbitrum (Algebra V3)
===============================================================================

Demonstrates the Camelot swap path end-to-end on Arbitrum:

1. First run: BUY — swap USDC -> WETH via Camelot
2. Second run: SELL — swap WETH -> USDC via Camelot
3. Subsequent runs: alternate BUY/SELL

KEY CAMELOT SWAP CONCEPTS:
- Camelot V3 is an Algebra V1.9 fork (Arbitrum-native DEX)
- No fixed fee tiers — fees are determined dynamically by the pool
- Algebra SwapRouter uses an exactInputSingle signature without a fee
  parameter (different from Uniswap V3); the connector encodes this.
- The Camelot connector supports SWAP only (LP / collect are fail-closed).

USAGE:
    almanak strat demo --name camelot_swap
    cd camelot_swap
    almanak strat run --network anvil --once   # run twice to see BUY then SELL

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)

# Dust threshold (USD-denominated) shared by teardown preview
# (``get_open_positions``) and teardown execution (``generate_teardown_intents``)
# so a residual base-token balance below this value is treated the same way in
# both paths — preview hides it AND execution skips emitting a swap for it.
# Without a shared threshold, preview could report "no positions" while
# execution still submits a dust swap that wastes gas / drifts state.
MIN_TEARDOWN_DUST_USD = Decimal("0.01")


@almanak_strategy(
    name="demo_camelot_swap",
    description="Camelot swap demo — BUY/SELL lifecycle on Arbitrum (Algebra V3)",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "swap", "camelot", "arbitrum", "algebra"],
    supported_chains=["arbitrum"],
    supported_protocols=["camelot"],
    intent_types=["SWAP", "HOLD"],
    default_chain="arbitrum",
    quote_asset="USD",
)
class CamelotSwapStrategy(IntentStrategy):
    """Camelot swap demo: alternates BUY and SELL each iteration.

    Configuration Parameters (from config.json):
        trade_size_usd: Amount to trade per signal (default: 1)
        max_slippage_bps: Maximum slippage in basis points (default: 200 = 2%)
        base_token: Token to buy/sell (default: WETH)
        quote_token: Stable token (default: USDC)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        if not hasattr(self, "state") or self.state is None:
            self.state: dict[str, Any] = {}

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "1")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 200))
        self.base_token: str = self.get_config("base_token", "WETH")
        self.quote_token: str = self.get_config("quote_token", "USDC")

        self._max_slippage = Decimal(self.max_slippage_bps) / Decimal(10000)

    def decide(self, market: MarketSnapshot) -> Intent:
        """Decide whether to BUY or SELL based on state.

        First call: BUY (swap quote -> base).
        After a BUY: SELL (swap base -> quote).
        After a SELL: BUY again.
        """
        # Determine action from persisted state
        last_action = self.state.get("last_action", "SELL")  # default triggers BUY first

        base_price = market.price(self.base_token)
        quote_balance = market.balance(self.quote_token)
        base_balance = market.balance(self.base_token)

        logger.info(
            "Camelot swap decision | last=%s | %s=%s | %s=%s | %s=%s",
            last_action,
            self.base_token,
            format_usd(base_price),
            self.quote_token,
            format_usd(quote_balance.balance_usd),
            self.base_token,
            format_usd(base_balance.balance_usd),
        )

        if last_action == "SELL":
            # BUY: swap quote -> base
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.quote_token} for BUY "
                    f"(need {format_usd(self.trade_size_usd)}, have {format_usd(quote_balance.balance_usd)})"
                )

            logger.info("BUY: %s %s -> %s via Camelot", format_usd(self.trade_size_usd), self.quote_token, self.base_token)

            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=self._max_slippage,
                protocol="camelot",
            )

        else:
            # SELL: swap base -> quote
            if base_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.base_token} for SELL "
                    f"(need {format_usd(self.trade_size_usd)}, have {format_usd(base_balance.balance_usd)})"
                )

            # Compute base token amount from USD (round down to avoid overspend)
            if base_price and base_price > 0:
                sell_amount = (self.trade_size_usd / base_price).quantize(Decimal("1E-18"), rounding=ROUND_DOWN)
            else:
                return Intent.hold(reason=f"No price available for {self.base_token}")

            logger.info("SELL: %s %s -> %s via Camelot", sell_amount, self.base_token, self.quote_token)

            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount=sell_amount,
                max_slippage=self._max_slippage,
                protocol="camelot",
            )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state only after successful execution."""
        if not success:
            return
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        from_token = getattr(intent, "from_token", None)
        if from_token == self.quote_token:
            self.state["last_action"] = "BUY"
            logger.info("BUY executed successfully")
        elif from_token == self.base_token:
            self.state["last_action"] = "SELL"
            logger.info("SELL executed successfully")

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        if self.state.get("last_action"):
            state["last_action"] = self.state["last_action"]
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        if "last_action" in state:
            self.state["last_action"] = state["last_action"]

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Return open positions (base token holdings) for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        try:
            base_balance = self.market.balance(self.base_token) if hasattr(self, "market") and self.market else None
            if base_balance and base_balance.balance_usd > MIN_TEARDOWN_DUST_USD:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id=f"camelot_swap_{self.base_token.lower()}",
                        chain=self.chain,
                        protocol="camelot",
                        value_usd=base_balance.balance_usd,
                        details={
                            "token": self.base_token,
                            "balance": str(base_balance.balance),
                        },
                    )
                )
        except Exception:
            logger.warning("Failed to query balance for teardown; reporting no positions", exc_info=True)

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "demo_camelot_swap"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions (swap base -> quote)."""
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        try:
            m = market or (self.market if hasattr(self, "market") else None)
            if m is None:
                return intents

            base_balance = m.balance(self.base_token)
            # Match the preview filter in ``get_open_positions``: skip
            # base-token dust below ``MIN_TEARDOWN_DUST_USD`` so we don't
            # emit a swap for a residual the preview already hid.
            if base_balance and base_balance.balance_usd > MIN_TEARDOWN_DUST_USD:
                slippage = max(Decimal("0.03"), self._max_slippage) if mode == TeardownMode.HARD else self._max_slippage
                intents.append(
                    Intent.swap(
                        from_token=self.base_token,
                        to_token=self.quote_token,
                        amount=base_balance.balance,
                        max_slippage=slippage,
                        protocol="camelot",
                    )
                )
        except Exception:
            logger.warning("Failed to generate teardown intents", exc_info=True)

        return intents
