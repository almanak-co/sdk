"""
===============================================================================
Uniswap V4 Swap Strategy — BUY + SELL via UniversalRouter
===============================================================================

Demonstrates Uniswap V4 token swaps using the UniversalRouter with Permit2
approval flow. This is the simplest V4 strategy — pure swap lifecycle.

WHAT THIS STRATEGY DOES:
1. BUY: Swaps USDC -> WETH via Uniswap V4 UniversalRouter
2. SELL: Swaps WETH -> USDC via Uniswap V4 UniversalRouter
3. DONE: Holds after completing the full cycle

KEY V4 SWAP DIFFERENCES FROM V3:
- Routes through UniversalRouter (not SwapRouter)
- Uses Permit2 for token approvals (ERC-20 approve -> Permit2 -> UniversalRouter)
- V4_SWAP_EXACT_IN_SINGLE command (0x06) in UniversalRouter.execute()
- Pool keys include hooks address (zero address for hookless pools)

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_uniswap_v4_swap",
    description="Uniswap V4 swap demo — BUY + SELL lifecycle via UniversalRouter on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "swap", "uniswap-v4", "arbitrum", "v4", "universal-router"],
    supported_chains=["arbitrum", "ethereum", "base"],
    supported_protocols=["uniswap_v4"],
    intent_types=["SWAP", "HOLD"],
    default_chain="arbitrum",
)
class UniswapV4SwapStrategy(IntentStrategy):
    """Uniswap V4 swap strategy demonstrating BUY + SELL lifecycle.

    State machine:
        BUY -> SELL -> DONE

    Uses protocol="uniswap_v4" to route through the V4 compiler path,
    which builds UniversalRouter.execute() transactions with Permit2 approvals.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "5")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        # State machine: BUY -> SELL -> DONE
        self._phase = "BUY"

        logger.info(
            f"UniswapV4SwapStrategy initialized: "
            f"trade_size=${self.trade_size_usd}, "
            f"pair={self.base_token}/{self.quote_token}, "
            f"slippage={self.max_slippage_bps}bps"
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        """Execute BUY -> SELL -> DONE lifecycle."""
        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        base_price = market.price(self.base_token)
        logger.info(f"{self.base_token} price: {format_usd(base_price)}")

        if self._phase == "BUY":
            quote_bal = market.balance(self.quote_token)
            if quote_bal.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.quote_token}: "
                    f"{format_usd(quote_bal.balance_usd)} < {format_usd(self.trade_size_usd)}"
                )

            logger.info(
                f"V4 BUY: {format_usd(self.trade_size_usd)} "
                f"{self.quote_token} -> {self.base_token}"
            )
            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=max_slippage,
                protocol="uniswap_v4",
            )

        elif self._phase == "SELL":
            base_bal = market.balance(self.base_token)
            if base_bal.balance <= 0:
                return Intent.hold(
                    reason=f"No {self.base_token} to sell"
                )

            logger.info(
                f"V4 SELL: all {self.base_token} ({base_bal.balance:.6f}) "
                f"-> {self.quote_token}"
            )
            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="uniswap_v4",
            )

        else:
            return Intent.hold(reason="V4 swap lifecycle complete (BUY + SELL done)")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Advance state machine on successful execution."""
        if not success:
            logger.warning(f"V4 swap failed in {self._phase} phase")
            return

        if self._phase == "BUY":
            swap_amounts = getattr(result, "swap_amounts", None)
            if swap_amounts:
                logger.info(
                    f"V4 BUY complete: "
                    f"in={swap_amounts.amount_in_decimal:.6f} {swap_amounts.token_in or self.quote_token}, "
                    f"out={swap_amounts.amount_out_decimal:.6f} {swap_amounts.token_out or self.base_token}"
                )
            self._phase = "SELL"
            logger.info("Phase advanced: BUY -> SELL")

        elif self._phase == "SELL":
            swap_amounts = getattr(result, "swap_amounts", None)
            if swap_amounts:
                logger.info(
                    f"V4 SELL complete: "
                    f"in={swap_amounts.amount_in_decimal:.6f} {swap_amounts.token_in or self.base_token}, "
                    f"out={swap_amounts.amount_out_decimal:.6f} {swap_amounts.token_out or self.quote_token}"
                )
            self._phase = "DONE"
            logger.info("Phase advanced: SELL -> DONE. V4 swap lifecycle complete.")

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        try:
            market = self.create_market_snapshot()
            base_balance = market.balance(self.base_token)
            if base_balance.balance > 0:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id="v4_swap_token_0",
                        chain=self.chain,
                        protocol="uniswap_v4",
                        value_usd=base_balance.balance_usd,
                        details={
                            "asset": self.base_token,
                            "balance": str(base_balance.balance),
                        },
                    )
                )
        except Exception:
            logger.warning("Failed to query balance for teardown")

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_uniswap_v4_swap"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")

        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="uniswap_v4",
            )
        ]
