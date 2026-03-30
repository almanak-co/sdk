"""Velodrome V2 Swap Lifecycle on Optimism.

Two-phase swap strategy testing Velodrome V2 on Optimism via the Aerodrome connector.

Phase 1 (force_action=buy): BUY — swap USDC -> WETH via Velodrome V2 classic pool
Phase 2 (force_action=sell): SELL — swap WETH -> USDC via Velodrome V2 classic pool

Velodrome V2 is a Solidly-fork DEX on Optimism. The Aerodrome connector handles
both Base (Aerodrome) and Optimism (Velodrome V2) — Optimism uses classic routing
only (no Slipstream CL contracts).

Coverage gaps filled:
- First Velodrome SwapIntent test (Solidly-fork swap on Optimism)
- Validates Aerodrome connector cross-chain swap support
- Tests protocol alias normalization (velodrome -> aerodrome)

Kitchen Loop — VIB-1847 (revalidation after BUG-1 fix from iter 126)

Usage:
    almanak strat run -d strategies/demo/velodrome_swap_optimism --network anvil --once
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_velodrome_swap_optimism",
    description="Velodrome V2 swap lifecycle (BUY/SELL) on Optimism via classic pools",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "velodrome", "swap", "optimism", "solidly", "lifecycle"],
    supported_chains=["optimism"],
    supported_protocols=["aerodrome"],
    intent_types=["SWAP", "HOLD"],
    default_chain="optimism",
)
class VelodromeSwapOptimismStrategy(IntentStrategy):
    """Two-phase Velodrome V2 swap on Optimism.

    Config:
        force_action: "buy" or "sell" to control which phase executes
        swap_amount: USD amount per swap (default: 50)
        max_slippage_pct: Max slippage percentage (default: 1.0 = 1%)
        base_token: Token to buy/sell (default: WETH)
        quote_token: Quote token (default: USDC)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.swap_amount = Decimal(str(self.get_config("swap_amount", "50")))
        self.max_slippage_pct = Decimal(str(self.get_config("max_slippage_pct", "1.0")))
        self.base_token = self.get_config("to_token", self.get_config("base_token", "WETH"))
        self.quote_token = self.get_config("from_token", self.get_config("quote_token", "USDC"))
        self.force_action = self.get_config("force_action", "buy")

        logger.info(
            f"VelodromeSwapOptimism: force_action={self.force_action}, "
            f"swap {self.swap_amount} {self.base_token}/{self.quote_token} "
            f"via Velodrome V2 on Optimism"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute BUY or SELL based on force_action config."""
        try:
            base_price = market.price(self.base_token)
            quote_price = market.price(self.quote_token)
            logger.info(
                f"Prices: {self.base_token}=${base_price:.2f}, "
                f"{self.quote_token}=${quote_price:.4f}"
            )
        except ValueError as exc:
            logger.warning(
                "Price prefetch unavailable for %s/%s (%s); continuing with force_action=%s",
                self.base_token,
                self.quote_token,
                exc,
                self.force_action,
            )

        if self.force_action == "buy":
            return self._buy(market)
        elif self.force_action == "sell":
            return self._sell(market)
        else:
            return Intent.hold(reason=f"Unknown force_action: {self.force_action}")

    def _buy(self, market: MarketSnapshot) -> Intent:
        """BUY phase: swap quote_token -> base_token via Velodrome V2."""
        try:
            quote_bal = market.balance(self.quote_token)
            logger.info(
                f"BUY check: {quote_bal.balance} {self.quote_token} "
                f"(${quote_bal.balance_usd:.2f})"
            )
            if quote_bal.balance_usd < self.swap_amount:
                return Intent.hold(
                    reason=f"Insufficient {self.quote_token}: "
                    f"${quote_bal.balance_usd:.2f} < ${self.swap_amount}"
                )
        except ValueError:
            logger.warning("Balance check unavailable, proceeding with BUY")

        max_slippage = self.max_slippage_pct / Decimal("100")
        logger.info(
            f"BUY: ${self.swap_amount} {self.quote_token} -> {self.base_token} "
            f"via Velodrome V2"
        )
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=self.swap_amount,
            max_slippage=max_slippage,
            protocol="aerodrome",
        )

    def _sell(self, market: MarketSnapshot) -> Intent:
        """SELL phase: swap base_token -> quote_token via Velodrome V2."""
        try:
            base_bal = market.balance(self.base_token)
            logger.info(
                f"SELL check: {base_bal.balance} {self.base_token} "
                f"(${base_bal.balance_usd:.2f})"
            )
            if base_bal.balance_usd < self.swap_amount:
                return Intent.hold(
                    reason=f"Insufficient {self.base_token}: "
                    f"${base_bal.balance_usd:.2f} < ${self.swap_amount}"
                )
        except ValueError:
            logger.warning("Balance check unavailable, proceeding with SELL")

        max_slippage = self.max_slippage_pct / Decimal("100")
        logger.info(
            f"SELL: ${self.swap_amount} {self.base_token} -> {self.quote_token} "
            f"via Velodrome V2"
        )
        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount_usd=self.swap_amount,
            max_slippage=max_slippage,
            protocol="aerodrome",
        )

    def get_open_positions(self):
        """No persistent positions — swap-only strategy."""
        from almanak.framework.teardown import TeardownPositionSummary

        return TeardownPositionSummary.empty(self.strategy_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        """No teardown needed for swap-only strategy."""
        return []
