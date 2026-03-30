"""Uniswap V3 Swap Lifecycle on Optimism.

Two-phase swap strategy testing Uniswap V3 on Optimism (chain_id=10).

Phase 1 (first --once): BUY — swap USDC -> WETH
Phase 2 (second --once): SELL — swap WETH -> USDC

Uses force_action config to control which phase executes.
This validates the full swap lifecycle including receipt parsing and
swap enrichment on Optimism's L2 gas model. Uniswap V3 swap has been
tested on Arbitrum, Polygon, Avalanche, Ethereum, and BSC but never
standalone on Optimism.

Kitchen Loop iter 137 — VIB-1959
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_uniswap_v3_swap_optimism",
    description="Uniswap V3 swap lifecycle (BUY/SELL) on Optimism",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "uniswap_v3", "swap", "optimism", "lifecycle"],
    supported_chains=["optimism"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="optimism",
)
class UniswapV3SwapOptimismStrategy(IntentStrategy):
    """Two-phase Uniswap V3 swap on Optimism.

    Config:
        force_action: "buy" or "sell" to force a specific phase
        trade_size_usd: USD amount per swap (default: 10)
        max_slippage: Max slippage as decimal (default: 0.01 = 1%)
        base_token: Token to buy/sell (default: WETH)
        quote_token: Quote token (default: USDC)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "10")))
        self.max_slippage = Decimal(str(self.get_config("max_slippage", "0.01")))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")
        self.force_action = self.get_config("force_action", "buy")

        logger.info(
            f"UniswapV3SwapOptimismStrategy: force_action={self.force_action}, "
            f"trade=${self.trade_size_usd} {self.base_token}/{self.quote_token}"
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
        """BUY phase: swap quote_token -> base_token."""
        try:
            quote_bal = market.balance(self.quote_token)
            logger.info(
                f"BUY check: {quote_bal.balance} {self.quote_token} "
                f"(${quote_bal.balance_usd:.2f})"
            )
            if quote_bal.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.quote_token}: "
                    f"${quote_bal.balance_usd:.2f} < ${self.trade_size_usd}"
                )
        except ValueError:
            logger.warning("Balance check unavailable, proceeding with BUY")

        logger.info(
            f"BUY: ${self.trade_size_usd} {self.quote_token} -> {self.base_token} "
            f"via Uniswap V3"
        )
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=self.trade_size_usd,
            max_slippage=self.max_slippage,
            protocol="uniswap_v3",
        )

    def _sell(self, market: MarketSnapshot) -> Intent:
        """SELL phase: swap base_token -> quote_token."""
        try:
            base_bal = market.balance(self.base_token)
            logger.info(
                f"SELL check: {base_bal.balance} {self.base_token} "
                f"(${base_bal.balance_usd:.2f})"
            )
            if base_bal.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.base_token}: "
                    f"${base_bal.balance_usd:.2f} < ${self.trade_size_usd}"
                )
        except ValueError:
            logger.warning("Balance check unavailable, proceeding with SELL")

        logger.info(
            f"SELL: ${self.trade_size_usd} {self.base_token} -> {self.quote_token} "
            f"via Uniswap V3"
        )
        return Intent.swap(
            from_token=self.base_token,
            to_token=self.quote_token,
            amount_usd=self.trade_size_usd,
            max_slippage=self.max_slippage,
            protocol="uniswap_v3",
        )

    def get_open_positions(self):
        """No persistent positions — swap-only strategy."""
        from almanak.framework.teardown import TeardownPositionSummary

        return TeardownPositionSummary.empty(self.strategy_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        """No teardown needed for swap-only strategy."""
        return []
