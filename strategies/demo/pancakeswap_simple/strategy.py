"""
PancakeSwap V3 Simple Swap Strategy.

This is a minimal strategy to test PancakeSwap V3 swap execution.
It simply swaps WETH -> USDC on every call (no RSI, no conditions).

Purpose: Debug and verify PancakeSwap V3 integration works correctly.
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
    name="demo_pancakeswap_simple",
    description="Simple WETH->USDC swap on PancakeSwap V3 for testing",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "pancakeswap", "debug"],
    supported_chains=["arbitrum"],
    supported_protocols=["pancakeswap_v3"],
    intent_types=["SWAP"],
)
class PancakeSwapSimpleStrategy(IntentStrategy):
    """Simple PancakeSwap V3 swap strategy for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.swap_amount_usd = Decimal(str(self.get_config("swap_amount_usd", "10")))
        self.max_slippage = Decimal(str(self.get_config("max_slippage", "0.01")))
        self.from_token = self.get_config("from_token", "WETH")
        self.to_token = self.get_config("to_token", "USDC")

        logger.info(
            f"PancakeSwapSimpleStrategy initialized: "
            f"swap ${self.swap_amount_usd} {self.from_token} -> {self.to_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Always swap - no conditions, just execute."""
        # IMPORTANT: Fetch prices for tokens we'll swap
        # This populates the price cache used by IntentCompiler
        from_price = market.price(self.from_token)
        to_price = market.price(self.to_token)
        logger.info(f"Prices: {self.from_token}=${from_price:.2f}, {self.to_token}=${to_price:.6f}")

        # Get balances to verify we have funds (skip if no balance provider)
        try:
            from_balance = market.balance(self.from_token)
            logger.info(
                f"Balance: {from_balance.balance} {self.from_token} "
                f"(${from_balance.balance_usd:.2f})"
            )

            # Check sufficient balance
            if from_balance.balance_usd < self.swap_amount_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.from_token}: "
                    f"${from_balance.balance_usd:.2f} < ${self.swap_amount_usd}"
                )
        except ValueError:
            logger.warning("Balance check unavailable, proceeding with swap")

        # Execute swap
        logger.info(
            f"Swapping ${self.swap_amount_usd} {self.from_token} -> {self.to_token} "
            f"via PancakeSwap V3"
        )

        return Intent.swap(
            from_token=self.from_token,
            to_token=self.to_token,
            amount_usd=self.swap_amount_usd,
            max_slippage=self.max_slippage,
            protocol="pancakeswap_v3",
        )

if __name__ == "__main__":
    print("=" * 60)
    print("PancakeSwapSimpleStrategy - Debug Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {PancakeSwapSimpleStrategy.STRATEGY_NAME}")
    print(f"Supported Chains: {PancakeSwapSimpleStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {PancakeSwapSimpleStrategy.SUPPORTED_PROTOCOLS}")
    print("\nTo test on Anvil:")
    print("  python strategies/demo/pancakeswap_simple/run_anvil.py")
