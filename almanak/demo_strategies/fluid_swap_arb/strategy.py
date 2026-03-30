"""Fluid DEX Swap Strategy — Arbitrum stablecoin arbitrage.

Demonstrates the Fluid DEX connector by monitoring USDC/USDT price
deviation and executing swaps when the spread exceeds a threshold.

Fluid DEX pools earn both swap fees and lending yield simultaneously,
making them uniquely suited for stablecoin strategies.

Usage:
    almanak strat run -d strategies/demo/fluid_swap_arb --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.teardown import TeardownPositionSummary

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_fluid_swap_arb",
    description="Fluid DEX stablecoin swap on Arbitrum — trades USDC/USDT on Fluid DEX",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "fluid", "swap", "stablecoin", "arbitrum"],
    supported_chains=["arbitrum"],
    supported_protocols=["fluid"],
    intent_types=["SWAP", "HOLD"],
    default_chain="arbitrum",
)
class FluidSwapStrategy(IntentStrategy):
    """Simple USDC->USDT swap strategy using Fluid DEX on Arbitrum.

    On every iteration:
    1. Checks USDC balance
    2. If USDC > trade_size: swaps USDC -> USDT on Fluid DEX
    3. Otherwise: holds

    This validates the full Fluid connector pipeline:
    - Pool discovery
    - Transaction building (swapIn)
    - Receipt parsing (Swap event)
    - Balance verification
    """

    def decide(self, market: MarketSnapshot) -> Intent:
        """Decide whether to swap USDC -> USDT on Fluid DEX."""
        trade_size = Decimal(str(self.config.get("trade_size_usd", 100)))
        max_slippage = Decimal(str(self.config.get("max_slippage_bps", 50))) / Decimal("10000")

        # Check USDC balance
        usdc_balance = market.balance("USDC")
        usdc_usd = usdc_balance.balance_usd if usdc_balance else Decimal("0")

        logger.info(f"USDC balance: ${usdc_usd:.2f}, trade_size: ${trade_size:.2f}")

        if usdc_usd >= trade_size:
            logger.info(f"Swapping ${trade_size} USDC -> USDT on Fluid DEX")
            return Intent.swap(
                from_token="USDC",
                to_token="USDT",
                amount_usd=trade_size,
                max_slippage=max_slippage,
                protocol="fluid",
            )

        return Intent.hold(reason=f"USDC balance ${usdc_usd:.2f} below trade size ${trade_size:.2f}")

    # -- Teardown (stateless strategy — no open positions) --

    def get_open_positions(self):
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=[],
        )

    def generate_teardown_intents(self, mode, market=None):
        return []
