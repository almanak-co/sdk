"""Stablecoin Swap Strategy — Arbitrum USDC/USDT via Uniswap V3.

Simple swap strategy that converts USDC to USDT when balance exceeds
a configurable threshold. Demonstrates the basic swap flow on Arbitrum.

NOTE: Originally used Fluid DEX but the USDC/USDT pool on Arbitrum has
insufficient liquidity. Switched to Uniswap V3 which has deep stablecoin
pools on Arbitrum.

Usage:
    almanak strat run -d almanak/demo_strategies/fluid_swap_arb --network anvil --once
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
    description="Stablecoin swap on Arbitrum — trades USDC/USDT via Uniswap V3",
    version="1.1.0",
    author="Almanak",
    tags=["demo", "swap", "stablecoin", "arbitrum", "uniswap_v3"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="arbitrum",
)
class FluidSwapStrategy(IntentStrategy):
    """Simple USDC->USDT swap strategy on Arbitrum.

    On every iteration:
    1. Checks USDC balance
    2. If USDC > trade_size: swaps USDC -> USDT via Uniswap V3
    3. Otherwise: holds
    """

    def decide(self, market: MarketSnapshot) -> Intent:
        """Decide whether to swap USDC -> USDT."""
        trade_size = Decimal(str(self.config.get("trade_size_usd", 100)))
        max_slippage = Decimal(str(self.config.get("max_slippage_bps", 50))) / Decimal("10000")

        # Check USDC balance
        usdc_balance = market.balance("USDC")
        usdc_usd = usdc_balance.balance_usd if usdc_balance else Decimal("0")

        logger.info(f"USDC balance: ${usdc_usd:.2f}, trade_size: ${trade_size:.2f}")

        if usdc_usd >= trade_size:
            logger.info(f"Swapping ${trade_size} USDC -> USDT via Uniswap V3")
            return Intent.swap(
                from_token="USDC",
                to_token="USDT",
                amount_usd=trade_size,
                max_slippage=max_slippage,
                protocol="uniswap_v3",
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
