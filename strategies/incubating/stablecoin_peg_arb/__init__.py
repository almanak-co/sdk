"""Stablecoin Peg Arbitrage Strategy.

This strategy monitors stablecoin prices and profits from depeg events
by executing Curve swaps when stablecoins trade away from their $1.00 peg.

Example:
    from strategies.stablecoin_peg_arb import (
        StablecoinPegArbStrategy,
        StablecoinPegArbConfig,
    )

    config = StablecoinPegArbConfig(
        strategy_id="peg_arb_1",
        chain="ethereum",
        wallet_address="0x...",
        stablecoins=["USDC", "USDT", "DAI", "FRAX"],
        depeg_threshold_bps=50,  # 0.5% depeg triggers opportunity
    )

    strategy = StablecoinPegArbStrategy(config)
"""

from .config import StablecoinPegArbConfig
from .strategy import (
    CURVE_POOL_TOKENS,
    DepegDirection,
    DepegOpportunity,
    PegArbState,
    StablecoinPegArbStrategy,
    get_pool_for_tokens,
)

__all__ = [
    "StablecoinPegArbStrategy",
    "StablecoinPegArbConfig",
    "PegArbState",
    "DepegDirection",
    "DepegOpportunity",
    "CURVE_POOL_TOKENS",
    "get_pool_for_tokens",
]
