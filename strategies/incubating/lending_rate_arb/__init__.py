"""Lending Rate Arbitrage Strategy Module.

This module provides a strategy that captures lending rate differentials
across DeFi protocols (Aave V3, Morpho Blue, Compound V3).

Example:
    from strategies.lending_rate_arb import (
        LendingRateArbStrategy,
        LendingRateArbConfig,
    )

    config = LendingRateArbConfig(
        strategy_id="lending_arb_1",
        chain="ethereum",
        wallet_address="0x...",
        tokens=["USDC", "USDT", "DAI"],
        protocols=["aave_v3", "morpho_blue", "compound_v3"],
        min_spread_bps=50,  # 0.5% minimum spread
    )

    strategy = LendingRateArbStrategy(config=config)
"""

from .config import LendingRateArbConfig
from .strategy import (
    LendingRateArbStrategy,
    RebalanceOpportunity,
    TokenPosition,
)

__all__ = [
    "LendingRateArbStrategy",
    "LendingRateArbConfig",
    "TokenPosition",
    "RebalanceOpportunity",
]
