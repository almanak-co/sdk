"""Leverage Loop Cross-Chain Strategy.

NORTH STAR TEST CASE for Multi-Chain PRD Implementation.

This strategy is the canonical validation test for the entire multi-chain system.
If this strategy doesn't work elegantly, the implementation has gaps.

Flow:
    1. Swap USDC → WETH on Base (Uniswap V3)
    2. Bridge WETH from Base → Arbitrum (Across/Stargate)
    3. Supply WETH as collateral on Aave V3 (Arbitrum)
    4. Borrow USDC against collateral (Arbitrum)
    5. Open ETH long perps on GMX V2 (Arbitrum)

See: tasks/prd-multi-chain-strategy-support.md "NORTH STAR" section
"""

from .strategy import LeverageLoopConfig, LeverageLoopStrategy

__all__ = ["LeverageLoopStrategy", "LeverageLoopConfig"]
