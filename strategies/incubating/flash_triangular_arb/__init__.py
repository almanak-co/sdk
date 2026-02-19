"""Flash Loan Triangular Arbitrage Strategy.

This strategy identifies triangular arbitrage opportunities across DEXs
(e.g., ETH->USDC->WBTC->ETH) and executes them atomically using flash loans.

Key Features:
    - Multi-hop path finding (3-4 tokens)
    - Flash loan execution for capital-free arbitrage
    - Automatic provider selection (Aave/Balancer)
    - Configurable profit thresholds and gas limits
    - Support for multiple token paths

Example:
    If ETH->USDC->WBTC->ETH yields profit:
    1. Flash loan ETH from Balancer (0% fee)
    2. Swap ETH -> USDC on DEX A
    3. Swap USDC -> WBTC on DEX B
    4. Swap WBTC -> ETH on DEX C
    5. Repay flash loan, keep profit

    All steps execute atomically - if any step fails, entire trade reverts.
"""

from .config import FlashTriangularArbConfig
from .strategy import FlashTriangularArbStrategy, TriangularArbState, TriangularOpportunity

__all__ = [
    "FlashTriangularArbStrategy",
    "FlashTriangularArbConfig",
    "TriangularArbState",
    "TriangularOpportunity",
]
