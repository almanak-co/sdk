"""Cross-Chain Arbitrage Strategy.

This strategy monitors prices across multiple chains and executes arbitrage
when price spreads exceed configurable thresholds after accounting for:
- Bridge fees (0.1% - 0.5% depending on provider)
- Swap slippage (configurable, default 0.3%)
- Gas costs per chain
- Bridge latency risk

Flow:
    1. Monitor token price on all configured chains
    2. When spread exceeds threshold, calculate net profitability
    3. If profitable after fees:
       - Buy token on cheaper chain
       - Bridge token to more expensive chain
       - Sell token for profit

Key Features:
    - Multi-chain price monitoring (Arbitrum, Optimism, Base)
    - Bridge fee and latency accounting
    - Net profit calculations after all fees
    - Support for multi-Anvil fork testing

See: README.md for setup and configuration instructions
"""

from .config import (
    BRIDGE_FEES_BPS,
    BRIDGE_LATENCY_SECONDS,
    SUPPORTED_CHAINS,
    CrossChainArbConfig,
)
from .strategy import (
    ArbState,
    CrossChainArbitrageConfig,
    CrossChainArbitrageStrategy,
    CrossChainOpportunity,
)

__all__ = [
    # Strategy
    "CrossChainArbitrageStrategy",
    # Config (two names for compatibility)
    "CrossChainArbitrageConfig",
    "CrossChainArbConfig",
    # Types
    "CrossChainOpportunity",
    "ArbState",
    # Constants
    "BRIDGE_FEES_BPS",
    "BRIDGE_LATENCY_SECONDS",
    "SUPPORTED_CHAINS",
]
