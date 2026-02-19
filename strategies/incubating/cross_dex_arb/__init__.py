"""Cross-DEX Spot Arbitrage Strategy.

This module provides a strategy that captures price differences across DEXs
using flash loans for atomic, capital-efficient execution.

Key Features:
    - Multi-DEX price comparison (Uniswap V3, Curve, Enso)
    - Flash loan execution for capital efficiency
    - Configurable profit thresholds and gas limits
    - Support for multiple token pairs
"""

from .config import CrossDexArbConfig
from .strategy import CrossDexArbStrategy

__all__ = [
    "CrossDexArbStrategy",
    "CrossDexArbConfig",
]
