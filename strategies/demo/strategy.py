"""Demo Strategies - Entry point for auto-discovery.

This file exists to allow the strategy auto-discovery to find and import
all demo strategies. Each subdirectory contains a tutorial strategy.

The auto-discovery looks for strategies/<name>/strategy.py files,
so this file imports all demo strategies to register them.
"""

# Import all demo strategies to register them via @almanak_strategy decorator
from .aave_borrow import AaveBorrowStrategy
from .enso_rsi import EnsoRSIStrategy
from .enso_uniswap_arbitrage import EnsoUniswapArbitrageStrategy
from .gmx_perps import GMXPerpsStrategy
from .uniswap_lp import UniswapLPStrategy
from .uniswap_rsi import UniswapRSIStrategy

# Export all strategy classes
__all__ = [
    "UniswapRSIStrategy",
    "UniswapLPStrategy",
    "AaveBorrowStrategy",
    "GMXPerpsStrategy",
    "EnsoRSIStrategy",
    "EnsoUniswapArbitrageStrategy",
]
