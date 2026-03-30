"""Morpho Blue USDC Supply Yield Paper Trading Strategy.

Paper trades USDC supply to Morpho Blue isolated markets on Ethereum.
Exercises the paper trading pipeline with lending SUPPLY/WITHDRAW lifecycle.
"""

from .strategy import MorphoUSDCYieldPaperStrategy

__all__ = ["MorphoUSDCYieldPaperStrategy"]
