"""Uniswap V4 Swap Demo Strategy.

Demonstrates BUY + SELL swap lifecycle using Uniswap V4's UniversalRouter
with Permit2 approval flow.
"""

from .strategy import UniswapV4SwapStrategy

__all__ = ["UniswapV4SwapStrategy"]
