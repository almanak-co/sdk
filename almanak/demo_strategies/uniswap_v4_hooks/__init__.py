"""Uniswap V4 Hook-Aware LP Demo Strategy.

Hook-aware concentrated liquidity on Uniswap V4 with dynamic fee hooks.
Forward-looking: compiles and runs once V4 Phases 0-3 are merged.
"""

from .strategy import UniswapV4HooksStrategy

__all__ = ["UniswapV4HooksStrategy"]
