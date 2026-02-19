"""Morpho Leveraged wstETH Yield Strategy with Dynamic Deleverage.

A production-grade leveraged yield strategy that supplies wstETH as collateral
to Morpho Blue (wstETH/WETH market, 94.5% LLTV), borrows WETH, swaps to more
wstETH, and repeats to build leverage. Continuously monitors health factor and
auto-deleverages when HF drops below threshold.

Example:
    from strategies.incubating.morpho_leverage_lst import MorphoLeverageLSTStrategy
"""

from .strategy import MorphoLeverageLSTStrategy

__all__ = ["MorphoLeverageLSTStrategy"]
