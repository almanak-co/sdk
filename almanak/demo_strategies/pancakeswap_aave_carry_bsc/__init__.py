"""PancakeSwap V3 + Aave V3 Carry Trade on BSC.

T2 composition: supply WBNB collateral to Aave V3, borrow USDC,
swap USDC to USDT via PancakeSwap V3, then teardown in reverse.
"""

from .strategy import PancakeswapAaveCarryBscStrategy

__all__ = ["PancakeswapAaveCarryBscStrategy"]
