"""Swap-router classification metadata for Camelot (Algebra V1.9 DEX).

VIB-4872 (W6-followup): Camelot uses the Algebra V1.9 router interface
(VIB-1636). The router signature is:

    exactInputSingle(
        (address tokenIn, address tokenOut, address recipient,
         uint256 deadline, uint256 amountIn, uint256 amountOutMinimum,
         uint160 limitSqrtPrice)
    ) -> uint256 amountOut

Function selector ``0xbc651188``. The struct lacks a ``fee`` field
(Algebra pools quote their own dynamic fee), so there are no
``SWAP_FEE_TIERS`` / ``DEFAULT_SWAP_FEE_TIER`` entries for Camelot — the
swap adapter's fee-tier branch is bypassed entirely for this protocol.
"""

from __future__ import annotations

SWAP_ROUTER_ALGEBRA_PROTOCOLS: frozenset[str] = frozenset({"camelot"})


__all__ = ["SWAP_ROUTER_ALGEBRA_PROTOCOLS"]
