"""Swap-router classification for Camelot (Algebra V1.9 DEX).

VIB-4928 (PR-3b): Camelot uses the Algebra V1.9 router interface (VIB-1636).
The router signature is:

    exactInputSingle(
        (address tokenIn, address tokenOut, address recipient,
         uint256 deadline, uint256 amountIn, uint256 amountOutMinimum,
         uint160 limitSqrtPrice)
    ) -> uint256 amountOut

Function selector ``0xbc651188``. The struct lacks a ``fee`` field (Algebra
pools quote their own dynamic fee), so Camelot publishes no fee tiers — the swap
adapter's fee-tier branch is bypassed entirely for this protocol. The
classification surfaces only via ``SWAP_ROUTER_ALGEBRA_PROTOCOLS``.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.swap_classification_registry import (
    SwapClassificationSpec,
)

SWAP_CLASSIFICATION: tuple[SwapClassificationSpec, ...] = (
    SwapClassificationSpec(
        protocol="camelot",
        router_algebra=True,
    ),
)


__all__ = ["SWAP_CLASSIFICATION"]
