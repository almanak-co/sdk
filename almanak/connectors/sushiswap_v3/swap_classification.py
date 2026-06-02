"""Swap-router classification for SushiSwap V3.

VIB-4928 (PR-3b): see the canonical docstring on
``almanak.connectors.uniswap_v3.swap_classification`` — same shape, scoped to
the SushiSwap V3 connector, which uses the original SwapRouter interface
(V1-style, 8-param ``exactInputSingle`` WITH ``deadline``) on every chain.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.swap_classification_registry import (
    SwapClassificationSpec,
)

SWAP_CLASSIFICATION: tuple[SwapClassificationSpec, ...] = (
    SwapClassificationSpec(
        protocol="sushiswap_v3",
        fee_tiers=(100, 500, 3000, 10000),
        default_fee_tier=3000,
        router_v1=True,
    ),
)


__all__ = ["SWAP_CLASSIFICATION"]
