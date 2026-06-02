"""Swap-router classification for PancakeSwap V3.

VIB-4928 (PR-3b): see the canonical docstring on
``almanak.connectors.uniswap_v3.swap_classification`` — same shape, scoped to
the PancakeSwap V3 connector. PancakeSwap V3 replaces the canonical Uniswap V3
``3000`` bp tier with ``2500`` bp (the rest mirror Uniswap V3); AUTO selection
defaults to the 2500 bp tier. It uses the SwapRouter02 interface (7-param, no
deadline) — no V1 classification.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.swap_classification_registry import (
    SwapClassificationSpec,
)

SWAP_CLASSIFICATION: tuple[SwapClassificationSpec, ...] = (
    SwapClassificationSpec(
        protocol="pancakeswap_v3",
        fee_tiers=(100, 500, 2500, 10000),
        default_fee_tier=2500,
    ),
)


__all__ = ["SWAP_CLASSIFICATION"]
