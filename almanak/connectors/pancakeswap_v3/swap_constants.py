"""Swap-router classification + fee-tier metadata for PancakeSwap V3.

VIB-4872 (W6-followup): see the canonical docstring on
``almanak.connectors.uniswap_v3.swap_constants`` — same shape, scoped
to the PancakeSwap V3 connector. PancakeSwap V3's fee tiers replace
the canonical Uniswap V3 ``3000`` bp tier with ``2500`` bp (the rest
mirror Uniswap V3); the default for AUTO selection picks the 2500 bp
tier.

PancakeSwap V3 uses the SwapRouter02 interface (7-param, no deadline);
no V1-classification surface.
"""

from __future__ import annotations

SWAP_FEE_TIERS: dict[str, tuple[int, ...]] = {
    "pancakeswap_v3": (100, 500, 2500, 10000),
}

DEFAULT_SWAP_FEE_TIER: dict[str, int] = {
    "pancakeswap_v3": 2500,
}

SWAP_ROUTER_V1_PROTOCOLS: frozenset[str] = frozenset()

SWAP_ROUTER_V1_CHAIN_OVERRIDES: dict[str, frozenset[str]] = {}


__all__ = [
    "DEFAULT_SWAP_FEE_TIER",
    "SWAP_FEE_TIERS",
    "SWAP_ROUTER_V1_CHAIN_OVERRIDES",
    "SWAP_ROUTER_V1_PROTOCOLS",
]
