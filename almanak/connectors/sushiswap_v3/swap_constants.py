"""Swap-router classification + fee-tier metadata for SushiSwap V3.

VIB-4872 (W6-followup): see the canonical docstring on
``almanak.connectors.uniswap_v3.swap_constants`` — same shape, scoped
to the SushiSwap V3 connector.

SushiSwap V3 uses the original SwapRouter interface (V1-style, 8-param
``exactInputSingle`` WITH ``deadline``) — that classification surfaces
via ``SWAP_ROUTER_V1_PROTOCOLS``.
"""

from __future__ import annotations

SWAP_FEE_TIERS: dict[str, tuple[int, ...]] = {
    "sushiswap_v3": (100, 500, 3000, 10000),
}

DEFAULT_SWAP_FEE_TIER: dict[str, int] = {
    "sushiswap_v3": 3000,
}

SWAP_ROUTER_V1_PROTOCOLS: frozenset[str] = frozenset({"sushiswap_v3"})

SWAP_ROUTER_V1_CHAIN_OVERRIDES: dict[str, frozenset[str]] = {}


__all__ = [
    "DEFAULT_SWAP_FEE_TIER",
    "SWAP_FEE_TIERS",
    "SWAP_ROUTER_V1_CHAIN_OVERRIDES",
    "SWAP_ROUTER_V1_PROTOCOLS",
]
