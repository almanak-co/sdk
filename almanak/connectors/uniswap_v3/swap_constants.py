"""Swap-router classification + fee-tier metadata for Uniswap V3 + forks.

VIB-4872 (W6-followup): centralises the per-protocol swap-tier-selection
metadata that lived in scattered dicts in
``almanak/framework/intents/compiler_constants.py``. The Uniswap V3
connector publishes:

* The canonical Uniswap V3 entries (fee tiers, default fee tier, V1-vs-V2
  router-interface classification).
* The Agni Finance entries (Uniswap V3 fork on Mantle that rides on the
  Uniswap V3 receipt parser / adapter, hence the data lives here).

Other DEX connectors publish their own copies of the same shape via a
sibling ``swap_constants.py`` module (PancakeSwap V3, SushiSwap V3,
Camelot). The framework derives the legacy module-level dicts at view-
build time by reading from each connector.

The shape:

* ``SWAP_FEE_TIERS`` — protocol name -> tuple of supported fee tiers in
  bps (e.g. ``(100, 500, 3000, 10000)`` for canonical Uniswap V3).
* ``DEFAULT_SWAP_FEE_TIER`` — protocol name -> fee tier in bps the
  heuristic uses when AUTO selection has no signal.
* ``SWAP_ROUTER_V1_PROTOCOLS`` — protocols using the original SwapRouter
  ABI (8-param ``exactInputSingle`` WITH ``deadline``) rather than
  SwapRouter02 (7-param, no deadline).
* ``SWAP_ROUTER_V1_CHAIN_OVERRIDES`` — per-chain overrides where a V3
  fork uses the V1 interface on a specific chain (Agni on Mantle, JAINE
  DEX on 0G).

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

SWAP_FEE_TIERS: dict[str, tuple[int, ...]] = {
    "uniswap_v3": (100, 500, 3000, 10000),
    "agni_finance": (100, 500, 2500, 3000, 10000),
}

DEFAULT_SWAP_FEE_TIER: dict[str, int] = {
    "uniswap_v3": 3000,
    # Agni Finance on Mantle: heuristic picks 500 for USDC/WETH pairs,
    # 3000 is safer default for others.
    "agni_finance": 3000,
}

# Uniswap V3 itself uses SwapRouter02 everywhere (7-param). Agni Finance
# on Mantle is a V3 fork that uses the original V1-style SwapRouter (with
# deadline) — that override surfaces in ``SWAP_ROUTER_V1_CHAIN_OVERRIDES``.
SWAP_ROUTER_V1_PROTOCOLS: frozenset[str] = frozenset()

# Per-chain overrides where a V3 fork uses the V1 router interface.
SWAP_ROUTER_V1_CHAIN_OVERRIDES: dict[str, frozenset[str]] = {
    "mantle": frozenset({"agni_finance"}),
    # Jaine DEX SwapRouter on 0G chain accepts only the V1 8-arg form.
    "zerog": frozenset({"uniswap_v3"}),
}


__all__ = [
    "DEFAULT_SWAP_FEE_TIER",
    "SWAP_FEE_TIERS",
    "SWAP_ROUTER_V1_CHAIN_OVERRIDES",
    "SWAP_ROUTER_V1_PROTOCOLS",
]
