"""Swap-router classification for Uniswap V3 + the Agni Finance fork.

VIB-4928 (PR-3b): the connector declares its swap-router-ABI + fee-tier
classification as a ``SwapClassificationSpec`` tuple that the strategy-side
``SWAP_CLASSIFICATION_REGISTRY`` fans out over. The intent compiler's
``SWAP_FEE_TIERS`` / ``DEFAULT_SWAP_FEE_TIER`` / ``SWAP_ROUTER_V1_PROTOCOLS`` /
``SWAP_ROUTER_V1_CHAIN_OVERRIDES`` / ``SWAP_ROUTER_ALGEBRA_PROTOCOLS`` views
derive from the registry — no framework module imports this connector directly.

This connector owns two slugs:

* ``uniswap_v3`` — canonical Uniswap V3 (SwapRouter02 / 7-param everywhere,
  except the 0G-chain Jaine DEX deployment, which accepts only the V1 8-arg
  form).
* ``agni_finance`` — Uniswap V3 fork on Mantle that rides on the Uniswap V3
  receipt parser / adapter, using the original V1-style SwapRouter (with
  deadline) on Mantle.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.swap_classification_registry import (
    SwapClassificationSpec,
)

SWAP_CLASSIFICATION: tuple[SwapClassificationSpec, ...] = (
    SwapClassificationSpec(
        protocol="uniswap_v3",
        fee_tiers=(100, 500, 3000, 10000),
        default_fee_tier=3000,
        # Uniswap V3 uses SwapRouter02 (7-param) on every chain except 0G,
        # where the Jaine DEX SwapRouter accepts only the V1 8-arg form.
        router_v1_chains=("zerog",),
    ),
    SwapClassificationSpec(
        protocol="agni_finance",
        fee_tiers=(100, 500, 2500, 3000, 10000),
        # Heuristic picks 500 for USDC/WETH pairs; 3000 is the safer default
        # for everything else.
        default_fee_tier=3000,
        # Agni Finance on Mantle uses the original V1-style SwapRouter (8-param,
        # with deadline).
        router_v1_chains=("mantle",),
    ),
)


__all__ = ["SWAP_CLASSIFICATION"]
