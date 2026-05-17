"""VIB-4471 regression — real V3 / Aerodrome / Solidly inputs still flow.

Prior to VIB-4471, ``_clean_pool_address_candidate`` returned any non-slash
input unchanged. The tightening to ``^0x[0-9a-f]{40}$ | ^0x[0-9a-f]{64}$``
must NOT regress any existing legitimate caller. This file pins the real-
world inputs collected from production matrix fixtures and the existing
``test_lp_perp_vault_handlers.py`` suite — every one must still pass through.

If this file regresses, the no-slash tightening over-rejected something the
production system relies on; treat any FAIL here as a hard merge gate.
"""

from __future__ import annotations

import pytest

from almanak.framework.accounting.category_handlers.lp_handler import (
    _clean_pool_address_candidate,
)


# ──────────────────────────────────────────────────────────────────────────────
# 20-byte EVM pool addresses — real on-chain values from existing test suite
# ──────────────────────────────────────────────────────────────────────────────


# (label, address) pairs sourced from real chain captures in the existing
# test suite — keeps the regression coverage anchored in evidence.
_REAL_20_BYTE_POOLS: list[tuple[str, str]] = [
    # Uniswap V3 — Arbitrum WETH/USDC 500 (captured in test_valuation.py
    # and test_lp_perp_vault_handlers.py VIB-4274 / VIB-4396 tests).
    ("uniswap_v3_arbitrum_weth_usdc_500", "0xc6962004f452be9203591991d15f6b388e09e8d0"),
    # Aerodrome v2 (Solidly) — Base USDC/cbETH (representative; the shape
    # is what we're guarding, not the specific pool).
    ("aerodrome_v2_base_pool", "0xcdac0d6c6c59727a65f871236188350531885c43"),
    # Sushiswap V3 — Arbitrum (mirrors uniswap_v3 shape; canonical
    # 40-char hex 0x-prefixed lowercase).
    ("sushiswap_v3_arbitrum_pool", "0x37b1eecf52a4ebf09c69af69f31eb88f96cca44a"),
    # PancakeSwap V3 — Arbitrum (same shape).
    ("pancakeswap_v3_arbitrum_pool", "0x7fcdc2c1ef3e4a0bcc8155a558bb20a7218f2b05"),
]


@pytest.mark.parametrize("label,addr", _REAL_20_BYTE_POOLS, ids=[p[0] for p in _REAL_20_BYTE_POOLS])
def test_real_20_byte_pool_addresses_pass_through(label: str, addr: str) -> None:
    """Every real V3-family / Aerodrome-V2 pool address surveyed today
    must pass ``_clean_pool_address_candidate`` unchanged. Locks the
    20-byte branch open for the protocols the matrix harness covers."""
    assert _clean_pool_address_candidate(addr) == addr, (
        f"VIB-4471 regression: {label} ({addr}) was passing pre-tightening "
        f"but is now rejected — the no-slash tightening over-shrank the "
        f"accepted set."
    )


# ──────────────────────────────────────────────────────────────────────────────
# Solidly-style descriptors — the only stable position identifier for
# classic Aerodrome (no per-pool NPM; descriptor IS the canonical key).
# ──────────────────────────────────────────────────────────────────────────────


# The numeric-tail rule (VIB-4274 / VIB-4396) rejects "TOKEN/TOKEN/<number>"
# descriptors — that's Uniswap V3-style, not Solidly. Solidly's tail is
# "stable" or "volatile" (a non-numeric string). The regression here is the
# Solidly shape, NOT the rejected V3 shape.
_SOLIDLY_DESCRIPTORS: list[str] = [
    "USDC/DAI/stable",
    "WETH/USDC/volatile",
    "USDC/USDT/stable",
    "WETH/cbETH/volatile",
]


@pytest.mark.parametrize("descriptor", _SOLIDLY_DESCRIPTORS)
def test_solidly_descriptors_pass_through(descriptor: str) -> None:
    """Canonical Solidly descriptors (Aerodrome v1 / Velodrome v1) must
    still pass — VIB-4471 only tightened the no-slash branch."""
    assert _clean_pool_address_candidate(descriptor) == descriptor


# ──────────────────────────────────────────────────────────────────────────────
# V3 fee-tier descriptors — were rejected pre-VIB-4471, must still be
# rejected post-VIB-4471 (VIB-4274 / VIB-4396).
# ──────────────────────────────────────────────────────────────────────────────


_V3_FEE_TIER_DESCRIPTORS: list[str] = [
    "WETH/USDC/500",
    "WETH/USDC/3000",
    "WETH/USDC/10000",
    "USDC/USDT/100",
    "weth/usdc/500",  # lowercase variant
]


@pytest.mark.parametrize("descriptor", _V3_FEE_TIER_DESCRIPTORS)
def test_v3_fee_tier_descriptors_still_rejected(descriptor: str) -> None:
    """VIB-4274 / VIB-4396 — V3-style ``TOKEN/TOKEN/<numeric-fee>``
    descriptors must NOT leak into ``accounting_events.pool_address``
    (the runner falls back to ``market_id`` for these venues)."""
    assert _clean_pool_address_candidate(descriptor) == ""


# ──────────────────────────────────────────────────────────────────────────────
# Whitespace / type tolerance — pre-VIB-4471 behaviour is preserved on
# the empty / None branch.
# ──────────────────────────────────────────────────────────────────────────────


def test_leading_trailing_whitespace_stripped_on_20_byte_address() -> None:
    addr = "0xc6962004f452be9203591991d15f6b388e09e8d0"
    assert _clean_pool_address_candidate(f"  {addr}  ") == addr


def test_none_input_returns_empty() -> None:
    assert _clean_pool_address_candidate(None) == ""


def test_empty_string_returns_empty() -> None:
    assert _clean_pool_address_candidate("") == ""


def test_whitespace_only_returns_empty() -> None:
    assert _clean_pool_address_candidate("   ") == ""
