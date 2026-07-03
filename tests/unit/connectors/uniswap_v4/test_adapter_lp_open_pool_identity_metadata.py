"""VIB-5582 — V4 LP_OPEN compiles its pool identity + protocol + registry_handle
into the ActionBundle metadata at COMPILE time (before signing/submission).

This is the anchor the pre-execution registry-collision preflight
(``accounting/registry_preflight.py``) needs to reject a same-pool V4 reopen
BEFORE minting a second NFT (mirrors how the V3 compiler already carries
``metadata["pool"]``). Without ``pool_id`` on the bundle, the preflight has
nothing to key on for V4 and always fails open.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
from almanak.connectors.uniswap_v4.hooks import compute_pool_id
from almanak.connectors.uniswap_v4.sdk import PoolKey
from almanak.framework.intents.vocabulary import LPOpenIntent


def _make_resolver():
    resolver = MagicMock()

    def resolve_for_swap(symbol, chain):
        tokens = {
            "WETH": MagicMock(address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1", decimals=18, is_native=False),
            "USDC": MagicMock(address="0xaf88d065e77c8cc2239327c5edb3a432268e5831", decimals=6, is_native=False),
        }
        return tokens[symbol.upper()]

    resolver.resolve_for_swap = resolve_for_swap
    resolver.resolve = lambda symbol_or_addr, chain: resolve_for_swap(symbol_or_addr, chain)
    return resolver


@pytest.fixture()
def adapter():
    config = UniswapV4Config(
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
    )
    return UniswapV4Adapter(config=config, token_resolver=_make_resolver())


def _open_intent(**overrides) -> LPOpenIntent:
    defaults = {
        "pool": "WETH/USDC/3000",
        "amount0": Decimal("0.1"),
        "amount1": Decimal("200"),
        "range_lower": Decimal("1500"),
        "range_upper": Decimal("2500"),
        "protocol": "uniswap_v4",
        "protocol_params": {"allow_estimated_price": True},
    }
    defaults.update(overrides)
    return LPOpenIntent(**defaults)


def test_bundle_carries_pool_id_matching_compute_pool_id(adapter):
    """`pool_id` on the bundle must equal `compute_pool_id` over the SAME
    sorted (currency0, currency1, fee, tick_spacing, hooks) tuple the mint
    transaction itself was built against — no independent re-derivation."""
    intent = _open_intent()
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}

    bundle = adapter.compile_lp_open_intent(intent, price_oracle)

    assert "error" not in bundle.metadata
    pool_id = bundle.metadata.get("pool_id")
    assert isinstance(pool_id, str) and pool_id.startswith("0x") and len(pool_id) == 66

    # WETH < USDC numerically? Recompute sorted order the same way the
    # adapter does and confirm compute_pool_id(pool_key) reproduces it.
    weth = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
    usdc = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    currency0, currency1 = sorted([weth, usdc], key=lambda a: int(a, 16))
    pool_key = PoolKey(currency0=currency0, currency1=currency1, fee=3000, tick_spacing=60)
    assert pool_id == compute_pool_id(pool_key)


def test_bundle_carries_protocol_from_intent(adapter):
    intent = _open_intent(protocol="uniswap_v4")
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
    assert bundle.metadata.get("protocol") == "uniswap_v4"


def test_bundle_defaults_protocol_when_intent_protocol_empty(adapter):
    """Falls back to this connector's own slug when the intent didn't set one
    (defensive — LPOpenIntent.protocol is normally required, but the metadata
    build must never emit `protocol=None`, which would break the preflight's
    `(protocol or "").strip().lower()` dispatch)."""
    intent = _open_intent(protocol="")
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
    assert bundle.metadata.get("protocol") == "uniswap_v4"


def test_bundle_carries_registry_handle_passthrough(adapter):
    intent = _open_intent(registry_handle="leg_b")
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
    assert bundle.metadata.get("registry_handle") == "leg_b"


def test_bundle_registry_handle_none_by_default(adapter):
    intent = _open_intent()
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
    assert bundle.metadata.get("registry_handle") is None


def test_pool_id_stable_across_two_opens_same_pool(adapter):
    """Two LP_OPENs against the SAME pool (same token pair/fee/hooks) must
    compile to the IDENTICAL pool_id — this is exactly the invariant the
    registry preflight's collision check depends on."""
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
    bundle1 = adapter.compile_lp_open_intent(_open_intent(), price_oracle)
    bundle2 = adapter.compile_lp_open_intent(
        _open_intent(amount0=Decimal("0.05"), amount1=Decimal("100")), price_oracle
    )
    assert bundle1.metadata["pool_id"] == bundle2.metadata["pool_id"]


def test_pool_id_differs_across_different_fee_tiers(adapter):
    """Different fee tier ⇒ different PoolKey ⇒ different pool_id (V4 pools
    are keyed on the full (currency0, currency1, fee, tickSpacing, hooks)
    tuple, not just the token pair)."""
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
    bundle_3000 = adapter.compile_lp_open_intent(_open_intent(pool="WETH/USDC/3000"), price_oracle)
    bundle_500 = adapter.compile_lp_open_intent(_open_intent(pool="WETH/USDC/500"), price_oracle)
    assert bundle_3000.metadata["pool_id"] != bundle_500.metadata["pool_id"]
