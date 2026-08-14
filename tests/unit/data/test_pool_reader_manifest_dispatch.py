"""Completeness + drift guards for manifest-driven pool-reader dispatch (VIB-5047).

Blueprint 05 (§position-read exemplar): a central consumer must dispatch by
the connector manifest, never a hardcoded protocol-name set — and a guard
must fail CI if the seam regresses (a hardcoded map reappears, a spec drifts
from its framework class, or a manifest spec stops being dispatchable).
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

import almanak.framework.data.pools.reader as reader_module
from almanak.connectors._connector import ImportRef
from almanak.connectors._strategy_base.pool_data import PoolDataFacet
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY
from almanak.framework.data.pools.reader import (
    CurvePoolReader,
    PoolReaderRegistry,
    UniswapV3PoolPriceReader,
    UniswapV4PoolReader,
)


def _noop_rpc(chain: str, to: str, data: str) -> bytes:  # pragma: no cover - never called
    raise AssertionError("guard tests never issue RPC")


def _reader_ref(attribute: str) -> ImportRef:
    return ImportRef(module="almanak.framework.data.pools.reader", attribute=attribute)


def test_every_manifest_spec_key_dispatches() -> None:
    """Every key (canonical + alias) of every manifest spec resolves a reader."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    manifest_keys = {key.lower() for spec in POOL_READER_REGISTRY.all() for key in spec.keys}
    assert manifest_keys, "manifest pool-reader registry is empty"
    assert set(registry.supported_protocols) == manifest_keys
    for key in manifest_keys:
        assert registry.get_reader("ethereum", key) is not None


def test_no_hardcoded_dispatch_map_reintroduced() -> None:
    """The framework must not regain protocol or family-string dispatch maps."""
    src = inspect.getsource(reader_module)
    assert "_PROTOCOL_READER_CLASSES" not in src
    assert "_READER_CLASS_BY_PROTOCOL" not in src
    assert "_READER_CLASS_BY_KIND" not in src
    init_src = inspect.getsource(PoolReaderRegistry.__init__)
    assert "POOL_READER_REGISTRY.all()" in init_src
    assert "spec.reader.load()" in init_src


def test_framework_classes_match_their_manifest_specs() -> None:
    """Every spec resolves its connector-selected reader and binds its knobs."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    for spec in POOL_READER_REGISTRY.all():
        reader = registry.get_reader("ethereum", spec.protocol)
        assert type(reader) is spec.reader.load(), spec.protocol
        assert reader._factory_addresses is spec.factory_addresses, spec.protocol
        assert reader._known_pools is spec.known_pools, spec.protocol
        assert reader._get_pool_selector == spec.get_pool_selector, spec.protocol
        assert reader._candidate_pool_keys == spec.candidate_pool_keys, spec.protocol


def test_aerodrome_slipstream_dispatch_is_not_aliased_to_classic() -> None:
    """Only Slipstream enters the concentrated-liquidity live-reader lane."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    with pytest.raises(ValueError, match="Unknown protocol"):
        registry.get_reader("base", "aerodrome")
    assert registry.get_reader("base", "aerodrome_slipstream") is not None
    on_base = registry.protocols_for_chain("base")
    assert "aerodrome" not in on_base
    assert "aerodrome_slipstream" in on_base


def test_spec_without_dedicated_class_uses_spec_bound_base(monkeypatch: pytest.MonkeyPatch) -> None:
    """A NEW v3-family connector needs ONLY a manifest spec — zero framework edits.

    The registry must dispatch an unknown-protocol spec onto the base reader
    bound to that spec (its factories, selector, and sweep keys), and gate its
    chains from the spec.
    """
    fake = PoolReaderSpec(
        protocol="fakeswap_v3",
        factory_addresses={"ethereum": "0x000000000000000000000000000000000000dEaD"},
        reader=_reader_ref("UniswapV3PoolPriceReader"),
        get_pool_selector="0x1698ee82",
        candidate_pool_keys=(42, 4242),
    )
    real_all = POOL_READER_REGISTRY.all

    def _all_with_fake() -> tuple[Any, ...]:
        return (*real_all(), fake)

    monkeypatch.setattr(POOL_READER_REGISTRY, "all", _all_with_fake)
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)

    reader = registry.get_reader("ethereum", "fakeswap_v3")
    assert type(reader) is UniswapV3PoolPriceReader  # spec-bound base, not a subclass
    assert reader.protocol_name == "fakeswap_v3"
    assert reader._candidate_pool_keys == (42, 4242)
    assert reader._factory_addresses is fake.factory_addresses
    assert "fakeswap_v3" in registry.protocols_for_chain("ethereum")
    assert "fakeswap_v3" not in registry.protocols_for_chain("base")


# ---------------------------------------------------------------------------
# Fine-grained capabilities replace family-string dispatch.
# ---------------------------------------------------------------------------


def test_registry_capability_accessor() -> None:
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    assert not registry.supports("curve", PoolDataFacet.TICK_LIQUIDITY)
    assert registry.supports("uniswap_v3", PoolDataFacet.TICK_LIQUIDITY)
    assert not registry.supports("uniswap_v4", PoolDataFacet.TICK_LIQUIDITY)
    registry.register_protocol("customswap", UniswapV3PoolPriceReader)
    assert registry.supports("customswap", PoolDataFacet.TICK_LIQUIDITY)
    registry.register_protocol("customcurve", CurvePoolReader, supported_facets=())
    assert not registry.supports("customcurve", PoolDataFacet.TICK_LIQUIDITY)


def test_curve_dispatches_via_connector_binding() -> None:
    """Curve explicitly binds its get_dy/coins reader; no slot0 fallback exists."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    reader = registry.get_reader("ethereum", "curve")
    assert type(reader) is CurvePoolReader
    assert POOL_READER_REGISTRY.require("curve").reader.load() is CurvePoolReader


def test_curve_reader_binds_its_manifest_spec() -> None:
    """Drift guard for the connector-selected Curve reader.

    CurvePoolReader carries NO class-level spec attributes and no protocol
    literal (coupling ratchet, blueprint 22) — identity binds per-instance
    from the connector spec at registry construction. Bare construction
    without a spec must fail loudly, never silently inherit the v3 base
    defaults.
    """
    spec = POOL_READER_REGISTRY.require("curve")
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    reader = registry.get_reader("ethereum", "curve")
    assert reader.protocol_name == spec.protocol
    assert reader._factory_addresses is spec.factory_addresses
    assert reader._known_pools is spec.known_pools
    assert reader._candidate_pool_keys == spec.candidate_pool_keys
    # No fee-tier discriminator: the best-pool sweep must be a single lookup.
    assert spec.candidate_pool_keys == (0,)
    with pytest.raises(ValueError, match="kind-dispatched"):
        CurvePoolReader(rpc_call=_noop_rpc)


def test_curve_chain_gating_comes_from_connector_support() -> None:
    """Exact-address Curve reads use connector support, not a pool catalogue."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    spec = POOL_READER_REGISTRY.require("curve")
    assert spec.factory_addresses == {}
    assert spec.known_pools == {}
    for chain in ("ethereum", "arbitrum", "optimism", "polygon", "base"):
        assert "curve" in registry.protocols_for_chain(chain), chain
    assert "curve" not in registry.protocols_for_chain("solana")


def test_invalid_reader_binding_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-reader binding fails instead of falling back to a V3 ABI."""
    bogus = PoolReaderSpec(
        protocol="mysteryswap",
        factory_addresses={"ethereum": "0x000000000000000000000000000000000000dEaD"},
        reader=_reader_ref("PoolPrice"),
    )
    real_all = POOL_READER_REGISTRY.all

    def _all_with_bogus() -> tuple[Any, ...]:
        return (*real_all(), bogus)

    monkeypatch.setattr(POOL_READER_REGISTRY, "all", _all_with_bogus)
    with pytest.raises(TypeError, match="expected the PoolPriceReader interface"):
        PoolReaderRegistry(rpc_call=_noop_rpc)


def test_new_curve_shaped_spec_needs_only_a_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    """A new Curve-shaped connector explicitly selects CurvePoolReader."""
    fake = PoolReaderSpec(
        protocol="fakecurve",
        factory_addresses={},
        reader=_reader_ref("CurvePoolReader"),
        known_pools={"ethereum": {("0xaa", "0xbb", 0): "0x000000000000000000000000000000000000dEaD"}},
        candidate_pool_keys=(0,),
    )
    real_all = POOL_READER_REGISTRY.all

    def _all_with_fake() -> tuple[Any, ...]:
        return (*real_all(), fake)

    monkeypatch.setattr(POOL_READER_REGISTRY, "all", _all_with_fake)
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)

    reader = registry.get_reader("ethereum", "fakecurve")
    assert type(reader) is CurvePoolReader
    assert reader.protocol_name == "fakecurve"
    assert reader._known_pools is fake.known_pools
    assert "fakecurve" in registry.protocols_for_chain("ethereum")
    assert "fakecurve" not in registry.protocols_for_chain("base")


def test_uniswap_v4_dispatches_via_connector_binding() -> None:
    """V4 binds its StateView reader directly; chain gate = StateView table."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    reader = registry.get_reader("base", "uniswap_v4")
    assert type(reader) is UniswapV4PoolReader

    spec = POOL_READER_REGISTRY.require("uniswap_v4")
    assert spec.reader.load() is UniswapV4PoolReader
    # Drift guard: instance identity binds from the connector spec (the class
    # carries NO spec attributes / protocol literal — coupling ratchet), and
    # bare construction without a spec fails loudly instead of inheriting the
    # v3 base defaults.
    assert reader.protocol_name == spec.protocol
    assert reader._factory_addresses is spec.factory_addresses
    assert reader._known_pools is spec.known_pools
    assert reader._candidate_pool_keys == spec.candidate_pool_keys
    with pytest.raises(ValueError, match="kind-dispatched"):
        UniswapV4PoolReader(rpc_call=_noop_rpc)
    # Chain gating comes from the per-chain StateView deployments, and the
    # gate values ARE the StateView addresses from the connector table.
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

    for chain, addrs in UNISWAP_V4.items():
        assert spec.factory_addresses[chain] == addrs["state_view"], chain
        assert "uniswap_v4" in registry.protocols_for_chain(chain), chain
    assert "uniswap_v4" not in registry.protocols_for_chain("solana")


def test_register_protocol_custom_class_keeps_legacy_constructor_contract() -> None:
    """PR #3198 review (codex): a custom reader registered through the public
    extension point may keep the pre-VIB-5047 constructor shape — get_reader
    must not pass ``spec`` to it (register_protocol drops any manifest binding)."""

    class LegacyReader(UniswapV3PoolPriceReader):
        protocol_name = "legacyswap"

        def __init__(self, rpc_call, token_resolver=None, cache_ttl_seconds=2.0, source_name="alchemy_rpc"):
            super().__init__(rpc_call, token_resolver, cache_ttl_seconds, source_name)

    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    registry.register_protocol("legacyswap", LegacyReader)
    reader = registry.get_reader("ethereum", "legacyswap")
    assert type(reader) is LegacyReader
    # Overriding a manifest key with a custom class must also drop its spec.
    registry.register_protocol("uniswap_v3", LegacyReader)
    assert type(registry.get_reader("ethereum", "uniswap_v3")) is LegacyReader


def test_register_protocol_rejects_bare_custom_class_before_mutation() -> None:
    """Custom registrations must implement the complete reader interface."""

    class BareReader:  # deliberately NOT a UniswapV3PoolPriceReader subclass
        protocol_name = "bare"

    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    with pytest.raises(TypeError, match="expected the PoolPriceReader interface"):
        registry.register_protocol("bare", BareReader)  # type: ignore[arg-type]

    assert "bare" not in registry.supported_protocols
