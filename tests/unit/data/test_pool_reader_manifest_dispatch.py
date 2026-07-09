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
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY
from almanak.framework.data.pools.reader import (
    _READER_CLASS_BY_KIND,
    _READER_CLASS_BY_PROTOCOL,
    CurvePoolReader,
    PoolReaderRegistry,
    UniswapV3PoolPriceReader,
    UniswapV4PoolReader,
)


def _noop_rpc(chain: str, to: str, data: str) -> bytes:  # pragma: no cover - never called
    raise AssertionError("guard tests never issue RPC")


def test_every_manifest_spec_key_dispatches() -> None:
    """Every key (canonical + alias) of every manifest spec resolves a reader."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    manifest_keys = {key.lower() for spec in POOL_READER_REGISTRY.all() for key in spec.keys}
    assert manifest_keys, "manifest pool-reader registry is empty"
    assert set(registry.supported_protocols) == manifest_keys
    for key in manifest_keys:
        assert registry.get_reader("ethereum", key) is not None


def test_no_hardcoded_dispatch_map_reintroduced() -> None:
    """The dispatch table must never regress to a hardcoded protocol->class set.

    ``_READER_CLASS_BY_PROTOCOL`` maps canonical protocols to framework
    classes (the classes ARE framework code); the dispatch KEYS must come
    from the manifest registry. This pins the deletion of the old
    ``_PROTOCOL_READER_CLASSES`` five-key literal map.
    """
    src = inspect.getsource(reader_module)
    assert "_PROTOCOL_READER_CLASSES" not in src
    init_src = inspect.getsource(PoolReaderRegistry.__init__)
    assert "POOL_READER_REGISTRY.all()" in init_src


def test_framework_classes_match_their_manifest_specs() -> None:
    """Drift guard: each framework subclass's knobs equal its connector spec."""
    for protocol, cls in _READER_CLASS_BY_PROTOCOL.items():
        spec = POOL_READER_REGISTRY.require(protocol)
        assert cls._factory_addresses is spec.factory_addresses, protocol
        assert cls._known_pools is spec.known_pools, protocol
        assert cls._get_pool_selector == spec.get_pool_selector, protocol
        assert cls._candidate_pool_keys == spec.candidate_pool_keys, protocol


def test_alias_keys_dispatch_same_class_and_chain_gate() -> None:
    """Aerodrome's alias stays a first-class dispatch key (both listed on base)."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    canonical = registry.get_reader("base", "aerodrome")
    alias = registry.get_reader("base", "aerodrome_slipstream")
    assert type(canonical) is type(alias)
    on_base = registry.protocols_for_chain("base")
    assert "aerodrome" in on_base and "aerodrome_slipstream" in on_base


def test_spec_without_dedicated_class_uses_spec_bound_base(monkeypatch: pytest.MonkeyPatch) -> None:
    """A NEW v3-family connector needs ONLY a manifest spec — zero framework edits.

    The registry must dispatch an unknown-protocol spec onto the base reader
    bound to that spec (its factories, selector, and sweep keys), and gate its
    chains from the spec.
    """
    fake = PoolReaderSpec(
        protocol="fakeswap_v3",
        factory_addresses={"ethereum": "0x000000000000000000000000000000000000dEaD"},
        get_pool_selector="0x1698ee82",
        candidate_pool_keys=(42, 4242),
    )
    real_all = POOL_READER_REGISTRY.all

    def _all_with_fake() -> tuple[Any, ...]:
        return (*real_all(), fake)

    monkeypatch.setattr(POOL_READER_REGISTRY, "all", _all_with_fake)
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)

    assert "fakeswap_v3" not in _READER_CLASS_BY_PROTOCOL  # no framework edit happened
    reader = registry.get_reader("ethereum", "fakeswap_v3")
    assert type(reader) is UniswapV3PoolPriceReader  # spec-bound base, not a subclass
    assert reader.protocol_name == "fakeswap_v3"
    assert reader._candidate_pool_keys == (42, 4242)
    assert reader._factory_addresses is fake.factory_addresses
    assert "fakeswap_v3" in registry.protocols_for_chain("ethereum")
    assert "fakeswap_v3" not in registry.protocols_for_chain("base")


# ---------------------------------------------------------------------------
# reader_kind dispatch (Curve — non-slot0 read shapes)
# ---------------------------------------------------------------------------


def test_kind_map_pins_default_and_curve() -> None:
    """The kind map owns read-shape dispatch: v3_slot0 -> base, curve_pool -> Curve.

    The default kind MUST stay the spec-bound base reader (that is what the
    fakeswap guard above relies on), and Curve's shape must never regress to
    a slot0 read — a slot0 call on a Curve pool reverts, and a "successful"
    wrong-ABI decode would be a garbage price on a money path.
    """
    assert _READER_CLASS_BY_KIND["v3_slot0"] is UniswapV3PoolPriceReader
    assert _READER_CLASS_BY_KIND["curve_pool"] is CurvePoolReader
    assert _READER_CLASS_BY_KIND["uniswap_v4_stateview"] is UniswapV4PoolReader
    default_kind = PoolReaderSpec(protocol="x", factory_addresses={}).reader_kind
    assert default_kind == "v3_slot0"


def test_registry_reader_kind_accessor() -> None:
    """Consumers gate v3-only lanes on ``reader_kind`` (slippage tick sim,
    gateway slot0 LWAP profile) — pin the accessor's contract: manifest kinds
    surface as declared, custom-registered classes (no manifest spec, v3-family
    by ``register_protocol``'s contract) report the v3 default."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    assert registry.reader_kind("curve") == "curve_pool"
    assert registry.reader_kind("uniswap_v3") == "v3_slot0"
    registry.register_protocol("customswap", UniswapV3PoolPriceReader)
    assert registry.reader_kind("customswap") == "v3_slot0"


def test_curve_dispatches_via_kind_map_not_protocol_map() -> None:
    """Curve has NO dedicated protocol-map entry — its class binds via reader_kind."""
    assert "curve" not in _READER_CLASS_BY_PROTOCOL
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    reader = registry.get_reader("ethereum", "curve")
    assert type(reader) is CurvePoolReader


def test_curve_reader_binds_its_manifest_spec() -> None:
    """Drift guard (kind-map analogue of the protocol-map guard above).

    CurvePoolReader carries NO class-level spec attributes and no protocol
    literal (coupling ratchet, blueprint 22) — identity binds per-instance
    from the connector spec at registry construction. Bare construction
    without a spec must fail loudly, never silently inherit the v3 base
    defaults.
    """
    spec = POOL_READER_REGISTRY.require("curve")
    assert spec.reader_kind == "curve_pool"
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


def test_curve_chain_gating_comes_from_curated_pools() -> None:
    """Curve is claimed exactly on chains with curated pools (no factory table)."""
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    spec = POOL_READER_REGISTRY.require("curve")
    assert spec.factory_addresses == {}
    for chain in ("ethereum", "arbitrum", "optimism", "polygon", "base"):
        assert "curve" in registry.protocols_for_chain(chain), chain
        assert spec.known_pools.get(chain), f"no curated pools gate {chain}"
    assert "curve" not in registry.protocols_for_chain("solana")


def test_unknown_reader_kind_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    """A spec declaring a shape the framework cannot read must fail at build.

    Silently falling back to the slot0 base reader would read a foreign ABI
    as a price — the registry must refuse to construct instead.
    """
    bogus = PoolReaderSpec(
        protocol="mysteryswap",
        factory_addresses={"ethereum": "0x000000000000000000000000000000000000dEaD"},
        reader_kind="balancer_weighted",
    )
    real_all = POOL_READER_REGISTRY.all

    def _all_with_bogus() -> tuple[Any, ...]:
        return (*real_all(), bogus)

    monkeypatch.setattr(POOL_READER_REGISTRY, "all", _all_with_bogus)
    with pytest.raises(ValueError, match="unknown reader_kind 'balancer_weighted'"):
        PoolReaderRegistry(rpc_call=_noop_rpc)


def test_new_curve_shaped_spec_needs_only_a_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    """A NEW curve-shaped connector binds CurvePoolReader via kind — zero framework edits."""
    fake = PoolReaderSpec(
        protocol="fakecurve",
        factory_addresses={},
        known_pools={"ethereum": {("0xaa", "0xbb", 0): "0x000000000000000000000000000000000000dEaD"}},
        candidate_pool_keys=(0,),
        reader_kind="curve_pool",
    )
    real_all = POOL_READER_REGISTRY.all

    def _all_with_fake() -> tuple[Any, ...]:
        return (*real_all(), fake)

    monkeypatch.setattr(POOL_READER_REGISTRY, "all", _all_with_fake)
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)

    assert "fakecurve" not in _READER_CLASS_BY_PROTOCOL
    reader = registry.get_reader("ethereum", "fakecurve")
    assert type(reader) is CurvePoolReader
    assert reader.protocol_name == "fakecurve"
    assert reader._known_pools is fake.known_pools
    assert "fakecurve" in registry.protocols_for_chain("ethereum")
    assert "fakecurve" not in registry.protocols_for_chain("base")


def test_uniswap_v4_dispatches_via_kind_map_not_protocol_map() -> None:
    """V4 binds its StateView reader via reader_kind; chain gate = StateView table."""
    assert "uniswap_v4" not in _READER_CLASS_BY_PROTOCOL
    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    reader = registry.get_reader("base", "uniswap_v4")
    assert type(reader) is UniswapV4PoolReader

    spec = POOL_READER_REGISTRY.require("uniswap_v4")
    assert spec.reader_kind == "uniswap_v4_stateview"
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


def test_protocols_for_chain_tolerates_bare_custom_class() -> None:
    """PR #3198 review (gemini): a duck-typed custom class without the base
    attributes must not crash chain gating — it is simply gated out."""

    class BareReader:  # deliberately NOT a UniswapV3PoolPriceReader subclass
        protocol_name = "bare"

    registry = PoolReaderRegistry(rpc_call=_noop_rpc)
    registry.register_protocol("bare", BareReader)  # type: ignore[arg-type]
    chains = registry.protocols_for_chain("ethereum")
    assert "bare" not in chains
    assert "uniswap_v3" in chains
