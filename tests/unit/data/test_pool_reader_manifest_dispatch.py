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
    _READER_CLASS_BY_PROTOCOL,
    PoolReaderRegistry,
    UniswapV3PoolPriceReader,
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
