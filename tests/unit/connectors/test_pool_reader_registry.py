"""Unit tests for the strategy-side pool reader spec registry.

Covers the case-insensitive registration / lookup / de-duplication contract of
``PoolReaderRegistry`` (the spec registry in ``_strategy_base``), distinct from
the reader-class ``PoolReaderRegistry`` in ``framework.data.pools.reader``.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_base.pool_reader_registry import (
    PoolReaderRegistry,
    PoolReaderRegistryError,
)

_ADDR_1 = "0x" + "1" * 40
_ADDR_2 = "0x" + "2" * 40


def _spec(protocol: str, *, factory: str = _ADDR_1, aliases: tuple[str, ...] = ()) -> PoolReaderSpec:
    return PoolReaderSpec(protocol=protocol, factory_addresses={"ethereum": factory}, aliases=aliases)


class TestPoolReaderRegistryDedup:
    def test_all_returns_each_spec_once_across_alias_keys(self) -> None:
        registry = PoolReaderRegistry()
        spec = _spec("mock_v3", aliases=("mock_clone", "mock_alias"))
        registry.register(spec)
        # Stored under three keys (protocol + two aliases) but exposed exactly once.
        assert registry.all() == (spec,)

    def test_mixed_case_protocol_collapses_to_first_registered(self) -> None:
        registry = PoolReaderRegistry()
        first = _spec("Mock_V3", factory=_ADDR_1)
        second = _spec("mock_v3", factory=_ADDR_2)
        registry.register(first)
        registry.register(second)  # same lowercased key + same protocol (case-insensitive) -> skipped
        assert registry.all() == (first,)
        # Case-insensitive lookup resolves every casing to the first-registered spec.
        assert registry.require("MOCK_V3") is first
        assert registry.lookup("mock_v3") is first

    def test_all_dedupes_by_lowercased_protocol_key(self) -> None:
        # register() normally prevents two case-variant protocols from coexisting,
        # so inject the state directly to exercise all()'s de-dup guard in isolation.
        # Regression cover for the ``spec.protocol.lower()`` de-dup key: reverting it
        # to ``spec.protocol`` would let ``variant`` leak through as a second entry.
        registry = PoolReaderRegistry()
        canonical = _spec("Mock", factory=_ADDR_1)
        variant = _spec("mock", factory=_ADDR_2)
        registry._specs["mock"] = canonical
        registry._specs["mock_alias"] = variant
        assert registry.all() == (canonical,)

    def test_distinct_protocols_sharing_a_key_raise(self) -> None:
        registry = PoolReaderRegistry()
        registry.register(_spec("uniswap_v3"))
        with pytest.raises(PoolReaderRegistryError, match="already registered"):
            registry.register(_spec("sushiswap_v3", aliases=("uniswap_v3",)))
