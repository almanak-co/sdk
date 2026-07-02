"""Builder-contract tests for ``CHAIN_ID_MAP`` / ``_build_chain_id_map`` (VIB-4933).

VIB-4933 folded the hand-maintained ``dict[Chain, int]`` literal in
``tokens/models.py`` onto :class:`ChainRegistry`. ``CHAIN_ID_MAP`` is now a
read-only view derived from ``ChainRegistry.all()`` rather than a literal that
drifted whenever a chain was added without a matching entry. These tests pin the
three contract guarantees the fold must preserve:

1. Every chain with ``chain_id != 0`` is present with the registry's exact id.
2. Chains with ``chain_id == 0`` (the non-EVM EIP-155 sentinel, e.g. Solana)
   are excluded, so ``CHAIN_ID_MAP.get(chain, 0)`` keeps returning ``0`` for
   them — preserving pre-fold behaviour.
3. The mapping is immutable (``MappingProxyType``): the registry stays the only
   mutation surface and accidental ``CHAIN_ID_MAP[...] = ...`` raises.

Expectations are derived from ``ChainRegistry.all()`` (the single source of
truth), not hardcoded enums, so the test stays resilient as chains are added.
"""

from __future__ import annotations

from types import MappingProxyType

import pytest

from almanak.core.chains import ChainRegistry
from almanak.framework.data.tokens.models import CHAIN_ID_MAP


class TestChainIdMapMembership:
    """``CHAIN_ID_MAP`` mirrors every EVM (non-zero ``chain_id``) descriptor."""

    def test_every_nonzero_chain_present_with_matching_id(self) -> None:
        expected = {
            descriptor.name: descriptor.chain_id
            for descriptor in ChainRegistry.all()
            if descriptor.chain_id != 0
        }
        # Sanity: the registry actually has EVM chains to assert on, otherwise
        # an empty-vs-empty comparison would pass vacuously.
        assert expected, "ChainRegistry exposed no chains with chain_id != 0"
        assert dict(CHAIN_ID_MAP) == expected

    def test_each_entry_matches_registry_chain_id(self) -> None:
        for descriptor in ChainRegistry.all():
            if descriptor.chain_id == 0:
                continue
            assert CHAIN_ID_MAP[descriptor.name] == descriptor.chain_id, (
                f"{descriptor.name}: CHAIN_ID_MAP has "
                f"{CHAIN_ID_MAP.get(descriptor.name)}, registry has "
                f"{descriptor.chain_id}"
            )

    def test_no_extra_entries_beyond_registry(self) -> None:
        registry_evm_names = {
            descriptor.name
            for descriptor in ChainRegistry.all()
            if descriptor.chain_id != 0
        }
        assert set(CHAIN_ID_MAP) == registry_evm_names


class TestChainIdMapExcludesSentinelChains:
    """Chains using the ``0`` EIP-155 sentinel (non-EVM) are excluded."""

    def test_zero_chain_id_chains_excluded(self) -> None:
        sentinel_names = [
            descriptor.name
            for descriptor in ChainRegistry.all()
            if descriptor.chain_id == 0
        ]
        # Sanity: there is at least one non-EVM chain (e.g. Solana) so this
        # assertion is meaningful rather than vacuous.
        assert sentinel_names, "expected at least one chain with chain_id == 0 (e.g. Solana)"
        for name in sentinel_names:
            assert name not in CHAIN_ID_MAP, (
                f"{name} has chain_id 0 but leaked into CHAIN_ID_MAP"
            )

    def test_get_returns_zero_default_for_excluded_chain(self) -> None:
        for descriptor in ChainRegistry.all():
            if descriptor.chain_id != 0:
                continue
            # The ``.get(chain, 0)`` call pattern across the codebase relies on
            # excluded chains falling through to the ``0`` default.
            assert CHAIN_ID_MAP.get(descriptor.name, 0) == 0


class TestChainIdMapImmutability:
    """``CHAIN_ID_MAP`` is a read-only ``MappingProxyType``."""

    def test_is_mapping_proxy(self) -> None:
        assert isinstance(CHAIN_ID_MAP, MappingProxyType)

    def test_setitem_raises_type_error(self) -> None:
        some_chain = next(iter(CHAIN_ID_MAP))
        with pytest.raises(TypeError):
            CHAIN_ID_MAP[some_chain] = 123  # type: ignore[index]

    def test_delitem_raises_type_error(self) -> None:
        some_chain = next(iter(CHAIN_ID_MAP))
        with pytest.raises(TypeError):
            del CHAIN_ID_MAP[some_chain]  # type: ignore[attr-defined]
