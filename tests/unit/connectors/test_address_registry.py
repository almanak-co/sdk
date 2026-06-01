"""Unit tests for the strategy-side ``AddressRegistry`` (W1 / VIB-4853).

The registry is the strategy-side seam that lets framework consumers
(pool validation, teardown discovery / post-conditions) resolve a connector's
contract addresses without importing the connector by name. These tests pin
its contract and act as a drift-catcher: every registered protocol must import
cleanly and expose a non-empty per-chain address table.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.address_registry import (
    AbiFamily,
    AddressRegistry,
    address_supported_chains,
    addresses_for,
    resolve_contract_address,
)

KNOWN_PROTOCOLS = AddressRegistry.supported_protocols()


def test_supported_protocols_nonempty_and_sorted():
    protos = AddressRegistry.supported_protocols()
    assert protos == tuple(sorted(protos))
    assert {"aave_v3", "uniswap_v3", "gmx_v2"} <= set(protos)


@pytest.mark.parametrize("protocol", KNOWN_PROTOCOLS)
def test_every_loader_resolves_to_a_nonempty_address_table(protocol: str):
    # Each registered protocol's connector module must import cleanly and
    # expose a per-chain address dict — catches registration / attribute drift.
    chains = AddressRegistry.address_supported_chains(protocol)
    assert isinstance(chains, frozenset)
    assert chains, f"{protocol} publishes no non-empty address table"
    assert all(c == c.lower() for c in chains), f"{protocol} chains not lowercased"
    for chain in chains:
        table = AddressRegistry.addresses_for(protocol, chain)
        assert table, f"{protocol}/{chain} resolved empty"
        assert all(isinstance(k, str) and isinstance(v, str) and v for k, v in table.items())


def test_unknown_protocol_is_fail_closed():
    assert AddressRegistry.has("not_a_protocol") is False
    assert AddressRegistry.addresses_for("not_a_protocol", "ethereum") == {}
    assert AddressRegistry.address_supported_chains("not_a_protocol") == frozenset()
    assert AddressRegistry.resolve_contract_address("not_a_protocol", "ethereum", "pool") is None


def test_lookup_is_case_insensitive():
    chain = next(iter(AddressRegistry.address_supported_chains("aave_v3")))
    assert AddressRegistry.addresses_for("AAVE_V3", chain.upper()) == AddressRegistry.addresses_for(
        "aave_v3", chain
    )


def test_resolve_contract_address_round_trips_a_present_kind():
    chain = next(iter(AddressRegistry.address_supported_chains("uniswap_v3")))
    table = AddressRegistry.addresses_for("uniswap_v3", chain)
    kind, expected = next(iter(table.items()))
    # exact kind resolves
    assert AddressRegistry.resolve_contract_address("uniswap_v3", chain, kind) == expected
    # tuple form: absent kinds are skipped, first present wins
    assert (
        AddressRegistry.resolve_contract_address("uniswap_v3", chain, ("definitely_absent_kind", kind))
        == expected
    )
    # all-absent → fail-closed None
    assert AddressRegistry.resolve_contract_address("uniswap_v3", chain, "definitely_absent_kind") is None


def test_agni_finance_quirk_is_owned_by_uniswap_v3_module():
    # agni_finance has no own connector folder; it lives in uniswap_v3.addresses
    # under a distinct attribute and must still resolve (mantle-only).
    assert AddressRegistry.has("agni_finance")
    assert "mantle" in AddressRegistry.address_supported_chains("agni_finance")


def test_abi_families_are_consistent():
    factory = AddressRegistry.protocols_with_abi(AbiFamily.V3_FACTORY)
    npm = AddressRegistry.protocols_with_abi(AbiFamily.V3_NPM)
    assert factory and npm
    # every family member is a registered protocol
    for proto in set(factory) | set(npm):
        assert AddressRegistry.has(proto), f"{proto} in an ABI family but not registered"
    assert AddressRegistry.has_abi("uniswap_v3", AbiFamily.V3_FACTORY)
    # Camelot (Algebra ABI) and Uniswap V4 (singleton PoolManager) are
    # deliberately NOT in the canonical V3 families.
    assert not AddressRegistry.has_abi("camelot", AbiFamily.V3_FACTORY)
    assert not AddressRegistry.has_abi("uniswap_v4", AbiFamily.V3_NPM)


def test_module_level_wrappers_delegate():
    chain = next(iter(address_supported_chains("aave_v3")))
    assert addresses_for("aave_v3", chain) == AddressRegistry.addresses_for("aave_v3", chain)
    assert address_supported_chains("aave_v3") == AddressRegistry.address_supported_chains("aave_v3")
    kind = next(iter(addresses_for("aave_v3", chain)))
    assert resolve_contract_address("aave_v3", chain, kind) == AddressRegistry.resolve_contract_address(
        "aave_v3", chain, kind
    )
