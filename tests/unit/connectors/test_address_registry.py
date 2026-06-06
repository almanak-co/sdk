"""Unit tests for the strategy-side ``AddressRegistry`` (W1 / VIB-4853).

The registry is the strategy-side seam that lets framework consumers
(pool validation, teardown discovery / post-conditions) resolve a connector's
contract addresses without importing the connector by name. These tests pin
its contract and act as a drift-catcher: every registered protocol must import
cleanly and expose a non-empty per-chain address table.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

import almanak.connectors._strategy_base.address_registry as address_registry_module
from almanak.connectors._strategy_base.address_registry import (
    AbiFamily,
    AddressRegistry,
    address_supported_chains,
    addresses_for,
    resolve_contract_address,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

KNOWN_PROTOCOLS = AddressRegistry.supported_protocols()


@pytest.fixture(autouse=True)
def _reset_address_registry() -> None:
    AddressRegistry.reset_cache()
    yield
    AddressRegistry.reset_cache()


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


def test_lookup_does_not_import_unrelated_address_table_modules(monkeypatch: pytest.MonkeyPatch):
    good_module = ModuleType("fake_good_address_table_module")
    good_module.GOOD = {"ethereum": {"router": "0x0000000000000000000000000000000000000001"}}
    monkeypatch.setitem(sys.modules, good_module.__name__, good_module)

    good_spec = AddressTableSpec(protocol="good", module=good_module.__name__, attribute="GOOD")
    broken_spec = AddressTableSpec(
        protocol="broken",
        module="fake_broken_address_table_module",
        attribute="BROKEN",
    )
    fake_registry = SimpleNamespace(
        with_address_tables=lambda: (SimpleNamespace(address_tables=(good_spec, broken_spec)),)
    )
    monkeypatch.setattr(address_registry_module, "CONNECTOR_REGISTRY", fake_registry)

    assert AddressRegistry.supported_protocols() == ("broken", "good")
    assert AddressRegistry.addresses_for("good", "ethereum") == good_module.GOOD["ethereum"]
    assert AddressRegistry.addresses_for("broken", "ethereum") == {}
    assert "fake_broken_address_table_module" not in sys.modules


def test_lookup_is_case_insensitive():
    chain = next(iter(AddressRegistry.address_supported_chains("aave_v3")))
    assert AddressRegistry.addresses_for("AAVE_V3", chain.upper()) == AddressRegistry.addresses_for("aave_v3", chain)


def test_resolve_contract_address_round_trips_a_present_kind():
    chain = next(iter(AddressRegistry.address_supported_chains("uniswap_v3")))
    table = AddressRegistry.addresses_for("uniswap_v3", chain)
    kind, expected = next(iter(table.items()))
    # exact kind resolves
    assert AddressRegistry.resolve_contract_address("uniswap_v3", chain, kind) == expected
    # tuple form: absent kinds are skipped, first present wins
    assert AddressRegistry.resolve_contract_address("uniswap_v3", chain, ("definitely_absent_kind", kind)) == expected
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
    assert factory == ("uniswap_v3", "agni_finance", "pancakeswap_v3", "sushiswap_v3")
    assert npm == factory
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


def test_address_chains_ordered_contract(monkeypatch):
    """Pin ``address_chains_ordered``'s own contract (VIB-4928 PR-3a).

    It is load-bearing for the intent compiler's per-chain address-table order,
    so test it directly rather than only via downstream consumers. Patch
    ``_load_table`` to return controlled tables covering the three contract
    cases: unknown protocol, empty-entry chains dropped, and declaration order
    preserved exactly.
    """

    def fake_load(table):
        # _load_table is a classmethod; the patched replacement ignores the
        # protocol arg and returns a fixed table so the ordering/filtering logic
        # is exercised in isolation from real connector data.
        return classmethod(lambda cls, protocol: table)

    # Unknown protocol -> _load_table returns None -> ().
    monkeypatch.setattr(AddressRegistry, "_load_table", fake_load(None))
    assert AddressRegistry.address_chains_ordered("anything") == ()

    # Chains whose contract entry is empty are omitted; non-empty chains kept.
    mixed = {
        "ethereum": {"router": "0xabc"},
        "arbitrum": {},  # empty entry -> dropped
        "base": {"router": "0xdef"},
    }
    monkeypatch.setattr(AddressRegistry, "_load_table", fake_load(mixed))
    assert AddressRegistry.address_chains_ordered("anything") == ("ethereum", "base")

    # Multiple non-empty chains: declaration/insertion order preserved exactly.
    ordered = {
        "optimism": {"router": "0x1"},
        "polygon": {"router": "0x2"},
        "avalanche": {"router": "0x3"},
        "bsc": {"router": "0x4"},
    }
    monkeypatch.setattr(AddressRegistry, "_load_table", fake_load(ordered))
    assert AddressRegistry.address_chains_ordered("anything") == (
        "optimism",
        "polygon",
        "avalanche",
        "bsc",
    )
