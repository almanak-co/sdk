"""Compound V3 amount resolver address lookup tests."""

from __future__ import annotations

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors.compound_v3.addresses import COMPOUND_V3_COMET_ADDRESSES
from almanak.framework.intents.amount_resolver import CompoundV3BalanceReader


def test_compound_v3_amount_resolver_uses_manifest_address_table() -> None:
    """Compound V3 balance reads resolve Comets through connector-owned addresses."""
    AddressRegistry.reset_cache()
    reader = CompoundV3BalanceReader()

    assert reader._get_comet_address("base", "usdc") == COMPOUND_V3_COMET_ADDRESSES["base"]["usdc"]
    assert reader._get_comet_address("optimism", None) == COMPOUND_V3_COMET_ADDRESSES["optimism"]["usdc"]
    assert reader._get_comet_address("base", None) is None


def test_compound_v3_amount_resolver_returns_none_for_unsupported_chain() -> None:
    """A chain with no Compound V3 Comet table resolves to None (not a crash),
    so the caller falls back to withdraw_all rather than guessing an address."""
    AddressRegistry.reset_cache()
    reader = CompoundV3BalanceReader()

    # avalanche ships no compound_v3 deployment in the connector address table.
    assert "avalanche" not in COMPOUND_V3_COMET_ADDRESSES
    assert reader._get_comet_address("avalanche", None) is None
    assert reader._get_comet_address("avalanche", "usdc") is None
