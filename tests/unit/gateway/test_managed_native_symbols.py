"""Managed gateway native labels remain metadata, never funding identity."""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.gateway.managed import ManagedGateway


def test_chain_native_symbol_keys_unchanged() -> None:
    """The display-only per-chain map stays EVM-only and registry-backed."""
    evm = {d.name for d in ChainRegistry.all() if d.family is ChainFamily.EVM}
    assert set(ManagedGateway.CHAIN_NATIVE_SYMBOL) == evm


def test_chain_native_symbols_are_registry_display_metadata() -> None:
    for descriptor in ChainRegistry.all():
        if descriptor.family is ChainFamily.EVM:
            assert ManagedGateway.CHAIN_NATIVE_SYMBOL[descriptor.name] == descriptor.native.symbol
