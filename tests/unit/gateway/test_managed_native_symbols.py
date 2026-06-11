"""Frozen-verbatim equivalence for ``ManagedGateway.NATIVE_TOKEN_SYMBOLS``.

VIB-4851 inversion recipe: the legacy hand-maintained union of native gas
symbols is now derived from ``ChainDescriptor.native`` (union of
``native_symbols_for`` over EVM chains). The frozen set below is the legacy
literal verbatim — if it ever diverges from the derived set, either a chain
was added (update the frozen set consciously) or the derivation regressed.
"""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.core.enums import ChainFamily
from almanak.gateway.managed import ManagedGateway

# The exact literal removed from almanak/gateway/managed.py — do not "fix" this
# set to make the test pass; it is the anti-widening contract.
LEGACY_NATIVE_TOKEN_SYMBOLS = frozenset(
    {"ETH", "AVAX", "MATIC", "BNB", "S", "POL", "MNT", "BERA", "MON", "OKB", "XPL", "A0GI"}
)


def test_native_token_symbols_matches_legacy_verbatim() -> None:
    assert ManagedGateway.NATIVE_TOKEN_SYMBOLS == LEGACY_NATIVE_TOKEN_SYMBOLS


def test_native_token_symbols_is_the_evm_registry_union() -> None:
    expected: set[str] = set()
    for descriptor in ChainRegistry.all():
        if descriptor.family is ChainFamily.EVM:
            expected |= native_symbols_for(descriptor.name)
    assert ManagedGateway.NATIVE_TOKEN_SYMBOLS == expected


def test_chain_native_symbol_keys_unchanged() -> None:
    """The sibling per-chain map (VIB-4801) stays EVM-only and registry-backed."""
    evm = {d.name for d in ChainRegistry.all() if d.family is ChainFamily.EVM}
    assert set(ManagedGateway.CHAIN_NATIVE_SYMBOL) == evm
