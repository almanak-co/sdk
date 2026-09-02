"""``NativeToken.address_aliases`` is the chain-scoped source of native-coin address aliases."""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.chains._descriptor import NativeToken
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.framework.data.tokens.resolver import fold_native_address_alias, is_native_address_alias

POLYGON_PRECOMPILE = "0x0000000000000000000000000000000000001010"
WRAPPED = "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270"


def _native(**overrides: object) -> NativeToken:
    fields: dict[str, object] = {"symbol": "MATIC", "name": "Polygon", "decimals": 18, "wrapped_address": WRAPPED}
    fields.update(overrides)
    return NativeToken(**fields)  # type: ignore[arg-type]


def test_polygon_declares_the_precompile_alias() -> None:
    aliases = {alias.lower() for alias in ChainRegistry.resolve("polygon").native.address_aliases}
    assert aliases == {POLYGON_PRECOMPILE}


def test_no_other_chain_declares_the_polygon_precompile() -> None:
    owners = {
        descriptor.name
        for descriptor in ChainRegistry.all()
        if any(alias.lower() == POLYGON_PRECOMPILE for alias in descriptor.native.address_aliases)
    }
    assert owners == {"polygon"}


def test_resolver_fold_reads_the_descriptor() -> None:
    assert fold_native_address_alias(POLYGON_PRECOMPILE, "polygon") == NATIVE_SENTINEL
    assert fold_native_address_alias(POLYGON_PRECOMPILE.upper().replace("0X", "0x"), "eip155:137") == NATIVE_SENTINEL
    assert is_native_address_alias(POLYGON_PRECOMPILE, "polygon")
    assert fold_native_address_alias(POLYGON_PRECOMPILE, "arbitrum") == POLYGON_PRECOMPILE
    assert fold_native_address_alias(WRAPPED, "polygon") == WRAPPED


def test_alias_must_be_an_evm_address() -> None:
    with pytest.raises(ValueError, match="40-hex"):
        _native(address_aliases=("1010",))


def test_alias_must_not_be_the_wrapped_address() -> None:
    with pytest.raises(ValueError, match="wrapped-native"):
        _native(address_aliases=(WRAPPED.lower(),))


def test_duplicate_aliases_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        _native(address_aliases=(POLYGON_PRECOMPILE, POLYGON_PRECOMPILE.upper().replace("0X", "0x")))
