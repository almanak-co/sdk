"""Registry-projection contract for wrapped/native symbol tables (ALM-3198)."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from almanak.connectors._strategy_base.base.swap_adapter import _CHAIN_WRAPPED_NATIVE
from almanak.core.chains import ChainRegistry, NativeToken
from almanak.core.chains._helpers import (
    chain_wrapped_native_symbol_map,
    native_price_alias_map,
    native_to_wrapped_symbol_map,
    wrapped_to_native_symbol_map,
)
from almanak.framework.data.models import _NATIVE_TO_WRAPPED
from almanak.framework.intents.compiler import IntentCompiler
from almanak.gateway.services.market_service import NATIVE_PRICE_ALIASES

FROZEN_NATIVE_TO_WRAPPED = {
    "ETH": "WETH",
    "MATIC": "WMATIC",
    "AVAX": "WAVAX",
    "BNB": "WBNB",
    "MNT": "WMNT",
    "S": "WS",
    "XPL": "WXPL",
    "BERA": "WBERA",
    "MON": "WMON",
    "OKB": "WOKB",
    "A0GI": "W0G",
    "HYPE": "WHYPE",
    "SOL": "WSOL",
}

FROZEN_WRAPPED_TO_NATIVE = {
    "WETH": "ETH",
    "WMATIC": "MATIC",
    "WAVAX": "AVAX",
    "WBNB": "BNB",
    "WMNT": "MNT",
    "WS": "S",
    "WXPL": "XPL",
    "WPOL": "POL",
    "WOKB": "OKB",
    "WMON": "MON",
    "WBERA": "BERA",
    "W0G": "A0GI",
    "WHYPE": "HYPE",
    "WSOL": "SOL",
}

FROZEN_CHAIN_TO_WRAPPED = {
    "ethereum": "WETH",
    "arbitrum": "WETH",
    "optimism": "WETH",
    "base": "WETH",
    "polygon": "WMATIC",
    "avalanche": "WAVAX",
    "plasma": "WXPL",
    "bsc": "WBNB",
    "mantle": "WMNT",
    "sonic": "WS",
    "xlayer": "WOKB",
    "monad": "WMON",
    "zerog": "W0G",
    "berachain": "WBERA",
}

FROZEN_NATIVE_PRICE_ALIASES = {
    "MNT": "WMNT",
    "MATIC": "WMATIC",
    "POL": "WMATIC",
    "AVAX": "WAVAX",
    "FTM": "WFTM",
    "BNB": "WBNB",
    "S": "WS",
}


def test_framework_native_to_wrapped_is_byte_equivalent() -> None:
    assert dict(_NATIVE_TO_WRAPPED) == FROZEN_NATIVE_TO_WRAPPED


def test_compiler_wrapped_to_native_is_byte_equivalent() -> None:
    assert dict(IntentCompiler._WRAPPED_TO_NATIVE) == FROZEN_WRAPPED_TO_NATIVE


def test_swap_projection_preserves_legacy_and_widens_to_all_descriptors() -> None:
    for chain, symbol in FROZEN_CHAIN_TO_WRAPPED.items():
        assert _CHAIN_WRAPPED_NATIVE[chain] == symbol
    assert set(_CHAIN_WRAPPED_NATIVE) - set(FROZEN_CHAIN_TO_WRAPPED) == {
        "blast",
        "hyperevm",
        "linea",
        "robinhood",
        "solana",
    }


def test_gateway_projection_reconciles_known_drift() -> None:
    expected = {k: v for k, v in FROZEN_NATIVE_PRICE_ALIASES.items() if k != "FTM"}
    expected.update(
        {
            "ETH": "WETH",
            "BERA": "WBERA",
            "HYPE": "WHYPE",
            "MON": "WMON",
            "XPL": "WXPL",
            "SOL": "WSOL",
            "OKB": "WOKB",
            "A0GI": "W0G",
        }
    )
    assert dict(NATIVE_PRICE_ALIASES) == expected


@pytest.mark.parametrize(
    "projection",
    [
        _NATIVE_TO_WRAPPED,
        IntentCompiler._WRAPPED_TO_NATIVE,
        _CHAIN_WRAPPED_NATIVE,
        NATIVE_PRICE_ALIASES,
    ],
)
def test_compatibility_views_are_immutable(projection: object) -> None:
    assert isinstance(projection, MappingProxyType)
    with pytest.raises(TypeError):
        projection["NEW"] = "VALUE"  # type: ignore[index]


def test_public_views_equal_their_registry_builders() -> None:
    assert dict(_NATIVE_TO_WRAPPED) == dict(native_to_wrapped_symbol_map())
    assert dict(IntentCompiler._WRAPPED_TO_NATIVE) == dict(wrapped_to_native_symbol_map())
    assert dict(_CHAIN_WRAPPED_NATIVE) == dict(chain_wrapped_native_symbol_map())
    assert dict(NATIVE_PRICE_ALIASES) == dict(native_price_alias_map())


def test_every_descriptor_pair_is_projected() -> None:
    for descriptor in ChainRegistry.all():
        native = descriptor.native
        assert native.wrapped_symbol is not None, descriptor.name
        native_symbol = native.symbol.upper()
        wrapped_symbol = native.wrapped_symbol.upper()
        assert _NATIVE_TO_WRAPPED[native_symbol] == wrapped_symbol
        assert IntentCompiler._WRAPPED_TO_NATIVE[wrapped_symbol] == native_symbol
        assert _CHAIN_WRAPPED_NATIVE[descriptor.name] == wrapped_symbol
        for accepted_symbol in (native.symbol, *native.accepted_symbols):
            assert NATIVE_PRICE_ALIASES[accepted_symbol.upper()] == wrapped_symbol
        for native_alias, wrapped_alias in native.wrapped_alias_pairs:
            assert IntentCompiler._WRAPPED_TO_NATIVE[wrapped_alias.upper()] == native_alias.upper()


def test_polygon_compatibility_pair_is_descriptor_owned() -> None:
    polygon = ChainRegistry.get("polygon")
    assert polygon.native.wrapped_alias_pairs == (("POL", "WPOL"),)


def test_native_token_new_field_preserves_existing_positional_slots() -> None:
    native = NativeToken("AAA", "A", 18, None, (), "a", "WAAA", "wrapped-a", 60)
    assert native.wrapped_coingecko_id == "wrapped-a"
    assert native.slip44 == 60
    assert native.wrapped_alias_pairs == ()


def test_wrapped_alias_pair_requires_declared_native_spelling() -> None:
    with pytest.raises(ValueError, match="must be symbol or one of accepted_symbols"):
        NativeToken(
            symbol="AAA",
            name="A",
            decimals=18,
            wrapped_symbol="WAAA",
            wrapped_alias_pairs=(("BBB", "WBBB"),),
        )


def test_wrapped_alias_pair_requires_canonical_wrapper() -> None:
    with pytest.raises(ValueError, match="require wrapped_symbol"):
        NativeToken(
            symbol="AAA",
            name="A",
            decimals=18,
            accepted_symbols=("BBB",),
            wrapped_alias_pairs=(("BBB", "WBBB"),),
        )


@pytest.mark.parametrize("invalid_pair", ["AW", ["A", "WA"], ("A", 7)])
def test_wrapped_alias_pair_rejects_non_tuple_string_pairs(invalid_pair: object) -> None:
    with pytest.raises(ValueError, match=r"must be .* pairs"):
        NativeToken(
            symbol="A",
            name="A",
            decimals=18,
            wrapped_symbol="WA",
            wrapped_alias_pairs=(invalid_pair,),  # type: ignore[arg-type]
        )
