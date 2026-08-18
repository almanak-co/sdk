"""Compound V3 catalogue-key / Comet-address resolution."""

from __future__ import annotations

from almanak.connectors.compound_v3.addresses import (
    COMPOUND_V3_COMET_ADDRESSES,
    resolve_compound_v3_market_key,
)

ARBITRUM_WETH_COMET = COMPOUND_V3_COMET_ADDRESSES["arbitrum"]["weth"]
POLYGON_USDC_COMET = COMPOUND_V3_COMET_ADDRESSES["polygon"]["usdc_e"]


def test_catalogue_key_passthrough() -> None:
    assert resolve_compound_v3_market_key("arbitrum", "weth") == "weth"
    assert resolve_compound_v3_market_key("ethereum", "usdc") == "usdc"


def test_comet_address_resolves_to_key() -> None:
    assert resolve_compound_v3_market_key("arbitrum", ARBITRUM_WETH_COMET) == "weth"
    assert resolve_compound_v3_market_key("arbitrum", ARBITRUM_WETH_COMET.lower()) == "weth"
    assert resolve_compound_v3_market_key("arbitrum", ARBITRUM_WETH_COMET.upper()) == "weth"


def test_polygon_aliased_comet_prefers_chain_default() -> None:
    assert COMPOUND_V3_COMET_ADDRESSES["polygon"]["usdc_bridged"] == POLYGON_USDC_COMET
    assert resolve_compound_v3_market_key("polygon", POLYGON_USDC_COMET) == "usdc_e"


def test_unknown_address_or_key_returns_none() -> None:
    assert resolve_compound_v3_market_key("arbitrum", "0x1111111111111111111111111111111111111111") is None
    assert resolve_compound_v3_market_key("arbitrum", "nonexistent") is None
    assert resolve_compound_v3_market_key("bsc", "weth") is None
    assert resolve_compound_v3_market_key("arbitrum", "") is None


def test_address_on_wrong_chain_returns_none() -> None:
    assert resolve_compound_v3_market_key("ethereum", ARBITRUM_WETH_COMET) is None
