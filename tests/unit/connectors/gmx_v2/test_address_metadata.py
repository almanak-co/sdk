"""Tests for GMX V2 connector address metadata."""

from almanak.connectors.gmx_v2.addresses import (
    GMX_V2,
    GMX_V2_INDEX_TOKEN_DECIMALS,
    GMX_V2_MARKETS,
)


def test_gmx_v2_index_token_decimals_cover_every_listed_market() -> None:
    for chain, markets in GMX_V2_MARKETS.items():
        market_addresses = {address.lower() for address in markets.values()}
        decimal_addresses = set(GMX_V2_INDEX_TOKEN_DECIMALS[chain])

        assert decimal_addresses == market_addresses


def test_gmx_v2_decimal_keys_are_normalized() -> None:
    for decimals_by_market in GMX_V2_INDEX_TOKEN_DECIMALS.values():
        assert all(address == address.lower() for address in decimals_by_market)


def test_avalanche_avax_market_matches_core_address_table() -> None:
    assert GMX_V2_MARKETS["avalanche"]["AVAX/USD"] == GMX_V2["avalanche"]["avax_usd_market"]


def test_known_synthetic_market_decimals_are_not_defaulted_to_18() -> None:
    assert GMX_V2_INDEX_TOKEN_DECIMALS["arbitrum"][GMX_V2_MARKETS["arbitrum"]["NEAR/USD"].lower()] == 24
    assert GMX_V2_INDEX_TOKEN_DECIMALS["avalanche"][GMX_V2_MARKETS["avalanche"]["LTC/USD"].lower()] == 8
