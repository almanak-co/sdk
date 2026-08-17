"""Pendle market/LP-token static registration (VIB-5487, BUG B).

A Pendle market contract address IS its own fungible LP token. A strategy that
holds LP carries the market address in its tracked-token set, so every
portfolio/teardown snapshot resolves that address as a token. Before this fix the
market address was NOT in the static registry, so resolution fell to the gateway
``GetTokenMetadata`` fallback and timed out (30s × 3 ≈ 90-180s per snapshot),
crawling Pendle-LP strategies on mainnet.

These tests pin the registration so the market address resolves from the static
index (no gateway) as an 18-decimal LP token, exactly like PT/YT.
"""

import pytest

from almanak.connectors.pendle.metadata_provider import PendleProtocolMetadataConnector
from almanak.connectors.pendle.permission_hints import _market_grid
from almanak.connectors.pendle.sdk import (
    MARKET_BY_PT_TOKEN,
    MARKET_BY_YT_TOKEN,
    MARKET_TOKEN_MINT_SY,
    PENDLE_LP_TOKEN_DECIMALS,
    PT_TOKEN_INFO,
    YT_TOKEN_INFO,
)
from almanak.framework.data.tokens import get_token_resolver

_SUSDAI_MARKET = "0xcbf629c8d396b1261f81f55175afa010e94787d8"
_ETHEREUM_SUSDE_MARKET = "0x47ad2cd1dd15739a7a035b9d3b7828d916fef77e"
_ETHEREUM_SUSDE_PT = "0xb195b618ea52b77cb2a58846f452f59f8dfa9390"
_ETHEREUM_SUSDE_YT = "0x89e6e5f7c3a60e7d6347f054051a29a272f4ce44"
_ETHEREUM_EXPIRED_SUSDE_MARKET = "0x177768caf9d0e036725a51d3f60d7e20f2d4d194"
_ETHEREUM_EXPIRED_SUSDE_PT = "0x5a19fa369f2895dcd8d2cee62e4ceae58ef92bbb"
_ETHEREUM_EXPIRED_SUSDE_YT = "0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2"
_SUSDE = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"


def _lp_tokens():
    return [t for t in PendleProtocolMetadataConnector().synthetic_tokens() if t.family == "LP"]


def test_every_market_address_registered_as_lp_token():
    """Each unique (chain, market address) from the market maps has an LP entry."""
    lp_by_addr = {(t.chain.lower(), t.address.lower()) for t in _lp_tokens()}
    for token_map in (MARKET_BY_PT_TOKEN, MARKET_BY_YT_TOKEN):
        for chain, chain_markets in token_map.items():
            for market_address in chain_markets.values():
                assert (chain.lower(), market_address.lower()) in lp_by_addr, (
                    f"Pendle market {market_address} on {chain} is not registered as an "
                    f"LP token — its balance read will hit the 30s gateway fallback (VIB-5487)."
                )


def test_lp_tokens_are_18_decimals():
    for t in _lp_tokens():
        assert t.decimals == PENDLE_LP_TOKEN_DECIMALS == 18


def test_lp_symbols_do_not_collide_with_pt_yt():
    """LP symbols use the PLP- prefix, distinct from PT-/YT-."""
    for t in _lp_tokens():
        assert t.symbol.startswith("PLP-"), t.symbol
        assert not t.symbol.startswith(("PT-", "YT-"))


def test_susdai_market_resolves_without_gateway():
    """The live sUSDai market resolves from the static index (skip_gateway)."""
    resolved = get_token_resolver().resolve(_SUSDAI_MARKET, "arbitrum", skip_gateway=True)
    assert resolved.address.lower() == _SUSDAI_MARKET
    assert resolved.decimals == 18
    assert resolved.symbol.upper().startswith("PLP-")


def test_ethereum_canonical_grid_uses_active_susde_market():
    """The permission grid must rotate atomically with the executable market."""
    assert _market_grid("ethereum") == {
        "sy_token": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
        "pt_name": "PT-SUSDE-26NOV2026",
        "yt_name": "YT-SUSDE-26NOV2026",
        "market_addr": _ETHEREUM_SUSDE_MARKET,
        "pre_swap_token": "USDC",
    }


def test_ethereum_susde_rotation_pins_active_and_historical_generations():
    """A same-count address substitution must not erase either generation."""
    assert MARKET_BY_PT_TOKEN["ethereum"]["PT-sUSDe-26NOV2026"] == _ETHEREUM_SUSDE_MARKET
    assert PT_TOKEN_INFO["ethereum"]["PT-sUSDe-26NOV2026"] == (_ETHEREUM_SUSDE_PT, 18)
    assert YT_TOKEN_INFO["ethereum"]["YT-sUSDe-26NOV2026"] == (_ETHEREUM_SUSDE_YT, 18)
    assert MARKET_BY_YT_TOKEN["ethereum"]["YT-sUSDe-26NOV2026"] == _ETHEREUM_SUSDE_MARKET
    assert MARKET_TOKEN_MINT_SY["ethereum"][_ETHEREUM_SUSDE_MARKET] == _SUSDE

    assert MARKET_BY_PT_TOKEN["ethereum"]["PT-sUSDe-13AUG2026"] == _ETHEREUM_EXPIRED_SUSDE_MARKET
    assert PT_TOKEN_INFO["ethereum"]["PT-sUSDe-13AUG2026"] == (_ETHEREUM_EXPIRED_SUSDE_PT, 18)
    assert YT_TOKEN_INFO["ethereum"]["YT-sUSDe-13AUG2026"] == (_ETHEREUM_EXPIRED_SUSDE_YT, 18)
    assert MARKET_BY_YT_TOKEN["ethereum"]["YT-sUSDe-13AUG2026"] == _ETHEREUM_EXPIRED_SUSDE_MARKET
    assert MARKET_TOKEN_MINT_SY["ethereum"][_ETHEREUM_EXPIRED_SUSDE_MARKET] == _SUSDE


@pytest.mark.parametrize(
    "market_address",
    sorted({m.lower() for cm in MARKET_BY_PT_TOKEN.values() for m in cm.values()}),
)
def test_all_pt_markets_resolve_without_gateway(market_address):
    """No PT market address falls through to the gateway metadata timeout."""
    # chain lookup: find the chain this market belongs to
    chain = next(c for c, cm in MARKET_BY_PT_TOKEN.items() if any(m.lower() == market_address for m in cm.values()))
    resolved = get_token_resolver().resolve(market_address, chain, skip_gateway=True)
    assert resolved.address.lower() == market_address
    assert resolved.decimals == 18
