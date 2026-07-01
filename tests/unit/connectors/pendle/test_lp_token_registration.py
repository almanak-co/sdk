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
from almanak.connectors.pendle.sdk import (
    MARKET_BY_PT_TOKEN,
    MARKET_BY_YT_TOKEN,
    PENDLE_LP_TOKEN_DECIMALS,
)
from almanak.framework.data.tokens import get_token_resolver

_SUSDAI_MARKET = "0xcbf629c8d396b1261f81f55175afa010e94787d8"


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
