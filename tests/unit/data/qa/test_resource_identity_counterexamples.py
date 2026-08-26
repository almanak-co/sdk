"""Permanent Data QA counterexamples from ALM-3252, ALM-3227 and ALM-3241-A."""

from __future__ import annotations

from dataclasses import replace

import pytest

from almanak.framework.data.qa.resource_identity import (
    AuthoritativeFeed,
    DataCapability,
    DataRequirement,
    FeedIdentity,
    PriceRoute,
    PriceRouteKind,
    discriminate_feed_identity,
    evaluate_data_requirement,
    validate_price_route,
)
from almanak.integrations.chainlink.catalog import CATALOG

_RETH_FEED = "0x536218f9E9Eb48863970252233c8F271f554C2d0"
_ETH_USD_FEED = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"


def _feed(*, address: str, pair: str, decimals: int, kind: str = "crypto") -> FeedIdentity:
    return FeedIdentity(
        provider="chainlink",
        chain="ethereum",
        address=address,
        pair=pair,
        decimals=decimals,
        kind=kind,
    )


def _catalog_feed(pair: str) -> FeedIdentity:
    spec = CATALOG.feed("ethereum", pair)
    assert spec is not None
    return _feed(address=spec.address, pair=spec.pair, decimals=spec.decimals, kind=spec.kind.value)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("provider", "not-chainlink", "feed_provider_mismatch"),
        ("chain", "arbitrum", "feed_chain_mismatch"),
        ("address", "0x0000000000000000000000000000000000000001", "feed_address_mismatch"),
        ("pair", "RETH/USD", "feed_pair_mismatch"),
        ("decimals", 8, "feed_decimals_mismatch"),
        ("kind", "usd", "feed_kind_mismatch"),
    ],
)
def test_resource_identity_discriminates_every_authoritative_field(field: str, value: object, reason: str) -> None:
    identity = _feed(address=_RETH_FEED, pair="RETH/ETH", decimals=18, kind="eth")
    authority = AuthoritativeFeed(
        identity=identity,
        authority_uri="https://data.chain.link/ethereum/mainnet/crypto-eth/reth-eth",
    )

    result = discriminate_feed_identity(replace(identity, **{field: value}), authority)

    assert result.reason_codes == (reason,)


def test_alm_3252_reth_usd_mislabelling_is_rejected_by_authoritative_identity() -> None:
    """ALM-3252: an address match cannot excuse wrong pair/kind/decimals."""
    authority = AuthoritativeFeed(
        identity=_feed(address=_RETH_FEED, pair="RETH/ETH", decimals=18, kind="eth"),
        authority_uri="https://data.chain.link/ethereum/mainnet/crypto-eth/reth-eth",
    )
    mislabeled = _feed(address=_RETH_FEED, pair="RETH/USD", decimals=8, kind="usd")

    result = discriminate_feed_identity(mislabeled, authority)

    assert result.passed is False
    assert result.reason_codes == ("feed_pair_mismatch", "feed_decimals_mismatch", "feed_kind_mismatch")


def test_alm_3252_reth_eth_authoritative_identity_positive_control() -> None:
    authority = AuthoritativeFeed(
        identity=_feed(address=_RETH_FEED, pair="RETH/ETH", decimals=18, kind="eth"),
        authority_uri="https://data.chain.link/ethereum/mainnet/crypto-eth/reth-eth",
    )

    assert discriminate_feed_identity(_catalog_feed("RETH/ETH"), authority).passed is True
    assert CATALOG.feed("ethereum", "RETH/USD") is None


def test_alm_3252_reth_usd_requires_dimensionally_valid_derived_route() -> None:
    reth_eth = _feed(address=_RETH_FEED, pair="RETH/ETH", decimals=18)
    eth_usd = _feed(address=_ETH_USD_FEED, pair="ETH/USD", decimals=8)
    resources = {feed.resource_id: feed for feed in (reth_eth, eth_usd)}
    route = PriceRoute(
        chain="ethereum",
        output_pair="RETH/USD",
        kind=PriceRouteKind.DERIVED_PRODUCT,
        component_resource_ids=(reth_eth.resource_id, eth_usd.resource_id),
    )

    assert validate_price_route(route, resources).passed is True

    invalid = PriceRoute(
        chain="ethereum",
        output_pair="RETH/USD",
        kind=PriceRouteKind.DERIVED_PRODUCT,
        component_resource_ids=(eth_usd.resource_id, reth_eth.resource_id),
    )
    assert validate_price_route(invalid, resources).reason_codes == ("derived_route_denominator_mismatch",)


def test_alm_3227_missing_ethereum_xau_route_is_an_explicit_capability_failure() -> None:
    """ALM-3227: a feed on BSC cannot satisfy the Ethereum requirement."""
    requirement = DataRequirement(
        requirement_id="alm-3227.ethereum-xau",
        chain="ethereum",
        pair="XAU/USD",
        capabilities=frozenset({DataCapability.REFERENCE_PRICE}),
    )
    bsc_route = PriceRoute(
        chain="bsc",
        output_pair="XAU/USD",
        kind=PriceRouteKind.DIRECT,
        component_resource_ids=("chainlink:bsc:0x86896feb19d8a607c3b11f2af50a0f239bd71cd0",),
    )

    result = evaluate_data_requirement(
        requirement,
        routes=[bsc_route],
        calendar_pairs=set(),
        band_depth_pairs=set(),
    )

    assert result.reason_codes == ("required_price_route_missing",)


def test_alm_3241_a_equity_price_and_calendar_absence_cannot_pass_vacuously() -> None:
    """ALM-3241-A: unsupported TSLA price/calendar requirements stay explicit."""
    requirement = DataRequirement(
        requirement_id="alm-3241-a.ethereum-tsla",
        chain="ethereum",
        pair="TSLA/USD",
        capabilities=frozenset({DataCapability.REFERENCE_PRICE, DataCapability.MARKET_CALENDAR}),
    )

    result = evaluate_data_requirement(
        requirement,
        routes=[],
        calendar_pairs=set(),
        band_depth_pairs=set(),
    )

    assert result.passed is False
    assert result.reason_codes == ("required_price_route_missing", "required_market_calendar_missing")


def test_alm_3241_a_equity_price_and_calendar_positive_control() -> None:
    requirement = DataRequirement(
        requirement_id="alm-3241-a.ethereum-tsla",
        chain="ethereum",
        pair="TSLA/USD",
        capabilities=frozenset({DataCapability.REFERENCE_PRICE, DataCapability.MARKET_CALENDAR}),
    )
    hypothetical_route = PriceRoute(
        chain="ethereum",
        output_pair="TSLA/USD",
        kind=PriceRouteKind.DIRECT,
        component_resource_ids=("authoritative:ethereum:tsla",),
    )

    result = evaluate_data_requirement(
        requirement,
        routes=[hypothetical_route],
        calendar_pairs={("ethereum", "TSLA/USD")},
        band_depth_pairs=set(),
    )

    assert result.passed is True
