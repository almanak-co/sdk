"""Unit contracts for the integration-owned Chainlink feed catalogue."""

from __future__ import annotations

from decimal import Decimal

from almanak.integrations.chainlink.catalog import ChainlinkCatalog
from almanak.integrations.chainlink.models import FeedKind


def test_feeds_normalizes_chain_and_filters_by_kind() -> None:
    catalog = ChainlinkCatalog()

    all_feeds = catalog.feeds(" Ethereum ")
    usd_feeds = catalog.feeds("ETHEREUM", kind=FeedKind.USD)
    eth_feeds = catalog.feeds(" ethereum ", kind=FeedKind.ETH)

    assert all_feeds["ETH/USD"].kind is FeedKind.USD
    assert set(usd_feeds).isdisjoint(eth_feeds)
    assert set(eth_feeds) == {"WSTETH/ETH", "RETH/ETH"}
    assert catalog.feeds("unknown") == {}


def test_reference_feeds_are_catalogued_but_excluded_from_generic_usd_feeds() -> None:
    catalog = ChainlinkCatalog()

    reference = catalog.feed("bsc", "XAU/USD")

    assert reference is not None
    assert reference.kind is FeedKind.REFERENCE
    assert "XAU/USD" not in catalog.feeds("bsc", kind=FeedKind.USD)


def test_feed_normalizes_chain_and_pair_and_fails_closed() -> None:
    catalog = ChainlinkCatalog()

    feed = catalog.feed(" Ethereum ", " eth/usd ")

    assert feed is not None
    assert feed.pair == "ETH/USD"
    assert catalog.feed("unknown", "ETH/USD") is None
    assert catalog.feed("ethereum", "MISSING/USD") is None


def test_token_alias_resolution_and_derived_feeds() -> None:
    catalog = ChainlinkCatalog()

    assert catalog.feed_for_token(" ethereum ", " weth ") == catalog.feed("ethereum", "ETH/USD")
    assert catalog.feed_for_token("ethereum", "not-a-token") is None
    assert catalog.feed_for_token("unknown", "WETH") is None
    assert catalog.derived_feed_for_token(" ETHEREUM ", " steth ") == catalog.feed("ethereum", "WSTETH/ETH")
    assert catalog.derived_feed_for_token("ethereum", "reth") == catalog.feed("ethereum", "RETH/ETH")
    assert catalog.feed_for_token("ethereum", "reth") is None
    assert catalog.derived_feed_for_token("ethereum", "ETH") is None
    assert catalog.derived_feed_for_token("unknown", "WSTETH") is None


def test_reth_address_is_catalogued_as_18_decimal_eth_feed_not_usd() -> None:
    catalog = ChainlinkCatalog()

    reth_eth = catalog.feed("ethereum", "RETH/ETH")

    assert reth_eth is not None
    assert reth_eth.address == "0x536218f9E9Eb48863970252233c8F271f554C2d0"
    assert reth_eth.kind is FeedKind.ETH
    assert reth_eth.decimals == 18
    assert reth_eth.heartbeat_seconds == 86_400
    assert reth_eth.deviation_threshold_pct == Decimal("2.0")
    assert catalog.feed("ethereum", "RETH/USD") is None


def test_chain_aliases_resolve_to_the_canonical_feed_catalogue() -> None:
    catalog = ChainlinkCatalog()
    assert catalog.feeds("bnb") == catalog.feeds("bsc")
    assert catalog.feed_for_token("bnb", "WBNB") == catalog.feed("bsc", "BNB/USD")
    assert catalog.supports_chain("bnb")
