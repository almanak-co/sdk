import pytest

from almanak import ax_lending_markets
from almanak.cli_arguments import LISTED_LENDING_CONNECTORS
from almanak.connectors.composition import CONNECTOR_IDS
from almanak.connectors.discovery import (
    NOT_DISCOVERED,
    lending_discovery,
    perp_discovery,
    pool_connectors,
    pool_discovery,
)

INSTALLED = {str(key) for key in CONNECTOR_IDS}


def test_every_installed_connector_declares_its_discovery_or_says_why_not():
    pools, lending = set(pool_discovery()), set(lending_discovery())
    perps = set(perp_discovery())
    assert not pools & lending and not pools & perps and not lending & perps
    declared = pools | lending | perps
    assert declared | set(NOT_DISCOVERED) == INSTALLED
    assert perps == {"gmx_v2"}
    assert not declared & set(NOT_DISCOVERED)
    assert all(reason.strip() for reason in NOT_DISCOVERED.values())


def test_a_vendor_label_selects_one_connector():
    # Labels match as prefixes; one that prefixes another connector's label is ambiguous.
    labels = [(label, c) for c, found in pool_discovery().items() for label in found.vendor_labels]
    for label, connector in labels:
        for other, owner in labels:
            assert owner == connector or not other.startswith(label), (label, other)


def test_families_partition_the_pool_connectors():
    families = ("concentrated", "resolved", "keyed")
    assert sorted(c for f in families for c in pool_connectors(f)) == sorted(pool_discovery())
    assert pool_connectors("keyed") == ("uniswap_v4",)
    assert set(pool_connectors("resolved")) == {"aerodrome", "curve"}


def test_every_lending_declaration_names_a_listing_ax_reads():
    assert set(ax_lending_markets.LISTINGS) == set(lending_discovery())
    # The CLI offers exactly the connectors a listing reads.
    assert set(LISTED_LENDING_CONNECTORS) == set(ax_lending_markets.LISTINGS)
    # Reserve flags come from the records that keep them per reserve.
    assert set(ax_lending_markets.RESERVE_FLAGS) == {"aave_v3", "sparklend", "aave_v4"}


def test_every_resolved_pool_connector_declares_how_it_is_priced_and_read():
    for connector in pool_connectors("resolved"):
        found = pool_discovery()[connector]
        assert callable(found.price_reader()), connector
        assert found.currencies, connector
    with pytest.raises(ValueError, match="reads no declared price function"):
        pool_discovery()["uniswap_v3"].price_reader()
