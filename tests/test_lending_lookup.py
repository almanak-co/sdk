"""Lending lookups resolve an author market selection and report its price sources."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors import lending_lookup
from almanak.connectors.lending_lookup import (
    LENDING_CONNECTORS,
    Family,
    family,
    market_entry,
    observed_rates,
    optional_observation,
    read_lending_position,
    read_lending_reserve,
    selection,
)
from almanak.connectors.lending_risk import Collateral, RiskQuery
from almanak.gateway.ethereum import DataError
from almanak.models import Account, Block, Chain

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
BLOCK = Block(chain=CHAIN, number=25908472, hash="0x" + "ab" * 32, timestamp=1_700_000_000)
WBTC = "eip155:1/erc20:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
ORACLE = "0x" + "54" * 20
FEED = "0x" + "24" * 20
SPOKE = "0x" + "94" * 20
MARKET_ID = "0x" + "87" * 32

SELECTIONS = {
    "aave_v3": {"chain": "eip155:1", "asset": WBTC},
    "sparklend": {"chain": "eip155:1", "asset": WBTC},
    "aave_v4": {"chain": "eip155:1", "spoke": SPOKE, "reserve_id": 7},
    "compound_v3": {"chain": "eip155:1", "comet": "0x" + "c3" * 20, "asset": WBTC},
    "morpho_blue": {"chain": "eip155:1", "market_id": MARKET_ID, "leg": "loan"},
}


class Reader:
    """Answers the named contract calls; any other call is a test failure."""

    chain = CHAIN

    def __init__(self, replies):
        self.replies = replies
        self.calls = []

    def call(self, target, signature, block, **kwargs):
        assert block == BLOCK
        self.calls.append((target, signature, kwargs.get("args")))
        return (self.replies[(target, signature)],)


def test_every_lending_connector_has_a_family():
    assert set(LENDING_CONNECTORS) == set(lending_lookup.FAMILIES)
    for connector in LENDING_CONNECTORS:
        selected = family(connector)
        assert selected.resolution.MarketSelection.model_fields["connector"]
        assert callable(selected.resolution.resolve_market)


@pytest.mark.parametrize("connector", LENDING_CONNECTORS)
def test_selection_validates_as_the_author_market_entry(connector):
    request = selection(connector, SELECTIONS[connector])
    assert request.connector == connector
    assert market_entry(request, None) == {"connector": connector, **SELECTIONS[connector]}


def test_selection_refuses_fields_of_another_connector():
    with pytest.raises(ValueError, match="comet"):
        selection("aave_v3", {**SELECTIONS["aave_v3"], "comet": "0x" + "c3" * 20})
    with pytest.raises(ValueError, match="unsupported lending connector"):
        selection("euler_v2", {})


def test_market_entry_adds_resolved_assertions_as_optional_pins():
    request = selection("compound_v3", {**SELECTIONS["compound_v3"], "assertions": {"decimals": 8}})
    entry = market_entry(request, {"price_feed": FEED})
    assert entry["assertions"] == {"decimals": 8, "price_feed": FEED}


def test_aave_v3_reports_the_oracle_and_feed_as_allocation_pins():
    provider = "0x" + "2f" * 20
    reader = Reader(
        {(provider, "getPriceOracle()"): ORACLE, (ORACLE, "getSourceOfAsset(address)"): FEED}
    )
    reserve = SimpleNamespace(asset=SimpleNamespace(reference="0x2260"))
    selected = SimpleNamespace(reader=SimpleNamespace(pool_provider=lambda r, s, b: provider))
    sources, pins = lending_lookup.pool_sources(
        reader, selected, SimpleNamespace(reserve=reserve), BLOCK
    )
    assert sources == {"oracle": ORACLE, "price_source": FEED}
    assert pins == {"allocation": {"oracle": ORACLE, "price_source": FEED}}
    assert reader.calls[-1] == (ORACLE, "getSourceOfAsset(address)", ("0x2260",))


def test_aave_v4_reports_the_spoke_oracle_and_reserve_source():
    reader = Reader({(SPOKE, "ORACLE()"): ORACLE, (ORACLE, "getReserveSource(uint256)"): FEED})
    resolved = SimpleNamespace(reserve=SimpleNamespace(spoke=SPOKE, reserve_id=7))
    sources, pins = lending_lookup.spoke_sources(reader, None, resolved, BLOCK)
    assert sources == {"oracle": ORACLE, "price_source": FEED}
    assert pins["debt"] == {"oracle": ORACLE, "debt_price_source": FEED}
    assert pins["collateral_price_source"] == FEED
    assert reader.calls[-1] == (ORACLE, "getReserveSource(uint256)", (7,))


def test_compound_and_morpho_report_resolved_sources_as_assertions():
    market = SimpleNamespace(base_price_feed="0x" + "b1" * 20)
    comet = SimpleNamespace(reserve=SimpleNamespace(price_feed=FEED, market=market))
    sources, pins = lending_lookup.comet_sources(None, None, comet, BLOCK)
    assert sources == {"price_feed": FEED, "base_price_feed": market.base_price_feed}
    assert pins == {"assertions": {"price_feed": FEED}}
    morpho = SimpleNamespace(market=SimpleNamespace(oracle=ORACLE))
    assert lending_lookup.morpho_sources(None, None, morpho, BLOCK) == (
        {"oracle": ORACLE},
        {"assertions": {"oracle": ORACLE}},
    )


def fake_family(monkeypatch, sources, observe, position=None, risk=None):
    resolved = SimpleNamespace(
        summary=lambda: {"block": BLOCK.number}, reserve=SimpleNamespace(asset=WBTC)
    )
    seen = {}

    def resolve_market(reader, request, block):
        seen.update(request=request, block=block)
        return resolved

    selected = Family(
        resolution=SimpleNamespace(resolve_market=resolve_market),
        reader=SimpleNamespace(),
        sources=sources,
        observe=observe,
        position=position,
        risk=risk,
    )
    monkeypatch.setattr(lending_lookup, "family", lambda connector: selected)
    return seen


class Dumped:
    def model_dump(self, mode):
        return {"state": "observed"}


def test_reserve_lookup_returns_resolution_sources_config_and_state(monkeypatch):
    sources = lambda r, s, resolved, b: ({"price_feed": FEED}, {"assertions": {"price_feed": FEED}})  # noqa: E731
    request = selection("compound_v3", SELECTIONS["compound_v3"])
    assessed = []

    def risk(reader, resolved, observation, block):
        assessed.append((type(observation), block))
        return [Collateral(WBTC, Decimal("0.7"), Decimal("0.75"), 500)], {"scope": "stub"}

    seen = fake_family(monkeypatch, sources, lambda r, s, resolved, b: Dumped(), risk=risk)
    converted = []

    def rates_entry(connector, observation, selected):
        converted.append((connector, observation, selected))
        return {"rates": {"supply_apr": "0.03"}}

    monkeypatch.setattr(lending_lookup, "rates_entry", rates_entry)
    result = read_lending_reserve(Reader({}), request, BLOCK, RiskQuery(health_floor=Decimal(2)))
    # The rates are converted from the very observation reported, with no read of their own.
    assert [(c, type(o), s) for c, o, s in converted] == [("compound_v3", Dumped, WBTC)]
    assert assessed == [(Dumped, BLOCK)]
    risk_block = result.pop("risk")
    assert risk_block["scope"] == "stub"
    assert risk_block["collateral"][0]["max_borrow_fraction"] == {
        "by_ltv": "0.7000",
        "at_floor": "0.3750",
    }
    assert seen == {"request": request, "block": BLOCK}
    assert result == {
        "product": "lending_reserve",
        "connector": "compound_v3",
        "resolution": {"block": BLOCK.number},
        "price_sources": {"price_feed": FEED},
        "config": {"market": {**market_entry(request, None), "assertions": {"price_feed": FEED}}},
        "observation": {"state": "observed"},
        "rates": {"supply_apr": "0.03"},
    }


def test_refused_state_read_keeps_the_resolved_sources(monkeypatch):
    def refused(reader, selected, resolved, block):
        raise DataError("market", "Morpho market is uninitialized")

    sources = lambda r, s, resolved, b: ({"oracle": ORACLE}, {"assertions": {"oracle": ORACLE}})  # noqa: E731
    request = selection("morpho_blue", SELECTIONS["morpho_blue"])
    fake_family(monkeypatch, sources, refused)
    result = read_lending_reserve(Reader({}), request, BLOCK)
    assert result["price_sources"] == {"oracle": ORACLE}
    assert result["observation"] is None
    assert result["observation_error"].startswith("market: ")
    assert "rates" not in result and "risk" not in result
    assert optional_observation("aave_v4", None, Dumped) == {
        "observation": {"state": "observed"},
        "rates": None,
        "rates_unavailable": "Aave V4's reserve observation reads no Hub drawn rate or liquidity",
    }


def test_market_rates_reuse_the_reserve_observation_and_report_a_refused_read(monkeypatch):
    observed = []

    def observe(reader, selected, resolved, block):
        observed.append(resolved.name)
        if resolved.name == "refused":
            raise DataError("market", "Morpho market is uninitialized")
        if resolved.name == "invalid":
            raise ValueError("Morpho observation has mixed chain/environment scope")
        return "observation"

    def market(name):
        return SimpleNamespace(name=name, reserve=SimpleNamespace(asset=WBTC))

    fake_family(monkeypatch, None, observe)
    monkeypatch.setattr(lending_lookup, "rates_entry", lambda c, o, s: {"rates": {c: (o, s)}})
    assert observed_rates(None, "morpho_blue", market("ok"), BLOCK) == {
        "rates": {"morpho_blue": ("observation", WBTC)}
    }
    assert observed_rates(None, "morpho_blue", market("refused"), BLOCK) == {
        "rates": None,
        "rates_error": "market: Morpho market is uninitialized",
    }
    assert observed_rates(None, "morpho_blue", market("invalid"), BLOCK) == {
        "rates": None,
        "rates_error": "invalid: Morpho observation has mixed chain/environment scope",
    }
    assert observed == ["ok", "refused", "invalid"]


def test_market_rates_read_nothing_where_the_observation_holds_no_rate(monkeypatch):
    def observe(*_):
        raise AssertionError("an Aave V4 reserve is not read for rates it does not hold")

    fake_family(monkeypatch, None, observe)
    result = observed_rates(None, "aave_v4", None, BLOCK)
    assert result["rates"] is None
    assert "drawn rate" in result["rates_unavailable"]


def test_position_lookup_reads_the_account_in_the_resolved_market(monkeypatch):
    account = Account(chain=CHAIN, address="0x" + "11" * 20)
    seen = {}

    def position(reader, selected, resolved, owner, block):
        seen.update(owner=owner, block=block)
        return Dumped()

    request = selection("aave_v3", SELECTIONS["aave_v3"])
    fake_family(monkeypatch, None, None, position)
    result = read_lending_position(Reader({}), request, account, BLOCK)
    assert seen == {"owner": account, "block": BLOCK}
    assert result["product"] == "lending_position"
    assert result["observation"] == {"state": "observed"}


def test_request_on_another_chain_is_refused_before_resolution(monkeypatch):
    base_weth = "eip155:8453/erc20:0x4200000000000000000000000000000000000006"
    request = selection("aave_v3", {"chain": "eip155:8453", "asset": base_weth})
    fake_family(monkeypatch, None, None)
    with pytest.raises(ValueError, match="differs from the reader chain"):
        read_lending_reserve(Reader({}), request, BLOCK)
