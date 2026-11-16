"""Fourth-batch lending production admission and its exact production rehearsal selection."""

from datetime import UTC, datetime

import pytest

from almanak.caip import ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.support import OP_FAMILY_CHAINS, RunMode
from tests.strategy_fork.rehearsal import ROOT, production_references

MORPHO_CHAINS = {
    "ethereum": "eip155:1",
    "base": "eip155:8453",
    "arbitrum": "eip155:42161",
    "optimism": "eip155:10",
    "polygon": "eip155:137",
    "robinhood": "eip155:4663",
    "bnb": "eip155:56",
    "avalanche": "eip155:43114",
}
COMPOUND_CHAINS = {
    name: MORPHO_CHAINS[name] for name in ("ethereum", "base", "arbitrum", "optimism")
}
LENDING_START = datetime(2026, 9, 7, 15, tzinfo=UTC)
# Retained backtest boundaries are unchanged by production admission.
CELLS = {
    **{("morpho_blue", chain): LENDING_START for chain in MORPHO_CHAINS.values()},
    **{("compound_v3", chain): LENDING_START for chain in COMPOUND_CHAINS.values()},
    ("sparklend", "eip155:1"): datetime(2026, 9, 7, 5, 33, 47, tzinfo=UTC),
}
STANDALONE = {
    *(f"morpho_blue_{name}_lending" for name in MORPHO_CHAINS),
    *(f"compound_v3_{name}_lending" for name in COMPOUND_CHAINS),
    "sparklend_ethereum_lending",
}
# Every other connector in these compositions already declares production on its chain.
COMPOSED = {
    "arbitrum_compound_borrow_and_trade",
    "arbitrum_morpho_borrow_and_trade",
    "avalanche_morpho_borrow_and_trade_v4",
    "base_compound_borrow_and_trade",
    "base_morpho_borrow_and_trade",
    "base_morpho_leverage_loop",
    "bnb_morpho_curve_borrow_and_trade",
    "ethereum_compound_curve_borrow_and_trade",
    "ethereum_morpho_curve_borrow_and_trade",
    "ethereum_spark_curve_borrow_and_trade",
    "optimism_compound_curve_borrow_and_trade",
    "optimism_morpho_curve_borrow_and_trade",
    "polygon_morpho_curve_borrow_and_trade",
    "robinhood_morpho_borrow_and_trade_v3",
    "robinhood_morpho_borrow_and_trade_v4",
    "robinhood_morpho_curve_borrow_and_trade",
}
LENDING = {"morpho_blue", "compound_v3", "sparklend"}
# Admitted by its own 20 October 2026 acceptance; tests/test_hyperevm_morpho.py owns it.
LATER = {"morpho_blue_hyperevm_lending"}


@pytest.mark.parametrize(("connector", "chain"), sorted(CELLS))
def test_fourth_batch_cell_admits_production_with_unchanged_scope(connector, chain):
    support = connector_from_wire({"connector": connector}).support_for(ChainId(chain))
    assert support.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert support.account_profiles == ("eoa", "safe_roles")
    for action in support.actions:
        support.require(RunMode.PRODUCTION, action, "eoa")
        support.require(RunMode.PRODUCTION, action, "safe_roles")
    assert not any("production unavailable" in item for item in support.restrictions)
    assert support.historical
    assert {h.backtest_start_at for h in support.historical} == {CELLS[connector, chain]}
    assert ("pinned Jovian L1 and operator fee accounting" in support.restrictions) == (
        support.chain in OP_FAMILY_CHAINS
    )


def test_compound_polygon_stays_undeclared():
    # Both official Polygon Comets keep zero collateral supply caps.
    chains = {s.chain for s in connector_from_wire({"connector": "compound_v3"}).chain_support}
    assert ChainId("eip155:137") not in chains


def test_fourth_batch_production_rehearsal_selects_every_lending_reference():
    admitted = set(production_references())
    lending = set()
    for name in admitted:
        inputs = read_inputs(ROOT / "strategies" / name / "config.yaml").inputs
        if any(str(market.connector) in LENDING for market in inputs.markets.values()):
            lending.add(name)
    assert lending == STANDALONE | COMPOSED | LATER
    assert len(STANDALONE) == 13
    assert len(COMPOSED) == 16
    assert "compound_v3_polygon_lending" not in admitted
