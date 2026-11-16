"""Exact Uniswap V3 Robinhood scope on the private qualification branch."""

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from almanak.caip import ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import EconomicAction, RunMode
from almanak.connectors.uniswap_v3.support import RESTRICTIONS
from scripts.legacy_configuration.author import read_author
from tests.qa.catalog import CANDIDATES, catalog, validate
from tests.strategy_fork.rehearsal import production_references

ROBINHOOD = ChainId("eip155:4663")
ROOT = Path(__file__).parents[1]
REFERENCES = {
    "uniswap_v3_robinhood_trading",
    "uniswap_v3_robinhood_lp",
    "robinhood_morpho_borrow_and_trade_v3",
}
PRODUCTION_REFERENCES = {
    "uniswap_v3_robinhood_trading",
    "uniswap_v3_robinhood_lp",
    "uniswap_v3_robinhood_lp_band",
}


def test_robinhood_v3_stages_the_existing_full_scope_and_earliest_retained_window():
    connector = connector_from_wire({"connector": "uniswap_v3"})
    assert connector.supported_chains(RunMode.PRODUCTION) == (
        ChainId("eip155:1"),
        ChainId("eip155:8453"),
        ChainId("eip155:42161"),
        ChainId("eip155:10"),
        ChainId("eip155:137"),
        ChainId("eip155:43114"),
        ChainId("eip155:56"),
        ROBINHOOD,
    )
    declaration = connector.support_for(ROBINHOOD)
    assert declaration.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert declaration.restrictions == RESTRICTIONS[1:]
    assert declaration.account_profiles == ("eoa", "safe_roles")
    assert len(declaration.actions) == 8
    historical = declaration.historical[0]
    assert historical.mode == "economic"
    assert historical.backtest_start_at == datetime(2026, 9, 8, 5, 28, tzinfo=UTC)
    assert historical.actions == tuple(
        action for action in declaration.actions if action != EconomicAction.REVOKE
    )


def test_robinhood_v3_maps_exact_matrix_and_composed_connector_dependencies():
    cases = catalog()
    validate(cases)
    selected = [case for case in cases if case.reference in REFERENCES]
    assert len(selected) == len({case.id for case in selected}) == 31
    assert Counter(case.lane for case in selected) == {"fork": 22, "backtest": 9}
    assert Counter(case.reference for case in selected) == {
        "uniswap_v3_robinhood_trading": 9,
        "uniswap_v3_robinhood_lp": 13,
        "robinhood_morpho_borrow_and_trade_v3": 9,
    }
    assert all(not case.blocked_reason for case in selected)
    assert all(case.chains == (ROBINHOOD,) for case in selected)
    assert not REFERENCES & CANDIDATES.keys()
    composed = [
        case for case in selected if case.reference == "robinhood_morpho_borrow_and_trade_v3"
    ]
    assert all(
        {(str(scope.connector), scope.chains) for scope in case.scopes}
        == {("morpho_blue", (ROBINHOOD,)), ("uniswap_v3", (ROBINHOOD,))}
        for case in composed
    )


def test_robinhood_v3_standalone_references_enter_production_rehearsal_selection():
    selected = set()
    for name in production_references():
        inputs = read_inputs(ROOT / "strategies" / name / "config.yaml").inputs
        if any(
            market.chain == ROBINHOOD and ConnectorId(market.connector) == ConnectorId("uniswap_v3")
            for market in inputs.markets.values()
        ):
            selected.add(name)
    # The fourth batch admits Morpho on Robinhood, so its V3 composition joins.
    assert selected == PRODUCTION_REFERENCES | {"robinhood_morpho_borrow_and_trade_v3"}
    for name in REFERENCES:
        config = read_author(ROOT / "tests/fixtures/author/candidates" / name / "config.yaml")
        assert {allocation.chain.chain_id for allocation in config.fork_config().allocations} == {
            ROBINHOOD
        }
