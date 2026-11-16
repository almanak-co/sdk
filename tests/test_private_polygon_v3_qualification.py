"""Frozen Polygon Uniswap V3 declaration and proof catalog checks."""

from pathlib import Path

from almanak.caip import ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.support import ConnectorChainSupport, EconomicAction, RunMode
from almanak.connectors.uniswap_v3.deployments import revisions
from almanak.connectors.uniswap_v3.economic_data import MODEL
from almanak.connectors.uniswap_v3.networks import deployment_for
from almanak.connectors.uniswap_v3.support import (
    CANDIDATE_STARTS,
    RESTRICTIONS,
)
from tests.qa.catalog import CANDIDATES, catalog, validate
from tests.strategy_fork.rehearsal import production_references

POLYGON = ChainId("eip155:137")
EXPECTED_CHAINS = {
    ChainId("eip155:1"),
    ChainId("eip155:10"),
    ChainId("eip155:56"),
    ChainId("eip155:4663"),
    ChainId("eip155:42161"),
    ChainId("eip155:8453"),
    ChainId("eip155:43114"),
}
REFERENCES = {
    "uniswap_v3_polygon_trading",
    "uniswap_v3_polygon_lp",
    "polygon_aave_borrow_and_trade_v3",
}
PRODUCTION_REFERENCES = {"uniswap_v3_polygon_trading", "uniswap_v3_polygon_lp"}
ROOT = Path(__file__).parents[1]


def test_polygon_declaration_admits_production_with_the_qualified_scope():
    entries = connector_from_wire({"connector": "uniswap_v3"}).chain_support
    assert {entry.chain for entry in entries} == EXPECTED_CHAINS | {POLYGON}
    declaration = next(entry for entry in entries if entry.chain == POLYGON)
    assert ConnectorChainSupport.model_validate(declaration.model_dump()) == declaration
    assert declaration.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert declaration.account_profiles == ("eoa", "safe_roles")
    assert declaration.restrictions == RESTRICTIONS[1:]
    assert "eip155:137" not in {chain for chain, _ in CANDIDATE_STARTS}
    assert set(declaration.actions) == {
        EconomicAction.SWAP,
        EconomicAction.EXIT_EXCHANGE,
        EconomicAction.REVOKE,
        EconomicAction.OPEN_LIQUIDITY,
        EconomicAction.INCREASE_LIQUIDITY,
        EconomicAction.DECREASE_LIQUIDITY,
        EconomicAction.COLLECT,
        EconomicAction.CLOSE_LIQUIDITY,
    }
    assert len(declaration.historical) == 1
    historical = declaration.historical[0]
    assert historical.model == MODEL and historical.mode == "economic"
    assert set(historical.actions) == set(declaration.actions) - {EconomicAction.REVOKE}


def test_private_declaration_retains_the_compiled_polygon_router_revision():
    deployment = deployment_for(POLYGON)
    assert str(deployment.router) == "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"
    router = revisions()["router"]
    assert router["compiler_input_sha256"] == (
        "a861f70e9ff00fdafe46bc09b619e79fd1cefdce4343632e519dd5ec762e709a"
    )
    assert router["runtime_length"] == 24497
    assert router["normalized_code_hash"] == (
        "0xe9acc1f5b963bf7640c14bb9547d82f5bf3b6f6cc82fb30189767a973fbd34b1"
    )


def test_private_catalog_maps_exactly_the_polygon_package():
    cases = catalog()
    validate(cases)
    selected = [case for case in cases if case.reference in REFERENCES]
    assert len(selected) == 31
    assert {case.reference for case in selected} == REFERENCES
    assert all(not case.blocked_reason for case in selected)
    assert not (REFERENCES & CANDIDATES.keys())
    assert (
        not {
            "uniswap_v3_robinhood_trading",
            "uniswap_v3_robinhood_lp",
            "robinhood_morpho_borrow_and_trade_v3",
        }
        & CANDIDATES.keys()
    )

    composed = [case for case in selected if case.reference == "polygon_aave_borrow_and_trade_v3"]
    assert len(composed) == 9
    assert all(
        {(str(scope.connector), tuple(map(str, scope.chains))) for scope in case.scopes}
        == {
            ("aave_v3", ("eip155:137",)),
            ("uniswap_v3", ("eip155:137",)),
        }
        for case in composed
    )

    author = read_inputs(ROOT / "strategies/polygon_aave_borrow_and_trade_v3/config.yaml").inputs
    assert {allocation.account for allocation in author.strategy.allocations.values()} == {"fund"}
    assert {item.account for item in author.inventory} == {"fund"}


def test_polygon_v3_production_rehearsal_follows_every_market_declaration():
    selected = {name for name in production_references() if name.startswith("uniswap_v3_polygon")}
    assert selected == PRODUCTION_REFERENCES
    # Joins once Aave V3 on this chain declares production.
    assert "polygon_aave_borrow_and_trade_v3" in production_references()
