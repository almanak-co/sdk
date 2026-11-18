"""Freeze the complete Avalanche Uniswap V3 acceptance denominator."""

from almanak.caip import ChainId
from almanak.connectors.composition import installed_connectors
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import EconomicAction, RunMode
from almanak.connectors.uniswap_v3.support import RESTRICTIONS
from tests.qa.catalog import CANDIDATES, catalog, validate
from tests.strategy_fork.rehearsal import production_references

REFERENCES = {
    "uniswap_v3_avalanche_trading",
    "uniswap_v3_avalanche_lp",
    "avalanche_aave_borrow_and_trade_v3",
}
PRODUCTION_REFERENCES = {"uniswap_v3_avalanche_trading", "uniswap_v3_avalanche_lp"}
AVALANCHE = ChainId("eip155:43114")
FULL_ACTIONS = {
    EconomicAction.SWAP,
    EconomicAction.EXIT_EXCHANGE,
    EconomicAction.REVOKE,
    EconomicAction.OPEN_LIQUIDITY,
    EconomicAction.INCREASE_LIQUIDITY,
    EconomicAction.DECREASE_LIQUIDITY,
    EconomicAction.COLLECT,
    EconomicAction.CLOSE_LIQUIDITY,
}


def selected_cases():
    return tuple(case for case in catalog() if case.reference in REFERENCES)


def test_avalanche_declaration_is_final_full_action_and_chain_scoped():
    support = dict(installed_connectors())[ConnectorId("uniswap_v3")].support_for(AVALANCHE)
    assert set(support.actions) == FULL_ACTIONS
    assert support.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert support.account_profiles == ("eoa", "safe_roles")
    assert len(support.historical) == 1
    assert set(support.historical[0].actions) == FULL_ACTIONS - {EconomicAction.REVOKE}
    assert support.restrictions == RESTRICTIONS[1:]
    assert support.restrictions == (
        "configured verified factory, pool and periphery revisions",
        "explicit token pair and fee; exact-input swaps only",
        "one allocation per fund",
        "economic execution assumes access; approval and revocation mechanics excluded",
        "broader NFT manager grants can affect compatible owned or approved NFTs; "
        "gateway operations require the retained strategy-owned NFT",
    )

    chains = {
        entry.chain
        for entry in dict(installed_connectors())[ConnectorId("uniswap_v3")].chain_support
    }
    assert AVALANCHE in chains
    assert ChainId("eip155:56") in chains
    assert ChainId("eip155:4663") in chains


def test_avalanche_acceptance_maps_exactly_31_cases():
    cases = selected_cases()
    assert len(cases) == 31
    assert len({case.id for case in cases}) == 31
    assert {case.reference for case in cases} == REFERENCES
    assert all(case.chains == (AVALANCHE,) for case in cases)
    assert sum(case.lane == "fork" for case in cases) == 22
    assert sum(case.lane == "backtest" for case in cases) == 9
    assert sum(case.id.startswith("cleanup/") for case in cases) == 4
    validate(catalog())


def test_prepared_v3_references_leave_the_blocked_candidate_catalog():
    assert "uniswap_v3_bnb_trading" not in CANDIDATES
    assert "uniswap_v3_bnb_lp" not in CANDIDATES
    assert "bnb_aave_borrow_and_trade_v3" not in CANDIDATES
    assert "uniswap_v3_robinhood_trading" not in CANDIDATES
    assert "uniswap_v3_robinhood_lp" not in CANDIDATES
    assert "polygon_aave_borrow_and_trade_v3" not in CANDIDATES
    assert "robinhood_morpho_borrow_and_trade_v3" not in CANDIDATES


def test_avalanche_v3_production_rehearsal_follows_every_market_declaration():
    selected = {name for name in production_references() if name.startswith("uniswap_v3_avalanche")}
    assert selected == PRODUCTION_REFERENCES
    # Joins once Aave V3 on this chain declares production.
    assert "avalanche_aave_borrow_and_trade_v3" in production_references()
