"""Final BNB Pancake declaration scope staged on an isolated qualification branch."""

from collections import Counter
from datetime import UTC, datetime

from almanak.caip import ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.support import EconomicAction, RunMode
from tests.qa.catalog import catalog, validate
from tests.strategy_fork.rehearsal import production_references

REFERENCES = {
    "pancakeswap_v3_bnb_trading",
    "pancakeswap_v3_bnb_lp",
    "bnb_aave_pancake_borrow_and_trade",
}
PRODUCTION_REFERENCES = {
    "pancakeswap_v3_bnb_trading",
    "pancakeswap_v3_bnb_lp",
    "pancakeswap_v3_bnb_lp_band",
    # Joins with Aave V3 production on BNB.
    "bnb_aave_pancake_borrow_and_trade",
}
JOINT_BNB_REFERENCES = REFERENCES | {
    "aave_v3_bnb_lending",
    "uniswap_v3_bnb_trading",
    "uniswap_v3_bnb_lp",
    "bnb_aave_borrow_and_trade",
    "bnb_aave_borrow_and_trade_v3",
    "bnb_aave_borrow_and_trade_v4",
}


def test_pancake_bnb_stages_final_actions_beside_the_third_batch_chains():
    connector = connector_from_wire({"connector": "pancakeswap_v3"})
    assert connector.supported_chains(RunMode.PRODUCTION) == (
        ChainId("eip155:56"),
        ChainId("eip155:1"),
        ChainId("eip155:8453"),
        ChainId("eip155:42161"),
    )
    declaration = connector.support_for(ChainId("eip155:56"))
    assert declaration.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert declaration.account_profiles == ("eoa", "safe_roles")
    assert len(declaration.actions) == 8
    assert declaration.historical[0].actions == tuple(
        a for a in declaration.actions if a != EconomicAction.REVOKE
    )
    assert declaration.historical[0].backtest_start_at == datetime(2026, 9, 7, 7, 30, tzinfo=UTC)
    assert all(
        entry.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
        for entry in connector.chain_support
    )
    assert ChainId("eip155:4663") not in {entry.chain for entry in connector.chain_support}


def test_existing_pancake_catalog_covers_all_three_production_references():
    cases = catalog()
    validate(cases)
    selected = [case for case in cases if case.reference in REFERENCES]
    assert len(selected) == 31
    assert Counter(case.lane for case in selected) == {"fork": 22, "backtest": 9}
    assert Counter(case.reference for case in selected) == {
        "pancakeswap_v3_bnb_trading": 9,
        "pancakeswap_v3_bnb_lp": 13,
        "bnb_aave_pancake_borrow_and_trade": 9,
    }
    assert {
        f"cleanup/{policy}/pancakeswap_v3_bnb_lp/{profile}"
        for policy in ("pinned", "fresh")
        for profile in ("eoa", "safe_roles")
    } <= {case.id for case in selected}
    assert all(not case.blocked_reason for case in selected)


def test_pancake_scope_extends_the_joint_bnb_matrix_to_89_cases():
    cases = catalog()
    validate(cases)
    selected = [case for case in cases if case.reference in JOINT_BNB_REFERENCES]
    assert len(selected) == 89
    assert Counter(case.lane for case in selected) == {"fork": 62, "backtest": 27}
    assert all(not case.blocked_reason for case in selected)


def test_bnb_pancake_production_rehearsals_follow_every_market_declaration():
    selected = {
        reference
        for reference in production_references()
        if "pancake" in reference and "bnb" in reference
    }
    assert selected == PRODUCTION_REFERENCES
