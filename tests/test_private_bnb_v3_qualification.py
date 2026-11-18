"""Freeze the BNB Uniswap V3 production and composed qualification scope."""

from collections import Counter
from pathlib import Path

from almanak.caip import ChainId
from almanak.connectors.aave_v3.support import verified_support as aave_support
from almanak.connectors.composition import installed_connectors
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import RunMode
from almanak.connectors.uniswap_v3.support import RESTRICTIONS
from almanak.connectors.uniswap_v3.support import verified_support as uniswap_support
from scripts.legacy_configuration.author import read_author
from tests.qa.catalog import CANDIDATES, catalog, validate
from tests.strategy_fork.rehearsal import production_references

BNB = ChainId("eip155:56")
ROOT = Path(__file__).parents[1]
NEW_V3_REFERENCES = {
    "uniswap_v3_bnb_trading",
    "uniswap_v3_bnb_lp",
    "bnb_aave_borrow_and_trade_v3",
}
JOINT_QUALIFICATION_REFERENCES = {
    "aave_v3_bnb_lending",
    "uniswap_v3_bnb_trading",
    "uniswap_v3_bnb_lp",
    "bnb_aave_borrow_and_trade",
    "bnb_aave_borrow_and_trade_v3",
    "bnb_aave_borrow_and_trade_v4",
}
EXPECTED_BNB_PRODUCTION_REFERENCES = {
    "curve_bnb_stable_lp",
    "curve_bnb_twocrypto_lp",
    "uniswap_v4_bnb_lp",
    "uniswap_v4_bnb_trading",
    "pancakeswap_v3_bnb_trading",
    "pancakeswap_v3_bnb_lp",
    "uniswap_v3_bnb_trading",
    "uniswap_v3_bnb_lp",
    "pancakeswap_v3_bnb_lp_band",
    # Aave V3 BNB production admits the composed BNB references whose every
    # market now declares production EOA support.
    "aave_v3_bnb_lending",
    "bnb_aave_borrow_and_trade",
    "bnb_aave_borrow_and_trade_v3",
    "bnb_aave_borrow_and_trade_v4",
    "bnb_aave_pancake_borrow_and_trade",
}


def test_private_branch_stages_final_bnb_production_declarations():
    connectors = dict(installed_connectors())
    aave = connectors[ConnectorId("aave_v3")]
    uniswap = connectors[ConnectorId("uniswap_v3")]
    aave_bnb = aave.support_for(BNB)
    uniswap_bnb = uniswap.support_for(BNB)
    assert aave_bnb.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert uniswap_bnb.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert aave_bnb.account_profiles == ("eoa", "safe_roles")
    assert uniswap_bnb.account_profiles == ("eoa", "safe_roles")

    assert aave_bnb.restrictions == (
        "verified revision-11 PoolInstance and reserve-token implementations",
        "complete proxy fingerprints; reserve proxy metadata difference retained",
    )
    assert uniswap_bnb.restrictions == RESTRICTIONS[1:]
    assert aave.chain_support == aave_support()
    assert uniswap.chain_support == uniswap_support()
    assert uniswap.chain_support[-2].chain == BNB
    assert ChainId("eip155:4663") in {entry.chain for entry in uniswap.chain_support}


def test_private_catalog_separates_new_v3_cases_from_joint_bnb_scope():
    cases = catalog()
    validate(cases)
    selected = [case for case in cases if case.reference in JOINT_QUALIFICATION_REFERENCES]
    new_v3 = [case for case in selected if case.reference in NEW_V3_REFERENCES]

    assert len(new_v3) == 31
    assert Counter(case.lane for case in new_v3) == {"fork": 22, "backtest": 9}
    assert len(selected) == 58
    assert Counter(case.lane for case in selected) == {"fork": 40, "backtest": 18}
    assert Counter(case.reference for case in selected) == {
        "aave_v3_bnb_lending": 9,
        "uniswap_v3_bnb_trading": 9,
        "uniswap_v3_bnb_lp": 13,
        "bnb_aave_borrow_and_trade": 9,
        "bnb_aave_borrow_and_trade_v3": 9,
        "bnb_aave_borrow_and_trade_v4": 9,
    }
    assert not NEW_V3_REFERENCES & CANDIDATES.keys()
    assert (
        not {
            "uniswap_v3_robinhood_trading",
            "uniswap_v3_robinhood_lp",
            "robinhood_morpho_borrow_and_trade_v3",
        }
        & CANDIDATES.keys()
    )


def test_private_declaration_loads_all_three_resolved_v3_references():
    fixtures = ROOT / "tests/fixtures/author/candidates"
    for reference in NEW_V3_REFERENCES:
        config = read_author(fixtures / reference / "config.yaml").fork_config()
        assert {allocation.chain.chain_id for allocation in config.allocations} == {BNB}


def test_bnb_production_rehearsals_follow_every_market_declaration():
    bnb = {reference for reference in production_references() if "bnb" in reference}
    # The fourth batch admits Morpho on BNB, standalone and composed with Curve.
    assert bnb == EXPECTED_BNB_PRODUCTION_REFERENCES | {
        "bnb_morpho_curve_borrow_and_trade",
        "morpho_blue_bnb_lending",
    }
