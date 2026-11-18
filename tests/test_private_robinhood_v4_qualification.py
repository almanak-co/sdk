"""Final Robinhood declarations and cases confined to the frozen V4 proof checkout."""

from pathlib import Path

from almanak.caip import ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.support import (
    NATIVE_V4_CURRENCIES,
    NATIVE_V4_LIQUIDITY,
    ONE_ALLOCATION_PER_FUND,
    UNISWAP_CANDIDATE_STARTS,
    ConnectorChainSupport,
    EconomicAction,
    RunMode,
    uniswap_support,
)
from tests.qa.catalog import CANDIDATES, catalog, validate
from tests.strategy_fork.rehearsal import production_references

ROOT = Path(__file__).parents[1]
REFERENCES = (
    "uniswap_v4_robinhood_lp",
    "uniswap_v4_robinhood_trading",
    "robinhood_morpho_borrow_and_trade_v4",
)


def test_final_declaration_includes_robinhood_and_is_validated():
    support = connector_from_wire({"connector": "uniswap_v4"}).chain_support
    robinhood = [entry for entry in support if entry.chain == ChainId("eip155:4663")]
    assert len(robinhood) == 1
    assert len(support) == 8
    assert support == uniswap_support()
    assert all(
        ConnectorChainSupport.model_validate(entry.model_dump()) == entry for entry in support
    )


def test_robinhood_claims_only_the_authored_economic_model():
    declaration = next(
        entry
        for entry in connector_from_wire({"connector": "uniswap_v4"}).chain_support
        if entry.chain == ChainId("eip155:4663")
    )
    assert declaration.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert declaration.restrictions == (
        "hookless pools, or reviewed hook profiles (Pons hooks for trading only)",
        NATIVE_V4_CURRENCIES,
        NATIVE_V4_LIQUIDITY,
        "single pool",
        ONE_ALLOCATION_PER_FUND,
        "fixed LP range or recentring band (bands backtest in economic mode only)",
    )
    assert "eip155:4663" not in {chain for chain, _ in UNISWAP_CANDIDATE_STARTS}
    assert {historical.mode for historical in declaration.historical} == {"economic"}
    (historical,) = declaration.historical
    assert EconomicAction.REVOKE not in historical.actions
    assert set(historical.actions) == set(declaration.actions) - {EconomicAction.REVOKE}


def test_robinhood_references_load_through_the_final_declaration():
    assert all(
        read_inputs(ROOT / "strategies" / name / "config.yaml").inputs for name in REFERENCES
    )


def test_robinhood_production_admission_includes_the_morpho_v4_composition():
    production_chains = {
        entry.chain
        for entry in connector_from_wire({"connector": "uniswap_v4"}).chain_support
        if RunMode.PRODUCTION in entry.modes
    }
    assert production_chains == {
        ChainId("eip155:1"),
        ChainId("eip155:43114"),
        ChainId("eip155:8453"),
        ChainId("eip155:42161"),
        ChainId("eip155:10"),
        ChainId("eip155:56"),
        ChainId("eip155:137"),
        ChainId("eip155:4663"),
    }
    morpho = connector_from_wire({"connector": "morpho_blue"}).support_for(ChainId("eip155:4663"))
    # Admitted by the fourth batch; the V4 composition then joins the rehearsal.
    assert morpho.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    admitted = set(production_references())
    assert {
        "uniswap_v4_robinhood_lp",
        "uniswap_v4_robinhood_trading",
    } <= admitted
    assert "robinhood_morpho_borrow_and_trade_v4" in admitted


def test_private_catalog_schedules_exact_robinhood_main_scope():
    cases = catalog()
    validate(cases)
    indexed = {case.id: case for case in cases}
    expected = {
        f"fork/{policy}/{reference}/{profile}/{operator}"
        for reference in REFERENCES
        for policy in ("pinned", "fresh")
        for profile, operator in (
            ("safe_roles", "configured"),
            ("safe_roles", "distinct"),
            ("eoa", "configured"),
        )
    }
    expected |= {
        f"backtest/{policy}/{reference}/economic"
        for reference in REFERENCES
        for policy in ("pinned", "fresh", "native")
    }
    expected |= {
        f"cleanup/{policy}/uniswap_v4_robinhood_lp/{profile}"
        for policy in ("pinned", "fresh")
        for profile in ("eoa", "safe_roles")
    }
    selected = {
        case_id
        for case_id in indexed
        if "robinhood" in case_id and "v4" in case_id and "_lp_band" not in case_id
    }
    assert selected == expected
    assert len(expected) == 31
    assert all(not indexed[case_id].blocked_reason for case_id in expected)
    assert not set(REFERENCES) & CANDIDATES.keys()
