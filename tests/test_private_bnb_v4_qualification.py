"""Final BNB declarations and cases confined to the frozen V4 proof checkout."""

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

ROOT = Path(__file__).parents[1]
REFERENCES = (
    "uniswap_v4_bnb_lp",
    "uniswap_v4_bnb_trading",
    "bnb_aave_borrow_and_trade_v4",
)


def test_final_declaration_includes_bnb_and_is_validated():
    support = connector_from_wire({"connector": "uniswap_v4"}).chain_support
    bnb = [entry for entry in support if entry.chain == ChainId("eip155:56")]
    assert len(bnb) == 1
    assert len(support) == 8
    assert support == uniswap_support()
    assert all(
        ConnectorChainSupport.model_validate(entry.model_dump()) == entry for entry in support
    )


def test_bnb_claims_only_the_authored_economic_model():
    declaration = next(
        entry
        for entry in connector_from_wire({"connector": "uniswap_v4"}).chain_support
        if entry.chain == ChainId("eip155:56")
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
    assert "eip155:56" not in {chain for chain, _ in UNISWAP_CANDIDATE_STARTS}
    assert {historical.mode for historical in declaration.historical} == {"economic"}
    (historical,) = declaration.historical
    assert EconomicAction.REVOKE in historical.actions
    assert set(historical.actions) == set(declaration.actions)


def test_bnb_references_load_through_the_final_declaration():
    assert all(
        read_inputs(ROOT / "strategies" / name / "config.yaml").inputs for name in REFERENCES
    )


def test_private_catalog_schedules_exact_bnb_main_scope():
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
        f"cleanup/{policy}/uniswap_v4_bnb_lp/{profile}"
        for policy in ("pinned", "fresh")
        for profile in ("eoa", "safe_roles")
    }
    expected.add("model-actions/uniswap_v4/eip155:56")
    selected = {
        case_id
        for case_id in indexed
        if ("bnb" in case_id and "v4" in case_id) or case_id == "model-actions/uniswap_v4/eip155:56"
    }
    assert selected == expected
    assert all(not indexed[case_id].blocked_reason for case_id in expected)
    assert indexed["model-actions/uniswap_v4/eip155:56"].nodes == (
        "tests/qa/test_uniswap_v4_bnb.py::test_prepared_revoke",
        "tests/qa/test_uniswap_v4_bnb.py::test_retained_quoter_comparisons_match_current_pool_math",
    )
    assert not set(REFERENCES) & CANDIDATES.keys()
