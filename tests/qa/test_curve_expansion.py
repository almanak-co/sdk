"""Each new Curve chain and revision needs independent retained proof coverage."""

import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.configuration.backtest_inputs import read_backtest_inputs
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.curve.domain import protocol_variant
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory
from tests.qa.catalog import (
    CURVE_VARIANTS,
    ROOT,
    catalog,
    curve_expansion_model_cases,
    curve_supplemental_cases,
    validate,
)
from tests.qa.inputs import source_config
from tests.qa.operators import example_operator
from tests.qa.test_curve_evidence import captured_directory

EXPANSION = (
    ("optimism", "eip155:10", ("stable", "twocrypto", "tricrypto")),
    ("bnb", "eip155:56", ("stable", "twocrypto")),
    ("polygon", "eip155:137", ("stable", "twocrypto", "tricrypto")),
    ("avalanche", "eip155:43114", ("stable",)),
)


def test_expansion_references_match_the_actual_configured_chain_and_revision():
    for name, chain, families in EXPANSION:
        for family in families:
            reference = f"curve_{name}_{family}_lp"
            config = read_author(harness_directory(reference) / "config.yaml").fork_config()
            assert {target.chain.chain_id for target in config.forks} == {ChainId(chain)}
            assert {
                protocol_variant(a.pool.family, a.pool.implementation_revision)
                for a in config.allocations
            } == {CURVE_VARIANTS[reference]}


def test_ethereum_mixed_proofs_do_not_claim_other_chains_tricrypto_revision():
    mixed = [case for case in curve_supplemental_cases() if case.scenario == "curve_mixed"]
    assert len(mixed) == 3
    assert all(ProtocolVariant("tricrypto_ng_2024") not in case.variants for case in mixed)


def test_ethereum_mixed_scenario_uses_public_inputs_and_explicit_operator_scope():
    cases = [case for case in curve_supplemental_cases() if case.scenario == "curve_mixed"]
    document = read_inputs(source_config(cases[0]))
    backtest = read_backtest_inputs(source_config(cases[0]).with_name("backtest-economic.yaml"))
    operator = example_operator(cases[0], "fork")

    assert set(document.inputs.strategy.allocations) == {"stable", "two", "three"}
    assert {str(market.connector) for market in document.inputs.markets.values()} == {"curve"}
    assert set(document.inputs.accounts) == {"stable", "two", "three"}
    assert {fund.account for fund in operator.fork.funds} == {"stable", "two", "three"}
    assert {binding.name for binding in operator.accounts.bindings} == {"stable", "two", "three"}
    backtest.require_accounts(document.inputs)


def test_expansion_revoke_proofs_route_each_family_to_its_captured_fixture():
    cases = list(curve_expansion_model_cases())
    assert len(cases) == 9
    for name, chain, families in EXPANSION:
        for index, family in enumerate(families):
            variant = CURVE_VARIANTS[f"curve_{name}_{family}_lp"]
            matching = [
                c for c in cases if c.chains == (ChainId(chain),) and c.variants == (variant,)
            ]
            assert len(matching) == 1
            assert matching[0].nodes == (
                f"tests/test_curve_{name}_prepared.py::"
                f"test_cleanup_preserves_the_captured_fund_inventory[captured{index}]",
            )


def test_expansion_has_explicit_fresh_source_lags():
    policy = json.loads((ROOT / "tests/qa/policy.json").read_text())
    assert all(policy["source_lags"][chain] > 0 for _, chain, _ in EXPANSION)


@pytest.mark.parametrize("name,chain,families", EXPANSION)
def test_omitting_an_expansion_chain_leaves_a_coverage_gap(name, chain, families):
    cases = catalog()
    validate(cases)
    selected = [
        case
        for case in cases
        if not (case.connector == ConnectorId("curve") and case.chains == (ChainId(chain),))
    ]
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate(selected)


def test_retained_directory_rejects_unknown_chain_and_revision():
    case = next(case for case in catalog() if case.reference == "curve_optimism_tricrypto_lp")
    assert captured_directory(case).parts[-4:] == (
        "2026-09-07-rollout",
        "curve-optimism-tricrypto-prepared",
        "lp",
        "run",
    )
    with pytest.raises(KeyError):
        captured_directory(replace(case, chains=(ChainId("eip155:999999"),)))
    with pytest.raises(KeyError):
        captured_directory(replace(case, variants=(ProtocolVariant("unproved"),)))
    with pytest.raises(ValueError, match="exactly one"):
        captured_directory(replace(case, chains=(ChainId("eip155:10"), ChainId("eip155:137"))))
