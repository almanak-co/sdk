"""Robinhood proof coverage stays bound to its declared chain and pool revisions."""

import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.curve.domain import protocol_variant
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory
from tests.qa import test_curve_evidence as retained
from tests.qa.catalog import (
    CURVE_LP,
    CURVE_VARIANTS,
    ROOT,
    catalog,
    curve_robinhood_model_cases,
    validate,
)

CHAIN = ChainId("eip155:4663")
REFERENCES = (
    ("curve_robinhood_stable_lp", ProtocolVariant("stableswap_ng"), "stable", "stable", 0),
    ("curve_robinhood_twocrypto_lp", ProtocolVariant("twocrypto_ng"), "two", "crypto", 0),
    ("curve_robinhood_tricrypto_lp", ProtocolVariant("tricrypto_ng_2024"), "tri", "crypto", 1),
)


@pytest.mark.parametrize("reference,variant,label,fixture,index", REFERENCES)
def test_robinhood_reference_and_proofs_match_the_configured_revision(
    reference, variant, label, fixture, index
):
    config = read_author(harness_directory(reference) / "config.yaml").fork_config()
    assert {target.chain.chain_id for target in config.forks} == {CHAIN}
    assert {
        protocol_variant(a.pool.family, a.pool.implementation_revision) for a in config.allocations
    } == {variant}
    assert CURVE_VARIANTS[reference] == variant
    cases = [case for case in catalog() if case.reference == reference]
    assert all(case.chains == (CHAIN,) and case.variants == (variant,) for case in cases)
    history = [case for case in cases if case.lane == "backtest"]
    assert {case.input_policy for case in history} == {"pinned", "fresh", "native"}
    assert len(history) == 3
    assert all(
        case.model_mode == "economic" and set(case.actions) == set(CURVE_LP) for case in history
    )
    assert all(
        retained.captured_directory(case)
        == retained.BASE / f"curve-robinhood-{label}-prepared/lp/run"
        for case in history
    )
    revoke = next(case for case in curve_robinhood_model_cases() if case.variants == (variant,))
    assert revoke.chains == (CHAIN,)
    assert revoke.nodes == (
        f"tests/test_curve_robinhood_{fixture}_prepared.py::"
        f"test_cleanup_preserves_the_captured_fund_inventory[captured{index}]",
    )


@pytest.mark.parametrize("reference,variant,label,fixture,index", REFERENCES)
def test_robinhood_native_coverage_requires_both_profiles_and_distinct_safe_operator(
    reference, variant, label, fixture, index
):
    cases = [case for case in retained.NATIVE_CASES if case.reference == reference]
    assert len(cases) == 5
    full = [case for case in cases if not case.scenario]
    cleanup = [case for case in cases if case.scenario == "curve_cleanup"]
    assert {(case.profile, case.operator_mode) for case in full} == {
        ("eoa", "configured"),
        ("safe_roles", "configured"),
        ("safe_roles", "distinct"),
    }
    assert {case.profile for case in cleanup} == {"eoa", "safe_roles"}
    for case in cases:
        source, author = retained.native_directory(case)
        assert source.parent.parent == retained.BASE / f"curve-robinhood-{label}-native"
        assert author == source.parent.parent / f"source/strategies/{reference}/config.yaml.gz"


@pytest.mark.parametrize("reference,variant,label,fixture,index", REFERENCES)
def test_omitting_one_robinhood_family_leaves_a_declared_coverage_gap(
    reference, variant, label, fixture, index
):
    cases = catalog()
    validate(cases)
    selected = [
        case
        for case in cases
        if not (
            case.connector == ConnectorId("curve")
            and case.chains == (CHAIN,)
            and variant in case.variants
        )
    ]
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate(selected)


def test_robinhood_fresh_inputs_have_an_explicit_source_lag():
    policy = json.loads((ROOT / "tests/qa/policy.json").read_text())
    assert policy["source_lags"][str(CHAIN)] > 0


def test_robinhood_old_tricrypto_revision_has_no_retained_evidence_route():
    case = next(case for case in catalog() if case.reference == "curve_robinhood_tricrypto_lp")
    with pytest.raises(KeyError):
        retained.captured_directory(replace(case, variants=(ProtocolVariant("tricrypto_ng"),)))


@pytest.mark.parametrize(
    "scenarios", [("approval_commit_then_teardown",), ("full_lifecycle", "full_lifecycle")]
)
def test_robinhood_native_manifest_requires_one_exact_profile_and_scenario(
    tmp_path, monkeypatch, scenarios
):
    case = next(
        case
        for case in retained.NATIVE_CASES
        if case.reference == "curve_robinhood_stable_lp"
        and case.profile == "eoa"
        and not case.scenario
    )
    source = tmp_path / "curve-robinhood-stable-native"
    source.mkdir()
    manifest = {
        "cases": [
            {
                "case": str(index),
                "profile": "eoa",
                "operator_mode": "configured",
                "scenario": scenario,
            }
            for index, scenario in enumerate(scenarios)
        ],
        "files": {},
    }
    (source / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(retained, "BASE", tmp_path)
    with pytest.raises(AssertionError, match="exactly one profile/scenario"):
        retained.native_directory(case)
