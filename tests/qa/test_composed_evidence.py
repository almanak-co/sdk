"""One retained composed run must independently satisfy each configured connector scope."""

import json
from dataclasses import replace

import pytest

from almanak.backtest.engine import Journal
from almanak.connectors.composed.economic_binding import Manifest
from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.connectors.support import EconomicAction
from tests.lending_captures import current_manifest
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.inputs import resolved_author, resolved_history
from tests.qa.intent_coverage import assert_economic_direct, assert_native_direct
from tests.qa.test_compound_evidence import PACKAGE, artifact
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.scope_only_sources import reviewed_json

REFERENCE = "base_compound_borrow_and_trade"
CASES = tuple(c for c in catalog() if c.reference == REFERENCE)


def test_composition_runs_once_per_profile_and_keeps_both_proof_scopes():
    assert len(CASES) == 9
    assert all(c.connector is None and not c.actions and len(c.scopes) == 2 for c in CASES)
    validate(catalog())
    for case in CASES:
        assert {str(o.connector) for o in case.obligations()} == {"compound_v3", "uniswap_v3"}
        assert len(case.obligations()) == 6


@pytest.mark.parametrize(
    "case",
    [c for c in CASES if c.lane == "fork" and c.input_policy == "pinned"],
    ids=lambda c: c.id,
)
def test_retained_native_composition_proves_each_connector_without_cross_credit(tmp_path, case):
    files = json.loads((PACKAGE / "manifest.json").read_text())["files"]
    prefix = f"compound-composition/{case.profile}-{case.operator_mode}-"
    records = [n for n in files if n.startswith(prefix) and n.endswith("/result.json.gz")]
    passed = [n for n in records if json.loads(artifact(n))["status"] == "passed"]
    assert len(passed) == 1
    checkpoint = passed[0].replace("/result.json.gz", "/checkpoint.json.gz")
    state = checkpoint_from_json(reviewed_json(artifact(checkpoint).decode()))
    assert state.mode == "closed" and state.delivered == len(state.outcomes)
    connector_for(state.config).lifecycle.validate_closure(state, state.closure)
    config = tmp_path / "config.yaml"
    config.write_bytes((ROOT / "strategies" / REFERENCE / "config.yaml").read_bytes())
    assert_fork_actions(case, state.model_dump(mode="json"), config)
    for check in case.checks():
        assert_native_direct(check, state)
    lending = next(c for c in case.checks() if str(c.connector) == "compound_v3")
    with pytest.raises(AssertionError, match="missing direct completed author intents"):
        assert_native_direct(replace(lending, actions=(EconomicAction.SWAP,)), state)


def test_retained_composed_prepared_state_cannot_substitute_a_sibling_model(tmp_path):
    case = next(c for c in CASES if c.lane == "backtest" and c.input_policy == "pinned")
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact("compound-composition-prepared/run/" + name + ".gz"))
    current_manifest(tmp_path)
    assert_backtest(case, tmp_path)
    manifest = Manifest.model_validate_json((tmp_path / "manifest.json").read_text())
    state = Journal(tmp_path / "state.sqlite", type(manifest.initial)).load()
    for check in case.checks():
        assert_economic_direct(check, state, manifest.config)
    wrong = replace(case.scopes[0], model=case.scopes[1].model)
    with pytest.raises(AssertionError, match="component model differs"):
        assert_backtest(replace(case, scopes=(wrong, case.scopes[1])), tmp_path)


def test_native_collection_caps_preserve_each_components_sampling_policy(tmp_path):
    case = next(c for c in CASES if c.lane == "backtest" and c.input_policy == "native")
    resolved_author(case, tmp_path, {})
    path = resolved_history(case, tmp_path, 1788880000)
    import yaml

    data = yaml.safe_load(path.read_text())["data"]["connectors"]
    assert data["compound_v3"]["max_accounting_samples"] == 41
    assert data["compound_v3"]["sample_seconds"] == "30s"
    assert data["compound_v3"]["max_age_seconds"] == "60s"
    assert data["uniswap_v3"]["max_samples"] == 21
    assert data["uniswap_v3"]["sample_seconds"] == "60s"
    assert data["uniswap_v3"]["max_age_seconds"] == "72s"


@pytest.mark.parametrize("fault", ("missing_component", "extra_component", "sampling_override"))
def test_composed_native_policy_refuses_partial_scopes_and_sampling_changes(
    tmp_path, monkeypatch, fault
):
    import shutil

    from tests.qa import inputs

    source = tmp_path / "source"
    reference = source / "strategies" / REFERENCE
    shutil.copytree(ROOT / "strategies" / REFERENCE, reference)
    policy = json.loads((ROOT / "tests/qa/policy.json").read_text())
    selected = policy["native_component_collection_limits"][REFERENCE]
    if fault == "missing_component":
        del selected["uniswap_v3"]
    elif fault == "extra_component":
        selected["curve"] = {"max_samples": 21}
    else:
        selected["compound_v3"]["sample_seconds"] = 600
    policy_path = source / "tests/qa/policy.json"
    policy_path.parent.mkdir(parents=True)
    policy_path.write_text(json.dumps(policy))
    monkeypatch.setattr(inputs, "ROOT", source)
    case = next(c for c in CASES if c.lane == "backtest" and c.input_policy == "native")
    output = tmp_path / "run"
    output.mkdir()
    resolved_author(case, output, {})
    with pytest.raises(ValueError, match="collection"):
        resolved_history(case, output, 1788880000)


@pytest.mark.parametrize("variant", ("stable", "volatile"))
def test_aerodrome_composition_credit_keeps_its_configured_invariant(variant):
    reference = f"base_aave_aerodrome_{variant}_borrow_and_trade"
    cases = [case for case in catalog() if case.reference == reference]
    assert len(cases) == 9
    for case in cases:
        scope = next(s for s in case.scopes if str(s.connector) == "aerodrome")
        assert tuple(map(str, scope.variants)) == (variant,)
        claims = [o for o in case.obligations() if str(o.connector) == "aerodrome"]
        assert claims and all(str(o.variant) == variant for o in claims)
