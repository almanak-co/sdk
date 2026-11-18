"""Arbitrum declarations require scoped native actions and captured economic outcomes."""

import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aave_v3.state import Checkpoint
from almanak.strategy import StrategyDefinition
from tests.legacy_references import current_imports_if_legacy
from tests.lending_captures import current_manifest
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.test_aave_v3 import artifact
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.scope_only_sources import reviewed_json
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_lifecycle_actions,
    assert_successful_outcomes,
    assert_zero_owned,
)

BASE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout"
NATIVE = BASE / "aave-v3-arbitrum-qa-native"
PREPARED = BASE / "aave-v3-arbitrum-prepared"
PRESENTATION = BASE / "aave-v3-arbitrum-presentation"
CASES = tuple(c for c in catalog() if c.reference == "aave_v3_arbitrum_lending")
FORKS = tuple(c for c in CASES if c.lane == "fork" and c.input_policy == "pinned")
MODELS = tuple(c for c in CASES if c.lane == "backtest" and c.input_policy == "pinned")


def native(case, directory):
    manifest = json.loads((NATIVE / "manifest.json").read_text())
    row = next(r for r in manifest["cases"] if r["case"] == case.id)
    state = Checkpoint.model_validate_json(reviewed_json(artifact(NATIVE, row["checkpoint_path"])))
    record = json.loads(artifact(NATIVE, row["result_path"]))
    config = directory / "config.yaml"
    config.write_bytes(artifact(NATIVE, "native/" + row["directory"] + "/config.yaml.gz"))
    return state, record, config


@pytest.mark.parametrize("case", FORKS, ids=lambda c: c.id)
def test_native_reference_proves_actual_scoped_lending_actions(tmp_path, case):
    state, record, config = native(case, tmp_path)
    assert current_imports_if_legacy(state.strategy) == StrategyDefinition.read(
        ROOT / "strategies" / case.reference / "strategy.py"
    )
    assert record["status"] == "passed"
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_lifecycle_actions(state)
    assert_zero_owned(state)
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    assert_fork_actions(case, state.model_dump(mode="json"), config)
    if case.profile == "safe_roles":
        assert record["operator_mode"] == case.operator_mode
        assert state.permissions and record["authority_checks"]["rejected"]


@pytest.mark.parametrize("fault", ["partial", "failed", "foreign", "chain"])
def test_false_native_evidence_cannot_satisfy_declaration(tmp_path, fault):
    case = FORKS[0]
    state, _, config = native(case, tmp_path)
    raw = state.model_dump(mode="json")
    if fault == "partial":
        for item in raw["outcomes"]:
            if item["operation"]["kind"] == "withdraw":
                item["operation"]["full"] = True
    elif fault in ("failed", "foreign"):
        item = next(o for o in raw["outcomes"] if o["operation"]["kind"] == "borrow")
        if fault == "failed":
            item["success"] = False
        else:
            item["operation"]["target"]["allocation"] = "foreign"
    else:
        case = replace(case, chains=(ChainId("eip155:1"),))
    with pytest.raises(AssertionError, match="missing actions"):
        assert_fork_actions(case, raw, config)


@pytest.mark.parametrize("case", MODELS, ids=lambda c: c.id)
def test_prepared_outcomes_satisfy_actual_economic_actions(tmp_path, case):
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(PRESENTATION, "run/" + name + ".gz"))
    current_manifest(tmp_path)
    assert_backtest(case, tmp_path)
    with pytest.raises(AssertionError, match="missing economic actions"):
        assert_backtest(replace(case, chains=(ChainId("eip155:1"),)), tmp_path)


def test_offline_recovery_keeps_full_state_and_real_process_kill_boundaries():
    (row,) = json.loads((PREPARED / "manifest.json").read_text())["results"]
    assert row["provider_requests"] == 0 and row["network_probe_errno"] == 101
    assert row["retained_resume_equal"] and row["reproduced"]
    assert {r["stage"] for r in row["recovery"]} == {"pending_commit", "effects_commit"}
    assert all(r["killed_returncode"] == -9 and r["exact_state_equal"] for r in row["recovery"])


def test_cli_progress_and_native_dashboards_preserve_measurement_scope():
    for phase in ("run", "resume", "reproduce"):
        progress = json.loads(artifact(PRESENTATION, phase + "-progress.json.gz"))
        assert progress["status"] == "completed" and progress["observer_errors"] == 0
    report = json.loads(artifact(PRESENTATION, "run/report.json.gz"))
    assert report["provider_requests_during_execution"] == 0
    assert all(
        p["measurement_status"] == "excluded" and p["cost"] is None for p in report["gas_payers"]
    )
    for profile in ("eoa-configured", "safe_roles-configured", "safe_roles-distinct"):
        dashboard = json.loads(artifact(PRESENTATION, profile + "-dashboard.json.gz"))
        assert dashboard["status"] == "ready"
        values = dashboard["snapshot"]["values"]
        assert values["loan-debt"]["amount"] == values["collateral-supply"]["amount"] == "0"
        assert int(values["fund-gas"]["amount"]) > 0


def test_missing_arbitrum_reference_exposes_a_coverage_gap():
    cases = catalog()
    validate(cases)
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate([c for c in cases if c.reference != "aave_v3_arbitrum_lending"])


def test_retained_recovery_journals_equal_the_complete_uninterrupted_state(tmp_path):
    from almanak.backtest.engine import Journal
    from almanak.connectors.aave_v3.economic_binding import Manifest

    run = tmp_path / "run"
    run.mkdir()
    for name in ("manifest.json", "state.sqlite"):
        (run / name).write_bytes(artifact(PREPARED, "isolated/arbitrum/run/" + name + ".gz"))
    manifest = Manifest.model_validate_json(reviewed_json((run / "manifest.json").read_text()))
    expected = Journal(run / "state.sqlite", type(manifest.initial)).load()
    assert expected.mode == "finished" and expected.pending is None
    assert len(expected.completed) == expected.delivered == 8
    package = json.loads((PREPARED / "manifest.json").read_text())
    reproduction = next(
        row["path"]
        for row in package["files"]
        if row["path"].startswith("isolated/arbitrum/run/reproduce-")
    )
    journals = [
        "isolated/arbitrum/pending_commit.sqlite.gz",
        "isolated/arbitrum/effects_commit.sqlite.gz",
        reproduction,
    ]
    for index, name in enumerate(journals):
        path = tmp_path / f"recovered-{index}.sqlite"
        path.write_bytes(artifact(PREPARED, name))
        assert Journal(path, type(manifest.initial)).load() == expected
