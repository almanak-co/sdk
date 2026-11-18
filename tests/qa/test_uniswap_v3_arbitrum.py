"""Arbitrum coverage requires independently retained actions, costs and recovery."""

import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.uniswap_v3.state import Checkpoint
from tests.exchange_scope import without_exchange_budgets
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.eoa_evidence import native_package
from tests.qa.frozen_reference import archived_reference
from tests.qa.test_uniswap_v3 import artifact, assert_positions_and_payers
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.strategy_fork.v3_pool_assertions import assert_successful_outcomes

BASE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout"
NATIVE = BASE / "uniswap-v3-arbitrum-qa-native"
PREPARED = BASE / "uniswap-v3-arbitrum-prepared-final"
PRESENTATION = BASE / "uniswap-v3-arbitrum-presentation-final"
CASES = tuple(c for c in catalog() if c.reference.startswith("uniswap_v3_arbitrum_"))
# Interrupted-cleanup cases have no retained native package yet; their proof is
# scheduled by the bench and tracked by the obligation ledger, not asserted here.
FORKS = tuple(
    c
    for c in CASES
    if c.lane == "fork" and c.input_policy == "pinned" and c.scenario != "uniswap_cleanup"
)
MODELS = tuple(c for c in CASES if c.lane == "backtest" and c.input_policy == "pinned")


def native(case, directory):
    rows = json.loads((NATIVE / "manifest.json").read_text())["cases"]
    row = next(r for r in rows if r["case"] == case.id)
    state = Checkpoint.model_validate_json(
        json.dumps(without_exchange_budgets(json.loads(artifact(NATIVE, row["checkpoint_path"]))))
    )
    record = json.loads(artifact(NATIVE, row["result_path"]))
    config = directory / "config.yaml"
    config.write_bytes(artifact(NATIVE, "native/" + row["directory"] + "/config.yaml.gz"))
    return state, record, config


@pytest.mark.parametrize("case", FORKS, ids=lambda c: c.id)
def test_native_arbitrum_reference_proves_actual_actions_and_payer_scope(tmp_path, case):
    state, record, config = native(case, tmp_path)
    assert record["status"] == "passed" and record["strategy"] == case.reference
    assert state.strategy == archived_reference(
        native_package(case, NATIVE), "tested-source.tar.gz", case.reference
    )
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_positions_and_payers(state)
    assert_fork_actions(case, state.model_dump(mode="json"), config)
    if case.profile == "safe_roles":
        assert state.permissions and record["operator_mode"] == case.operator_mode
        assert "generated_permissions_installed_and_verified" in record["phases"]


@pytest.mark.parametrize("fault", ["failed", "foreign", "chain", "missing_increase"])
def test_false_arbitrum_evidence_cannot_satisfy_lp_coverage(tmp_path, fault):
    case = next(c for c in FORKS if c.reference.endswith("_lp"))
    state, _, config = native(case, tmp_path)
    raw = state.model_dump(mode="json")
    if fault == "chain":
        case = replace(case, chains=(ChainId("eip155:1"),))
    else:
        item = next(o for o in raw["outcomes"] if o["operation"]["kind"] == "increase")
        if fault == "failed":
            item["success"] = False
        elif fault == "foreign":
            item["operation"]["target"]["allocation"] = "foreign"
        else:
            raw["outcomes"].remove(item)
    with pytest.raises(AssertionError, match="missing actions"):
        assert_fork_actions(case, raw, config)


@pytest.mark.parametrize("case", MODELS, ids=lambda c: c.id)
def test_economic_arbitrum_coverage_and_reporting_preserve_explicit_inputs(tmp_path, case):
    label = case.reference.removeprefix("uniswap_v3_").replace("_", "-")
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(PREPARED, label + "/run/" + name + ".gz"))
    assert_backtest(case, tmp_path)
    with pytest.raises(AssertionError, match="missing economic actions"):
        assert_backtest(replace(case, chains=(ChainId("eip155:1"),)), tmp_path)
    comparisons = json.loads(artifact(PREPARED, label + "/quoter-comparisons.json.gz"))
    assert len(comparisons) == 18 and all(r["equal"] for r in comparisons)
    for phase in ("run", "resume", "reproduce"):
        progress = json.loads(artifact(PRESENTATION, label + "-" + phase + "-progress.json.gz"))
        assert progress["status"] == "completed" and progress["observer_errors"] == 0
    report = json.loads(artifact(PRESENTATION, label + "-report.log.gz"))
    assert report["provider_requests_during_execution"] == 0
    assert all(
        p["measurement_status"] == "excluded" and p["cost"]["amount"] is None
        for p in report["gas_payers"]
    )


def test_both_references_retain_os_isolation_and_actual_commit_crashes():
    rows = json.loads(artifact(PREPARED, "isolated-results.json.gz"))
    assert {r["case"] for r in rows} == {"arbitrum-trading", "arbitrum-lp"}
    for row in rows:
        assert row["provider_requests"] == 0 and row["network_probe_errno"] == 101
        assert row["reproduced"] and row["retained_resume_equal"]
        assert {r["stage"] for r in row["recovery"]} == {"pending_commit", "effects_commit"}
        assert all(r["killed_returncode"] == -9 and r["exact_state_equal"] for r in row["recovery"])


def test_missing_arbitrum_reference_exposes_a_required_coverage_gap():
    cases = catalog()
    validate(cases)
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate([c for c in cases if not c.reference.startswith("uniswap_v3_arbitrum_")])
