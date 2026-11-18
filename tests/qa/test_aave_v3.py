"""Aave QA obligations require retained current references and real scoped effects."""

import gzip
import hashlib
import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aave_v3.state import Checkpoint
from almanak.connectors.identifiers import ConnectorId
from almanak.strategy import StrategyDefinition
from tests.legacy_references import reviewed_current_imports
from tests.lending_captures import current_manifest
from tests.qa.catalog import AAVE_V3, ROOT, catalog, validate
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.scope_only_sources import reviewed_json
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_lifecycle_actions,
    assert_payer_effects,
    assert_successful_outcomes,
    assert_wallet_effects,
    assert_zero_obligations,
    assert_zero_owned,
)

BASE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout"
NATIVE = BASE / "aave-v3-public-qa-native"
PREPARED = BASE / "aave-v3-anchor-isolated"
PRESENTATION = BASE / "aave-v3-public-presentation"
CASES = tuple(
    c
    for c in catalog()
    if c.connector == ConnectorId("aave_v3")
    and c.reference in ("aave_v3_ethereum_lending", "aave_v3_base_lending")
)
FORKS = tuple(c for c in CASES if c.lane == "fork" and c.input_policy == "pinned")
MODELS = tuple(c for c in CASES if c.lane == "backtest" and c.input_policy == "pinned")
LABELS = {ChainId("eip155:1"): "ethereum", ChainId("eip155:8453"): "base"}


def artifact(directory, name):
    manifest = json.loads((directory / "manifest.json").read_text())
    item = next(row for row in manifest["files"] if row["path"] == name)
    raw = (directory / name).read_bytes()
    assert len(raw) == item["bytes"] and hashlib.sha256(raw).hexdigest() == item["sha256"]
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def native(case, directory):
    from tests.qa.eoa_evidence import native_package

    source = native_package(case, NATIVE)
    manifest = json.loads((source / "manifest.json").read_text())
    row = next(row for row in manifest["cases"] if row["case"] == case.id)
    state = Checkpoint.model_validate_json(reviewed_json(artifact(source, row["checkpoint_path"])))
    record = json.loads(artifact(source, row["result_path"]))
    config = directory / "config.yaml"
    config.write_bytes(artifact(source, "native/" + row["directory"] + "/config.yaml.gz"))
    return state, record, config


@pytest.mark.parametrize("case", FORKS, ids=lambda c: c.id)
def test_native_qa_retains_current_strategy_and_every_core_operation(tmp_path, case):
    state, record, config = native(case, tmp_path)
    assert record["status"] == "passed" and record["strategy"] == case.reference
    assert reviewed_current_imports(state.strategy) == StrategyDefinition.read(
        ROOT / "strategies" / case.reference / "strategy.py"
    )
    assert {binding.profile.kind for binding in state.config.accounts.bindings} == {case.profile}
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_lifecycle_actions(state)
    assert_zero_owned(state)
    assert [o.operation.enabled for o in state.outcomes if o.operation.kind == "collateral"] == [
        False,
        True,
    ]
    for before, after in zip(state.baseline, state.closure, strict=True):
        assert_zero_obligations(after)
        assert_wallet_effects(state, before, after)
        assert_payer_effects(state, before, after)
    if case.profile == "safe_roles":
        assert state.permissions is not None
        assert record["operator_mode"] == case.operator_mode
        assert "generated_permissions_installed_and_verified" in record["phases"]
    assert_fork_actions(case, state.model_dump(mode="json"), config)


@pytest.mark.parametrize("fault", ["partial", "failed", "foreign", "chain"])
def test_missing_or_misclassified_native_effects_cannot_satisfy_aave_coverage(tmp_path, fault):
    case = FORKS[0]
    state, _, config = native(case, tmp_path)
    raw = state.model_dump(mode="json")
    if fault == "partial":
        for item in raw["outcomes"]:
            if item["operation"]["kind"] == "withdraw":
                item["operation"]["full"] = True
    elif fault in ("failed", "foreign"):
        item = next(item for item in raw["outcomes"] if item["operation"]["kind"] == "borrow")
        if fault == "failed":
            item["success"] = False
        else:
            item["operation"]["target"]["allocation"] = "foreign"
    else:
        case = replace(case, chains=(ChainId("eip155:42161"),))
    with pytest.raises(AssertionError, match="missing actions"):
        assert_fork_actions(case, raw, config)


@pytest.mark.parametrize("case", MODELS, ids=lambda c: c.id)
def test_prepared_qa_audits_actual_offline_reference_actions_and_measurements(tmp_path, case):
    label = LABELS[case.chains[0]]
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(
            artifact(PRESENTATION, label + "-current-run/" + name + ".gz")
        )
    assert set(case.actions) == set(AAVE_V3)
    current_manifest(tmp_path)
    assert_backtest(case, tmp_path)
    with pytest.raises(AssertionError, match="missing economic actions"):
        assert_backtest(replace(case, chains=(ChainId("eip155:42161"),)), tmp_path)
    with pytest.raises(AssertionError, match="retained model differs"):
        assert_backtest(replace(case, model="different-model"), tmp_path)
    report = json.loads((tmp_path / "report.json").read_text())
    report["positions"][0]["measurement_status"] = "unavailable"
    report["positions"][0]["observation"] = None
    (tmp_path / "report.json").write_text(json.dumps(report))
    with pytest.raises(AssertionError):
        assert_backtest(case, tmp_path)


@pytest.mark.parametrize("chain", [ChainId("eip155:1"), ChainId("eip155:8453")])
def test_removing_one_aave_anchor_leaves_an_explicit_coverage_gap(chain):
    cases = catalog()
    validate(cases)
    remaining = [
        case
        for case in cases
        if not (case.connector == ConnectorId("aave_v3") and chain in case.chains)
    ]
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate(remaining)


def test_isolated_recovery_evidence_requires_both_real_crash_boundaries_per_chain():
    manifest = json.loads((PREPARED / "manifest.json").read_text())
    assert {row["chain"] for row in manifest["results"]} == {"ethereum", "base"}
    for row in manifest["results"]:
        assert row["provider_requests"] == 0 and row["network_probe_errno"] == 101
        assert row["reproduced"] and row["retained_resume_equal"]
        assert {item["stage"] for item in row["recovery"]} == {"pending_commit", "effects_commit"}
        assert all(
            item["killed_returncode"] == -9 and item["exact_state_equal"]
            for item in row["recovery"]
        )


@pytest.mark.parametrize("chain", ["ethereum", "base"])
def test_public_cli_progress_and_dashboards_preserve_completion_and_missing_costs(chain):
    for phase in ("run", "resume", "reproduce"):
        progress = json.loads(artifact(PRESENTATION, f"{chain}-current-{phase}-progress.json.gz"))
        assert progress["schema_version"] == 1 and progress["status"] == "completed"
        assert progress["observer_errors"] == 0 and progress["sequence"] > 1
    report = json.loads(artifact(PRESENTATION, f"{chain}-current-run/report.json.gz"))
    assert report["provider_requests_during_execution"] == 0
    assert all(
        p["measurement_status"] == "excluded" and p["cost"] is None for p in report["gas_payers"]
    )
    for profile in ("eoa-configured", "safe_roles-configured", "safe_roles-distinct"):
        dashboard = json.loads(artifact(PRESENTATION, f"{chain}-{profile}-dashboard.json.gz"))
        assert dashboard["status"] == "ready"
        values = dashboard["snapshot"]["values"]
        assert values["loan-debt"]["amount"] == "0"
        assert values["collateral-supply"]["amount"] == "0"
        assert int(values["fund-gas"]["amount"]) > 0


def test_current_ethereum_reference_passes_production_path_on_identified_fork(tmp_path):
    directory = BASE / "aave-v3-public-production"
    state = Checkpoint.model_validate_json(reviewed_json(artifact(directory, "checkpoint.json.gz")))
    record = json.loads(artifact(directory, "result.json.gz"))
    case = next(c for c in FORKS if c.profile == "eoa" and c.chains == (ChainId("eip155:1"),))
    reference = ROOT / "strategies" / case.reference
    assert reviewed_current_imports(state.strategy) == StrategyDefinition.read(
        reference / "strategy.py"
    )
    assert record["status"] == "passed" and record["profile"] == "production_eoa_rehearsal"
    assert record["phases"] == [
        "production_eoa_entered",
        "runtime_resumed",
        "gateway_replaced",
        "canonical_receipts_and_zero_obligations",
    ]
    assert state.mode == "closed" and state.pending is None
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_lifecycle_actions(state)
    assert_zero_owned(state)
    config = tmp_path / "config.yaml"
    config.write_bytes((reference / "config.yaml").read_bytes())
    assert_fork_actions(case, state.model_dump(mode="json"), config)


PROMOTED = ROOT / "docs/internal/reports/evidence/2026-09-24-rollout"
PRODUCTION_PACKAGES = {
    ChainId("eip155:8453"): "aave-v3-base-public-production",
    ChainId("eip155:42161"): "aave-v3-arbitrum-public-production",
    ChainId("eip155:10"): "aave-v3-optimism-public-production",
    ChainId("eip155:56"): "aave-v3-bnb-public-production",
    ChainId("eip155:137"): "aave-v3-polygon-public-production",
    ChainId("eip155:43114"): "aave-v3-avalanche-public-production",
}
OP_FAMILY = {ChainId("eip155:8453"), ChainId("eip155:10")}


@pytest.mark.parametrize("chain", sorted(PRODUCTION_PACKAGES, key=str))
def test_promoted_reference_passes_production_path_on_identified_fork(tmp_path, chain):
    directory = PROMOTED / PRODUCTION_PACKAGES[chain]
    state = Checkpoint.model_validate_json(reviewed_json(artifact(directory, "checkpoint.json.gz")))
    record = json.loads(artifact(directory, "result.json.gz"))
    case = next(
        c
        for c in catalog()
        if c.connector == ConnectorId("aave_v3")
        and c.lane == "fork"
        and c.input_policy == "pinned"
        and c.profile == "eoa"
        and c.chains == (chain,)
    )
    reference = ROOT / "strategies" / case.reference
    assert state.strategy == StrategyDefinition.read(reference / "strategy.py")
    assert record["status"] == "passed" and record["profile"] == "production_eoa_rehearsal"
    assert record["phases"] == [
        "production_eoa_entered",
        "runtime_resumed",
        "gateway_replaced",
        "canonical_receipts_and_zero_obligations",
    ]
    assert state.mode == "closed" and state.pending is None
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_lifecycle_actions(state)
    assert_zero_owned(state)
    # The promoted production path debits payers through the same policy asset,
    # with pinned Jovian L1/operator evidence retained exactly on OP-family chains.
    assert all(outcome.op_fee_evidence is not None for outcome in state.outcomes) == (
        chain in OP_FAMILY
    )
    config = tmp_path / "config.yaml"
    config.write_bytes((reference / "config.yaml").read_bytes())
    assert_fork_actions(case, state.model_dump(mode="json"), config)
