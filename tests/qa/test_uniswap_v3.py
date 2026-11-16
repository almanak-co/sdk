"""V3 QA credits actual scoped actions and retained offline process recovery."""

import gzip
import hashlib
import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3.state import Checkpoint
from tests.exchange_scope import without_exchange_budgets, without_repeated_entry_quotes
from tests.legacy_references import historical_strategy_current_imports
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.eoa_evidence import native_package
from tests.qa.frozen_reference import archived_reference
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.strategy_fork.v3_pool_assertions import (
    assert_closed_position,
    assert_operations,
    assert_payer,
    assert_successful_outcomes,
    assert_wallet_effects,
    position_effects,
)

BASE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout"
NATIVE = BASE / "uniswap-v3-public-qa-native"
PREPARED = BASE / "uniswap-v3-public-prepared-final"
PRESENTATION = BASE / "uniswap-v3-public-presentation-final"
# Native fork evidence for the Base TA references #129 added to the catalog, captured
# through the same QA worker on 2026-10-20.
TA_NATIVE = BASE / "uniswap-v3-public-qa-native-2026-10-20"
# uniswap_v3_base_rsi never crosses RSI 30 on the pinned market; its scenario drives the
# pool's own hourly closes with real fork swaps, retained in each case record.
RSI_NATIVE = BASE / "uniswap-v3-public-qa-native-rsi-2026-10-20"
CASES = tuple(
    c
    for c in catalog()
    if c.connector == ConnectorId("uniswap_v3")
    and set(c.chains) <= {ChainId("eip155:1"), ChainId("eip155:8453")}
)
# Interrupted-cleanup cases have no retained native package yet; their proof is
# scheduled by the bench and tracked by the obligation ledger, not asserted here.
FORKS = tuple(
    c
    for c in CASES
    if c.lane == "fork" and c.input_policy == "pinned" and c.scenario != "uniswap_cleanup"
)
MODELS = tuple(c for c in CASES if c.lane == "backtest" and c.input_policy == "pinned")


def artifact(directory, name):
    manifest = json.loads((directory / "manifest.json").read_text())
    row = next(row for row in manifest["files"] if row["path"] == name)
    raw = (directory / name).read_bytes()
    assert len(raw) == row["bytes"] and hashlib.sha256(raw).hexdigest() == row["sha256"]
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def package(case):
    for source in (TA_NATIVE, RSI_NATIVE):
        manifest = json.loads((source / "manifest.json").read_text())
        if any(row["case"] == case.id for row in manifest["cases"]):
            return source
    return native_package(case, NATIVE)


def native(case, directory):
    source = package(case)
    manifest = json.loads((source / "manifest.json").read_text())
    row = next(row for row in manifest["cases"] if row["case"] == case.id)
    state = Checkpoint.model_validate_json(
        json.dumps(without_exchange_budgets(json.loads(artifact(source, row["checkpoint_path"]))))
    )
    record = json.loads(artifact(source, row["result_path"]))
    config = directory / "config.yaml"
    config.write_bytes(artifact(source, "native/" + row["directory"] + "/config.yaml.gz"))
    return state, record, config


@pytest.mark.parametrize("case", FORKS, ids=lambda c: c.id)
def test_native_references_exercise_declared_actions_and_reconcile_each_asset(tmp_path, case):
    state, record, config = native(case, tmp_path)
    assert record["status"] == "passed" and record["strategy"] == case.reference
    assert state.strategy == archived_reference(
        package(case), "tested-source.tar.gz", case.reference
    )
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_positions_and_payers(state)
    if case.profile == "safe_roles":
        assert_installed_authority(case, record, state)
        assert record["operator_mode"] == case.operator_mode
    assert_fork_actions(case, state.model_dump(mode="json"), config)


def assert_installed_authority(case, record, state):
    """A Safe case installed and verified its generated grants before execution.

    Converted references run through the supervised public CLI lane, which admits Safe
    execution only after `verify` writes its record beside the checkpoint; the internal
    harness lane records its own phase instead.
    """
    assert state.permissions is not None
    if record.get("driver") != "public_cli_supervised_fork":
        assert "generated_permissions_installed_and_verified" in record["phases"]
        return
    assert record["phases"][0] == "supervised_resolution_fixture_hooks_and_admission"
    source = package(case)
    manifest = json.loads((source / "manifest.json").read_text())
    row = next(row for row in manifest["cases"] if row["case"] == case.id)
    run = row["checkpoint_path"].rsplit("/", 1)[0]
    verification = json.loads(artifact(source, f"{run}/permission-verification.json.gz"))
    retained = state.permissions.partitions
    for verified, partition in zip(verification["partitions"], retained, strict=True):
        assert verified["account"] == partition.account


def assert_positions_and_payers(state):
    for owned, before, after in zip(state.owned, state.baseline, state.closure, strict=True):
        effects = position_effects(state, owned.target)
        assert_closed_position(owned, after)
        assert_operations(owned.target, effects)
        assert_wallet_effects(effects, before, after)
        assert_payer(state, owned.target, before, after, effects)


@pytest.mark.parametrize("fault", ["failed", "foreign", "chain", "missing_increase"])
def test_incomplete_or_foreign_native_effects_cannot_satisfy_lp_coverage(tmp_path, fault):
    case = next(c for c in FORKS if c.reference.endswith("_lp"))
    state, _, config = native(case, tmp_path)
    raw = state.model_dump(mode="json")
    if fault == "chain":
        case = replace(case, chains=(ChainId("eip155:42161"),))
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
def test_prepared_qa_requires_real_scoped_actions_and_present_measurements(tmp_path, case):
    label = case.reference.removeprefix("uniswap_v3_").replace("_", "-")
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(PREPARED, label + "/run/" + name + ".gz"))
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


def test_all_four_references_have_os_isolation_and_both_real_commit_crashes():
    rows = json.loads(artifact(PREPARED, "isolated-results.json.gz"))
    assert {r["case"] for r in rows} == {
        "ethereum-trading",
        "ethereum-lp",
        "base-trading",
        "base-lp",
    }
    for row in rows:
        assert row["provider_requests"] == 0 and row["network_probe_errno"] == 101
        assert row["reproduced"] and row["retained_resume_equal"]
        assert {r["stage"] for r in row["recovery"]} == {"pending_commit", "effects_commit"}
        assert all(r["killed_returncode"] == -9 and r["exact_state_equal"] for r in row["recovery"])


@pytest.mark.parametrize("case", MODELS, ids=lambda c: c.id)
def test_cli_progress_reports_and_quoter_comparisons_preserve_fidelity(case):
    label = case.reference.removeprefix("uniswap_v3_").replace("_", "-")
    for phase in ("run", "resume", "reproduce"):
        progress = json.loads(artifact(PRESENTATION, label + "-" + phase + "-progress.json.gz"))
        assert progress["schema_version"] == 1 and progress["status"] == "completed"
        assert progress["observer_errors"] == 0 and progress["sequence"] > 1
    report = json.loads(artifact(PRESENTATION, label + "-report.log.gz"))
    assert report["provider_requests_during_execution"] == 0
    assert all(
        p["measurement_status"] == "excluded" and p["cost"]["amount"] is None
        for p in report["gas_payers"]
    )
    comparisons = json.loads(artifact(PREPARED, label + "/quoter-comparisons.json.gz"))
    assert len(comparisons) == 18
    assert all(r["equal"] for r in comparisons)


@pytest.mark.parametrize("chain", [ChainId("eip155:1"), ChainId("eip155:8453")])
def test_removing_an_anchor_retains_a_required_coverage_gap(chain):
    cases = catalog()
    validate(cases)
    remaining = [
        c for c in cases if not (c.connector == ConnectorId("uniswap_v3") and chain in c.chains)
    ]
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate(remaining)


@pytest.mark.parametrize("case", MODELS, ids=lambda c: c.id)
def test_sealed_dataset_reproduces_the_entire_retained_state_offline(tmp_path, monkeypatch, case):
    import httpx

    from almanak.backtest.engine import Journal, Simulator
    from almanak.connectors.uniswap_v3.economic_binding import Manifest, UniswapEconomic
    from almanak.worker_session import preloaded_workers

    def forbidden(*args, **kwargs):
        raise AssertionError("sealed reproduction cannot obtain network inputs")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    label = case.reference.removeprefix("uniswap_v3_").replace("_", "-")
    manifest = Manifest.model_validate_json(
        json.dumps(
            without_exchange_budgets(
                json.loads(artifact(PREPARED, label + "/run/manifest.json.gz"))
            )
        )
    )
    assert manifest.strategy == archived_reference(PREPARED, "final-source.tar.gz", case.reference)
    strategy = historical_strategy_current_imports(manifest.strategy)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for name in ("dataset.json", "data.sqlite"):
        (dataset / name).write_bytes(artifact(PREPARED, label + "/dataset/" + name + ".gz"))
    expected_path = tmp_path / "expected.sqlite"
    expected_path.write_bytes(artifact(PREPARED, label + "/run/state.sqlite.gz"))
    expected = Journal(expected_path, type(manifest.initial)).load()
    book = UniswapEconomic.open_book(manifest.model_copy(update={"dataset": dataset}))
    try:
        model = UniswapEconomic.driver(manifest.config, manifest.settings, strategy, book)
        journal = Journal(tmp_path / "control.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        with preloaded_workers():
            actual = Simulator(journal, model).run()
        retained = json.loads(expected.model_dump_json())
        current = json.loads(actual.model_dump_json())
        assert without_repeated_entry_quotes(current, retained) == retained
    finally:
        book.close()
