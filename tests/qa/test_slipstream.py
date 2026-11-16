"""V3 QA credits actual scoped actions and retained offline process recovery."""

import gzip
import hashlib
import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aerodrome_slipstream.state import Checkpoint
from almanak.connectors.identifiers import ConnectorId
from tests.exchange_scope import without_exchange_budgets, without_repeated_entry_quotes
from tests.legacy_references import current_imports_if_legacy
from tests.qa.band_packages import (
    SUPPLEMENTS,
    assert_generated_authority,
    assert_supplement_audit,
    assert_supplement_source,
    holding,
    sealed,
)
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.intent_coverage import assert_economic_direct, assert_native_direct
from tests.qa.lp_refactor import assert_reference_source
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.strategy_fork.v3_pool_assertions import (
    assert_closed_position,
    assert_operations,
    assert_payer,
    assert_successful_outcomes,
    assert_wallet_effects,
    position_effects,
)

BASE = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout"
NATIVE = BASE / "slipstream-base-public-native"
BAND = SUPPLEMENTS / "slipstream-base-band-native"
PACKAGES = (NATIVE, BAND)
PREPARED = BASE / "slipstream-base-public-prepared"
PRESENTATION = PREPARED
CASES = tuple(
    c
    for c in catalog()
    if c.connector == ConnectorId("aerodrome_slipstream") and ChainId("eip155:8453") in c.chains
)
FORKS = tuple(c for c in CASES if c.lane == "fork")
MODELS = tuple(c for c in CASES if c.lane == "backtest")


def artifact(directory, name):
    manifest = json.loads((directory / "manifest.json").read_text())
    row = next(row for row in manifest["files"] if row["path"] == name)
    raw = (directory / name).read_bytes()
    assert len(raw) == row["bytes"] and hashlib.sha256(raw).hexdigest() == row["sha256"]
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def native(case, directory):
    package, row = holding(PACKAGES, case.id)
    state = Checkpoint.model_validate_json(
        json.dumps(without_exchange_budgets(json.loads(artifact(package, row["checkpoint_path"]))))
    )
    record = json.loads(artifact(package, row["result_path"]))
    config = directory / "config.yaml"
    config.write_bytes(artifact(package, row["directory"] + "/config.yaml.gz"))
    return state, record, config


@pytest.mark.parametrize("case", FORKS, ids=lambda c: c.id)
def test_native_references_exercise_declared_actions_and_reconcile_each_asset(tmp_path, case):
    state, record, config = native(case, tmp_path)
    assert record["status"] == "passed" and record["strategy"] == case.reference
    assert_reference_source(state.strategy, case.reference)
    if holding(PACKAGES, case.id)[0] == BAND:
        assert_supplement_source(BAND, case.reference, state.strategy)
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_positions_and_payers(state)
    if case.profile == "safe_roles":
        assert state.permissions is not None
        assert_generated_authority(PACKAGES, artifact, case.id, record, state)
        assert record["operator_mode"] == case.operator_mode
    assert_fork_actions(case, state.model_dump(mode="json"), config)
    assert_native_direct(case, state)


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
    label = case.input_policy + "-" + case.reference
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


def test_both_references_have_os_isolation_and_both_real_commit_crashes():
    rows = json.loads(artifact(PREPARED, "isolated-results.json.gz"))
    assert {r["case"] for r in rows} == {
        "pinned-aerodrome_slipstream_base_trading",
        "pinned-aerodrome_slipstream_base_lp",
    }
    for row in rows:
        assert row["provider_requests"] == 0 and row["network_probe_errno"] == 101
        assert row["reproduced"] and row["retained_resume_equal"]
        assert {r["stage"] for r in row["recovery"]} == {"pending_commit", "effects_commit"}
        assert all(r["killed_returncode"] == -9 and r["exact_state_equal"] for r in row["recovery"])


@pytest.mark.parametrize("case", MODELS, ids=lambda c: c.id)
def test_cli_progress_reports_and_quoter_comparisons_preserve_fidelity(case):
    label = case.input_policy + "-" + case.reference
    phases = ("resume", "reproduce") if case.input_policy == "pinned" else ()
    paths = [label + "/run.progress.json.gz"] + [
        "presentation/" + label + "-" + phase + "-progress.json.gz" for phase in phases
    ]
    for path in paths:
        progress = json.loads(artifact(PRESENTATION, path))
        assert progress["schema_version"] == 1 and progress["status"] == "completed"
        assert progress["observer_errors"] == 0 and progress["sequence"] > 1
    report = json.loads(artifact(PRESENTATION, "presentation/" + label + "-report.log.gz"))
    assert report["provider_requests_during_execution"] == 0
    assert all(
        p["measurement_status"] == "excluded" and p["cost"]["amount"] is None
        for p in report["gas_payers"]
    )
    comparisons = json.loads(artifact(PREPARED, label + "/quoter-comparisons.json.gz"))
    assert len(comparisons) == 18
    assert all(r["equal"] for r in comparisons)


@pytest.mark.parametrize("chain", [ChainId("eip155:8453")])
def test_removing_an_anchor_retains_a_required_coverage_gap(chain):
    cases = catalog()
    validate(cases)
    remaining = [
        c
        for c in cases
        if not (c.connector == ConnectorId("aerodrome_slipstream") and chain in c.chains)
    ]
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate(remaining)


@pytest.mark.parametrize(
    "case", tuple(c for c in MODELS if c.input_policy == "pinned"), ids=lambda c: c.id
)
def test_sealed_dataset_reproduces_the_entire_retained_state_offline(tmp_path, monkeypatch, case):
    import httpx

    from almanak.backtest.engine import Journal, Simulator
    from almanak.connectors.aerodrome_slipstream.economic_binding import (
        Manifest,
        SlipstreamEconomic,
    )
    from almanak.worker_session import preloaded_workers

    def forbidden(*args, **kwargs):
        raise AssertionError("sealed reproduction cannot obtain network inputs")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    label = case.input_policy + "-" + case.reference
    manifest = Manifest.model_validate_json(
        json.dumps(
            without_exchange_budgets(
                json.loads(artifact(PREPARED, label + "/run/manifest.json.gz"))
            )
        )
    )
    assert_reference_source(manifest.strategy, case.reference)
    strategy = current_imports_if_legacy(manifest.strategy)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for name in ("dataset.json", "data.sqlite"):
        (dataset / name).write_bytes(artifact(PREPARED, label + "/dataset/" + name + ".gz"))
    expected_path = tmp_path / "expected.sqlite"
    expected_path.write_bytes(artifact(PREPARED, label + "/run/state.sqlite.gz"))
    expected = Journal(expected_path, type(manifest.initial)).load()
    book = SlipstreamEconomic.open_book(manifest.model_copy(update={"dataset": dataset}))
    try:
        model = SlipstreamEconomic.driver(manifest.config, manifest.settings, strategy, book)
        journal = Journal(tmp_path / "control.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        with preloaded_workers():
            actual = Simulator(journal, model).run()
        retained = json.loads(expected.model_dump_json())
        current = json.loads(actual.model_dump_json())
        assert without_repeated_entry_quotes(current, retained) == retained
        assert_economic_direct(case, actual, manifest.config)
    finally:
        book.close()


def test_every_declared_case_is_sealed_once_and_all_fund_eoas_have_empty_origin_code(tmp_path):
    validate(catalog())
    native_manifest = json.loads((NATIVE / "manifest.json").read_text())
    prepared_manifest = json.loads((PREPARED / "manifest.json").read_text())
    held = sealed(PACKAGES)
    assert sorted(held) == sorted(case.id for case in FORKS)
    assert len(set(held)) == len(held) == len(FORKS) == 18
    assert len(native_manifest["cases"]) == 12
    assert {row["case"] for row in prepared_manifest["cases"]} == {case.id for case in MODELS}
    assert len(prepared_manifest["cases"]) == len(MODELS) == 6
    rows = json.loads(artifact(NATIVE, "audit/account-code-audit.json.gz"))
    funds = [r for r in rows if r["profile"] == "eoa" and r["scope"] == "fund"]
    base_eoa = {row["case"] for row in native_manifest["cases"] if row["profile"] == "eoa"}
    assert {r["case"] for r in funds} == base_eoa
    band_eoa = assert_supplement_audit(BAND, artifact, Checkpoint)
    assert base_eoa | band_eoa == {case.id for case in FORKS if case.profile == "eoa"}
    for row in funds:
        case = next(c for c in native_manifest["cases"] if c["case"] == row["case"])
        state = Checkpoint.model_validate_json(
            json.dumps(
                without_exchange_budgets(json.loads(artifact(NATIVE, case["checkpoint_path"])))
            )
        )
        assert row["result"] == "0x" and row["plain_eoa"]
        from scripts.legacy_configuration.author import read_author

        config = tmp_path / "config.yaml"
        config.write_bytes(artifact(NATIVE, case["directory"] + "/config.yaml.gz"))
        origin = read_author(config).fork_config().forks[0].block
        assert row["origin"] == origin.model_dump(mode="json")
        assert row["address"] == state.config.accounts.bindings[0].profile.fund.address
