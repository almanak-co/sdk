"""Local history remains visible without authorizing execution or painting admission."""

import json
import shutil

import pytest

from qa_lab import e2e_local_runs as local
from qa_lab import qa_nav
from qa_lab.e2e_card import canonical, digest
from tests.unit.scripts import test_e2e_fork_shutdown as shutdown_tests

released = shutdown_tests.released


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("ALMANAK_QA_RUNS_ROOT", raising=False)
    (tmp_path / "runs").mkdir(mode=0o700)
    return tmp_path


def make_run(store, name="local-inspection-001"):
    root = store / "runs" / name
    root.mkdir(mode=0o700)
    (root / "preparation").mkdir()
    (root / "preparation/card.json").write_bytes(
        canonical(
            {
                "run_id": name,
                "schema_version": 3,
                "source_commit": "a" * 40,
            }
        )
    )
    return root


def test_restart_preserves_records_and_never_writes_an_admitted_index(store):
    run = make_run(store)
    (run / "local-qualification.json").write_bytes(
        canonical(
            {
                "scope": "local_supervisor_qualification",
                "e2e_admission": "PASS",
                "error_type": "TimeoutError",
            }
        )
    )
    first = local.local_run_records(store)
    local.render_local_runs(store=store, lab_css="", snapshot=first)
    second = local.local_run_records(store)
    local.render_local_runs(store=store, lab_css="", snapshot=second)
    assert first["runs"] == second["runs"]
    row = second["runs"][0]
    assert row["purpose"] == "Process qualification"
    assert row["reported_errors"] == ["TimeoutError"]
    assert row["shutdown"] == "NOT VERIFIED"
    page = (store / "lab/local-runs.html").read_text()
    assert "e2e_admission" not in page
    assert "do not add to PASS counts" in page
    assert 'href="../runs/local-inspection-001/preparation/card.json"' in page
    assert 'href="../runs/local-inspection-001/hold-result.json"' not in page
    assert not (store / "index").exists()
    assert (run / "local-qualification.json").is_file()


def test_missing_and_unreadable_evidence_remain_distinct(store):
    run = make_run(store)
    (run / "hold-result.json").write_text("{unfinished")
    row = local.local_run_records(store)["runs"][0]
    assert row["record_status"] == "UNREADABLE"
    artifacts = {item["path"]: item for item in row["artifacts"]}
    assert artifacts["hold-result.json"]["state"] == "UNREADABLE"
    assert artifacts["positions-managed.json"]["state"] == "MISSING"
    assert artifacts["preparation/card.json"]["sha256"] == digest((run / "preparation/card.json").read_bytes())


def test_retained_progress_does_not_infer_process_liveness_or_admission(store):
    run = make_run(store)
    (run / "positions-managed.json").write_bytes(canonical({"status": "PASS"}))
    (run / "hold-result.json").write_text("{unfinished")
    snapshot = local.local_run_records(store)
    row = snapshot["runs"][0]
    assert row["purpose"] == "Local run"
    assert row["recorded_progress"] == "Rebalance observation file retained"
    assert row["record_status"] == "UNREADABLE"
    assert row["shutdown"] == "NOT VERIFIED"
    page = local.render_local_runs(store=store, lab_css="", snapshot=snapshot).read_text()
    assert "Rebalance observation file retained" in page
    assert "Prepared run" not in page
    assert not (store / "index").exists()


def test_timeline_failure_is_visible_without_disclosing_raw_log_or_painting_admission(store):
    run = make_run(store)
    raw = (
        b"[debug    ] Failed to send event to gateway (non-fatal): secret-test-url\n"
        b"[error    ] Failed to record timeline event: no such table: timeline_events\n"
        b'details = "Failed to record timeline event: no such table: timeline_events"\n'
    )
    (run / "subject-process.log").write_bytes(raw)
    row = local.local_run_records(store)["runs"][0]
    assert row["timeline_delivery"] == {
        "state": "FAILURES RECORDED",
        "failed_sends": 1,
        "missing_table_errors": 1,
        "sha256": digest(raw),
    }
    assert row["reported_errors"] == []
    page = local.render_local_runs(store=store, lab_css="").read_text()
    assert "Timeline delivery failures" in page
    assert "secret-test-url" not in page
    assert "does not certify timeline completeness" in page
    assert not (store / "index").exists()


@pytest.mark.parametrize(
    "kind,state",
    [
        ("absent", "MISSING LOG"),
        ("empty", "NO MATCHING FAILURES RECORDED"),
        ("symlink", "UNREADABLE"),
        ("large", "NOT SCANNED (SIZE LIMIT)"),
    ],
)
def test_unmeasured_timeline_scan_is_distinct_from_measured_no_matches(store, kind, state):
    run = make_run(store)
    path = run / "subject-process.log"
    if kind == "empty":
        path.write_bytes(b"")
    elif kind == "symlink":
        path.symlink_to(store / "unrelated.log")
    elif kind == "large":
        with path.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024 + 1)
    result = local.local_run_records(store)["runs"][0]["timeline_delivery"]
    assert result["state"] == state
    assert result["failed_sends"] == (0 if kind == "empty" else None)


def test_unsafe_root_is_unknown_not_an_empty_history(store):
    (store / "runs").chmod(0o755)
    snapshot = local.local_run_records(store)
    assert snapshot["status"] == "UNREADABLE"
    page = local.render_local_runs(store=store, lab_css="", snapshot=snapshot).read_text()
    assert "could not be read" in page
    assert "No local run directories" not in page


def test_symlinked_run_is_retained_as_unreadable_without_following_it(store):
    target = store / "external"
    target.mkdir()
    (target / "private.txt").write_text("private-test-value")
    (store / "runs/local-linked-001").symlink_to(target, target_is_directory=True)
    snapshot = local.local_run_records(store)
    assert snapshot["runs"][0]["record_status"] == "UNREADABLE"
    assert "private-test-value" not in local.render_local_runs(store=store, lab_css="", snapshot=snapshot).read_text()


@pytest.mark.parametrize("value", [{"unexpected": "field"}, ["PASS"], "<script>alert('secret-test')</script>"])
def test_arbitrary_producer_fields_are_not_exported_to_the_page(store, value):
    run = make_run(store)
    (run / "local-qualification.json").write_text(
        json.dumps({"scope": value, "error_type": value, "private_key": "secret-test"})
    )
    page = local.render_local_runs(store=store, lab_css="").read_text()
    assert "secret-test" not in page
    assert "Unrecognized error report" in page


def test_evidence_hub_links_local_records_without_changing_lane_totals(store):
    make_run(store)
    before = qa_nav.totals(qa_nav.lane_states(store))
    page = qa_nav.render_evidence_lab(store=store, lab_css="").read_text()
    assert 'href="local-runs.html"' in page
    assert "1 retained run directories" in page
    assert (store / "lab/local-runs.html").is_file()
    assert qa_nav.totals(qa_nav.lane_states(store)) == before


def test_explicit_persistent_root_is_used_without_copying_raw_evidence(store, monkeypatch):
    other = store / "operator-runs"
    other.mkdir(mode=0o700)
    run = other / "custom-local-001"
    run.mkdir(mode=0o700)
    monkeypatch.setenv("ALMANAK_QA_RUNS_ROOT", str(other))
    snapshot = local.local_run_records(store)
    assert snapshot["root"] == str(other)
    assert snapshot["runs"][0]["run_id"] == run.name
    assert snapshot["runs"][0]["record_status"] == "UNVERIFIED PREPARATION"
    local.render_local_runs(store=store, lab_css="", snapshot=snapshot)
    assert not (store / "lab/custom-local-001").exists()


@pytest.mark.asyncio
async def test_unreadable_hold_does_not_erase_independent_shutdown_proof(store, released):
    root = store / "runs/released-test-run"
    root.mkdir(mode=0o700)
    for name in local.ARTIFACTS:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(released / name, target)
    (root / "hold-result.json").write_text("{unfinished")
    row = local.local_run_records(store)["runs"][0]
    assert row["record_status"] == "UNREADABLE"
    assert row["shutdown"] == "VERIFIED FROM RETAINED HANDSHAKE"


@pytest.mark.parametrize("assembly_status", ["INCOMPLETE", "ASSEMBLED"])
def test_reported_stage_statuses_expose_failed_cleanup_without_certifying_run(store, assembly_status):
    run = make_run(store)
    (run / "controller-result.json").write_bytes(
        canonical(
            {
                "status": "FAIL",
                "sampled_hold": {"status": "PASS"},
                "subject_cleanup": {"status": "PASS", "quantity_capture": {"status": "UNMEASURED"}},
                "stimulus_cleanup": {"status": "FAIL"},
            }
        )
    )
    (run / "bundle-result.json").write_bytes(canonical({"status": assembly_status}))
    snapshot = local.local_run_records(store)
    statuses = {stage["label"]: stage["status"] for stage in snapshot["runs"][0]["reported_stages"]}
    assert statuses == {
        "Controller": "FAIL",
        "Measured hold": "PASS",
        "Subject cleanup": "PASS",
        "Stimulus cleanup": "FAIL",
        "Subject quantities": "UNMEASURED",
        "Bundle assembly": assembly_status,
    }
    page = local.render_local_runs(store=store, lab_css="", snapshot=snapshot).read_text()
    assert "not independently admitted verdicts" in page
    assert "<td>Stimulus cleanup</td><td>FAIL</td>" in page
    assert not (store / "index").exists()


@pytest.mark.parametrize(
    "payload",
    [{"status": "<script>secret</script>"}, {"status": True}, {"sampled_hold": "PASS"}, {"status": {"value": "PASS"}}],
)
def test_stage_projection_rejects_unknown_or_malformed_report_values(store, payload):
    run = make_run(store)
    (run / "controller-result.json").write_bytes(canonical(payload))
    snapshot = local.local_run_records(store)
    assert all(stage["status"] == "NOT REPORTED" for stage in snapshot["runs"][0]["reported_stages"])
    page = local.render_local_runs(store=store, lab_css="", snapshot=snapshot).read_text()
    assert "secret" not in page
    assert not (store / "index").exists()
