import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from qa_lab.e2e_card import canonical, digest, load_json
from qa_lab.e2e_final_state import capture_final_subject, observe_final_subject
from qa_lab.e2e_ownership import OwnershipStore


@pytest.fixture
def completed(tmp_path):
    (tmp_path / "preparation").mkdir()
    (tmp_path / "subject").mkdir()
    card = tmp_path / "preparation/card.json"
    card.write_bytes(canonical({"run_id": "final-subject-test"}))
    store = OwnershipStore(tmp_path / "ownership.sqlite")
    store.initialize(run_id="final-subject-test", card_hash=digest(card.read_bytes()))
    lease = store.acquire("controller", seconds=300)
    token = store.reserve_launch(lease, "subject")
    write = "import sqlite3; d=sqlite3.connect('subject/almanak_state.db'); d.execute('CREATE TABLE final_rows (value INTEGER)'); d.execute('INSERT INTO final_rows VALUES (73)'); d.commit(); d.close()"
    launcher = (
        "from pathlib import Path; from qa_lab.e2e_ownership import OwnershipStore,Lease; "
        f"OwnershipStore(Path({str(store.path)!r})).exec_launch(Lease({lease.owner!r},{lease.generation}),"
        f"role='subject',token={token!r},argv=({sys.executable!r},'-c',{write!r}))"
    )
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[3])}
    worker = subprocess.Popen([sys.executable, "-c", launcher], cwd=tmp_path, env=env)
    try:
        assert worker.wait(timeout=15) == 0
        store.request_cleanup(lease)
        yield SimpleNamespace(root=tmp_path, require_owned=lambda p: p), store, lease, worker
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)


def test_final_database_follows_the_actual_owned_process_exit(completed):
    context, store, lease, worker = completed
    result = capture_final_subject(context, store, lease, worker)
    assert result["status"] == "CAPTURED"
    assert result["worker_pid"] == worker.pid
    assert result["exit_observed_monotonic_ns"] <= result["snapshot_started_monotonic_ns"]
    assert result["snapshot_started_monotonic_ns"] <= result["snapshot_finished_monotonic_ns"]
    target = context.root / "subject-final/almanak_state.db"
    assert digest(target.read_bytes()) == result["database_sha256"]
    with sqlite3.connect(target) as db:
        assert db.execute("SELECT value FROM final_rows").fetchall() == [(73,)]
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert result["e2e_admission"] == "UNMEASURED"
    before = (context.root / "subject-final/result.json").read_bytes()
    assert capture_final_subject(context, store, lease, worker)["status"] == "UNMEASURED"
    assert (context.root / "subject-final/result.json").read_bytes() == before


@pytest.mark.parametrize("fault", ["different_pid", "failed_exit", "wrong_card", "no_database"])
def test_missing_or_contradictory_final_proof_cannot_be_captured(completed, fault):
    context, store, lease, worker = completed
    if fault == "different_pid":
        with sqlite3.connect(store.path) as db:
            db.execute("UPDATE workers SET pid=pid+1")
    elif fault == "failed_exit":
        worker.returncode = 2
    elif fault == "wrong_card":
        (context.root / "preparation/card.json").write_bytes(canonical({"run_id": "another-run"}))
    else:
        (context.root / "subject/almanak_state.db").unlink()
    result = capture_final_subject(context, store, lease, worker)
    assert result["status"] == "UNMEASURED"
    assert not (context.root / "subject-final/almanak_state.db").exists()
    assert load_json(context.root / "subject-final/result.json")["status"] == "UNMEASURED"


def prepare_replay(completed):
    context, store, lease, worker = completed
    assert capture_final_subject(context, store, lease, worker)["status"] == "CAPTURED"
    (context.root / "ownership.json").write_bytes(canonical(store.export_cleanup_evidence(lease)))
    return context.root


def test_replay_verifies_final_snapshot_without_promoting_chain_reconciliation(completed):
    root = prepare_replay(completed)
    result = observe_final_subject(root)
    assert result["status"] == "PASS"
    assert result["terminal_chain_reconciliation"] == "UNMEASURED"


@pytest.mark.parametrize("fault", ["database", "card", "ownership", "exit", "ordering", "worker", "journal"])
def test_replay_rejects_altered_final_evidence(completed, fault):
    root = prepare_replay(completed)
    proof = root / "subject-final/result.json"
    observed = load_json(proof)
    if fault == "database":
        with sqlite3.connect(root / "subject-final/almanak_state.db") as db:
            db.execute("UPDATE final_rows SET value=74")
    elif fault == "card":
        (root / "preparation/card.json").write_bytes(canonical({"run_id": "other"}))
    elif fault == "ownership":
        path = root / "ownership.json"
        ownership = load_json(path)
        ownership["generation"] += 1
        path.write_bytes(canonical(ownership))
    elif fault == "exit":
        observed["returncode"] = 1
    elif fault == "ordering":
        observed["exit_observed_monotonic_ns"] = observed["snapshot_finished_monotonic_ns"] + 1
    elif fault == "worker":
        observed["worker"]["pid"] += 1
    else:
        (root / "subject-final/almanak_state.db-wal").touch()
    proof.write_bytes(canonical(observed))
    assert observe_final_subject(root)["status"] == "FAIL"


def test_missing_final_capture_remains_unmeasured(tmp_path):
    assert observe_final_subject(tmp_path)["status"] == "UNMEASURED"


def test_residual_admission_replays_snapshot_but_still_requires_wallet_and_chain_evidence(completed):
    from qa_lab.e2e_residual_policy import assess_residual_policy

    root = prepare_replay(completed)
    contract = {
        "residual_policy": {
            "known_nft_liquidity_raw": "0",
            "pending_orders": 0,
            "wallet_policy": "inventory_all_tokens_and_reconcile_subject_transactions",
        }
    }
    result = assess_residual_policy(contract, generations=None, quantities=None, actor=None, bundle=root)
    assert result["subject_final_snapshot"]["status"] == "PASS"
    assert result["status"] == "UNMEASURED"
    assert "subject_producer_quiescence_unmeasured" in result["reason_codes"]
    with sqlite3.connect(root / "subject-final/almanak_state.db") as db:
        db.execute("UPDATE final_rows SET value=74")
    result = assess_residual_policy(contract, generations=None, quantities=None, actor=None, bundle=root)
    assert result["status"] == "FAIL"
    assert "subject_final_snapshot_failed" in result["reason_codes"]


def test_missing_release_artifacts_cannot_leave_final_capture_marked_captured(completed):
    context, store, lease, worker = completed
    (context.root / "terminal-boundary.json").write_bytes(canonical({"status": "CAPTURED"}))
    result = capture_final_subject(context, store, lease, worker, release_started_monotonic_ns=1)
    assert result["status"] == "UNMEASURED"
    assert result["error_type"] == "FileNotFoundError"


def test_terminal_bound_capture_without_release_timestamp_is_unmeasured(completed):
    context, store, lease, worker = completed
    (context.root / "terminal-boundary.json").write_bytes(canonical({"status": "CAPTURED"}))
    result = capture_final_subject(context, store, lease, worker)
    assert result["status"] == "UNMEASURED"
    assert result["error_type"] == "ValueError"
