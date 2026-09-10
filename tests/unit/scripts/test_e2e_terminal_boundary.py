import copy
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from qa_lab.e2e_card import canonical, digest
from qa_lab.e2e_terminal_boundary import capture_terminal_boundary, observe_terminal_boundary


@pytest.fixture
def boundary(tmp_path):
    (tmp_path / "subject-final").mkdir()
    identity = {"chain": "arbitrum", "instance_id": "owned"}
    release = {"schema_version": 1, "scope": "qa_fork_release", "run_id": "boundary-test", "fork_identity": identity}
    gateway = {**release, "scope": "qa_subject_gateway_stopped", "stopped_monotonic_ns": 10}
    ownership = {"run_id": "boundary-test"}
    actor = {
        "scope": "owned_actor_process_exit",
        "child_reaped": True,
        "child_returncode": 0,
        "reason": "SDK_EXITED",
        "error_type": None,
    }
    block = {"number": 123, "hash": "0x" + "ab" * 32}
    evidence = {
        "fork-release.json": release,
        "subject-gateway-stopped.json": gateway,
        "actor-process-result.json": actor,
        "ownership.json": ownership,
        "positions-terminal.json": {
            "fork_identity": identity,
            "end_block": block["number"],
            "end_block_hash": block["hash"],
        },
    }
    terminal = {
        "schema_version": 1,
        "scope": "owned_producers_terminal_boundary",
        "status": "CAPTURED",
        "run_id": "boundary-test",
        "ownership_sha256": digest(canonical(ownership)),
        "fork_identity": identity,
        "block": dict(block),
        "head_after": dict(block),
        "started_monotonic_ns": 20,
        "finished_monotonic_ns": 30,
        "worker_returncode": 0,
        "worker": {"role": "stimulus"},
        "txpool": {"pending": {}, "queued": {}},
        "source_sha256": {
            name: digest(canonical(evidence[name]))
            for name in ("subject-gateway-stopped.json", "actor-process-result.json")
        },
    }
    evidence["terminal-boundary.json"] = terminal
    evidence["subject-final/result.json"] = {
        "exit_observed_monotonic_ns": 40,
        "terminal_boundary_sha256": digest(canonical(terminal)),
    }
    for name, value in evidence.items():
        (tmp_path / name).write_bytes(canonical(value))
    return tmp_path, evidence


def replay(root):
    return observe_terminal_boundary(root, {"status": "PASS"}, {"status": "PASS"})


def test_terminal_boundary_binds_stopped_producers_to_unchanged_chain(boundary):
    root, evidence = boundary
    result = replay(root)
    assert result["status"] == "PASS"
    assert result["block"] == evidence["terminal-boundary.json"]["block"]


@pytest.mark.parametrize(
    "fault",
    [
        "late_block",
        "head_drift",
        "pending",
        "missing_queue",
        "early_capture",
        "late_gateway_stop",
        "late_capture",
        "actor_failure",
        "source_edit",
        "missing",
        "unbound",
    ],
)
def test_terminal_boundary_cannot_hide_late_work_or_missing_ordering(boundary, fault):
    root, evidence = boundary
    terminal = copy.deepcopy(evidence["terminal-boundary.json"])
    if fault == "late_block":
        terminal["block"]["number"] += 1
    elif fault == "head_drift":
        terminal["head_after"]["hash"] = "0x" + "cd" * 32
    elif fault == "pending":
        terminal["txpool"]["pending"] = {"0x" + "aa" * 20: {"1": {}}}
    elif fault == "missing_queue":
        del terminal["txpool"]["queued"]
    elif fault == "early_capture":
        terminal["started_monotonic_ns"] = 5
    elif fault == "late_gateway_stop":
        gateway = evidence["subject-gateway-stopped.json"]
        gateway["stopped_monotonic_ns"] = 25
        (root / "subject-gateway-stopped.json").write_bytes(canonical(gateway))
        terminal["source_sha256"]["subject-gateway-stopped.json"] = digest(canonical(gateway))
    elif fault == "late_capture":
        terminal["finished_monotonic_ns"] = 50
    elif fault == "actor_failure":
        terminal["worker_returncode"] = 1
    elif fault == "source_edit":
        actor = evidence["actor-process-result.json"]
        actor["child_returncode"] = 1
        (root / "actor-process-result.json").write_bytes(canonical(actor))
    elif fault == "missing":
        (root / "subject-gateway-stopped.json").unlink()
    (root / "terminal-boundary.json").write_bytes(canonical(terminal))
    final = evidence["subject-final/result.json"]
    if fault == "unbound":
        final.pop("terminal_boundary_sha256")
    else:
        final["terminal_boundary_sha256"] = digest(canonical(terminal))
    (root / "subject-final/result.json").write_bytes(canonical(final))
    assert replay(root)["status"] == ("UNMEASURED" if fault == "missing" else "FAIL")


@pytest.mark.parametrize("capture,database", [("UNMEASURED", "PASS"), ("PASS", "FAIL")])
def test_terminal_boundary_cannot_replace_final_database_or_process_proof(boundary, capture, database):
    root, _ = boundary
    assert observe_terminal_boundary(root, {"status": capture}, {"status": database})["status"] == "UNMEASURED"


def test_capture_waits_for_real_owned_actor_and_retains_raw_chain_observation(tmp_path):
    from qa_lab.e2e_ownership import OwnershipStore

    store = OwnershipStore(tmp_path / "ownership.sqlite")
    store.initialize(run_id="capture-boundary", card_hash="a" * 64)
    lease = store.acquire("controller", seconds=300)
    token = store.reserve_launch(lease, "stimulus")
    launcher = (
        "from pathlib import Path; from types import SimpleNamespace; "
        "from qa_lab.e2e_ownership import OwnershipStore,Lease; from qa_lab.e2e_actor_guard import guard_actor; "
        f"root=Path({str(tmp_path)!r}); "
        f"child=OwnershipStore(root/'ownership.sqlite').spawn_launch(Lease({lease.owner!r},{lease.generation}),"
        f"role='stimulus',token={token!r},argv=({sys.executable!r},'-c','pass')); "
        "raise SystemExit(guard_actor(SimpleNamespace(root=root,require_owned=lambda p:p),child))"
    )
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[3])}
    worker = subprocess.Popen([sys.executable, "-c", launcher], cwd=tmp_path, env=env)
    try:
        assert worker.wait(timeout=15) == 0
        store.request_cleanup(lease)
        (tmp_path / "subject-gateway-stopped.json").write_bytes(
            canonical({"scope": "qa_subject_gateway_stopped", "run_id": "capture-boundary"})
        )
        block = {"number": 123, "hash": bytes.fromhex("ab" * 32)}
        client = SimpleNamespace(
            eth=SimpleNamespace(get_block=lambda name: block),
            provider=SimpleNamespace(make_request=lambda method, params: {"result": {"pending": {}, "queued": {}}}),
        )
        context = SimpleNamespace(
            root=tmp_path,
            fork_block=123,
            fork_hash="0x" + "ab" * 32,
            require_owned=lambda p: p,
            assert_rpc_identity=lambda *args: client,
            public_identity=lambda: {"instance_id": "owned"},
        )
        result = capture_terminal_boundary(context, store, lease, [worker])
        assert result["status"] == "CAPTURED"
        assert result["worker"]["pid"] == worker.pid
        assert result["block"] == {"number": 123, "hash": "0x" + "ab" * 32}
        assert result["txpool"] == {"pending": {}, "queued": {}}
        assert result["wallet_census_sha256"] == digest((tmp_path / "wallet-receipt-census.json").read_bytes())
        assert result["source_sha256"]["actor-process-result.json"] == digest(
            (tmp_path / "actor-process-result.json").read_bytes()
        )
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)


def test_capture_write_failure_does_not_prevent_caller_releasing_fork(tmp_path, monkeypatch):
    from qa_lab import e2e_terminal_boundary

    def failed(*args):
        raise OSError("disk full")

    monkeypatch.setattr(e2e_terminal_boundary, "_write", failed)
    context = SimpleNamespace(root=tmp_path, require_owned=lambda p: p)
    result = capture_terminal_boundary(context, None, None, [])
    assert result["status"] == "UNMEASURED"
    assert result["error_type"] == "OSError"
