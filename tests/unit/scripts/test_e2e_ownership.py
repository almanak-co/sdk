"""Competing controllers use independent connections to the same durable store."""

import fcntl
import hashlib
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from qa_lab.e2e_ownership import OwnershipError, OwnershipStore


@pytest.fixture
def owned(tmp_path):
    now = [1000.0]
    store = OwnershipStore(tmp_path / "ownership.sqlite", clock=lambda: now[0])
    store.initialize(run_id="run", card_hash="a" * 64)
    return store, now, store.acquire("controller")


def test_fork_release_is_bound_idempotent_and_cleanup_only(owned):
    from qa_lab.e2e_card import canonical

    store, _, lease = owned
    root = store.path.parent
    manifest = canonical({"preparation_sha256": "a" * 64})
    identity = {"manifest_sha256": hashlib.sha256(manifest).hexdigest(), "instance_id": "original"}
    (root / "pool-input.json").write_bytes(manifest)
    (root / "gateway-startup.json").write_bytes(canonical({"run_id": "run", "fork_identity": identity}))
    with pytest.raises(OwnershipError, match="cleanup ownership"):
        store.release_fork(lease)
    assert not (root / "fork-release.json").exists()
    store.request_cleanup(lease)
    store.release_fork(lease)
    before = (root / "fork-release.json").read_bytes()
    assert json.loads(before)["fork_identity"] == identity
    store.release_fork(lease)
    assert (root / "fork-release.json").read_bytes() == before
    assert not list(root.glob(".fork-release-*"))


def test_stale_cleanup_owner_cannot_release_fork(owned):
    store, now, stale = owned
    store.request_cleanup(stale)
    now[0] += 31
    store.acquire("replacement")
    with pytest.raises(OwnershipError, match="stale"):
        store.release_fork(stale)
    assert not (store.path.parent / "fork-release.json").exists()


def test_expired_controller_cannot_renew_or_launch_after_cleanup_takeover(owned):
    store, now, stale = owned
    token = store.reserve_launch(stale, "subject")
    now[0] += 30
    replacement = store.acquire("cleanup")
    assert replacement.generation == stale.generation + 1
    assert store.snapshot()["ownership"]["cleanup"] == 1
    with pytest.raises(OwnershipError, match="stale"):
        store.renew(stale)
    with pytest.raises(OwnershipError, match="stale"):
        store.claim_launch(stale, role="subject", token=token)
    with pytest.raises(OwnershipError, match="cleanup"):
        store.reserve_launch(replacement, "stimulus")


def test_cleanup_cancels_unclaimed_reservation_and_survives_reopen(owned):
    store, now, lease = owned
    token = store.reserve_launch(lease, "stimulus")
    store.request_cleanup(lease)
    reopened = OwnershipStore(store.path, clock=lambda: now[0])
    reopened.initialize(run_id="run", card_hash="a" * 64)
    with pytest.raises(OwnershipError, match="cleanup"):
        reopened.claim_launch(lease, role="stimulus", token=token)
    reopened.renew(lease)
    assert reopened.snapshot()["ownership"]["cleanup"] == 1


def test_competing_workers_cannot_claim_same_launch(owned):
    store, now, lease = owned
    token = store.reserve_launch(lease, "subject")

    def claim(_):
        independent = OwnershipStore(store.path, clock=lambda: now[0])
        try:
            independent.claim_launch(lease, role="subject", token=token)
            return "claimed"
        except OwnershipError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(claim, range(8)))
    assert outcomes.count("claimed") == 1
    assert outcomes.count("rejected") == 7
    with pytest.raises(OwnershipError, match="already reserved"):
        store.reserve_launch(lease, "subject")


def test_competing_reconcilers_have_one_cleanup_owner(owned):
    store, now, _ = owned
    now[0] += 31

    def acquire(index):
        independent = OwnershipStore(store.path, clock=lambda: now[0])
        try:
            return independent.acquire(f"cleanup-{index}")
        except OwnershipError:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        leases = [lease for lease in pool.map(acquire, range(8)) if lease is not None]
    assert len(leases) == 1
    assert leases[0].generation == 2
    assert store.snapshot()["ownership"]["cleanup"] == 1


def test_wrong_token_does_not_consume_valid_reservation(owned):
    store, _, lease = owned
    token = store.reserve_launch(lease, "subject")
    with pytest.raises(OwnershipError, match="reservation"):
        store.claim_launch(lease, role="subject", token="forged")
    store.claim_launch(lease, role="subject", token=token)


def test_clock_regression_cannot_extend_authority(owned):
    store, now, lease = owned
    now[0] -= 1
    with pytest.raises(OwnershipError, match="backwards"):
        store.renew(lease)


def test_reinitialization_cannot_replace_bound_card(owned):
    store, _, _ = owned
    with pytest.raises(OwnershipError, match="another run or card"):
        store.initialize(run_id="run", card_hash="b" * 64)
    assert store.snapshot()["ownership"]["card_hash"] == "a" * 64


def test_worker_exec_keeps_registered_pid_and_releases_barrier(owned, tmp_path, monkeypatch):
    store, _, lease = owned
    token = store.reserve_launch(lease, "subject")
    marker = tmp_path / "executed.json"
    command = (
        sys.executable,
        "-c",
        "import json, os, pathlib, time; "
        f"pathlib.Path({str(marker)!r}).write_text(json.dumps({{'pid': os.getpid()}})); time.sleep(30)",
    )
    worker = (
        "from pathlib import Path; from qa_lab.e2e_ownership import OwnershipStore, Lease; "
        f"OwnershipStore(Path({str(store.path)!r}), clock=lambda:1000).exec_launch("
        f"Lease({lease.owner!r}, {lease.generation}), role='subject', token={token!r}, argv={command!r})"
    )
    monkeypatch.chdir(tmp_path)
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[3])}
    process = subprocess.Popen([sys.executable, "-c", worker], env=environment)
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists(), f"worker failed to exec: {process.poll()}"
        assert json.loads(marker.read_text())["pid"] == process.pid
        store.request_cleanup(lease)
        snapshot = store.snapshot()
        assert snapshot["workers"][0]["pid"] == process.pid
        assert snapshot["ownership"]["cleanup"] == 1
        assert process.poll() is None
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=10)


def test_failed_exec_cannot_be_retried_and_does_not_block_cleanup(owned):
    store, _, lease = owned
    token = store.reserve_launch(lease, "subject")
    with pytest.raises(FileNotFoundError):
        store.exec_launch(lease, role="subject", token=token, argv=("/does-not-exist/e2e-worker",))
    with pytest.raises(OwnershipError, match="already claimed"):
        store.exec_launch(lease, role="subject", token=token, argv=("/does-not-exist/e2e-worker",))
    store.request_cleanup(lease)
    assert store.snapshot()["ownership"]["cleanup"] == 1


def test_exec_after_cleanup_never_starts_command(owned, tmp_path):
    store, _, lease = owned
    token = store.reserve_launch(lease, "subject")
    store.request_cleanup(lease)
    with pytest.raises(OwnershipError, match="cleanup"):
        store.exec_launch(lease, role="subject", token=token, argv=("/does-not-exist/e2e-worker",))
    assert store.snapshot()["workers"] == []


def test_cleanup_lock_is_held_at_the_exec_boundary(owned, monkeypatch):
    store, _, lease = owned
    token = store.reserve_launch(lease, "subject")

    def paused_exec(*_):
        descriptor = os.open(store.path.with_suffix(".lock"), os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        raise OSError("injected failure at exec boundary")

    monkeypatch.setattr(os, "execvp", paused_exec)
    with pytest.raises(OSError, match="injected failure"):
        store.exec_launch(lease, role="subject", token=token, argv=("unused",))
    store.request_cleanup(lease)


def test_clock_regression_allows_only_fenced_cleanup(owned):
    store, now, stale = owned
    token = store.reserve_launch(stale, "subject")
    now[0] -= 100
    assert store.snapshot()["ownership"]["owner"] == stale.owner
    with pytest.raises(OwnershipError, match="backwards"):
        store.claim_launch(stale, role="subject", token=token)
    with pytest.raises(OwnershipError, match="backwards"):
        store.acquire("replacement")

    def adopt(index):
        independent = OwnershipStore(store.path, clock=lambda: now[0])
        return independent.adopt_expired(f"backup-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        leases = [lease for lease in pool.map(adopt, range(8)) if lease is not None]
    assert len(leases) == 1
    cleanup = leases[0]
    assert cleanup.generation == stale.generation + 1
    reopened = OwnershipStore(store.path, clock=lambda: now[0])
    reopened.request_cleanup(cleanup)
    reopened.renew(cleanup)
    with pytest.raises(OwnershipError, match="stale"):
        reopened.renew(stale)
    with pytest.raises(OwnershipError, match="stale"):
        reopened.claim_launch(stale, role="subject", token=token)
    with pytest.raises(OwnershipError, match="cleanup"):
        reopened.reserve_launch(cleanup, "stimulus")
    with pytest.raises(OwnershipError, match="cleanup"):
        reopened.renew(cleanup, require_running=True)
    now[0] += 31
    successor = reopened.adopt_expired("next-backup")
    assert successor.generation == cleanup.generation + 1
    assert reopened.snapshot()["ownership"]["cleanup"] == 1


def test_clock_regression_cannot_adopt_unstarted_run(tmp_path):
    now = [1000.0]
    store = OwnershipStore(tmp_path / "ownership.sqlite", clock=lambda: now[0])
    store.initialize(run_id="run", card_hash="a" * 64)
    now[0] -= 100
    assert store.adopt_expired("backup") is None
    assert store.snapshot()["ownership"]["generation"] == 0


@pytest.mark.parametrize("clock", [float("nan"), float("inf"), -1])
def test_invalid_clock_never_grants_cleanup_authority(owned, clock):
    store, now, lease = owned
    now[0] = clock
    with pytest.raises(OwnershipError, match="invalid ownership clock"):
        store.adopt_expired("backup")
    assert store.snapshot()["ownership"]["generation"] == lease.generation
