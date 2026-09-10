import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from qa_lab import e2e_watchdog as watchdog
from qa_lab.e2e_card import canonical, digest, load_json
from qa_lab.e2e_ownership import OwnershipError, OwnershipStore


@pytest.fixture
def run(tmp_path, monkeypatch):
    preparation = tmp_path / "preparation"
    preparation.mkdir()
    card = canonical({"run_id": "watchdog-unit-run"})
    (preparation / "card.json").write_bytes(card)
    clock = [1000.0]
    store = OwnershipStore(tmp_path / "ownership.sqlite", clock=lambda: clock[0])
    store.initialize(run_id="watchdog-unit-run", card_hash=digest(card))
    context = SimpleNamespace(root=tmp_path, require_owned=lambda path: path, chain="arbitrum", network="anvil")
    cleanup = []

    def observe(context, store, lease, **kwargs):
        cleanup.append(lease)
        return {"status": "PASS", "scope": "subject_lp_teardown", "actor_cleanup": "UNMEASURED"}

    monkeypatch.setattr(watchdog, "cleanup_subject", observe)
    monkeypatch.setattr(watchdog.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    return SimpleNamespace(context=context, store=store, clock=clock, cleanup=cleanup, output=tmp_path / "watcher")


def execute(run):
    return watchdog.watch_cleanup(
        run.context, run.store, owner="backup", wallet="subject", output=run.output, supervised=True
    )


def test_standby_never_claims_prepared_but_unstarted_run(run):
    assert run.store.adopt_expired("backup") is None
    assert run.store.snapshot()["ownership"]["generation"] == 0
    assert run.store.snapshot()["ownership"]["cleanup"] == 0


def test_live_renewal_delays_adoption_and_expired_owner_is_fenced(run, monkeypatch):
    primary = run.store.acquire("primary", seconds=10)
    sleeps = []

    def sleep(seconds):
        run.clock[0] += seconds
        sleeps.append(run.clock[0])
        if len(sleeps) == 1:
            run.store.renew(primary, seconds=10)

    monkeypatch.setattr(watchdog.time, "sleep", sleep)
    result = execute(run)
    assert sleeps == [1005, 1010, 1015]
    assert len(run.cleanup) == 1
    assert run.cleanup[0].generation == primary.generation + 1
    with pytest.raises(OwnershipError, match="stale"):
        run.store.reserve_launch(primary, "subject")
    with pytest.raises(OwnershipError, match="cleanup prohibits"):
        run.store.reserve_launch(run.cleanup[0], "subject")
    assert result["actor_cleanup"] == "UNMEASURED"
    assert load_json(run.output / "adopted.json")["generation"] == 2


def test_two_cleanup_watchers_cannot_both_adopt_same_expiry(run):
    run.store.acquire("primary", seconds=1)
    run.clock[0] += 2
    with ThreadPoolExecutor(max_workers=2) as executor:
        attempts = list(executor.map(run.store.adopt_expired, ["backup-one", "backup-two"]))
    assert sum(lease is not None for lease in attempts) == 1
    assert run.store.snapshot()["ownership"]["generation"] == 2


def test_interrupted_standby_does_not_claim_cleanup_coverage(run, monkeypatch):
    run.store.acquire("primary", seconds=10)

    def interrupted(seconds):
        raise KeyboardInterrupt()

    monkeypatch.setattr(watchdog.time, "sleep", interrupted)
    with pytest.raises(KeyboardInterrupt):
        execute(run)
    assert run.cleanup == []
    assert not (run.output / "adopted.json").exists()
    assert load_json(run.output / "interrupted.json")["cleanup"] == "UNMEASURED"


def test_failed_cleanup_preserves_unmeasured_outcome(run, monkeypatch):
    run.store.acquire("primary", seconds=1)
    run.clock[0] += 2

    def failed(*args, **kwargs):
        raise OSError("fork lost")

    monkeypatch.setattr(watchdog, "cleanup_subject", failed)
    with pytest.raises(OSError, match="fork lost"):
        execute(run)
    assert not (run.output / "result.json").exists()
    assert load_json(run.output / "interrupted.json")["error_type"] == "OSError"


def test_primary_cannot_pose_as_independent_standby(run):
    run.store.acquire("backup")
    with pytest.raises(ValueError, match="distinct owner"):
        execute(run)


def test_restarted_cleanup_watcher_can_adopt_its_previous_expired_lease(run):
    previous = run.store.acquire("backup", seconds=1, cleanup=True)
    run.clock[0] += 2
    execute(run)
    assert run.cleanup[0].owner == "backup"
    assert run.cleanup[0].generation == previous.generation + 1


def test_wrong_preparation_cannot_redirect_watcher(run):
    (run.context.root / "preparation/card.json").write_bytes(canonical({"run_id": "different"}))
    with pytest.raises(ValueError, match="differs from the prepared"):
        execute(run)


def test_separate_controller_process_death_leaves_adoptable_ownership(tmp_path, monkeypatch):
    preparation = tmp_path / "preparation"
    preparation.mkdir()
    card = canonical({"run_id": "process-death-unit"})
    (preparation / "card.json").write_bytes(card)
    store = OwnershipStore(tmp_path / "ownership.sqlite")
    store.initialize(run_id="process-death-unit", card_hash=digest(card))
    code = """
import sys, time
from pathlib import Path
from qa_lab.e2e_ownership import OwnershipStore
OwnershipStore(Path(sys.argv[1])).acquire('disposable-controller', seconds=1)
time.sleep(30)
"""
    monkeypatch.chdir(tmp_path)
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[3])}
    process = subprocess.Popen([sys.executable, "-c", code, str(store.path)], env=environment)
    try:
        deadline = time.monotonic() + 5
        while store.snapshot()["ownership"]["owner"] is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert store.snapshot()["ownership"]["owner"] == "disposable-controller"
        assert process.poll() is None
        process.terminate()
        process.wait(timeout=5)
        adopted = []

        def cleanup(context, current_store, lease, **kwargs):
            adopted.append(lease)
            return {"status": "UNMEASURED", "scope": "simulated_chain_cleanup"}

        monkeypatch.setattr(watchdog, "cleanup_subject", cleanup)
        context = SimpleNamespace(root=tmp_path, require_owned=lambda path: path, chain="arbitrum", network="anvil")
        result = watchdog.watch_cleanup(
            context, store, owner="independent-cleanup", wallet="subject", output=tmp_path / "watcher", supervised=True
        )
        assert result["status"] == "UNMEASURED"
        assert len(adopted) == 1 and adopted[0].generation == 2
        assert load_json(tmp_path / "watcher/started.json")["pid"] != process.pid
        with pytest.raises(OwnershipError, match="cleanup prohibits"):
            store.reserve_launch(adopted[0], "subject")
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


def test_watcher_started_after_clock_regression_can_reach_cleanup(run):
    primary = run.store.acquire("primary", seconds=300)
    run.clock[0] -= 100
    result = execute(run)
    assert len(run.cleanup) == 1
    assert run.cleanup[0].generation == primary.generation + 1
    assert result["status"] == "UNMEASURED"
    with pytest.raises(OwnershipError, match="stale"):
        run.store.renew(primary)
    with pytest.raises(OwnershipError, match="cleanup"):
        run.store.reserve_launch(run.cleanup[0], "subject")


def test_watcher_retains_ownership_until_unbooted_subject_becomes_targetable(run, monkeypatch):
    primary = run.store.acquire("primary", seconds=1)
    run.clock[0] += 2
    attempts = []

    def observe(context, store, lease, *, output, **kwargs):
        attempts.append((lease, output))
        if len(attempts) <= 2:
            return {"status": "UNMEASURED", "dispatch_status": "NOT_ATTEMPTED", "deployment_id": None}
        return {"status": "PASS", "deployment_id": "deployment:aaaaaaaaaaaa"}

    monkeypatch.setattr(watchdog, "cleanup_subject", observe)
    result = execute(run)
    assert len(attempts) == 3
    assert len({lease for lease, _ in attempts}) == 1
    assert [path.name for _, path in attempts] == ["subject", "subject-0001", "subject-0002"]
    assert load_json(run.output / "boot-pending-0000.json")["cleanup"] == "UNMEASURED"
    assert load_json(run.output / "boot-pending-0001.json")["cleanup"] == "UNMEASURED"
    assert result["subject_cleanup"]["deployment_id"] == "deployment:aaaaaaaaaaaa"
    assert result["subject_artifacts"] == "subject-0002"
    with pytest.raises(OwnershipError, match="stale"):
        run.store.renew(primary)


def test_interrupted_unbooted_cleanup_keeps_pending_evidence(run, monkeypatch):
    run.store.acquire("primary", seconds=1)
    run.clock[0] += 2
    monkeypatch.setattr(
        watchdog,
        "cleanup_subject",
        lambda *args, **kwargs: {
            "status": "FAIL",
            "dispatch_status": "NOT_ATTEMPTED",
            "deployment_id": None,
        },
    )

    def stop(seconds):
        raise KeyboardInterrupt()

    monkeypatch.setattr(watchdog.time, "sleep", stop)
    with pytest.raises(KeyboardInterrupt):
        execute(run)
    assert load_json(run.output / "boot-pending-0000.json")["subject_cleanup"]["status"] == "FAIL"
    assert load_json(run.output / "interrupted.json")["cleanup"] == "UNMEASURED"
    assert not (run.output / "result.json").exists()


def test_unbooted_cleanup_has_a_bounded_retry_budget(run, monkeypatch):
    lease = run.store.acquire("primary", seconds=10)
    run.output.mkdir()
    monkeypatch.setattr(
        watchdog,
        "cleanup_subject",
        lambda *args, **kwargs: {"status": "UNMEASURED", "dispatch_status": "NOT_ATTEMPTED", "deployment_id": None},
    )
    with pytest.raises(TimeoutError, match="bounded retry budget"):
        watchdog._cleanup_until_boot(
            run.context, run.store, lease, wallet="subject", output=run.output, max_attempts=2
        )
    assert load_json(run.output / "unbooted-timeout.json")["attempts"] == 2
