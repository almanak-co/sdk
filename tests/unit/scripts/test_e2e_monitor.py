from types import SimpleNamespace

import pytest

from qa_lab import e2e_monitor as monitor
from qa_lab.e2e_ownership import OwnershipError, OwnershipStore


@pytest.fixture
def run(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(monitor.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(monitor.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    store = OwnershipStore(tmp_path / "ownership.sqlite", clock=lambda: clock[0])
    store.initialize(run_id="monitor", card_hash="a" * 64)
    lease = store.acquire("controller")
    observer = SimpleNamespace(output=tmp_path / "samples", sequence=0)

    def capture():
        observer.sequence += 1

    observer.capture = capture
    return clock, store, lease, observer


def test_measured_completion_latches_cleanup_and_prevents_launch(run, monkeypatch):
    clock, store, lease, observer = run
    results = iter(
        [
            {"status": "UNMEASURED"},
            {"status": "FAIL", "elapsed_seconds": 59},
            {"status": "PASS", "elapsed_seconds": 119},
        ]
    )
    monkeypatch.setattr(monitor, "held_exposure", lambda *args, **kwargs: next(results))
    result = monitor.monitor_hold(observer, store, lease, monitor.HoldSchedule(100, 90, 60, 200))
    assert result["elapsed_seconds"] == 119
    assert observer.sequence == 3
    assert clock[0] == 1120
    assert store.snapshot()["ownership"]["cleanup"] == 1
    with pytest.raises(OwnershipError, match="cleanup prohibits"):
        store.reserve_launch(lease, "stimulus")


def test_contradictory_observation_stops_immediately(run, monkeypatch):
    _, store, lease, observer = run
    monkeypatch.setattr(monitor, "held_exposure", lambda *args, **kwargs: {"status": "FAIL", "reason": "NFT changed"})
    with pytest.raises(ValueError, match="NFT changed"):
        monitor.monitor_hold(observer, store, lease, monitor.HoldSchedule(100, 90, 60, 200))
    assert observer.sequence == 1
    assert store.snapshot()["ownership"]["cleanup"] == 1


def test_capture_error_preserved_and_cleanup_requested(run):
    _, store, lease, observer = run

    def capture():
        raise OSError("RPC unavailable")

    observer.capture = capture
    with pytest.raises(OSError, match="RPC unavailable"):
        monitor.monitor_hold(observer, store, lease, monitor.HoldSchedule(100, 90, 60, 200))
    assert store.snapshot()["ownership"]["cleanup"] == 1


def test_expired_capture_cannot_reclaim_cleanup_authority(run):
    clock, store, lease, observer = run

    def capture():
        clock[0] += 301

    observer.capture = capture
    with pytest.raises(BaseExceptionGroup, match="cleanup was not established") as failure:
        monitor.monitor_hold(observer, store, lease, monitor.HoldSchedule(100, 90, 60, 400))
    assert all(isinstance(error, OwnershipError) for error in failure.value.exceptions)
    replacement = store.acquire("independent-cleanup", cleanup=True)
    assert replacement.generation == lease.generation + 1
    assert store.snapshot()["ownership"]["cleanup"] == 1


def test_cleanup_latched_elsewhere_prevents_another_capture(run):
    _, store, lease, observer = run
    store.request_cleanup(lease)
    with pytest.raises(OwnershipError, match="continued scenario monitoring"):
        monitor.monitor_hold(observer, store, lease, monitor.HoldSchedule(100, 90, 60, 200))
    assert observer.sequence == 0


def test_deadline_is_not_a_successful_hold(run, monkeypatch):
    _, store, lease, observer = run
    monkeypatch.setattr(monitor, "held_exposure", lambda *args, **kwargs: {"status": "FAIL", "elapsed_seconds": 0})
    with pytest.raises(TimeoutError, match="without measured completion"):
        monitor.monitor_hold(observer, store, lease, monitor.HoldSchedule(100, 90, 60, 200))
    assert observer.sequence == 4
    assert store.snapshot()["ownership"]["cleanup"] == 1


@pytest.mark.parametrize("bounds", [(True, 90, 60, 200), (100, 60, 60, 200), (100, 90, 60, 160)])
def test_impossible_or_ambiguous_schedule_rejected(bounds):
    with pytest.raises(ValueError):
        monitor.HoldSchedule(*bounds)


@pytest.mark.parametrize("exit_at", ["before_capture", "during_capture", "between_captures"])
def test_child_exit_invalidates_hold_and_latches_cleanup(run, monkeypatch, exit_at):
    clock, store, lease, observer = run
    monkeypatch.setattr(monitor, "held_exposure", lambda *args, **kwargs: {"status": "UNMEASURED"})

    def check():
        dead = (
            exit_at == "before_capture"
            or (exit_at == "during_capture" and observer.sequence > 0)
            or (exit_at == "between_captures" and clock[0] > 1000)
        )
        if dead:
            raise RuntimeError("Required cleanup child exited")

    with pytest.raises(RuntimeError, match="cleanup child exited"):
        monitor.monitor_hold(observer, store, lease, monitor.HoldSchedule(100, 90, 60, 200), check_processes=check)
    assert observer.sequence == (0 if exit_at == "before_capture" else 1)
    assert clock[0] <= 1030
    assert store.snapshot()["ownership"]["cleanup"] == 1
