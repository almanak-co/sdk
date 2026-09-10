"""Long read operations retain one lease, with a finite renewal budget."""

import queue
import threading
import time
from types import SimpleNamespace

import pytest

from qa_lab.e2e_lease_guard import observe_with_lease
from qa_lab.e2e_ownership import OwnershipError, OwnershipStore


@pytest.fixture
def owned(tmp_path):
    clock = [1000.0]
    store = OwnershipStore(tmp_path / "ownership.sqlite", clock=lambda: clock[0])
    store.initialize(run_id="lease-guard", card_hash="a" * 64)
    return store, clock, store.acquire("controller")


def test_collection_renews_across_original_expiry_without_granting_takeover(owned):
    store, clock, lease = owned
    renewals = queue.Queue()

    def renew(*args, **kwargs):
        store.renew(*args, **kwargs)
        renewals.put(clock[0])

    def observe():
        assert renewals.get(timeout=2) == 1000
        for _ in range(4):
            clock[0] += 100
            deadline = time.monotonic() + 2
            while renewals.get(timeout=2) != clock[0]:
                assert time.monotonic() < deadline
            assert store.adopt_expired("backup") is None
        return "measured"

    assert observe_with_lease(SimpleNamespace(renew=renew), lease, observe, renewal_seconds=0.01) == "measured"
    assert store.snapshot()["ownership"]["generation"] == lease.generation


def test_result_after_takeover_is_rejected(owned):
    store, clock, lease = owned

    def observe():
        clock[0] += 301
        replacement = store.adopt_expired("backup")
        assert replacement.generation == lease.generation + 1
        return {"status": "PASS"}

    with pytest.raises(OwnershipError, match="stale"):
        observe_with_lease(store, lease, observe)


def test_stuck_reader_does_not_renew_forever_or_return_pass(owned):
    store, clock, lease = owned
    stopped = threading.Event()
    renewals = []

    def renew(*args, **kwargs):
        store.renew(*args, **kwargs)
        renewals.append(clock[0])

    def observe():
        stopped.wait(0.1)
        count = len(renewals)
        clock[0] += 301
        replacement = store.adopt_expired("backup")
        assert replacement is not None
        stopped.wait(0.05)
        assert len(renewals) == count
        return {"status": "PASS"}

    with pytest.raises(TimeoutError, match="ownership budget"):
        observe_with_lease(SimpleNamespace(renew=renew), lease, observe, renewal_seconds=0.005, budget_seconds=0.025)


def test_running_capture_cannot_continue_after_cleanup_latch(owned):
    store, _, lease = owned

    def observe():
        store.request_cleanup(lease)
        return {"status": "PASS"}

    with pytest.raises(OwnershipError, match="cleanup"):
        observe_with_lease(store, lease, observe, require_running=True)
    assert observe_with_lease(store, lease, lambda: "cleanup measurement") == "cleanup measurement"


def test_failed_observation_stops_renewal_and_preserves_original_error(owned):
    store, _, lease = owned
    threads_before = {thread.ident for thread in threading.enumerate() if thread.name == "e2e-observation-lease"}

    def observe():
        raise OSError("missing observation")

    with pytest.raises(OSError, match="missing observation"):
        observe_with_lease(store, lease, observe)
    assert {
        thread.ident for thread in threading.enumerate() if thread.name == "e2e-observation-lease"
    } == threads_before


@pytest.mark.parametrize("budget,interval", [(601, 30), (600, 31), (10, 10), (0, 1), (float("inf"), 1)])
def test_unbounded_or_invalid_observation_budget_is_rejected(owned, budget, interval):
    store, _, lease = owned
    with pytest.raises(ValueError, match="bounded budget"):
        observe_with_lease(
            store, lease, lambda: pytest.fail("invalid budget ran"), budget_seconds=budget, renewal_seconds=interval
        )
