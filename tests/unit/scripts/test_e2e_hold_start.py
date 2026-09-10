import json
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import pytest

from qa_lab import e2e_hold_start as start
from qa_lab.e2e_ownership import OwnershipStore
from tests.unit.scripts import test_e2e_continuity as continuity

stream = continuity.stream


@pytest.fixture
def run(stream, monkeypatch):
    path, records = stream
    clock = [1000.0]
    monkeypatch.setattr(start.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(start.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(start, "datetime", SimpleNamespace(now=lambda zone: continuity.START))
    monkeypatch.setattr(start, "boot_deployment_id", lambda subject: continuity.IDENTITY)
    store = OwnershipStore(path.parent / "ownership.sqlite", clock=lambda: clock[0])
    store.initialize(run_id="hold-start", card_hash="a" * 64)
    lease = store.acquire("controller", seconds=300)
    context = SimpleNamespace(root=path.parent, require_owned=lambda value: value)
    path.write_text("")
    return SimpleNamespace(path=path, event=records[0], clock=clock, store=store, lease=lease, context=context)


def append(run, event, *, complete=True):
    with run.path.open("a") as stream:
        stream.write(json.dumps(event) + ("\n" if complete else ""))


def wait(run, check=lambda: None):
    start.wait_for_runner_hold(run.context, run.store, run.lease, check_processes=check, timeout_seconds=10)


def test_waits_past_old_hold_and_late_replacement_summary(run, monkeypatch):
    stale = {**run.event, "timestamp": (continuity.START - timedelta(seconds=1)).isoformat()}
    append(run, stale)

    def advance(seconds):
        run.clock[0] += seconds
        event = deepcopy(run.event)
        event["timestamp"] = (continuity.START + timedelta(seconds=run.clock[0] - 1000)).isoformat()
        if run.clock[0] == 1002:
            event.update(decision="LP_OPEN", status="SUCCESS", txs_sent=4)
        append(run, event)

    monkeypatch.setattr(start.time, "sleep", advance)
    wait(run)
    assert run.clock[0] == 1004
    assert run.store.snapshot()["ownership"]["cleanup"] == 0


def test_partial_live_record_is_not_a_completed_hold(run, monkeypatch):
    append(run, run.event, complete=False)

    def finish(seconds):
        run.clock[0] += seconds
        with run.path.open("a") as stream:
            stream.write("\n")

    monkeypatch.setattr(start.time, "sleep", finish)
    wait(run)
    assert run.clock[0] == 1002


@pytest.mark.parametrize("mutation", ["reason", "transaction", "identity", "failure", "naive"])
def test_contradictory_summary_cannot_start_hold(run, mutation):
    event = deepcopy(run.event)
    if mutation == "reason":
        event["hold_reason"] = "market unavailable"
    elif mutation == "transaction":
        event["txs_sent"] = 1
    elif mutation == "identity":
        event["deployment_id"] = "deployment:another"
    elif mutation == "failure":
        event.update(decision="LP_OPEN", status="FAILED")
    else:
        event["timestamp"] = event["timestamp"].replace("+00:00", "")
    append(run, event)
    with pytest.raises(ValueError):
        wait(run)


def test_no_fresh_hold_has_a_bounded_wait(run):
    with pytest.raises(TimeoutError):
        wait(run)
    assert run.clock[0] == 1010


def test_child_loss_before_hold_prevents_sampling(run):
    append(run, run.event)

    def dead():
        raise RuntimeError("subject exited")

    with pytest.raises(RuntimeError, match="subject exited"):
        wait(run, dead)
