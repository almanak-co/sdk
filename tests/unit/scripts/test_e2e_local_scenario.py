"""Scenario waiting consumes real owned phase records without certifying prefixes."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from qa_lab import e2e_local_scenario as scenario
from qa_lab.e2e_phase_capture import PhasePending
from tests.unit.scripts import test_e2e_phase_capture as phases

captured = phases.captured
rebalanced = phases.rebalanced
lane = phases.lane


def test_rebalance_wait_publishes_only_completed_replacement(lane, monkeypatch):
    phases.capture(lane, "open")
    phases.launch_stimulus(lane)
    checks = []

    def advance(seconds):
        lane.current[0] = deepcopy(lane.managed)

    monkeypatch.setattr(scenario.time, "sleep", advance)
    result = scenario.wait_for_rebalance(
        lane.context, lane.store, lane.lease, wallet=lane.wallet, check_processes=lambda: checks.append(True)
    )
    assert result["capture"]["phase"] == "managed"
    assert len(checks) == 3
    assert lane.store.read_phase(lane.lease, "managed")


def test_rebalance_wait_allows_reserved_worker_to_claim_before_observation(lane, monkeypatch):
    phases.capture(lane, "open")
    token = lane.store.reserve_launch(lane.lease, "stimulus")

    def claim(seconds):
        assert lane.store.snapshot()["ownership"]["cleanup"] == 0
        assert not (lane.context.root / "positions-managed.json").exists()
        lane.store.claim_launch(lane.lease, role="stimulus", token=token)
        lane.current[0] = deepcopy(lane.managed)

    monkeypatch.setattr(scenario.time, "sleep", claim)
    result = scenario.wait_for_rebalance(
        lane.context, lane.store, lane.lease, wallet=lane.wallet, check_processes=lambda: None
    )
    assert result["capture"]["phase"] == "managed"
    assert len(lane.store.snapshot()["launches"]) == 2


def test_unclaimed_stimulus_wait_remains_bounded(lane, monkeypatch):
    phases.capture(lane, "open")
    lane.store.reserve_launch(lane.lease, "stimulus")
    clock = [0]
    monkeypatch.setattr(scenario.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(scenario.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(TimeoutError):
        scenario.wait_for_rebalance(
            lane.context, lane.store, lane.lease, wallet=lane.wallet, check_processes=lambda: None, timeout_seconds=3
        )
    assert clock[0] == 3
    assert not (lane.context.root / "positions-managed.json").exists()


def test_rebalance_wait_does_not_swallow_a_broken_wide_position(lane, captured):
    phases.capture(lane, "open")
    phases.launch_stimulus(lane)
    lane.current[0] = captured[1]
    with pytest.raises(ValueError) as failure:
        scenario.wait_for_rebalance(
            lane.context, lane.store, lane.lease, wallet=lane.wallet, check_processes=lambda: None
        )
    assert not isinstance(failure.value, PhasePending)
    assert not (lane.context.root / "positions-managed.json").exists()


def test_rebalance_wait_has_a_deadline(lane, monkeypatch):
    phases.capture(lane, "open")
    phases.launch_stimulus(lane)
    clock = [0]
    monkeypatch.setattr(scenario.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(scenario.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(TimeoutError, match="did not observe"):
        scenario.wait_for_rebalance(
            lane.context, lane.store, lane.lease, wallet=lane.wallet, check_processes=lambda: None, timeout_seconds=4
        )
    assert clock[0] == 4
    assert not (lane.context.root / "positions-managed.json").exists()


@pytest.mark.parametrize("role", ["subject", "cleanup", "stimulus"])
def test_scenario_requires_each_live_child(role):
    handles = {name: SimpleNamespace(poll=lambda: None) for name in ("subject", "cleanup", "stimulus")}
    handles[role] = SimpleNamespace(poll=lambda: 0)
    with pytest.raises(RuntimeError, match=f"Required {role} child exited"):
        scenario.check_children(handles["subject"], handles["cleanup"], handles["stimulus"])
