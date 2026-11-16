"""Decision cadence, clock boundaries and durable scheduler transitions."""

import json
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter, ValidationError

from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway.service import GatewayService
from almanak.lending import Decision, Memory
from almanak.runtime import strategy_decision
from almanak.state import Journal
from almanak.triggers import (
    DecisionSchedule,
    IntervalEvent,
    IntervalSelection,
    IntervalTrigger,
    TeardownEvent,
    TriggerEvent,
    utc_now,
)
from tests.test_runtime_recovery import CapturedGateway, coordinator, tick_until

START = datetime(2026, 9, 5, 0, 0, 0, 123456, tzinfo=UTC)
TRIGGER = IntervalTrigger(kind="interval", seconds=5)


class Clock:
    def __init__(self, now=START):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


@pytest.mark.parametrize("seconds", [0, -1, True, False, 0.5, 5.0, "5", None])
def test_interval_requires_positive_whole_seconds(seconds):
    with pytest.raises(ValidationError):
        IntervalTrigger.model_validate({"kind": "interval", "seconds": seconds})


@pytest.mark.parametrize("trigger", [None, {}, {"kind": "block", "seconds": 5}])
def test_author_requires_a_supported_trigger(trigger):
    from tests.test_fork_config import CONFIG

    raw = json.loads(CONFIG.read_text())
    if trigger is None:
        raw.pop("decision_trigger")
    else:
        raw["decision_trigger"] = trigger
    with pytest.raises(ValidationError):
        ForkConfig.model_validate_json(json.dumps(raw))


def test_trigger_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        IntervalTrigger(kind="interval", seconds=5, clock="chain")


def test_timestamps_preserve_precision_and_normalize_utc():
    local = START.astimezone(timezone(timedelta(hours=8)))
    event = IntervalEvent(started_at=local, due_at=START)
    assert event.started_at.tzinfo == UTC
    assert event.started_at == START
    encoded = event.model_dump_json()
    assert "2026-09-05T00:00:00.123456Z" in encoded
    assert TypeAdapter(TriggerEvent).validate_json(encoded) == event
    assert utc_now().tzinfo == UTC


@pytest.mark.parametrize("kind", ["interval", "teardown"])
def test_naive_timestamps_are_rejected(kind):
    raw = {"kind": kind, "started_at": START.replace(tzinfo=None)}
    if kind == "interval":
        raw["due_at"] = START
    with pytest.raises(ValidationError, match="explicit timezone"):
        TypeAdapter(TriggerEvent).validate_python(raw)


def test_early_or_mismatched_events_cannot_consume_a_deadline():
    with pytest.raises(ValidationError, match="precede"):
        IntervalEvent(started_at=START, due_at=START + timedelta(seconds=1))
    schedule = DecisionSchedule().after(TRIGGER, DecisionSchedule().event(START))
    wrong = IntervalEvent(started_at=START + timedelta(seconds=6), due_at=START)
    with pytest.raises(ValueError, match="retained deadline"):
        schedule.after(TRIGGER, wrong)


@pytest.mark.parametrize("field", ["last_evaluation", "next_due_at"])
def test_incomplete_schedule_cannot_be_loaded(field):
    event = IntervalEvent(started_at=START, due_at=START)
    raw = DecisionSchedule().after(TRIGGER, event).model_dump()
    raw.pop(field)
    with pytest.raises(ValidationError, match="both evaluation and deadline"):
        DecisionSchedule.model_validate(raw)


@pytest.mark.parametrize("override", [0, -1, True, 4, 5, 6.5, "60"])
def test_reduced_cadence_must_be_an_explicit_coarser_interval(override):
    with pytest.raises(ValidationError):
        IntervalSelection(configured=TRIGGER, override_seconds=override)


@pytest.mark.parametrize(
    "override,effective,fidelity", [(None, 5, "configured"), (60, 60, "reduced")]
)
def test_selection_preserves_declared_and_effective_intervals(override, effective, fidelity):
    selection = IntervalSelection(configured=TRIGGER, override_seconds=override)
    assert selection.effective.seconds == effective
    assert IntervalSelection.model_validate_json(selection.model_dump_json()) == selection
    assert selection.report() == {
        "configured_seconds": 5,
        "effective_seconds": effective,
        "decision_interval_override_seconds": override,
        "cadence_fidelity": fidelity,
    }
    assert TRIGGER.seconds == 5


@pytest.mark.parametrize("completion,expected", [(2, [0, 5, 10]), (12, [0, 12, 17])])
def test_wall_and_simulated_clocks_have_identical_eligibility(completion, expected):
    wall = Clock()
    simulated = Clock(datetime(1970, 1, 1, tzinfo=UTC))
    sequences = []
    for clock in (wall, simulated):
        start = clock.now
        schedule = DecisionSchedule()
        events = []
        for seconds in (0, completion, max(completion, 5), max(completion, 5) + 5):
            clock.now = start + timedelta(seconds=seconds)
            event = schedule.event(clock())
            if event is not None:
                events.append(int((event.started_at - start).total_seconds()))
                schedule = schedule.after(TRIGGER, event)
        sequences.append(events)
    assert sequences == [expected, expected]


@pytest.fixture
def scheduled(tmp_path, recorded, reference_strategy, monkeypatch):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, reference_strategy)
    clock = Clock()
    gateway = CapturedGateway(journal, recorded)
    calls = SimpleNamespace(observe=0, decisions=[])
    original = gateway.observe

    def observe():
        calls.observe += 1
        return original()

    def hold(_definition, context):
        calls.decisions.append(context)
        return Decision(memory=Memory(values={"evaluations": len(calls.decisions)}))

    gateway.observe = observe
    monkeypatch.setattr("almanak.runtime.strategy_decision", hold)
    runner = coordinator(journal, gateway, clock=clock)
    runner.tick()
    assert journal.load().initialized
    assert not calls.decisions
    return SimpleNamespace(
        journal=journal, clock=clock, gateway=gateway, calls=calls, runner=runner
    )


def test_due_boundary_skips_reads_and_worker_until_eligible(scheduled):
    s = scheduled
    s.runner.tick()
    state = s.journal.load()
    assert state.schedule.last_evaluation.started_at == START
    assert state.memory.values == {"evaluations": 1}
    reads = s.calls.observe
    s.clock.advance(4.999999)
    s.runner.tick()
    assert s.calls.observe == reads
    assert s.journal.load() == state
    s.clock.advance(0.000001)
    s.runner.tick()
    assert s.calls.observe == reads + 1
    assert s.journal.load().memory.values == {"evaluations": 2}


def test_restart_and_time_jumps_preserve_deadline_without_catchup(scheduled):
    s = scheduled
    s.runner.tick()
    before = s.journal.load()
    resumed = coordinator(Journal(s.journal.path), s.gateway, clock=s.clock)
    s.clock.advance(-10)
    resumed.tick()
    assert s.journal.load() == before
    s.clock.advance(110)
    resumed.tick()
    after = s.journal.load()
    assert after.memory.values == {"evaluations": 2}
    assert after.schedule.next_due_at == START + timedelta(seconds=105)
    resumed.tick()
    assert s.journal.load() == after


def test_clock_rollback_during_observation_does_not_invoke_strategy(scheduled):
    s = scheduled
    s.runner.tick()
    state = s.journal.load()
    original = s.gateway.observe

    def observe():
        s.clock.advance(-1)
        return original()

    s.gateway.observe = observe
    s.clock.advance(5)
    s.runner.tick()
    assert s.journal.load() == state
    assert len(s.calls.decisions) == 1


@pytest.mark.parametrize("failure", ["observation", "worker"])
def test_failed_evaluation_never_advances_schedule(scheduled, monkeypatch, failure):
    s = scheduled
    before = s.journal.load()

    def fail(*args):
        raise RuntimeError("unavailable evaluation")

    if failure == "observation":
        monkeypatch.setattr(s.gateway, "observe", fail)
    else:
        monkeypatch.setattr("almanak.runtime.strategy_decision", fail)
    with pytest.raises(RuntimeError, match="unavailable evaluation"):
        s.runner.tick()
    assert s.journal.load() == before


def test_slow_decision_schedules_from_its_start(scheduled, monkeypatch):
    s = scheduled

    def slow(_definition, context):
        s.clock.advance(7)
        return Decision(memory=context.memory)

    monkeypatch.setattr("almanak.runtime.strategy_decision", slow)
    s.runner.tick()
    assert s.journal.load().schedule.next_due_at == START + timedelta(seconds=5)
    s.runner.tick()
    assert s.journal.load().schedule.last_evaluation.started_at == START + timedelta(seconds=7)


@pytest.mark.parametrize("stage", ["evaluated", "decision_committed"])
@pytest.mark.parametrize("with_intent", [False, True])
def test_crash_around_decision_commit_preserves_atomic_progress(
    scheduled, monkeypatch, stage, with_intent
):
    s = scheduled

    def decide(_definition, context):
        allocation = context.config.allocations[0]
        intent = context.config.supply(allocation.name, allocation.capital) if with_intent else None
        return Decision(memory=Memory(values={"committed": True}), intent=intent)

    def crash(point, event):
        if point == stage:
            raise RuntimeError("interrupted")

    monkeypatch.setattr("almanak.runtime.strategy_decision", decide)
    s.runner.fault = crash
    with pytest.raises(RuntimeError, match="interrupted"):
        s.runner.tick()
    state = s.journal.load()
    committed = stage == "decision_committed"
    assert bool(state.memory.values) == committed
    assert (state.schedule.last_evaluation is not None) == committed
    assert (state.decision is not None) == (committed and with_intent)
    resumed = coordinator(Journal(s.journal.path), s.gateway, clock=s.clock)
    resumed.tick()
    after = s.journal.load()
    assert after.memory.values == {"committed": True}
    assert after.schedule.last_evaluation.started_at == START
    if committed and with_intent:
        assert after.plan.decision == state.decision


def test_lost_commit_acknowledgement_does_not_repeat_memory(scheduled, monkeypatch):
    s = scheduled
    original = s.journal.update

    def lost_ack(change):
        original(change)
        raise RuntimeError("acknowledgement lost")

    monkeypatch.setattr(s.journal, "update", lost_ack)
    with pytest.raises(RuntimeError, match="acknowledgement"):
        s.runner.tick()
    resumed = coordinator(Journal(s.journal.path), s.gateway, clock=s.clock)
    resumed.tick()
    assert len(s.calls.decisions) == 1
    assert resumed.journal.load().memory.values == {"evaluations": 1}


def test_teardown_racing_with_decision_discards_stale_result(scheduled, monkeypatch):
    s = scheduled

    def racing(_definition, context):
        s.journal.teardown()
        allocation = context.config.allocations[0]
        return Decision(
            memory=Memory(values={"stale": True}),
            intent=context.config.supply(allocation.name, allocation.capital),
        )

    monkeypatch.setattr("almanak.runtime.strategy_decision", racing)
    s.runner.tick()
    state = s.journal.load()
    assert state.mode == "teardown"
    assert state.decision is None and state.memory == Memory()
    assert state.schedule == DecisionSchedule()
    assert s.gateway.broadcasts == []


def test_teardown_bypasses_deadline_without_advancing_interval(scheduled):
    s = scheduled
    s.runner.tick()
    schedule = s.journal.load().schedule
    s.journal.teardown()
    assert s.runner.tick()
    state = s.journal.load()
    assert state.mode == "closed" and state.schedule == schedule
    assert isinstance(s.calls.decisions[-1].trigger, TeardownEvent)
    assert s.calls.decisions[-1].teardown


def test_approval_continues_same_intent_with_fresh_observations(scheduled, monkeypatch):
    s = scheduled
    monkeypatch.setattr("almanak.runtime.strategy_decision", strategy_decision)
    holding = tick_until(s.runner, lambda state: bool(state.memory.values.get("supplied_at")))
    approval, supply = holding.outcomes
    assert approval.decision == supply.decision
    assert not approval.strategy_result().complete and supply.strategy_result().complete
    assert supply.before == approval.after
    assert approval.decision.observations[0].allowance == 0
    assert supply.before.allowance > 0
    assert holding.schedule.last_evaluation.started_at == START
    assert s.clock() < holding.schedule.next_due_at
    assert holding.decision is None
    reads = s.calls.observe
    s.runner.tick()
    assert s.calls.observe == reads
    assert s.journal.load() == holding


def test_synthetic_withdrawal_residual_continues_without_decision(scheduled, recorded):
    from almanak.connectors.aave_v4.operations import RevokeOperation
    from almanak.models import AssetAmount
    from almanak.runtime import commit_outcome
    from tests.test_runtime_recovery import pending_for

    s = scheduled
    original = recorded.outcomes[-1]
    withdrawal = original.model_copy(
        update={
            "before": original.before.model_copy(update={"allowance": 1}),
            "after": original.after.model_copy(update={"allowance": 1}),
        }
    )
    pending = pending_for(recorded.config, withdrawal)
    before = recorded.model_copy(
        update={
            "mode": "teardown",
            "pending": pending,
            "decision": pending.decision,
            "outcomes": recorded.outcomes[:-1],
            "delivered": len(recorded.outcomes) - 1,
            "owned": (recorded.owned[0].model_copy(update={"shares": withdrawal.shares}),),
            "closure": (),
        }
    )
    s.journal.update(lambda _: commit_outcome(before, pending, withdrawal))
    revoke = withdrawal.model_copy(
        update={
            "operation": RevokeOperation(
                target=withdrawal.operation.target,
                amount=AssetAmount(asset=withdrawal.operation.target.asset, amount=0),
            ),
            "before": withdrawal.after,
            "after": withdrawal.after.model_copy(update={"allowance": 0}),
            "assets": 0,
            "shares": 0,
        }
    )
    s.gateway.recorded = recorded.model_copy(update={"outcomes": (*recorded.outcomes, revoke)})
    s.gateway.confirmed[withdrawal.tx_hash] = withdrawal
    assert s.journal.load().decision == pending.decision
    s.runner.tick()
    assert s.journal.load().delivered == len(recorded.outcomes)
    s.runner.tick()
    plan = s.journal.load().plan
    assert isinstance(plan.operation, RevokeOperation)
    assert plan.decision == pending.decision and plan.before == withdrawal.after
    assert s.calls.decisions == []


def test_deadline_inconsistent_with_configuration_cannot_be_saved(scheduled):
    s = scheduled
    s.runner.tick()
    state = s.journal.load()
    schedule = state.schedule.model_copy(update={"next_due_at": START})
    with pytest.raises(ValidationError, match="configured interval"):
        s.journal.update(lambda current: current.model_copy(update={"schedule": schedule}))
    assert s.journal.load() == state


def schedule_service(s):
    return GatewayService(s.journal.load().config, s.journal.path, s.gateway, lease_seconds=2)


def test_gateway_accepts_atomic_schedule_progress_and_refuses_reset(scheduled):
    s = scheduled
    before = s.journal.load()
    s.runner.tick()
    after = s.journal.load()
    service = schedule_service(s)
    service.validate_update(before, after)
    with pytest.raises(ValueError, match="eligible new decision"):
        service.validate_update(after, after.model_copy(update={"schedule": DecisionSchedule()}))


def test_gateway_refuses_schedule_advancement_during_execution(scheduled, monkeypatch):
    s = scheduled
    monkeypatch.setattr("almanak.runtime.strategy_decision", strategy_decision)
    s.runner.tick()
    state = s.journal.load()
    schedule = state.schedule.after(TRIGGER, state.schedule.event(state.schedule.next_due_at))
    with pytest.raises(ValueError, match="eligible new decision"):
        schedule_service(s).validate_update(state, state.model_copy(update={"schedule": schedule}))


def test_gateway_requires_decision_and_event_to_commit_together(scheduled, monkeypatch):
    s = scheduled
    before = s.journal.load()
    monkeypatch.setattr("almanak.runtime.strategy_decision", strategy_decision)
    s.runner.tick()
    after = s.journal.load()
    service = schedule_service(s)
    service.validate_update(before, after)
    unchanged = after.model_copy(update={"schedule": before.schedule})
    with pytest.raises(ValueError, match="atomic scheduling"):
        service.validate_update(before, unchanged)
    changed = after.decision.model_copy(update={"trigger": TeardownEvent(started_at=START)})
    with pytest.raises(ValueError, match="retain its interval event"):
        service.validate_update(before, after.model_copy(update={"decision": changed}))


def test_gateway_rejects_incorrect_next_deadline(scheduled):
    s = scheduled
    before = s.journal.load()
    s.runner.tick()
    after = s.journal.load()
    invalid = after.schedule.model_copy(update={"next_due_at": START})
    with pytest.raises(ValueError, match="cannot reset"):
        schedule_service(s).validate_update(before, after.model_copy(update={"schedule": invalid}))


def test_gateway_cannot_bypass_cadence_with_teardown_reason(scheduled, monkeypatch):
    s = scheduled
    before = s.journal.load()
    monkeypatch.setattr("almanak.runtime.strategy_decision", strategy_decision)
    s.runner.tick()
    record = s.journal.load().decision.model_copy(
        update={"trigger": TeardownEvent(started_at=START)}
    )
    after = before.model_copy(update={"decision": record})
    with pytest.raises(ValueError, match="teardown request"):
        schedule_service(s).validate_update(before, after)
    schedule_service(s).validate_update(
        before.model_copy(update={"mode": "teardown"}),
        after.model_copy(update={"mode": "teardown"}),
    )


def test_gateway_preserves_active_decision_evidence(scheduled, monkeypatch):
    s = scheduled
    monkeypatch.setattr("almanak.runtime.strategy_decision", strategy_decision)
    s.runner.tick()
    before = s.journal.load()
    changed = before.decision.model_copy(update={"identity": "replacement"})
    with pytest.raises(ValueError, match="active decision"):
        schedule_service(s).validate_update(before, before.model_copy(update={"decision": changed}))


def test_gateway_blocks_continuation_before_outcome_delivery(scheduled, monkeypatch):
    from almanak.gateway.service import Request

    s = scheduled
    monkeypatch.setattr("almanak.runtime.strategy_decision", strategy_decision)
    state = tick_until(s.runner, lambda state: bool(state.outcomes))
    assert state.decision is not None and state.delivered == 0
    service = schedule_service(s)
    with pytest.raises(ValueError, match="deliver recorded outcomes"):
        service.prepare_plan(Request(), state)
    plan = s.gateway.prepare(state, state.decision.intent)
    with pytest.raises(ValueError, match="deliver recorded outcomes"):
        service.validate_new_plan(state, plan)
