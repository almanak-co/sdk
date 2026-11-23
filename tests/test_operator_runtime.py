"""Operator deployments: requests without a strategy on the shared execution path.

Component evidence against captured Aave V4 receipts, without signing or chain
access. It is not fork or process recovery evidence.
"""

import json
from datetime import UTC, datetime

import pytest

from almanak.connectors.aave_v4.state import TransactionPlan
from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import DataError
from almanak.gateway.service import GatewayService
from almanak.operator_deployment import supports_operator
from almanak.operator_runtime import OperatorRuntime
from almanak.state import Journal
from almanak.triggers import DecisionSchedule, IntervalEvent, OperatorEvent
from tests.test_runtime_recovery import CapturedGateway, Interrupted

START = datetime(2026, 9, 5, tzinfo=UTC)
REQUEST = "a1" * 16


class Backend(CapturedGateway):
    """The captured gateway with the adapter surface the service re-verifies against."""

    def snapshot(self, allocation, block, observation):
        return self.verify_snapshot(observation)

    def prepare(self, state, intent, observed=None, attempt_id=None):
        plan = super().prepare(state, intent)
        return plan if attempt_id is None else plan.model_copy(update={"attempt_id": attempt_id})

    def unsigned_plan(self, pending):
        return TransactionPlan.model_validate(
            pending.model_dump(exclude={"raw_transaction", "tx_hash", "family"})
        )

    def verify_signed_admission(self, before, pending):
        # Captured bytes are synthetic; signature checks belong to the fork proofs.
        return True


class GatewayJournal:
    """Commits an update only after the gateway's own validation accepts it."""

    def __init__(self, journal, service):
        self.journal, self.service = journal, service
        self.path = journal.path

    def load(self):
        return self.journal.load()

    def update(self, change):
        before = self.journal.load()
        after = change(before)
        if after != before:
            self.service.validate_update(before, after)
        return self.journal.update(lambda _: after)


class Desk:
    def __init__(self, tmp_path, recorded):
        self.recorded = recorded
        self.store = Journal(tmp_path / "state.sqlite")
        self.store.create(recorded.config, None)
        self.gateway = Backend(self.store, recorded)
        self.service = GatewayService(
            recorded.config, self.store.path, self.gateway, lease_seconds=2, operator_actions=True
        )
        self.journal = GatewayJournal(self.store, self.service)

    def runtime(self, fault=None):
        runtime = object.__new__(OperatorRuntime)
        runtime.connector = connector_for(self.recorded.config)
        runtime.journal = self.journal
        runtime.gateway = self.gateway
        runtime.closed = False
        runtime.clock = lambda: START
        runtime.fault = fault or (lambda *args: None)
        runtime.poll = 0
        return runtime

    def intent(self, kind):
        return next(o.decision.intent for o in self.recorded.outcomes if o.operation.kind == kind)

    def kinds(self):
        return [item.operation.kind for item in self.store.load().outcomes]


@pytest.fixture
def desk(tmp_path, recorded):
    return Desk(tmp_path, recorded)


def crash_at(expected, kind):
    def fault(stage, attempt):
        if stage == expected and getattr(attempt, "operation", None) is not None:
            if attempt.operation.kind == kind:
                raise Interrupted(stage)

    return fault


def test_operator_deployment_retains_no_strategy_and_declared_capabilities(desk, recorded):
    state = desk.store.load()
    codec = connector_for(recorded.config).codec
    assert state.operator and state.strategy is None
    assert state.capabilities == codec.capability.declared(recorded.config)
    wire = state.model_dump(mode="json")
    assert wire["operator"] is True
    # A strategy deployment's checkpoint wire form is unchanged.
    assert "operator" not in recorded.model_dump(mode="json")
    with pytest.raises(ValueError, match="retains no strategy"):
        changed = {**wire, "strategy": recorded.strategy.model_dump(mode="json")}
        type(state).model_validate_json(json.dumps(changed))


def test_supply_withdraw_and_close_without_a_strategy(desk, recorded):
    runtime = desk.runtime()
    record = runtime.submit(desk.intent("supply"), REQUEST)
    admitted = desk.store.load()
    assert admitted.decision == record and admitted.plan is None
    assert record.trigger == OperatorEvent(started_at=START, request=REQUEST)
    assert desk.gateway.broadcasts == []

    plan = runtime.preview()
    assert plan.operation.kind == "approve" and desk.store.load().pending is None
    assert desk.gateway.broadcasts == []

    outcomes = runtime.execute()
    assert [item.operation.kind for item in outcomes] == ["approve", "supply"]
    holding = desk.store.load()
    assert holding.decision is None and holding.delivered == 2
    assert holding.owned[0].shares == desk.gateway.outcome("supply").shares
    assert holding.memory == type(holding.memory)()
    assert holding.schedule == DecisionSchedule()

    runtime.submit(desk.intent("withdraw"))
    assert [item.operation.kind for item in runtime.execute()] == ["withdraw"]
    closed = runtime.finish()
    assert closed.mode == "closed" and closed.closure
    assert closed.owned[0].shares == 0
    assert desk.gateway.broadcasts == [item.tx_hash for item in recorded.outcomes]
    assert [item.model_dump(exclude={"decision"}) for item in closed.outcomes] == [
        item.model_dump(exclude={"decision"}) for item in recorded.outcomes
    ]


def test_a_request_may_be_built_from_the_observations_it_is_admitted_with(desk):
    runtime = desk.runtime()
    seen = []

    def build(state, observed):
        seen.append((state.decision, observed))
        return desk.intent("supply")

    record = runtime.submit(build, REQUEST)
    assert record.intent == desk.intent("supply")
    # Built once, before admission, against the observations the record retains.
    assert seen == [(None, record.observations)]


def test_a_refusal_that_needs_no_observation_makes_no_read(desk):
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"), REQUEST)
    runtime.execute()

    def unread():
        raise AssertionError("refused only after reading the chain")

    desk.gateway.observe = unread
    # Whether the intent is stated or built, an executed request is refused unread.
    for intent in (desk.intent("supply"), lambda state, observed: desk.intent("supply")):
        with pytest.raises(ValueError, match="already executed"):
            runtime.submit(intent, REQUEST)
    desk.store.teardown()
    with pytest.raises(ValueError, match="teardown admits only"):
        runtime.submit(desk.intent("supply"))


def test_a_built_request_is_refused_new_exposure_after_it_is_built(desk):
    runtime = desk.runtime()
    desk.store.teardown()
    with pytest.raises(ValueError, match="teardown admits only"):
        runtime.submit(lambda state, observed: desk.intent("supply"))
    assert desk.store.load().decision is None


def test_declined_preview_leaves_no_effect(desk):
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"), REQUEST)
    assert runtime.preview() is not None
    runtime.discard()
    state = desk.store.load()
    assert (state.decision, state.plan, state.pending, state.outcomes) == (None, None, None, ())
    assert desk.gateway.broadcasts == []
    # Nothing was executed, so the same request may be admitted again.
    runtime.submit(desk.intent("supply"), REQUEST)


def test_unconfirmed_request_is_never_executed_by_a_later_coordinator(desk):
    desk.runtime().submit(desk.intent("supply"), REQUEST)
    later = desk.runtime()
    state = later.recover()
    assert state.decision is not None and state.plan is None
    assert desk.gateway.broadcasts == []
    with pytest.raises(ValueError, match="unfinished"):
        later.submit(desk.intent("supply"))
    with pytest.raises(ValueError, match="unfinished"):
        later.finish()


@pytest.mark.parametrize("stage", ["signed", "prepared", "broadcast", "receipt", "commit"])
def test_interrupted_signed_step_is_resolved_once_without_a_new_step(desk, stage):
    interrupted = desk.runtime(crash_at(stage, "approve"))
    interrupted.submit(desk.intent("supply"), REQUEST)
    with pytest.raises(Interrupted, match=stage):
        interrupted.execute()

    resumed = desk.runtime()
    state = resumed.recover()
    approve = desk.gateway.outcome("approve").tx_hash
    if stage == "signed":
        # Signed bytes were never recorded, so nothing was broadcast or retained.
        assert state.pending is None and desk.gateway.broadcasts == []
    else:
        assert desk.kinds() == ["approve"] and desk.gateway.broadcasts == [approve]
    # Recovery neither plans nor signs the supply the operator has not resumed.
    assert state.decision is not None and state.pending is None

    resumed.execute()
    assert desk.kinds() == ["approve", "supply"]
    assert desk.gateway.broadcasts == [approve, desk.gateway.outcome("supply").tx_hash]


def test_executed_request_identity_cannot_be_repeated(desk):
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"), REQUEST)
    runtime.execute()
    with pytest.raises(ValueError, match="already executed"):
        runtime.submit(desk.intent("withdraw"), REQUEST)


def test_a_request_has_no_capability_amount_ceiling(desk):
    # Scope-only authority (blueprint 27): capabilities carry no amount, so admission
    # takes any supply size; the gateway's balance check bounds it at planning.
    supply = desk.intent("supply")
    above = supply.amount.model_copy(update={"amount": supply.amount.amount * 1000})
    desk.runtime().submit(supply.model_copy(update={"amount": above}), REQUEST)
    assert desk.store.load().decision.intent.amount == above


def test_execute_requires_an_admitted_request(desk):
    runtime = desk.runtime()
    runtime.recover()
    with pytest.raises(ValueError, match="no operator request"):
        runtime.execute()
    with pytest.raises(ValueError, match="preview requires"):
        runtime.preview()


def admitted(desk):
    runtime = desk.runtime()
    before = runtime.recover()
    runtime.submit(desk.intent("supply"), REQUEST)
    return before, desk.store.load()


def test_gateway_refuses_operator_requests_on_a_strategy_deployment(
    tmp_path, recorded, reference_strategy, desk
):
    before, after = admitted(desk)
    journal = Journal(tmp_path / "strategy.sqlite")
    journal.create(recorded.config, reference_strategy)
    service = GatewayService(recorded.config, journal.path, desk.gateway, lease_seconds=2)
    strategy = journal.load().model_copy(update={"initialized": True, "baseline": before.baseline})
    with pytest.raises(ValueError, match="admit only each other"):
        service.validate_update(strategy, strategy.model_copy(update={"decision": after.decision}))


def test_gateway_refuses_scheduled_decisions_on_an_operator_deployment(desk):
    before, after = admitted(desk)
    event = IntervalEvent(started_at=START, due_at=START)
    interval = after.decision.model_copy(update={"trigger": event})
    with pytest.raises(ValueError, match="admit only each other"):
        desk.service.validate_update(before, before.model_copy(update={"decision": interval}))
    schedule = DecisionSchedule().after(before.config.decision_trigger, event)
    with pytest.raises(ValueError, match="no decision schedule"):
        desk.service.validate_update(before, before.model_copy(update={"schedule": schedule}))


def test_gateway_refuses_memory_and_kind_changes(desk, recorded):
    before, _ = admitted(desk)
    with pytest.raises(ValueError, match="no strategy memory"):
        desk.service.validate_update(before, before.model_copy(update={"memory": recorded.memory}))
    with pytest.raises(ValueError, match="immutable deployment identity"):
        desk.service.validate_update(before, before.model_copy(update={"operator": False}))


def test_gateway_admits_requests_only_when_idle_and_not_increasing_teardown(desk):
    before, after = admitted(desk)
    uninitialized = before.model_copy(update={"initialized": False, "baseline": ()})
    with pytest.raises(ValueError, match="idle initialized"):
        desk.service.validate_operator_admission(uninitialized, after)
    teardown = before.model_copy(update={"mode": "teardown"})
    with pytest.raises(ValueError, match="reduce exposure"):
        desk.service.validate_operator_admission(teardown, after)


def with_outcome(desk, before, **changes):
    outcome = desk.recorded.outcomes[0].model_copy(update=changes)
    return before.model_copy(update={"outcomes": (outcome,), "delivered": 1})


def test_unexplained_movements_block_new_exposure_in_gateway_and_runtime(desk):
    before, after = admitted(desk)
    tainted = with_outcome(desk, before, accounting_issues=("unexplained transfer",))
    with pytest.raises(ValueError, match=r"issue \(unexplained transfer\) blocks new exposure"):
        desk.service.validate_operator_admission(tainted, after)
    withdraw = after.decision.model_copy(update={"intent": desk.intent("withdraw")})
    desk.service.validate_operator_admission(
        tainted, after.model_copy(update={"decision": withdraw})
    )
    desk.store.update(lambda _: tainted)
    with pytest.raises(DataError, match=r"issue \(unexplained transfer\) blocks new exposure"):
        desk.runtime().submit(desk.intent("supply"), "b2" * 16)


def test_reverted_step_leaves_the_next_request_to_the_operator(desk):
    before, after = admitted(desk)
    reverted = with_outcome(desk, before, success=False)
    desk.service.validate_operator_admission(reverted, after)


def test_gateway_mode_must_match_the_durable_deployment(
    tmp_path, recorded, reference_strategy, desk
):
    strategy_gateway = GatewayService.__new__(GatewayService)
    strategy_gateway.__dict__.update(desk.service.__dict__, operator_actions=False)
    with pytest.raises(ValueError, match="operator mode differs"):
        strategy_gateway.check(desk.store.load())
    journal = Journal(tmp_path / "strategy.sqlite")
    journal.create(recorded.config, reference_strategy)
    with pytest.raises(ValueError, match="operator mode differs"):
        GatewayService(
            recorded.config, journal.path, desk.gateway, lease_seconds=2, operator_actions=True
        )


def test_create_shape_follows_the_gateway_mode(desk):
    assert desk.service.request_fields("create") == set()
    assert desk.service.request_fields("state") == set()
    strategy_gateway = GatewayService.__new__(GatewayService)
    strategy_gateway.operator_actions = False
    assert strategy_gateway.request_fields("create") == {"strategy"}


def test_connectors_without_the_marker_refuse_operator_deployments(tmp_path):
    from almanak.connectors.aave_v4.state import Checkpoint as AaveCheckpoint
    from almanak.connectors.gmx_v2.state import Checkpoint as GmxCheckpoint

    assert supports_operator(AaveCheckpoint)
    assert not supports_operator(GmxCheckpoint)


def superseded_evidence(pending):
    from almanak.operator_deployment import ConsumedNonce

    return ConsumedNonce(
        operation=pending.operation.kind,
        transaction=pending.tx_hash,
        submitter=pending.call.fund.address,
        nonce=pending.nonce,
        consumed_by="0x" + "bb" * 32,
        block=pending.before.position.reserve.block,
    )


def nonce_taken(desk, proof):
    """Stop a supply with its approval signed, then let another transaction take its nonce."""
    interrupted = desk.runtime(crash_at("prepared", "approve"))
    interrupted.submit(desk.intent("supply"), REQUEST)
    with pytest.raises(Interrupted):
        interrupted.execute()
    asked = []

    def occupied(pending):
        raise DataError("nonce_conflict", "unknown execution occupies the recorded nonce")

    def superseded(pending):
        asked.append(pending)
        return proof(pending)

    desk.gateway.broadcast, desk.gateway.superseded = occupied, superseded
    return desk.store.load().pending, asked


def test_a_signed_step_whose_nonce_a_final_transaction_took_is_dropped_with_its_evidence(desk):
    pending, asked = nonce_taken(desk, superseded_evidence)
    state = desk.runtime().recover()
    # The step and its request are gone, nothing was sent or planned in their place, and
    # the gateway proved the drop again when it was recorded with its proof.
    assert (state.pending, state.decision, state.plan, desk.kinds()) == (None, None, None, [])
    assert state.superseded == (superseded_evidence(pending),)
    assert asked == [pending, pending] and desk.gateway.broadcasts == []
    # The proof is in the checkpoint, so another process reads the same record.
    assert desk.store.load().superseded == state.superseded


def test_a_stop_requested_after_the_drop_does_not_end_the_recovery(desk):
    """Nothing is left to do once the step is dropped, so the caller still gets the
    state to report instead of an interrupt that would remove the record unreported."""
    pending, _ = nonce_taken(desk, superseded_evidence)

    def stop():
        if desk.store.load().pending is None:
            raise KeyboardInterrupt

    state = desk.runtime().recover(stop)
    assert (state.pending, state.superseded) == (None, (superseded_evidence(pending),))


def test_a_nonce_that_is_only_occupied_keeps_the_signed_step(desk):
    def unproven(pending):
        raise DataError("nonce_conflict", "not consumed at the finalized block")

    pending, asked = nonce_taken(desk, unproven)
    with pytest.raises(DataError, match="not consumed at the finalized block"):
        desk.runtime().recover()
    state = desk.store.load()
    assert (state.pending, state.superseded, asked) == (pending, (), [pending])


# What the proof answers about a step's own transaction that was just mined: above the
# finalized block, inside it, on a network that states no finality margin, and served
# by the proof's own receipt read.
OWN_STEP_REFUSALS = [
    ("nonce_conflict", "is not consumed at the finalized block"),
    ("inconsistent", "the recorded transaction is in block 200 but has no receipt"),
    ("nonce_conflict", "states no finality margin"),
    ("nonce_conflict", "a mined transaction is resolved only by its recorded outcome"),
]


def lagging_reads(desk, misses, refusal):
    """Serve each mined step's receipt and transaction only after `misses` ticks.

    The account's count already includes the step, as on an endpoint whose nodes do not
    all hold a block that was just mined, so asking for the broadcast again answers
    `nonce_conflict` and the proof refuses with `refusal`.
    """
    receipt, broadcast, missed, asked = desk.gateway.receipt, desk.gateway.broadcast, {}, []

    def lagging_receipt(pending):
        if pending.tx_hash not in desk.gateway.confirmed:
            return None
        missed[pending.tx_hash] = missed.get(pending.tx_hash, 0) + 1
        return receipt(pending) if missed[pending.tx_hash] > misses else None

    def lagging_broadcast(pending):
        if pending.tx_hash not in desk.gateway.confirmed:
            return broadcast(pending)
        raise DataError("nonce_conflict", "unknown execution occupies the recorded nonce")

    def superseded(pending):
        asked.append(pending.tx_hash)
        raise DataError(*refusal)

    desk.gateway.receipt, desk.gateway.broadcast = lagging_receipt, lagging_broadcast
    desk.gateway.superseded = superseded
    return asked


@pytest.mark.parametrize("refusal", OWN_STEP_REFUSALS)
def test_execution_waits_for_a_mined_step_the_endpoint_does_not_serve_yet(desk, refusal):
    asked = lagging_reads(desk, 3, refusal)
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"), REQUEST)
    outcomes = runtime.execute()
    # Both steps end with their outcomes, each sent once, and nothing is dropped.
    assert [item.operation.kind for item in outcomes] == ["approve", "supply"]
    sent = [item.tx_hash for item in outcomes]
    assert desk.gateway.broadcasts == sent and asked == [hash_ for hash_ in sent for _ in "123"]
    state = desk.store.load()
    assert (state.pending, state.decision, state.superseded) == (None, None, ())
    assert runtime.refused is None


def slow_proof(desk, refusal, seconds=25.0):
    """A proof that takes `seconds` on the returned clock and then refuses."""
    clock = [0.0]

    def unproven(pending):
        clock[0] += seconds
        raise DataError(*refusal)

    pending, asked = nonce_taken(desk, unproven)
    runtime = desk.runtime()
    runtime.monotonic = lambda: clock[0]
    return runtime, clock, pending, asked


@pytest.mark.parametrize("refusal", [*OWN_STEP_REFUSALS, ("inconsistent", "try again")])
def test_execution_stops_waiting_for_a_proof_after_its_bound(desk, refusal):
    """A nonce another transaction holds without a proof ends the command as before:
    the proof's refusal, with the signed step still recorded."""
    runtime, _, pending, asked = slow_proof(desk, refusal)
    assert runtime.proof_wait == 60.0
    with pytest.raises(DataError, match=refusal[1]) as ended:
        runtime.execute()
    state = desk.store.load()
    # The ticks that ended in the refusal took 25, 50 and then 75 seconds.
    assert ended.value.kind == refusal[0]
    assert (state.pending, state.superseded, asked) == (pending, (), [pending] * 3)
    assert desk.gateway.broadcasts == []


def test_held_transient_failures_between_proofs_do_not_spend_the_bound(desk):
    """A throttled read is held without limit, as before, and its time is not the
    proof's. Each refused proof still spends its own tick, so the wait ends."""
    runtime, clock, pending, asked = slow_proof(desk, OWN_STEP_REFUSALS[0])
    reads = []

    def throttled_every_other_read(pending):
        reads.append(pending)
        if len(reads) % 2:
            clock[0] += 100
            raise DataError("rpc", "throttled", code=429, transient=True)

    desk.gateway.receipt = throttled_every_other_read
    with pytest.raises(DataError, match="not consumed at the finalized block"):
        runtime.execute()
    assert (len(reads), asked, clock[0]) == (6, [pending] * 3, 375.0)
    assert desk.store.load().pending == pending


def test_execution_drops_the_step_when_the_proof_arrives_within_the_wait(desk):
    """Only the full proof drops the step, and the wait does not stand in its way."""
    answers = iter([None, None])

    def later(pending):
        if next(answers, pending) is None:
            raise DataError("nonce_conflict", "is not consumed at the finalized block")
        return superseded_evidence(pending)

    pending, _ = nonce_taken(desk, later)
    assert desk.runtime().execute() == ()
    state = desk.store.load()
    assert (state.pending, state.superseded) == (None, (superseded_evidence(pending),))
    assert desk.gateway.broadcasts == []


def test_a_refusal_outside_the_proof_is_not_waited_for(desk):
    pending, asked = nonce_taken(desk, superseded_evidence)

    def unsendable(pending):
        raise DataError("signed_attempt_unsendable", "current validation refuses it")

    desk.gateway.broadcast = unsendable
    with pytest.raises(DataError, match="current validation refuses it"):
        desk.runtime().execute()
    assert (desk.store.load().pending, asked) == (pending, [])


@pytest.mark.parametrize("refusal", OWN_STEP_REFUSALS)
def test_recovery_does_not_wait_for_a_refused_proof(desk, refusal):
    """One recovery asks once and the caller asks again when it chooses to."""

    def unproven(pending):
        raise DataError(*refusal)

    pending, asked = nonce_taken(desk, unproven)
    with pytest.raises(DataError, match=refusal[1]) as ended:
        desk.runtime().recover()
    state = desk.store.load()
    assert (ended.value.kind, ended.value.details) == (refusal[0], None)
    assert (state.pending, state.superseded, asked) == (pending, (), [pending])


def test_a_proof_that_names_another_step_is_refused(desk):
    def other(pending):
        return superseded_evidence(pending).model_copy(update={"transaction": "0x" + "cd" * 32})

    pending, _ = nonce_taken(desk, other)
    with pytest.raises(DataError, match="names another signed step") as refusal:
        desk.runtime().recover()
    assert (refusal.value.kind, desk.store.load().pending) == ("inconsistent", pending)


def test_any_other_refusal_of_a_signed_step_is_not_a_drop(desk):
    pending, asked = nonce_taken(desk, superseded_evidence)

    def unsendable(pending):
        raise DataError("signed_attempt_unsendable", "current validation refuses it")

    desk.gateway.broadcast = unsendable
    with pytest.raises(DataError, match="current validation refuses it"):
        desk.runtime().recover()
    assert (desk.store.load().pending, asked) == (pending, [])


def test_a_strategy_coordinator_never_drops_a_step_for_its_nonce(desk):
    from tests.test_runtime_recovery import coordinator

    pending, asked = nonce_taken(desk, superseded_evidence)
    strategy = coordinator(desk.store, desk.gateway)
    with pytest.raises(DataError, match="occupies the recorded nonce"):
        strategy.resolve_pending(pending)
    assert (desk.store.load().pending, asked) == (pending, [])
