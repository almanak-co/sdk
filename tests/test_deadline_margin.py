"""Signing refuses a deadline window mostly spent, and a deadline revert is named."""

from types import SimpleNamespace

import pytest

from almanak import runtime as runtime_module
from almanak.connectors.v3_pool_operations import Approval
from almanak.gateway.ethereum import DataError
from almanak.runtime import REPLAN_ATTEMPTS, REPLAN_BACKOFF_SECONDS, ReplansExhausted
from tests.test_expired_attempt_release import (
    COMPOSED,
    MemoryJournal,
    composed_pending_state,
    v4_gateway,
    v4_pending_state,
)
from tests.test_production import production_attempt as production_attempt
from tests.test_production import production_recorded as production_recorded
from tests.test_runtime_recovery import coordinator
from tests.test_simulation_reverts import events, planned


def window_of(state, operation):
    return state.config.allocation(operation.allocation).deadline_seconds


def at(head, timestamp):
    return head.model_copy(update={"timestamp": timestamp})


def margin_cases():
    state, pending, head = v4_pending_state(0)
    yield "uniswap_v4", state, pending, head
    for connector in sorted(COMPOSED):
        _, state, pending, head = composed_pending_state(connector, 0)
        yield f"composed-{connector}", state, pending, head


@pytest.mark.parametrize("case", list(margin_cases()), ids=lambda case: case[0])
def test_signing_needs_half_the_deadline_window_left(case):
    _, state, pending, head = case
    gateway = v4_gateway(state, pending, head)
    window = window_of(state, pending.operation)
    deadline = pending.operation.deadline
    half = (window + 1) // 2
    gateway.require_deadline_margin(pending, at(head, deadline - window))
    gateway.require_deadline_margin(pending, at(head, deadline - half))
    with pytest.raises(DataError) as error:
        gateway.require_deadline_margin(pending, at(head, deadline - half + 1))
    assert error.value.kind == "deadline_margin"
    assert f"only {half - 1}s of the {window}s" in str(error.value)


def test_an_approval_carries_no_deadline_and_is_never_refused():
    state, pending, head = v4_pending_state(0)
    gateway = v4_gateway(state, pending, head)
    approval = SimpleNamespace(operation=Approval.model_construct(allocation="unknown"))
    gateway.require_deadline_margin(approval, at(head, 2**63))


def test_sign_refuses_before_any_signature_is_made():
    state, pending, head = v4_pending_state(0)
    window = window_of(state, pending.operation)
    gateway = v4_gateway(state, pending, at(head, pending.operation.deadline - window // 4))
    signed = []
    gateway.authorize_signing = lambda state: ("authorized",)
    gateway._sign = lambda *args: signed.append(args)
    with pytest.raises(DataError, match="planning afresh") as error:
        gateway.sign(state.model_copy(update={"plan": pending}))
    assert error.value.kind == "deadline_margin" and signed == []


def test_a_revert_mined_past_the_deadline_reports_the_timing():
    state, pending, head = v4_pending_state(0)
    gateway = v4_gateway(state, pending, head)
    deadline = pending.operation.deadline
    # The router still accepts a block at its deadline, so only a later block names it.
    assert gateway.deadline_revert(pending.operation, at(head, deadline)) is None
    reason = gateway.deadline_revert(pending.operation, at(head, deadline + 37))
    assert reason == (
        f"mined past protocol deadline: block timestamp {deadline + 37} is 37s after"
        f" deadline {deadline}"
    )
    approval = Approval.model_construct()
    assert gateway.deadline_revert(approval, at(head, 2**63)) is None


def test_a_deadline_margin_refusal_is_replanned_then_stops_at_the_cap(production_attempt, capsys):
    state, pending, _ = production_attempt
    refusal = "only 27s of the 120s protocol deadline window remain at signing"

    def sign():
        raise DataError("deadline_margin", refusal)

    journal = MemoryJournal(planned(state, pending))
    runtime = coordinator(journal, SimpleNamespace(sign=sign))
    for attempt in range(1, REPLAN_ATTEMPTS):
        journal.state = planned(state, pending)
        assert runtime.tick() is False
        # Nothing was signed: the plan and its decision are dropped for a fresh deadline.
        assert journal.load().plan is None and journal.load().decision is None
        assert runtime_module.next_delay(runtime, 1, 0) == attempt * REPLAN_BACKOFF_SECONDS
    journal.state = planned(state, pending)
    with pytest.raises(ReplansExhausted, match="27s") as error:
        runtime.tick()
    assert error.value.kind == "deadline_margin"
    assert [(e["kind"], e["stage"]) for e in events(capsys)] == [
        ("deadline_margin", "sign")
    ] * REPLAN_ATTEMPTS
