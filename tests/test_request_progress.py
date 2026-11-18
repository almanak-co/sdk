"""Synthetic shared request contract checks; no GMX or provider proof is claimed."""

from types import SimpleNamespace
from typing import Any

import pytest

from almanak import request_runtime
from almanak.gateway import admission
from almanak.gateway import requests as gateway_requests
from almanak.gateway.service import FIELDS, GatewayService
from almanak.models import Model
from almanak.runtime import Runtime


class Event(Model):
    decision: str = "admitted"
    terminal: bool = True

    def strategy_result(self):
        return SimpleNamespace(complete=self.terminal)


class State(Model):
    config: str = "synthetic"
    decision: str | None = "admitted"
    pending: Any = None
    plan: Any = None
    delivered: int = 0
    outcomes: tuple[Event, ...] = ()
    history: tuple[str, ...] = ("accepted",)
    owned: tuple[str, ...] = ()
    memory: str = "unchanged"
    mode: str = "running"
    integrity_error: str | None = None
    initialized: bool = True
    baseline: tuple = ()
    closure: tuple = ()


@pytest.fixture
def contract(monkeypatch):
    event = Event()
    binding = SimpleNamespace(
        unresolved=lambda s: s.history[-1] != "settled",
        reconcile=lambda b, s: event,
        is_cancellation=lambda s, operation: operation == "cancel-retained",
    )

    def ownership(state, outcome=None):
        if outcome is None:
            return {"history": state.history, "owned": state.owned}
        status = "settled" if outcome.terminal else "frozen"
        return {"history": (*state.history, status), "owned": state.owned}

    connector = SimpleNamespace(
        requests=binding, external_effects=None, lifecycle=SimpleNamespace(ownership=ownership)
    )
    for module in (request_runtime, gateway_requests):
        monkeypatch.setattr(module, "connector_for", lambda _: connector)
    monkeypatch.setattr("almanak.connectors.composition.connector_for", lambda _: connector)
    backend = SimpleNamespace(audit_history=lambda _: None)
    return SimpleNamespace(binding=binding, connector=connector, backend=backend, event=event)


def test_terminal_commit_delivers_once_and_retains_history(contract):
    before = State()
    after = request_runtime.commit_request(before, before, contract.event)
    assert after.decision is None
    assert after.history == ("accepted", "settled")
    assert after.delivered == 0
    assert len(after.outcomes) == 1
    assert gateway_requests.verified_commit(contract.backend, before, after) == after
    with pytest.raises(RuntimeError, match="changed"):
        request_runtime.commit_request(after, before, contract.event)
    with pytest.raises(ValueError, match="unresolved"):
        request_runtime.commit_request(after, after, contract.event)


def test_freeze_keeps_decision_and_plan_discard_keeps_request(contract):
    before = State()
    after = request_runtime.commit_request(before, before, Event(terminal=False))
    assert after.decision == before.decision
    assert after.history == ("accepted", "frozen")
    assert request_runtime.discard_plan(after.model_copy(update={"plan": "unsigned"})).decision


@pytest.mark.parametrize(
    "change",
    [
        {"history": ("forged",)},
        {"owned": ("forged",)},
        {"memory": "forged"},
        {"delivered": 1},
        {"decision": "admitted"},
        {"plan": "forged"},
        {"outcomes": (Event(terminal=False),)},
    ],
)
def test_gateway_independently_rejects_forged_commit(contract, change):
    before = State()
    after = request_runtime.commit_request(before, before, contract.event).model_copy(update=change)
    with pytest.raises(ValueError, match="independently"):
        admission.validate_outcome(contract.backend, before, after)
    service = object.__new__(GatewayService)
    service.connector = contract.connector
    service.backend = contract.backend
    with pytest.raises(ValueError, match="independently"):
        service.validate_owned_evidence(before, after)


def test_request_history_cannot_change_without_outcome(contract):
    before = State()
    with pytest.raises(ValueError, match="ownership"):
        admission.validate_execution_update(
            contract.backend, before, before.model_copy(update={"history": ()})
        )


@pytest.mark.parametrize("mode", ["running", "teardown"])
def test_idle_poll_does_not_call_strategy(contract, mode):
    state = State(mode=mode)
    calls = []
    runtime = object.__new__(Runtime)
    runtime.gateway = SimpleNamespace(
        request_reconcile=lambda: None,
        request_cancel_plan=lambda: calls.append("cancel-plan"),
    )
    runtime.decide = lambda _: pytest.fail("request polling cannot decide")
    runtime.prepare_decision = lambda _: pytest.fail("request polling cannot repeat creation")
    assert runtime.tick_state(state) is False
    assert calls == (["cancel-plan"] if mode == "teardown" else [])


def test_cancellation_race_reconciles_before_planning(contract):
    state = State(mode="teardown")
    contract.binding.cancellation_plan = lambda *a, **k: pytest.fail(
        "settled event must commit first"
    )
    assert (
        gateway_requests.dispatch(contract.backend, state, "request-cancel-plan", "attempt") is None
    )


def test_cancellation_requires_teardown_and_exact_retained_operation(contract):
    contract.binding.reconcile = lambda *a: None
    contract.binding.cancellation_plan = lambda *a, **k: SimpleNamespace(
        decision="admitted", operation="cancel-foreign"
    )
    with pytest.raises(ValueError, match="teardown"):
        gateway_requests.dispatch(contract.backend, State(), "request-cancel-plan", "attempt")
    with pytest.raises(ValueError, match="retained|admitted"):
        gateway_requests.dispatch(
            contract.backend, State(mode="teardown"), "request-cancel-plan", "attempt"
        )


def test_request_endpoints_accept_no_caller_state_or_request_key():
    assert FIELDS["request-reconcile"] == {"coordinator", "deployment_id"}
    assert FIELDS["request-cancel-plan"] == {"coordinator", "deployment_id", "attempt_id"}


def test_cancellation_admission_reuses_verified_snapshot(contract):
    state = State(mode="teardown")
    plan = SimpleNamespace(decision="admitted", operation="cancel-retained", attempt_id="attempt")
    observed = object()
    seen = []
    contract.binding.reconcile = lambda *a: None

    def prepare(backend, current, attempt_id, *, reference=None):
        seen.append((backend, current, attempt_id, reference))
        return plan

    contract.binding.cancellation_plan = prepare
    gateway_requests.validate_cancellation_plan(contract.backend, state, plan, observed)
    assert seen == [(contract.backend, state, "attempt", observed)]
    contract.binding.reconcile = lambda *a: Event()
    with pytest.raises(ValueError, match="event"):
        gateway_requests.validate_cancellation_plan(contract.backend, state, plan, observed)


def test_unresolved_decision_cannot_be_erased_without_evidence(contract):
    before = State()
    with pytest.raises(ValueError, match="retain"):
        admission.validate_execution_update(
            contract.backend, before, before.model_copy(update={"decision": None})
        )


@pytest.mark.parametrize("stale", [False, True])
def test_unsigned_cancellation_discard_preserves_request(contract, stale):
    from almanak.gateway.ethereum import DataError

    state = State(
        mode="teardown",
        plan=SimpleNamespace(
            decision=SimpleNamespace(intent="open"),
            operation="cancel-retained" if stale else "open",
        ),
    )
    runtime = object.__new__(Runtime)
    runtime.connector = contract.connector
    runtime.connector.lifecycle.increases_exposure = lambda _: True
    saved = []
    runtime.journal = SimpleNamespace(update=lambda change: saved.append(change(state)))

    def sign():
        if not stale:
            pytest.fail("ordinary entry cannot sign during teardown")
        raise DataError("stale_plan", "keeper won")

    runtime.gateway = SimpleNamespace(sign=sign)
    assert runtime.admit_signed_plan(state) is False
    assert len(saved) == 1
    assert saved[0].decision == state.decision
    assert saved[0].history == state.history
    assert saved[0].plan is None
