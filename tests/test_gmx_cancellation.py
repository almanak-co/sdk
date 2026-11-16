"""Runtime cancellation remains bound to the accepted request and original admitted decision."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import request_binding
from almanak.connectors.gmx_v2.operations import CancelOrder
from tests.gmx_native_runtime import cancellation_state, runtime
from tests.retained_fixtures import retained


def test_cancellation_uses_original_admission_and_exact_reader_eligibility(monkeypatch):
    state, cancelled = cancellation_state()
    before = cancelled.before
    backend = SimpleNamespace(reader=SimpleNamespace(pin_block=lambda _: before.block))
    calls = []

    def prepare(current, intent, observed, identity):
        calls.append((current, intent, observed, identity))
        return cancelled.operation

    backend.prepare = prepare
    monkeypatch.setattr(request_binding, "next_evidence", lambda *_: None)
    monkeypatch.setattr(request_binding, "read_pending", lambda *_: before.pending_order)
    monkeypatch.setattr(request_binding, "snapshot", lambda *_, **__: before)
    planned = request_binding.cancellation_plan(backend, state, "retained")
    assert planned == cancelled.operation
    assert calls == [(state, state.decision.intent, before, "retained")]
    assert state.decision.intent.kind == "increase_position"
    assert request_binding.is_cancellation(state, planned)
    assert not request_binding.is_cancellation(state, state.outcomes[0].operation)
    assert not request_binding.is_cancellation(
        state, planned.model_copy(update={"request_key": "0x" + "99" * 32})
    )
    bad = before.model_copy(update={"request": None})
    with pytest.raises(ValueError, match="exact eligible retained"):
        request_binding.cancellation_plan(backend, state, "bad", reference=bad)
    with pytest.raises(ValueError, match="requires teardown"):
        request_binding.cancellation_plan(
            backend, state.model_copy(update={"mode": "running"}), "bad"
        )


def test_keeper_progress_or_disabled_cancellation_never_generates_a_plan(monkeypatch):
    state, cancelled = cancellation_state()
    before = cancelled.before
    backend = SimpleNamespace(reader=SimpleNamespace(pin_block=lambda _: before.block))
    monkeypatch.setattr(request_binding, "next_evidence", lambda *_: object())
    assert request_binding.cancellation_plan(backend, state, "pending") is None
    monkeypatch.setattr(request_binding, "next_evidence", lambda *_: None)
    monkeypatch.setattr(request_binding, "read_pending", lambda *_: None)
    assert request_binding.cancellation_plan(backend, state, "pending") is None
    disabled = before.pending_order.model_copy(update={"cancellation_disabled": True})
    monkeypatch.setattr(request_binding, "read_pending", lambda *_: disabled)
    assert request_binding.cancellation_plan(backend, state, "disabled") is None
    assert not disabled.cancellable
    early = before.pending_order.model_copy(update={"updated_at": before.block.timestamp})
    assert not early.cancellable
    assert isinstance(cancelled.operation, CancelOrder)


def test_cancellation_fixture_assertions_require_exactly_one_create_and_cancel():
    from tests.fork.test_gmx_requests import assert_cancelled

    state = runtime(
        retained("2026-09-08-gmx-request-fixtures"), "final-arbitrum-safe-cancellation-runtime"
    )
    key = state.requests[0].key
    assert_cancelled(state, key)
    with pytest.raises(AssertionError):
        assert_cancelled(state, "0x" + "ab" * 32)
    duplicate = state.model_copy(update={"outcomes": (*state.outcomes, state.outcomes[-1])})
    with pytest.raises(AssertionError):
        assert_cancelled(duplicate, key)
