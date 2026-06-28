"""Tests for the teardown structured decision log (VIB-5478 / TD-20).

The decision log emits ONE auditable structured entry per teardown decision —
enumerate → size → block/repair → verify — through the existing timeline
observability sink. These tests assert:

* the structured record shape for every phase,
* that NO money-shaped key ever lands in the payload (the timeline is a UX/audit
  channel, not an accounting record — PRD-TimelineEvents §6.1),
* ``cycle_id`` correlation (``teardown-{id}``),
* that emission is best-effort and NEVER raises (teardown's inverted failure
  semantics — observability must not block a risk-reducing intent),
* that a representative teardown sequence emits each phase, on BOTH the
  closure-success (``verified``) and closure-failure (``verify_failed``) paths.
"""

from __future__ import annotations

import pytest

from almanak.framework.api.timeline import TimelineEventType
from almanak.framework.observability.context import clear_cycle_id, set_cycle_id
from almanak.framework.teardown.decision_log import (
    TeardownDecisionPhase,
    build_decision_details,
    log_teardown_decision,
)

# The single source of truth for forbidden (money-shaped) timeline keys — reuse
# the producer-side static guard's set so the decision log can never drift from
# the contract it must honour.
from tests.static.test_timeline_payload_keys import FORBIDDEN_KEYS


@pytest.fixture
def captured_events(monkeypatch):
    """Capture decision-log emissions without touching the real timeline sink.

    Monkeypatches the ``add_event`` reference the module imported, so no file is
    written to the repo root and no gateway round-trip is attempted.
    """
    events: list = []
    monkeypatch.setattr(
        "almanak.framework.teardown.decision_log.add_event",
        events.append,
    )
    return events


# ---------------------------------------------------------------------------
# build_decision_details — pure record shape + money-key safety
# ---------------------------------------------------------------------------


def test_details_minimal_shape():
    details = build_decision_details(
        phase=TeardownDecisionPhase.ENUMERATE,
        outcome="enumerated",
    )
    assert details["teardown_decision"] is True
    assert details["decision_phase"] == "ENUMERATE"
    assert details["outcome"] == "enumerated"
    # None-valued optional fields are omitted (Empty ≠ Zero).
    assert "position_count" not in details
    assert "token" not in details
    assert "degraded" not in details


def test_details_omits_none_but_keeps_zero_and_false():
    """Measured zero / explicit False are real values and must be kept."""
    details = build_decision_details(
        phase=TeardownDecisionPhase.VERIFY,
        outcome="verify_failed",
        position_count=2,
        positions_closed=0,
        degraded=False,
        verification_status="FAILED",
    )
    assert details["positions_closed"] == 0  # measured zero kept
    assert details["degraded"] is False  # explicit False kept
    assert details["position_count"] == 2
    assert details["verification_status"] == "FAILED"


def test_details_full_payload():
    details = build_decision_details(
        phase=TeardownDecisionPhase.BLOCK,
        outcome="swap_clamp_skipped",
        teardown_id="td_abc",
        position_count=1,
        intent_count=1,
        positions_closed=0,
        token="WETH",
        reason="untracked_token",
        verification_status=None,
        degraded=True,
    )
    assert details["teardown_id"] == "td_abc"
    assert details["token"] == "WETH"
    assert details["reason"] == "untracked_token"
    assert details["degraded"] is True
    assert "verification_status" not in details  # None omitted


@pytest.mark.parametrize("phase", list(TeardownDecisionPhase))
def test_no_money_shaped_keys_in_any_payload(phase):
    """Every phase, with every field populated, stays money-key clean."""
    details = build_decision_details(
        phase=phase,
        outcome="x",
        teardown_id="td_1",
        position_count=3,
        intent_count=4,
        positions_closed=2,
        token="USDC",
        reason="r",
        verification_status="CHAIN_VERIFIED",
        degraded=True,
    )
    leaked = FORBIDDEN_KEYS & set(details.keys())
    assert not leaked, f"decision payload leaked money-shaped key(s): {leaked}"


# ---------------------------------------------------------------------------
# log_teardown_decision — emission, correlation, robustness
# ---------------------------------------------------------------------------


def test_emits_timeline_event(captured_events):
    log_teardown_decision(
        deployment_id="deployment:abc",
        teardown_id="td_42",
        phase=TeardownDecisionPhase.ENUMERATE,
        outcome="enumerated",
        position_count=2,
        intent_count=3,
    )
    assert len(captured_events) == 1
    event = captured_events[0]
    assert event.event_type == TimelineEventType.CUSTOM
    assert event.deployment_id == "deployment:abc"
    assert event.phase == "TEARDOWN_ENUMERATE"
    assert event.cycle_id == "teardown-td_42"
    assert event.details["decision_phase"] == "ENUMERATE"
    assert event.details["position_count"] == 2
    assert event.details["intent_count"] == 3


def test_cycle_id_already_prefixed_not_doubled(captured_events):
    log_teardown_decision(
        deployment_id="d",
        teardown_id="teardown-td_42",
        phase=TeardownDecisionPhase.SIZE,
        outcome="swap_clamp_applied",
    )
    assert captured_events[0].cycle_id == "teardown-td_42"


def test_cycle_id_falls_back_to_context(captured_events):
    set_cycle_id("teardown-ctx_99")
    try:
        log_teardown_decision(
            deployment_id="d",
            phase=TeardownDecisionPhase.VERIFY,
            outcome="verified",
        )
    finally:
        clear_cycle_id()
    assert captured_events[0].cycle_id == "teardown-ctx_99"


def test_default_description(captured_events):
    log_teardown_decision(
        deployment_id="d",
        phase=TeardownDecisionPhase.REPAIR,
        outcome="hf_safe_unwind_synthesized",
    )
    assert captured_events[0].description == "teardown repair: hf_safe_unwind_synthesized"


def test_emit_never_raises(monkeypatch):
    """A sink failure must be swallowed — observability never blocks teardown."""

    def boom(_event):
        raise RuntimeError("sink down")

    monkeypatch.setattr("almanak.framework.teardown.decision_log.add_event", boom)
    # Must not raise.
    log_teardown_decision(
        deployment_id="d",
        phase=TeardownDecisionPhase.BLOCK,
        outcome="swap_clamp_skipped",
        token="WETH",
        reason="untracked_token",
        degraded=True,
    )


def test_emitted_event_is_money_key_clean(captured_events):
    log_teardown_decision(
        deployment_id="d",
        teardown_id="td_1",
        phase=TeardownDecisionPhase.SIZE,
        outcome="swap_clamp_applied",
        token="WETH",
        reason="clamped_to_tracked_quantity",
        intent_count=1,
    )
    leaked = FORBIDDEN_KEYS & set(captured_events[0].details.keys())
    assert not leaked


# ---------------------------------------------------------------------------
# Representative teardown decision sequence (enumerate→size→block/repair→verify)
# ---------------------------------------------------------------------------


def _emit_representative_sequence(*, all_closed: bool, captured_events: list) -> None:
    """Emit one of each decision phase, as a real teardown would."""
    log_teardown_decision(
        deployment_id="d",
        teardown_id="td_seq",
        phase=TeardownDecisionPhase.ENUMERATE,
        outcome="enumerated",
        position_count=2,
        intent_count=3,
    )
    log_teardown_decision(
        deployment_id="d",
        teardown_id="td_seq",
        phase=TeardownDecisionPhase.SIZE,
        outcome="swap_clamp_applied",
        token="WETH",
        reason="clamped_to_tracked_quantity",
    )
    log_teardown_decision(
        deployment_id="d",
        teardown_id="td_seq",
        phase=TeardownDecisionPhase.REPAIR,
        outcome="hf_safe_unwind_synthesized",
        position_count=1,
    )
    log_teardown_decision(
        deployment_id="d",
        teardown_id="td_seq",
        phase=TeardownDecisionPhase.BLOCK,
        outcome="swap_clamp_skipped",
        token="USDC",
        reason="untracked_token",
        degraded=True,
    )
    log_teardown_decision(
        deployment_id="d",
        teardown_id="td_seq",
        phase=TeardownDecisionPhase.VERIFY,
        outcome="verified" if all_closed else "verify_failed",
        position_count=2,
        positions_closed=2 if all_closed else 1,
        verification_status="CHAIN_VERIFIED" if all_closed else "FAILED",
    )


def test_full_sequence_success_path(captured_events):
    _emit_representative_sequence(all_closed=True, captured_events=captured_events)
    phases = [e.phase for e in captured_events]
    assert phases == [
        "TEARDOWN_ENUMERATE",
        "TEARDOWN_SIZE",
        "TEARDOWN_REPAIR",
        "TEARDOWN_BLOCK",
        "TEARDOWN_VERIFY",
    ]
    # All correlated under one teardown cycle id.
    assert {e.cycle_id for e in captured_events} == {"teardown-td_seq"}
    verify = captured_events[-1]
    assert verify.details["outcome"] == "verified"
    assert verify.details["verification_status"] == "CHAIN_VERIFIED"


def test_full_sequence_failure_path_still_emits_verify(captured_events):
    """A FAILED closure still emits the full decision trail (audit on failure)."""
    _emit_representative_sequence(all_closed=False, captured_events=captured_events)
    assert len(captured_events) == 5
    verify = captured_events[-1]
    assert verify.phase == "TEARDOWN_VERIFY"
    assert verify.details["outcome"] == "verify_failed"
    assert verify.details["verification_status"] == "FAILED"
    assert verify.details["positions_closed"] == 1


# ---------------------------------------------------------------------------
# Runner wiring — BLOCK / REPAIR entries from the lending guard correlate under
# the teardown cycle id (VIB-5478 / Gemini finding).
#
# ``_apply_lending_unwind_guard`` runs in ``execute_teardown`` BEFORE the runner
# swaps the observability contextvar to ``teardown-{id}``. Without the explicit
# ``teardown_id`` thread, these decision-log entries would fall back to the
# AMBIENT iteration cycle id and break correlation. These tests pin the explicit
# id wins over a non-teardown ambient cycle id.
# ---------------------------------------------------------------------------


def test_lending_guard_decisions_carry_teardown_cycle_id(captured_events, monkeypatch):
    """BLOCK + REPAIR entries from the guard correlate under ``teardown-{id}``.

    Even when the ambient context cycle id is the iteration cycle id (the state
    at the point ``_apply_lending_unwind_guard`` runs), the entries must carry
    the teardown cycle id passed explicitly through ``teardown_id``.
    """
    from almanak.framework.runner.runner_teardown import _apply_lending_unwind_guard
    from almanak.framework.teardown.lending_unwind_guard import LendingGuardResult

    # Ambient cycle id is the iteration lane — NOT a teardown id. The fix must
    # override this with the explicit teardown_id.
    set_cycle_id("iter-12345")
    try:
        result = LendingGuardResult(
            intents=["KEPT_INTENT"],
            dropped=["REPAY aave:base:usdc: measured zero debt (no stale REPAY 0)"],
            degraded=True,
            synthesized_positions=["aave:base:weth"],
        )
        monkeypatch.setattr(
            "almanak.framework.teardown.lending_unwind_guard.sanitize_lending_teardown_intents",
            lambda *a, **k: result,
        )

        out = _apply_lending_unwind_guard(
            ["RAW_INTENT"],
            object(),  # market — unused by the stubbed guard
            "deployment:abc123",
            None,
            teardown_id="td_777",
        )
    finally:
        clear_cycle_id()

    assert out == ["KEPT_INTENT"]
    # One BLOCK (dropped) + one REPAIR (synthesized) + one BLOCK (degraded).
    phases = [e.phase for e in captured_events]
    assert phases == ["TEARDOWN_BLOCK", "TEARDOWN_REPAIR", "TEARDOWN_BLOCK"]
    # Every entry correlates under the teardown cycle id, NOT the ambient
    # iteration cycle id — the correlation the Gemini finding flagged as broken.
    assert {e.cycle_id for e in captured_events} == {"teardown-td_777"}
    assert all(e.cycle_id != "iter-12345" for e in captured_events)


def test_lending_guard_decisions_without_teardown_id_fall_back(captured_events):
    """Sanity: with no explicit id the entries fall back to the ambient cycle id.

    Documents the failure mode the fix prevents — when ``teardown_id`` is absent
    the BLOCK entry inherits whatever ambient cycle id is set (here a teardown
    id, but in ``execute_teardown`` it would be the iteration id).
    """
    from almanak.framework.runner.runner_teardown import _apply_lending_unwind_guard
    from almanak.framework.teardown.lending_unwind_guard import LendingGuardResult

    set_cycle_id("teardown-ambient_99")
    try:
        result = LendingGuardResult(intents=[], dropped=["WITHDRAW x: nothing to withdraw"])
        import almanak.framework.teardown.lending_unwind_guard as _guard_mod

        original = _guard_mod.sanitize_lending_teardown_intents
        _guard_mod.sanitize_lending_teardown_intents = lambda *a, **k: result
        try:
            _apply_lending_unwind_guard(["RAW"], object(), "deployment:abc123", None)
        finally:
            _guard_mod.sanitize_lending_teardown_intents = original
    finally:
        clear_cycle_id()

    assert [e.phase for e in captured_events] == ["TEARDOWN_BLOCK"]
    assert captured_events[0].cycle_id == "teardown-ambient_99"
