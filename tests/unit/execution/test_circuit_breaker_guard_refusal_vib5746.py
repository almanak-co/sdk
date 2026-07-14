"""VIB-5746: the circuit breaker must distinguish a pre-execution SAFETY-GUARD
refusal from a real execution failure.

A guard refusal (price impact above the configured max, or the on-chain quoter
returning no amount so liquidity cannot be verified) sends ZERO transactions and
leaves the position untouched — the guard did its job. Recording it as a
consecutive execution failure is what tripped an EMERGENCY STOP + ~1h cooldown on
morpho_looping after three correct refusals of an un-fillable recycle swap.

The fix classifies these as :class:`FailureKind.GUARD_REFUSED`, which the breaker
treats as NEUTRAL: it never increments the action/data trip counters and never
trips, but it does track a separate refusal streak the runner uses to back the
loop cadence off.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.runner.failure_kind import FailureKind


def _breaker(max_consecutive_failures: int = 3) -> CircuitBreaker:
    return CircuitBreaker(
        deployment_id="deployment:test5746",
        config=CircuitBreakerConfig(max_consecutive_failures=max_consecutive_failures),
    )


# ── (a) N consecutive guard-refusals do NOT trip the breaker ─────────────────


def test_many_guard_refusals_never_trip() -> None:
    """Ten consecutive safety-guard refusals leave the breaker CLOSED."""
    breaker = _breaker(max_consecutive_failures=3)

    for _ in range(10):
        breaker.record_failure("Price impact too high (81.7%)", kind=FailureKind.GUARD_REFUSED)

    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker.check().can_execute is True
    # Neutral to the action/data trip counters entirely...
    status = breaker.get_status()
    assert status["consecutive_action_failures"] == 0
    assert status["consecutive_data_failures"] == 0
    # ...but the refusal streak IS tracked for the runner's back-off.
    assert breaker.consecutive_guard_refusals == 10


def test_guard_refusal_does_not_pollute_loss_history() -> None:
    """A refusal carries no on-chain loss and must not enter the loss window."""
    breaker = _breaker()
    breaker.record_failure("quoter returned no amount", kind=FailureKind.GUARD_REFUSED)
    assert breaker.get_status()["cumulative_loss_usd"] == "0"
    assert breaker.get_status()["failure_history_count"] == 0


# ── (b) N real execution failures still DO trip ──────────────────────────────


def test_real_failures_still_trip_at_threshold() -> None:
    """Three real (action-class) failures trip exactly as before the fix."""
    breaker = _breaker(max_consecutive_failures=3)

    breaker.record_failure("revert 1", kind=FailureKind.EXECUTION_REVERTED)
    breaker.record_failure("revert 2", kind=FailureKind.EXECUTION_REVERTED)
    assert breaker.state == CircuitBreakerState.CLOSED
    breaker.record_failure("revert 3", kind=FailureKind.EXECUTION_REVERTED)
    assert breaker.state == CircuitBreakerState.OPEN
    assert breaker.check().can_execute is False


def test_unclassified_failures_still_trip() -> None:
    """UNKNOWN (unclassified) failures remain action-class and still trip."""
    breaker = _breaker(max_consecutive_failures=3)
    for _ in range(3):
        breaker.record_failure("boom", kind=None)
    assert breaker.state == CircuitBreakerState.OPEN


# ── (c) mixed sequences behave correctly ─────────────────────────────────────


def test_refusals_interleaved_with_real_failures_still_trip() -> None:
    """Guard refusals are neutral: they neither advance nor reset the action
    streak, so real failures around them still accumulate to the threshold.

    Sequence: refuse, fail, refuse, fail, refuse, fail → 3 real action failures
    (interleaved with neutral refusals) must trip.
    """
    breaker = _breaker(max_consecutive_failures=3)
    breaker.record_failure("refuse", kind=FailureKind.GUARD_REFUSED)
    breaker.record_failure("real 1", kind=FailureKind.EXECUTION_REVERTED)
    breaker.record_failure("refuse", kind=FailureKind.GUARD_REFUSED)
    breaker.record_failure("real 2", kind=FailureKind.EXECUTION_REVERTED)
    breaker.record_failure("refuse", kind=FailureKind.GUARD_REFUSED)
    breaker.record_failure("real 3", kind=FailureKind.EXECUTION_REVERTED)
    assert breaker.state == CircuitBreakerState.OPEN


def test_real_failure_resets_refusal_streak() -> None:
    """A real failure ends a refusal streak (the strategy stopped merely being
    refused and actually failed)."""
    breaker = _breaker(max_consecutive_failures=5)
    for _ in range(3):
        breaker.record_failure("refuse", kind=FailureKind.GUARD_REFUSED)
    assert breaker.consecutive_guard_refusals == 3
    breaker.record_failure("real", kind=FailureKind.EXECUTION_REVERTED)
    assert breaker.consecutive_guard_refusals == 0


def test_success_resets_refusal_streak() -> None:
    """A success ends a refusal streak and restores base cadence."""
    breaker = _breaker()
    for _ in range(4):
        breaker.record_failure("refuse", kind=FailureKind.GUARD_REFUSED)
    assert breaker.consecutive_guard_refusals == 4
    breaker.record_success()
    assert breaker.consecutive_guard_refusals == 0


def test_refusal_does_not_close_half_open() -> None:
    """A guard refusal proves nothing about execution, so it must not count as a
    HALF_OPEN success (which would prematurely close a real trip)."""
    breaker = _breaker(max_consecutive_failures=1)
    breaker.config.half_open_success_threshold = 1
    # Trip, then force cooldown to 0 so the next check → HALF_OPEN.
    breaker.record_failure("real", kind=FailureKind.EXECUTION_REVERTED)
    assert breaker.state == CircuitBreakerState.OPEN
    breaker.config.cooldown_seconds = 0
    assert breaker.check().state == CircuitBreakerState.HALF_OPEN
    # A refusal in HALF_OPEN is neutral — stays HALF_OPEN, does not close.
    breaker.record_failure("refuse", kind=FailureKind.GUARD_REFUSED)
    assert breaker.state == CircuitBreakerState.HALF_OPEN


def test_cumulative_loss_trip_unaffected_by_refusals() -> None:
    """Refusals carry no loss; a real loss-bearing failure still trips on the
    cumulative-loss threshold."""
    breaker = CircuitBreaker(
        deployment_id="deployment:loss",
        config=CircuitBreakerConfig(
            max_consecutive_failures=100,
            max_cumulative_loss_usd=Decimal("1000"),
        ),
    )
    breaker.record_failure("refuse", kind=FailureKind.GUARD_REFUSED)
    breaker.record_failure("loss", loss_usd=Decimal("1500"), kind=FailureKind.EXECUTION_REVERTED)
    assert breaker.state == CircuitBreakerState.OPEN
