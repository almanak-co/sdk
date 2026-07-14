"""VIB-5746: loop back-off for repeated safety-guard refusals + the typed
classification wiring that keeps a refusal off the breaker's trip counters.

Two layers are covered here:

* :func:`effective_iteration_wait_seconds` — the exponential back-off schedule
  the run loop applies while a strategy is stuck on a guard-refusal streak, so it
  idles-and-monitors instead of hot-looping the refused action.
* :class:`FailureKind` propagation — a terminal EXECUTION_FAILED whose underlying
  cause is a compile-time safety refusal carries ``failure_kind=GUARD_REFUSED``,
  which ``handle_iteration_failure`` records as neutral (no trip).
"""

from __future__ import annotations

from almanak.framework.runner._run_loop_helpers import (
    MAX_REFUSAL_BACKOFF_SECONDS,
    effective_iteration_wait_seconds,
)
from almanak.framework.runner.failure_kind import FailureKind


# ── (d) refused-swap back-off schedule ───────────────────────────────────────


def test_no_backoff_for_first_isolated_refusal() -> None:
    """Streak 0 and 1 keep the base interval — a single refusal is not penalised,
    inputs may well have changed by the next normal tick."""
    assert effective_iteration_wait_seconds(60, 0) == 60
    assert effective_iteration_wait_seconds(60, 1) == 60


def test_backoff_doubles_from_second_refusal() -> None:
    """From the second consecutive refusal the wait doubles each cycle."""
    assert effective_iteration_wait_seconds(60, 2) == 120
    assert effective_iteration_wait_seconds(60, 3) == 240
    assert effective_iteration_wait_seconds(60, 4) == 480


def test_backoff_is_capped() -> None:
    """The back-off never exceeds the base interval plus the cap, no matter how
    long the streak — so a permanently-refused strategy keeps polling (monitoring)
    at a bounded slow cadence rather than stopping."""
    assert effective_iteration_wait_seconds(60, 3) == 240
    capped = effective_iteration_wait_seconds(60, 50)
    assert capped == 60 + MAX_REFUSAL_BACKOFF_SECONDS
    # A very long streak cannot overflow before the cap clamps it.
    assert effective_iteration_wait_seconds(60, 10_000) == 60 + MAX_REFUSAL_BACKOFF_SECONDS


def test_backoff_scales_with_interval() -> None:
    """The back-off is derived from the deployment's own interval (no fixed
    magic seconds), so a slow strategy backs off in its own units."""
    assert effective_iteration_wait_seconds(300, 2) == 600
    assert effective_iteration_wait_seconds(10, 2) == 20


def test_zero_or_negative_interval_is_passthrough() -> None:
    """A non-positive interval (continuous mode) is returned unchanged."""
    assert effective_iteration_wait_seconds(0, 5) == 0


# ── typed GUARD_REFUSED classification is neutral end-to-end ──────────────────


class _FakeBreaker:
    """Minimal stand-in capturing what kind the runner records."""

    def __init__(self) -> None:
        self.recorded: list[FailureKind | None] = []
        self.state = "closed"

    def record_failure(self, error_message: str, kind: FailureKind | None = None) -> None:
        self.recorded.append(kind)


def test_handle_iteration_failure_prefers_typed_failure_kind() -> None:
    """When an IterationResult carries a typed ``failure_kind``, the breaker
    records THAT (GUARD_REFUSED) rather than inferring action-class from the
    EXECUTION_FAILED status string."""
    import asyncio
    from types import SimpleNamespace

    from almanak.framework.runner import _run_loop_helpers
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    breaker = _FakeBreaker()
    # Emergency + consecutive-error alerting are exercised elsewhere; here we
    # care only about which kind is recorded, so stub the side-effect surfaces.
    runner = SimpleNamespace(
        _circuit_breaker=breaker,
        _consecutive_errors=0,
        _first_error_at=None,
        config=SimpleNamespace(max_consecutive_errors=3),
        _maybe_trigger_emergency=_async_noop,
        _alert_consecutive_errors=_async_noop,
        _lifecycle_write_state=lambda *a, **k: None,
    )
    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        error="Price impact too high: quoter returned amount implying 81.7% price impact",
        deployment_id="deployment:test5746",
        failure_kind=FailureKind.GUARD_REFUSED,
    )

    asyncio.run(
        _run_loop_helpers.handle_iteration_failure(
            runner, strategy=SimpleNamespace(), deployment_id="deployment:test5746", result=result
        )
    )

    assert breaker.recorded == [FailureKind.GUARD_REFUSED]


def test_handle_iteration_failure_infers_kind_when_untyped() -> None:
    """Without a typed failure_kind the runner falls back to status inference —
    an EXECUTION_FAILED stays UNKNOWN/action-class (unchanged behaviour)."""
    import asyncio
    from types import SimpleNamespace

    from almanak.framework.runner import _run_loop_helpers
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    breaker = _FakeBreaker()
    runner = SimpleNamespace(
        _circuit_breaker=breaker,
        _consecutive_errors=0,
        _first_error_at=None,
        config=SimpleNamespace(max_consecutive_errors=3),
        _maybe_trigger_emergency=_async_noop,
        _alert_consecutive_errors=_async_noop,
        _lifecycle_write_state=lambda *a, **k: None,
    )
    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        error="reverted: out of gas",
        deployment_id="deployment:test5746",
    )

    asyncio.run(
        _run_loop_helpers.handle_iteration_failure(
            runner, strategy=SimpleNamespace(), deployment_id="deployment:test5746", result=result
        )
    )

    assert breaker.recorded == [FailureKind.UNKNOWN]


async def _async_noop(*args, **kwargs) -> None:
    return None
