"""Characterization tests for StrategyRunner.run_loop (Phase 6A.1).

These tests lock in the CURRENT behavior of ``run_loop`` at the top of
``almanak/framework/runner/strategy_runner.py`` so Phase 6A.2 can extract
helpers without regressing the strategy main loop. They are intentionally
behavior-pinning: they do not assert what ``run_loop`` *should* do, only
what it *does* today.

Scope: the while-loop body between ``while not self._shutdown_requested``
and the final ``_lifecycle_write_state`` write. We stub out
``run_iteration`` so each test exercises one branch in isolation and the
rest of the loop (callbacks, consecutive-errors accounting, shutdown
drain, max-iterations exit, sleep, teardown/circuit-breaker routing,
stuck-detection, lifecycle poll, ACCOUNTING_FAILED handling).

Notes on latent behavior captured (not bugs -- pinned so the extraction
can preserve them):

- Circuit breaker: run_loop does NOT directly call ``CircuitBreaker.check``.
  Tripping is observed indirectly via an ``IterationResult`` with
  ``IterationStatus.CIRCUIT_BREAKER_OPEN`` returned from ``run_iteration``
  (which delegates to ``_step_circuit_breaker_pre_execute``). run_loop
  then increments ``_consecutive_errors`` but does NOT call
  ``record_failure`` for that status (already recorded inline).
- Stuck detection / teardown routing: both live inside
  ``_step_teardown_and_cb_gate`` in ``run_iteration``. From run_loop's
  perspective, a successful teardown surfaces as ``IterationResult`` with
  ``IterationStatus.TEARDOWN`` (treated as a success -- not counted as
  an error). Teardown is expected to call ``request_shutdown`` itself;
  run_loop then drains on the next loop-head check.
- Pre-iteration callback: ``CriticalCallbackError`` propagates out of
  the try/except and breaks the while. Regular ``Exception`` subclasses
  are logged and the loop continues with the iteration anyway.
- Unexpected ``Exception`` mid-iteration: loop increments
  ``_consecutive_errors`` and sleeps for ``interval`` before retrying;
  ``asyncio.CancelledError`` breaks out.
- ACCOUNTING_FAILED branch: only the live-mode snapshot failure path
  rebuilds ``result`` into ``IterationStatus.ACCOUNTING_FAILED``. Non-
  live modes log and keep the original result. The iteration result
  IS rebuilt inline -- the helper ``_create_error_result`` is NOT used
  (double-count avoidance for ``_total_iterations``; post fix #1771
  ``_create_error_result`` no longer mutates ``_consecutive_errors``).
  The rebuilt result preserves ``result.duration_ms`` so iteration
  summaries reflect the full iteration cost (issue #1770, fixed).
- Max iterations: counter increments AFTER the iteration completes, so
  ``max_iterations=1`` runs exactly one iteration even if shutdown is
  requested mid-iteration.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
)
from almanak.framework.runner.strategy_runner import (
    CriticalCallbackError,
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)

# =============================================================================
# Fixtures
# =============================================================================


def _make_runner(
    *,
    enable_state_persistence: bool = False,
    enable_alerting: bool = False,
    default_interval_seconds: int = 0,
    max_consecutive_errors: int = 3,
    circuit_breaker: CircuitBreaker | None = None,
    emergency_manager: MagicMock | None = None,
    alert_manager: MagicMock | None = None,
) -> StrategyRunner:
    """Build a StrategyRunner with gateway/state side-effects stubbed out."""
    config = RunnerConfig(
        default_interval_seconds=default_interval_seconds,
        max_consecutive_errors=max_consecutive_errors,
        enable_state_persistence=enable_state_persistence,
        enable_alerting=enable_alerting,
    )
    state_mgr = AsyncMock()
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_mgr,
        alert_manager=alert_manager or MagicMock(),
        config=config,
        circuit_breaker=circuit_breaker,
        emergency_manager=emergency_manager,
    )
    # Stub gateway / lifecycle side-effects that the loop hits every pass.
    runner._register_with_gateway = MagicMock()
    runner._deregister_from_gateway = MagicMock()
    runner._gateway_heartbeat = MagicMock()
    runner._gateway_update_status = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    runner._lifecycle_write_state = MagicMock()
    runner._lifecycle_heartbeat = MagicMock()
    runner._lifecycle_poll_command = MagicMock(return_value=None)
    runner._lifecycle_handle_stop = MagicMock()
    runner._collect_position_snapshot = MagicMock(return_value=None)
    return runner


def _make_strategy(strategy_id: str = "test-strategy") -> MagicMock:
    """Mock strategy that avoids copy-trading / portfolio paths."""
    strategy = MagicMock()
    strategy.strategy_id = strategy_id
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.config = {}
    # Explicitly None so activity_provider branch is skipped
    strategy._wallet_activity_provider = None
    # Prevent accidental flush_pending_saves call
    del strategy.flush_pending_saves
    return strategy


def _make_result(
    status: IterationStatus = IterationStatus.SUCCESS,
    error: str | None = None,
    strategy_id: str = "test-strategy",
) -> IterationResult:
    return IterationResult(
        status=status,
        strategy_id=strategy_id,
        duration_ms=10.0,
        error=error,
    )


# =============================================================================
# Happy path
# =============================================================================


class TestHappyPath:
    """Pin the single-iteration, successful, no-optional-features path."""

    @pytest.mark.asyncio
    async def test_single_iteration_success_invokes_callback_once(self):
        runner = _make_runner()
        strategy = _make_strategy()
        received: list[IterationResult] = []

        async def mock_iter(s):
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=received.append,
                max_iterations=1,
            ),
            timeout=5,
        )

        assert len(received) == 1
        assert received[0].status == IterationStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_success_resets_consecutive_errors_to_zero(self):
        runner = _make_runner()
        strategy = _make_strategy()
        runner._consecutive_errors = 2  # pretend we had a streak

        async def mock_iter(s):
            # run_iteration is responsible for resetting; here we just simulate
            # a successful result and pin that run_loop leaves _consecutive_errors
            # at 0 after the success branch runs (it sets it explicitly).
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=1),
            timeout=5,
        )

        assert runner._consecutive_errors == 0
        assert runner._first_error_at is None

    @pytest.mark.asyncio
    async def test_loop_continues_for_multiple_iterations(self):
        runner = _make_runner()
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            return _make_result()

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=5),
            timeout=5,
        )

        assert count == 5


# =============================================================================
# Max iterations exit
# =============================================================================


class TestMaxIterationsExit:
    """Pin that max_iterations=N exits cleanly after N iterations."""

    @pytest.mark.asyncio
    async def test_max_iterations_stops_after_exact_count(self):
        runner = _make_runner()
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            return _make_result()

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=3),
            timeout=5,
        )

        assert count == 3

    @pytest.mark.asyncio
    async def test_max_iterations_none_runs_until_shutdown(self):
        runner = _make_runner()
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            if count >= 4:
                runner.request_shutdown()
            return _make_result()

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=None),
            timeout=5,
        )

        assert count == 4


# =============================================================================
# Pre-iteration callback
# =============================================================================


class TestPreIterationCallback:
    """Pin ordering + exception handling for the pre-iteration callback."""

    @pytest.mark.asyncio
    async def test_pre_iteration_callback_fires_before_each_iteration(self):
        runner = _make_runner()
        strategy = _make_strategy()
        sequence: list[str] = []

        def pre_cb():
            sequence.append("pre")

        async def mock_iter(s):
            sequence.append("iter")
            return _make_result()

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                pre_iteration_callback=pre_cb,
                max_iterations=3,
            ),
            timeout=5,
        )

        assert sequence == ["pre", "iter", "pre", "iter", "pre", "iter"]

    @pytest.mark.asyncio
    async def test_critical_callback_error_breaks_loop_on_first_call(self):
        """CriticalCallbackError is fail-closed: loop exits without calling run_iteration."""
        runner = _make_runner()
        strategy = _make_strategy()
        pre_calls = 0

        def critical_cb():
            nonlocal pre_calls
            pre_calls += 1
            raise CriticalCallbackError("fatal")

        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                pre_iteration_callback=critical_cb,
                max_iterations=5,
            ),
            timeout=5,
        )

        # Called once, then re-raised out of the inner try; the outer except
        # CriticalCallbackError clause logs + breaks.
        assert pre_calls == 1
        assert runner.run_iteration.await_count == 0

    @pytest.mark.asyncio
    async def test_regular_exception_from_pre_callback_is_swallowed(self):
        """A plain Exception from pre_iteration_callback is logged, loop continues."""
        runner = _make_runner()
        strategy = _make_strategy()
        pre_calls = 0

        def flaky():
            nonlocal pre_calls
            pre_calls += 1
            raise RuntimeError("transient")

        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                pre_iteration_callback=flaky,
                max_iterations=3,
            ),
            timeout=5,
        )

        assert pre_calls == 3
        assert runner.run_iteration.await_count == 3


# =============================================================================
# Iteration callback args
# =============================================================================


class TestIterationCallbackArgs:
    """Pin the callback contract."""

    @pytest.mark.asyncio
    async def test_callback_receives_iteration_result_instance(self):
        runner = _make_runner()
        strategy = _make_strategy()
        result_in = _make_result(IterationStatus.HOLD, error=None)
        captured: list[IterationResult] = []

        runner.run_iteration = AsyncMock(return_value=result_in)

        def cb(res):
            captured.append(res)

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=cb,
                max_iterations=1,
            ),
            timeout=5,
        )

        assert len(captured) == 1
        assert captured[0] is result_in
        assert captured[0].status == IterationStatus.HOLD
        assert captured[0].strategy_id == "test-strategy"
        assert captured[0].duration_ms == 10.0

    @pytest.mark.asyncio
    async def test_iteration_callback_error_does_not_stop_loop(self):
        runner = _make_runner()
        strategy = _make_strategy()
        call_count = 0

        def failing_cb(res):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("downstream boom")

        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=failing_cb,
                max_iterations=3,
            ),
            timeout=5,
        )

        assert call_count == 3


# =============================================================================
# Shutdown signal handling
# =============================================================================


class TestShutdownSignals:
    """Pin the behavior of request_shutdown() under various timings."""

    @pytest.mark.asyncio
    async def test_shutdown_mid_iteration_completes_current_iteration(self):
        """Shutdown set during run_iteration -> current iteration finishes, next loop-head check exits."""
        runner = _make_runner()
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            runner.request_shutdown()
            return _make_result()

        runner.run_iteration = mock_iter
        cb_results: list[IterationResult] = []

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=cb_results.append,
                max_iterations=10,
            ),
            timeout=5,
        )

        # The current iteration was allowed to complete AND the callback fired.
        assert count == 1
        assert len(cb_results) == 1

    @pytest.mark.asyncio
    async def test_shutdown_before_sleep_skips_sleep_and_exits(self):
        """When shutdown is requested, the post-iteration sleep is skipped."""
        runner = _make_runner()
        strategy = _make_strategy()
        sleep_calls: list[float] = []

        real_sleep = asyncio.sleep

        async def spy_sleep(delay):
            sleep_calls.append(delay)
            await real_sleep(0)

        async def mock_iter(s):
            runner.request_shutdown()
            return _make_result()

        runner.run_iteration = mock_iter

        with patch("almanak.framework.runner.strategy_runner.asyncio.sleep", spy_sleep):
            await asyncio.wait_for(
                runner.run_loop(strategy, interval_seconds=5, max_iterations=10),
                timeout=5,
            )

        # The post-iteration sleep (interval=5) must NOT have been issued.
        assert 5 not in sleep_calls

    @pytest.mark.asyncio
    async def test_cancelled_error_breaks_loop_cleanly(self):
        """asyncio.CancelledError during run_iteration exits the loop (no re-raise)."""
        runner = _make_runner()
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            raise asyncio.CancelledError()

        runner.run_iteration = mock_iter

        # No TimeoutError / CancelledError should escape run_loop.
        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=10),
            timeout=5,
        )

        assert count == 1


# =============================================================================
# Circuit breaker + consecutive errors
# =============================================================================


class TestCircuitBreakerAndConsecutiveErrors:
    """Pin how run_loop reacts to circuit-breaker-tripped / error results."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_result_increments_consecutive_errors(self):
        """A CIRCUIT_BREAKER_OPEN result increments _consecutive_errors but does NOT re-record on the breaker."""
        breaker = MagicMock(spec=CircuitBreaker)
        breaker.state = MagicMock()
        runner = _make_runner(circuit_breaker=breaker, max_consecutive_errors=10)
        strategy = _make_strategy()

        async def mock_iter(s):
            return _make_result(
                status=IterationStatus.CIRCUIT_BREAKER_OPEN,
                error="breaker open",
            )

        runner.run_iteration = mock_iter
        # Disable emergency trigger logic for this test.
        runner._maybe_trigger_emergency = AsyncMock()

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=3),
            timeout=5,
        )

        # CIRCUIT_BREAKER_OPEN is in the "already recorded inline" skip list:
        # record_failure should NOT have been called from run_loop itself.
        assert breaker.record_failure.call_count == 0
        # But _consecutive_errors still increments on every failed result.
        assert runner._consecutive_errors == 3
        assert runner._first_error_at is not None

    @pytest.mark.asyncio
    async def test_generic_failure_triggers_circuit_breaker_record(self):
        """Statuses NOT in the skip list should call record_failure via run_loop."""
        breaker = MagicMock(spec=CircuitBreaker)
        breaker.state = MagicMock()
        runner = _make_runner(circuit_breaker=breaker, max_consecutive_errors=10)
        strategy = _make_strategy()

        async def mock_iter(s):
            return _make_result(
                status=IterationStatus.EXECUTION_FAILED,
                error="swap failed",
            )

        runner.run_iteration = mock_iter
        runner._maybe_trigger_emergency = AsyncMock()

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=2),
            timeout=5,
        )

        assert breaker.record_failure.call_count == 2

    @pytest.mark.asyncio
    async def test_consecutive_errors_threshold_triggers_alert(self):
        """Hitting max_consecutive_errors calls _alert_consecutive_errors + writes ERROR lifecycle state."""
        runner = _make_runner(max_consecutive_errors=2)
        strategy = _make_strategy()

        async def mock_iter(s):
            return _make_result(
                status=IterationStatus.EXECUTION_FAILED,
                error="persistent failure",
            )

        runner.run_iteration = mock_iter
        runner._alert_consecutive_errors = AsyncMock()
        runner._maybe_trigger_emergency = AsyncMock()

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=3),
            timeout=5,
        )

        # Threshold is 2, we ran 3 iterations all failing -- alert fires every
        # iteration where _consecutive_errors >= threshold (iterations 2 and 3).
        assert runner._alert_consecutive_errors.await_count == 2
        # Lifecycle write state called with ERROR at least once.
        error_writes = [
            c
            for c in runner._lifecycle_write_state.call_args_list
            if len(c.args) >= 2 and c.args[1] == "ERROR"
        ]
        assert len(error_writes) >= 1

    @pytest.mark.asyncio
    async def test_success_after_failure_streak_writes_running_recovery(self):
        """When a success follows an error streak, lifecycle is reset to RUNNING."""
        runner = _make_runner(max_consecutive_errors=2)
        strategy = _make_strategy()

        # Seed an error streak manually so the pre-iteration snapshot sees it.
        runner._consecutive_errors = 2
        runner._first_error_at = datetime.now(UTC)

        async def mock_iter(s):
            # Simulate run_iteration's internal _record_success reset.
            runner._consecutive_errors = 0
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=1),
            timeout=5,
        )

        running_writes = [
            c
            for c in runner._lifecycle_write_state.call_args_list
            if len(c.args) >= 2 and c.args[1] == "RUNNING"
        ]
        # At minimum: the startup RUNNING write + the recovery RUNNING write.
        assert len(running_writes) >= 2


# =============================================================================
# Teardown routing (observed via iteration result)
# =============================================================================


class TestTeardownRouting:
    """run_loop treats teardown as a surfaced IterationResult; pin the flow.

    Teardown-mode dispatch lives inside run_iteration -> _step_teardown_and_cb_gate.
    From run_loop's vantage point, a completed teardown is an IterationResult
    with status=TEARDOWN (success-like), and teardown handlers typically call
    request_shutdown themselves so the next while-head check exits the loop.
    """

    @pytest.mark.asyncio
    async def test_teardown_result_treated_as_success_no_error_increment(self):
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iter(s):
            return _make_result(IterationStatus.TEARDOWN)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=1),
            timeout=5,
        )

        # Teardown success path does not add to the error counter.
        assert runner._consecutive_errors == 0

    @pytest.mark.asyncio
    async def test_teardown_handler_can_request_shutdown_mid_loop(self):
        """When teardown requests shutdown, run_loop drains after the iteration."""
        runner = _make_runner()
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            runner.request_shutdown()  # simulate teardown handler
            return _make_result(IterationStatus.TEARDOWN)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=10),
            timeout=5,
        )

        assert count == 1

    @pytest.mark.asyncio
    async def test_hold_status_is_also_treated_as_success(self):
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iter(s):
            return _make_result(IterationStatus.HOLD)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=2),
            timeout=5,
        )

        assert runner._consecutive_errors == 0


# =============================================================================
# Interval sleep duration
# =============================================================================


class TestIntervalSleep:
    """Pin that the between-iteration sleep is exactly ``interval_seconds``."""

    @pytest.mark.asyncio
    async def test_between_iteration_sleep_uses_interval(self):
        runner = _make_runner()
        strategy = _make_strategy()
        sleep_calls: list[float] = []

        real_sleep = asyncio.sleep

        async def spy_sleep(delay):
            sleep_calls.append(delay)
            await real_sleep(0)

        async def mock_iter(s):
            return _make_result()

        runner.run_iteration = mock_iter

        with patch("almanak.framework.runner.strategy_runner.asyncio.sleep", spy_sleep):
            await asyncio.wait_for(
                runner.run_loop(strategy, interval_seconds=7, max_iterations=3),
                timeout=5,
            )

        # Every inter-iteration sleep uses interval=7.
        interval_sleeps = [d for d in sleep_calls if d == 7]
        # Three iterations, three post-iteration sleeps (last one still sleeps
        # because max_iterations break happens BEFORE the sleep block -- pin
        # current behavior either way).
        # The break after max_iterations sits BEFORE the `if not shutdown:`
        # sleep, so we expect exactly 2 interval sleeps (between 3 iters).
        assert len(interval_sleeps) == 2

    @pytest.mark.asyncio
    async def test_interval_seconds_falls_back_to_config_default(self):
        runner = _make_runner(default_interval_seconds=9)
        strategy = _make_strategy()
        sleep_calls: list[float] = []

        real_sleep = asyncio.sleep

        async def spy_sleep(delay):
            sleep_calls.append(delay)
            await real_sleep(0)

        async def mock_iter(s):
            return _make_result()

        runner.run_iteration = mock_iter

        with patch("almanak.framework.runner.strategy_runner.asyncio.sleep", spy_sleep):
            await asyncio.wait_for(
                runner.run_loop(strategy, interval_seconds=None, max_iterations=2),
                timeout=5,
            )

        assert 9 in sleep_calls


# =============================================================================
# Unexpected exceptions inside the loop
# =============================================================================


class TestFirstIterationErrorDoesNotCrashLoop:
    """An unexpected Exception in the iteration body is caught + streak-counted.

    Pinned latent behavior: when ``run_iteration`` raises, the except clause
    increments ``_consecutive_errors`` but does NOT bump ``loop_iteration_count``.
    So an iteration that raised does not count toward ``max_iterations``.
    """

    @pytest.mark.asyncio
    async def test_unexpected_exception_increments_errors_and_continues(self):
        runner = _make_runner(max_consecutive_errors=10)
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            if count == 1:
                raise ValueError("first-iter boom")
            return _make_result()

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=3),
            timeout=5,
        )

        # The raised first iteration is "free" (does not count toward
        # max_iterations) -- pin this behavior. Three successful iterations
        # are still required to hit max_iterations=3, so count == 4.
        assert count == 4
        # Errors reset on the next successful iteration.
        assert runner._consecutive_errors == 0

    @pytest.mark.asyncio
    async def test_unexpected_exception_increments_consecutive_errors(self):
        """When run_iteration raises, _consecutive_errors is incremented."""
        runner = _make_runner(max_consecutive_errors=10)
        strategy = _make_strategy()
        count = 0

        async def mock_iter(s):
            nonlocal count
            count += 1
            # Shutdown after the second raise so we can inspect counters
            # without a success reset.
            if count >= 2:
                runner.request_shutdown()
            raise ValueError("always boom")

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=5),
            timeout=5,
        )

        assert runner._consecutive_errors >= 2


# =============================================================================
# Lifecycle poll commands (PAUSE / STOP / RESUME)
# =============================================================================


class TestLifecycleCommands:
    """Pin routing for STOP and PAUSE/RESUME lifecycle commands."""

    @pytest.mark.asyncio
    async def test_stop_command_calls_handle_stop(self):
        runner = _make_runner()
        strategy = _make_strategy()
        # First iteration: poll returns "STOP"
        runner._lifecycle_poll_command = MagicMock(side_effect=["STOP", None, None])

        def handle_stop(sid, strat):
            runner.request_shutdown()

        runner._lifecycle_handle_stop = MagicMock(side_effect=handle_stop)
        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=5),
            timeout=5,
        )

        assert runner._lifecycle_handle_stop.call_count == 1

    @pytest.mark.asyncio
    async def test_pause_then_resume_keeps_loop_alive(self):
        """PAUSE blocks in an inner while; RESUME releases it; loop then sleeps + exits."""
        runner = _make_runner()
        strategy = _make_strategy()
        # First outer iteration command is PAUSE; inside the pause loop the
        # first poll returns RESUME.
        commands = iter(["PAUSE", "RESUME"])

        def poll(sid):
            return next(commands, None)

        runner._lifecycle_poll_command = MagicMock(side_effect=poll)
        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=1),
            timeout=5,
        )

        # After RESUME, run_loop writes RUNNING via _lifecycle_write_state.
        running_writes = [
            c
            for c in runner._lifecycle_write_state.call_args_list
            if len(c.args) >= 2 and c.args[1] == "RUNNING"
        ]
        # Startup RUNNING + post-RESUME RUNNING.
        assert len(running_writes) >= 2

    @pytest.mark.asyncio
    async def test_pause_then_stop_inside_pause_invokes_handle_stop(self):
        """STOP received while paused must trigger _lifecycle_handle_stop."""
        runner = _make_runner()
        strategy = _make_strategy()
        commands = iter(["PAUSE", "STOP"])

        def poll(sid):
            return next(commands, None)

        runner._lifecycle_poll_command = MagicMock(side_effect=poll)

        def handle_stop(sid, strat):
            runner.request_shutdown()

        runner._lifecycle_handle_stop = MagicMock(side_effect=handle_stop)
        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=5),
            timeout=5,
        )

        assert runner._lifecycle_handle_stop.call_count == 1


# =============================================================================
# ACCOUNTING_FAILED snapshot branch
# =============================================================================


def _setup_accounting_failure_runner(
    *,
    live_mode: bool = True,
    write_kind: AccountingWriteKind = AccountingWriteKind.SNAPSHOT,
    cause: Exception | None = None,
) -> tuple[StrategyRunner, MagicMock, list[IterationResult]]:
    """Build a runner pre-wired for the ACCOUNTING_FAILED branch.

    Returns the configured ``runner``, mock ``strategy``, and the
    ``captured`` list that the tests should pass as the iteration
    callback. Callers still need to assign ``runner.run_iteration`` and
    invoke ``run_loop`` themselves -- this helper only covers the common
    mock wiring (addresses CodeRabbit nit on duplicated setup in PR #1777).
    """
    runner = _make_runner(enable_state_persistence=True, max_consecutive_errors=10)
    strategy = _make_strategy()
    runner._is_live_mode = MagicMock(return_value=live_mode)
    runner._update_state = AsyncMock()
    runner._capture_portfolio_snapshot = AsyncMock(
        side_effect=AccountingPersistenceError(
            write_kind,
            strategy_id="test-strategy",
            cause=cause or RuntimeError("disk full"),
        )
    )
    runner._alert_accounting_failure = AsyncMock()
    captured: list[IterationResult] = []
    return runner, strategy, captured


class TestAccountingFailedSnapshot:
    """Pin that live-mode snapshot AccountingPersistenceError rebuilds the result."""

    @pytest.mark.asyncio
    async def test_live_mode_snapshot_failure_escalates_to_accounting_failed(self):
        runner, strategy, captured = _setup_accounting_failure_runner(live_mode=True)

        # Use a distinctive duration so we can assert the rebuilt result
        # preserves the full iteration duration (issue #1770) rather than
        # measuring only the snapshot phase.
        iteration_duration_ms = 1234.5

        async def mock_iter(s):
            return IterationResult(
                status=IterationStatus.SUCCESS,
                strategy_id="test-strategy",
                duration_ms=iteration_duration_ms,
            )

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=captured.append,
                max_iterations=1,
            ),
            timeout=5,
        )

        # The iteration_callback receives the REBUILT result, not the original.
        assert len(captured) == 1
        assert captured[0].status == IterationStatus.ACCOUNTING_FAILED
        assert "Accounting persistence failed" in (captured[0].error or "")
        # And the alert hook was invoked.
        assert runner._alert_accounting_failure.await_count == 1
        # Regression: duration_ms on the rebuilt result MUST equal the full
        # iteration duration carried on the original ``result``, not the
        # snapshot-phase duration. See issue #1770.
        assert captured[0].duration_ms == iteration_duration_ms

    @pytest.mark.asyncio
    async def test_accounting_failed_duration_covers_full_iteration(self):
        """Regression for #1770: rebuilt ACCOUNTING_FAILED result reports the
        full iteration duration, not just the snapshot phase.

        Before the fix, ``capture_snapshot_with_accounting`` measured
        ``duration_ms`` from a fresh ``snapshot_start`` timestamp captured
        inside the helper, so a 30-second iteration that only failed during
        post-iteration snapshot persistence would be reported as a sub-
        millisecond event in iteration summaries. We now require the
        rebuilt result to preserve ``result.duration_ms``.
        """
        runner, strategy, captured = _setup_accounting_failure_runner(live_mode=True)

        # Simulate a long iteration: 30 seconds of iteration work.
        full_iteration_ms = 30_000.0

        async def mock_iter(s):
            return IterationResult(
                status=IterationStatus.SUCCESS,
                strategy_id="test-strategy",
                duration_ms=full_iteration_ms,
            )

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=captured.append,
                max_iterations=1,
            ),
            timeout=5,
        )

        assert len(captured) == 1
        rebuilt = captured[0]
        assert rebuilt.status == IterationStatus.ACCOUNTING_FAILED
        # The full iteration duration must be carried into the rebuilt result.
        assert rebuilt.duration_ms == full_iteration_ms

    @pytest.mark.asyncio
    async def test_accounting_failed_preserves_forensic_metadata(self):
        """Regression guard (CodeRabbit / Gemini review of PR #1777): the
        rebuilt ACCOUNTING_FAILED result must preserve ``intent``,
        ``execution_result``, and ``balance_reconciliation`` from the
        successful pre-snapshot result. Operators rely on this metadata
        (tx hashes, gas used, reconciliation deltas) to diagnose what
        on-chain actions preceded the accounting failure; dropping any
        of it leaves them guessing at the book-drift source.
        """
        runner, strategy, captured = _setup_accounting_failure_runner(live_mode=True)

        # Stand-in values for each forensic field -- we only care that
        # the rebuilt result carries the SAME object reference across.
        sentinel_intent = MagicMock(name="sentinel-intent")
        sentinel_execution_result = MagicMock(name="sentinel-execution-result")
        sentinel_balance_reconciliation = {"mismatches": [{"token": "USDC", "actual": 1}]}

        async def mock_iter(s):
            return IterationResult(
                status=IterationStatus.SUCCESS,
                strategy_id="test-strategy",
                duration_ms=100.0,
                intent=sentinel_intent,
                execution_result=sentinel_execution_result,
                balance_reconciliation=sentinel_balance_reconciliation,
            )

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=captured.append,
                max_iterations=1,
            ),
            timeout=5,
        )

        assert len(captured) == 1
        rebuilt = captured[0]
        assert rebuilt.status == IterationStatus.ACCOUNTING_FAILED
        # Forensic fields MUST be carried across -- identity, not just equality.
        assert rebuilt.intent is sentinel_intent
        assert rebuilt.execution_result is sentinel_execution_result
        assert rebuilt.balance_reconciliation is sentinel_balance_reconciliation

    @pytest.mark.asyncio
    async def test_non_live_mode_snapshot_failure_is_logged_only(self):
        runner, strategy, captured = _setup_accounting_failure_runner(live_mode=False)

        async def mock_iter(s):
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=captured.append,
                max_iterations=1,
            ),
            timeout=5,
        )

        # Non-live mode: result is NOT rebuilt, alert NOT fired.
        assert len(captured) == 1
        assert captured[0].status == IterationStatus.SUCCESS
        assert runner._alert_accounting_failure.await_count == 0


# =============================================================================
# Emergency trigger + stuck detection (observed via side effects)
# =============================================================================


class TestEmergencyAndStuckIntegration:
    """Pin that the helpers are invoked at the expected places in run_loop."""

    @pytest.mark.asyncio
    async def test_maybe_trigger_emergency_called_on_every_failure(self):
        """_maybe_trigger_emergency fires after record_failure on every failed iteration."""
        breaker = MagicMock(spec=CircuitBreaker)
        breaker.state = MagicMock()
        runner = _make_runner(circuit_breaker=breaker, max_consecutive_errors=10)
        strategy = _make_strategy()
        runner._maybe_trigger_emergency = AsyncMock()

        async def mock_iter(s):
            return _make_result(
                status=IterationStatus.EXECUTION_FAILED,
                error="exec fail",
            )

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=3),
            timeout=5,
        )

        assert runner._maybe_trigger_emergency.await_count == 3

    @pytest.mark.asyncio
    async def test_maybe_trigger_emergency_skipped_on_success(self):
        breaker = MagicMock(spec=CircuitBreaker)
        breaker.state = MagicMock()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        runner._maybe_trigger_emergency = AsyncMock()

        async def mock_iter(s):
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=3),
            timeout=5,
        )

        # Success path does not invoke _maybe_trigger_emergency.
        assert runner._maybe_trigger_emergency.await_count == 0


# =============================================================================
# Loop teardown (post-loop finalization)
# =============================================================================


class TestLoopTeardown:
    """Pin the finalization steps that run AFTER the while-loop exits."""

    @pytest.mark.asyncio
    async def test_loop_exit_deregisters_from_gateway(self):
        runner = _make_runner()
        strategy = _make_strategy()
        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=1),
            timeout=5,
        )

        runner._deregister_from_gateway.assert_called_once_with("test-strategy")

    @pytest.mark.asyncio
    async def test_loop_exit_writes_terminated_lifecycle_by_default(self):
        runner = _make_runner()
        strategy = _make_strategy()
        runner.run_iteration = AsyncMock(return_value=_make_result())

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=1),
            timeout=5,
        )

        # Final write uses "TERMINATED" (default when no terminal state set).
        last_call = runner._lifecycle_write_state.call_args_list[-1]
        assert last_call.args[1] == "TERMINATED"

    @pytest.mark.asyncio
    async def test_loop_exit_preserves_terminal_error_state(self):
        """If _terminal_lifecycle_state is set mid-loop, the final write honors it."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iter(s):
            runner._terminal_lifecycle_state = "ERROR"
            runner._terminal_lifecycle_error_message = "breaker tripped"
            runner.request_shutdown()
            return _make_result()

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=5),
            timeout=5,
        )

        last_call = runner._lifecycle_write_state.call_args_list[-1]
        assert last_call.args[1] == "ERROR"
        # error_message kwarg preserved
        assert last_call.kwargs.get("error_message") == "breaker tripped"


# =============================================================================
# Consecutive-errors counter single-ownership (issue #1771)
# =============================================================================


class TestConsecutiveErrorsSingleIncrement:
    """Regression guard for #1771: ``_consecutive_errors`` is incremented by
    ``handle_iteration_failure`` only. ``_create_error_result`` bumps only
    ``_total_iterations``. Previously both sites incremented, so any
    failure class returned via ``_create_error_result`` from ``run_iteration``
    double-counted and pushed the ``max_consecutive_errors`` threshold by one
    iteration.
    """

    def test_create_error_result_does_not_increment_consecutive_errors(self):
        """Direct unit check of ownership contract: calling
        ``_create_error_result`` must NOT mutate ``_consecutive_errors``.
        It MUST still increment ``_total_iterations`` (accounting for the
        failed iteration).

        Seed ``_consecutive_errors`` with a non-zero value so the assertion
        proves the counter is left fully untouched -- not merely that it
        did not go from ``0`` to ``1``. A regression that resets the streak
        to ``0`` would otherwise slip past a zero-seeded check.
        """
        runner = _make_runner()
        runner._consecutive_errors = 7
        runner._total_iterations = 0

        result = runner._create_error_result(
            strategy_id="test-strategy",
            status=IterationStatus.STRATEGY_ERROR,
            error="boom",
            start_time=datetime.now(UTC),
        )

        assert result.status == IterationStatus.STRATEGY_ERROR
        # _consecutive_errors stays untouched -- run_loop's handler owns it.
        # Seeding with 7 proves neither an increment nor a reset occurred.
        assert runner._consecutive_errors == 7
        # _total_iterations still ticks up to keep lifetime counts honest.
        assert runner._total_iterations == 1

    @pytest.mark.asyncio
    async def test_error_result_flows_increment_consecutive_errors_exactly_once(self):
        """End-to-end: a result that came from ``_create_error_result`` and
        flows back into ``run_loop`` bumps ``_consecutive_errors`` exactly
        once per iteration.

        We simulate this by bypassing ``run_iteration`` and returning a
        pre-built error result (as ``_create_error_result`` would, minus
        the counter mutation that used to happen inside it). If the loop
        body also double-incremented, we would see 2 instead of 1.
        """
        runner = _make_runner(max_consecutive_errors=10)
        strategy = _make_strategy()
        runner._maybe_trigger_emergency = AsyncMock()

        # Mimic what _create_error_result now does AFTER the fix: bump
        # _total_iterations but NOT _consecutive_errors, then return the
        # error result. run_loop's failure handler should be the sole
        # incrementer.
        async def mock_iter(s):
            runner._total_iterations += 1
            return _make_result(
                status=IterationStatus.STRATEGY_ERROR,
                error="decide() blew up",
            )

        runner.run_iteration = mock_iter

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=1),
            timeout=5,
        )

        # Exactly one increment per iteration -- no double-count.
        assert runner._consecutive_errors == 1

    @pytest.mark.asyncio
    async def test_max_consecutive_errors_threshold_reached_at_n_iterations(self):
        """Regression guard: with max_consecutive_errors=3, the ERROR
        lifecycle write must first fire at iteration 3 (not iteration 2
        as would happen under the old double-counting behavior).
        """
        runner = _make_runner(max_consecutive_errors=3)
        strategy = _make_strategy()
        runner._alert_consecutive_errors = AsyncMock()
        runner._maybe_trigger_emergency = AsyncMock()

        # Track ERROR lifecycle writes in the order they arrive alongside
        # the iteration count at the moment each write fires.
        iteration_at_error_write: list[int] = []
        completed_iterations = {"n": 0}

        async def mock_iter(s):
            completed_iterations["n"] += 1
            # Simulate _create_error_result post-fix: bump _total_iterations,
            # leave _consecutive_errors alone.
            runner._total_iterations += 1
            return _make_result(
                status=IterationStatus.STRATEGY_ERROR,
                error=f"iter {completed_iterations['n']}",
            )

        runner.run_iteration = mock_iter

        original_write = runner._lifecycle_write_state

        def spy(*args, **kwargs):
            state = args[1] if len(args) >= 2 else None
            if state == "ERROR":
                iteration_at_error_write.append(completed_iterations["n"])
            return original_write(*args, **kwargs)

        runner._lifecycle_write_state = spy

        await asyncio.wait_for(
            runner.run_loop(strategy, interval_seconds=0, max_iterations=5),
            timeout=5,
        )

        # The first ERROR write must land on iteration 3 (counter hits
        # threshold 3 exactly then). Under the buggy double-count, the
        # counter would reach 4 after iteration 2, so the first ERROR
        # write would land on iteration 2.
        assert iteration_at_error_write, "ERROR lifecycle write never fired"
        assert iteration_at_error_write[0] == 3


# =============================================================================
# _total_iterations single-ownership contract (issue #1780)
# =============================================================================


class TestRecordFailureIncrements:
    """Regression guard for #1780: ``_record_failure`` is the companion to
    ``_record_success`` for the failure path. It must bump ONLY
    ``_total_iterations`` -- ``_consecutive_errors`` and the circuit
    breaker remain owned by ``handle_iteration_failure`` in the run loop
    (fix #1771).
    """

    def test_record_failure_bumps_only_total_iterations(self) -> None:
        runner = _make_runner()
        runner._consecutive_errors = 4
        runner._total_iterations = 0
        runner._successful_iterations = 0

        runner._record_failure()

        # Total ticks up by one.
        assert runner._total_iterations == 1
        # Success counter untouched -- this is a failure record.
        assert runner._successful_iterations == 0
        # Consecutive-errors counter is owned by handle_iteration_failure.
        assert runner._consecutive_errors == 4

    def test_record_failure_does_not_touch_circuit_breaker(self) -> None:
        """``_record_failure`` must not call record_failure on the breaker.
        handle_iteration_failure owns that; calling it here would double-
        record every failure that flows back through run_loop.
        """
        runner = _make_runner()
        # Install a spy on the circuit breaker if present.
        runner._circuit_breaker = MagicMock()
        runner._record_failure()
        runner._circuit_breaker.record_failure.assert_not_called()

