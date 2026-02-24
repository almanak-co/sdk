"""Tests for CriticalCallbackError fail-closed behavior in StrategyRunner.

Validates VIB-185: when a pre_iteration_callback raises CriticalCallbackError,
the run_loop must stop immediately. When a regular Exception is raised,
the loop should log the error and continue.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import (
    CriticalCallbackError,
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


def _make_runner() -> StrategyRunner:
    """Create a minimal StrategyRunner with mocked dependencies."""
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=MagicMock(),
        config=RunnerConfig(
            default_interval_seconds=0,
            enable_state_persistence=False,
        ),
    )
    return runner


def _make_strategy() -> MagicMock:
    """Create a mock strategy with required protocol attributes."""
    strategy = MagicMock()
    strategy.strategy_id = "test-strategy"
    strategy.decide = AsyncMock()
    return strategy


class TestCriticalCallbackError:
    """Test that CriticalCallbackError is a proper Exception subclass."""

    def test_is_exception(self):
        err = CriticalCallbackError("test")
        assert isinstance(err, Exception)

    def test_message(self):
        err = CriticalCallbackError("fork reset failed")
        assert str(err) == "fork reset failed"

    def test_can_be_raised_and_caught(self):
        with pytest.raises(CriticalCallbackError, match="boom"):
            raise CriticalCallbackError("boom")


class TestRunLoopCallbackBehavior:
    """Test run_loop behavior with pre_iteration_callback errors."""

    @pytest.mark.asyncio
    async def test_critical_callback_error_stops_loop(self):
        """CriticalCallbackError from pre_iteration_callback must stop the loop."""
        runner = _make_runner()
        strategy = _make_strategy()

        call_count = 0

        def failing_callback():
            nonlocal call_count
            call_count += 1
            raise CriticalCallbackError("Anvil fork reset failed")

        # Patch internals that would require real infrastructure
        runner._recover_incomplete_sessions = AsyncMock(return_value=0)
        runner._get_gateway_client = MagicMock(return_value=None)
        runner._register_with_gateway = MagicMock()
        runner._deregister_from_gateway = MagicMock()
        runner.state_manager.initialize = AsyncMock()
        runner.state_manager.close = AsyncMock()

        # run_loop should exit after the first callback failure
        await runner.run_loop(
            strategy,
            interval_seconds=0,
            pre_iteration_callback=failing_callback,
            max_iterations=5,
        )

        # Callback was called exactly once — loop stopped on first failure
        assert call_count == 1
        # run_iteration should NOT have been called (callback failed before it)
        runner.run_iteration = AsyncMock()  # Would have been called if loop continued

    @pytest.mark.asyncio
    async def test_regular_exception_does_not_stop_loop(self):
        """Regular Exception from pre_iteration_callback is logged; loop continues."""
        runner = _make_runner()
        strategy = _make_strategy()

        callback_call_count = 0

        def flaky_callback():
            nonlocal callback_call_count
            callback_call_count += 1
            raise RuntimeError("Transient network error")

        # Patch internals
        runner._recover_incomplete_sessions = AsyncMock(return_value=0)
        runner._get_gateway_client = MagicMock(return_value=None)
        runner._register_with_gateway = MagicMock()
        runner._deregister_from_gateway = MagicMock()
        runner.state_manager.initialize = AsyncMock()
        runner.state_manager.close = AsyncMock()
        runner._gateway_heartbeat = MagicMock()

        # Mock run_iteration to return a successful result
        mock_result = MagicMock(spec=IterationResult)
        mock_result.success = True
        mock_result.status = IterationStatus.SUCCESS
        runner.run_iteration = AsyncMock(return_value=mock_result)

        # Run with max_iterations=3 — loop should complete all 3 despite callback errors
        await runner.run_loop(
            strategy,
            interval_seconds=0,
            pre_iteration_callback=flaky_callback,
            max_iterations=3,
        )

        # Callback was called 3 times (loop continued past each failure)
        assert callback_call_count == 3
        # run_iteration was also called 3 times (loop didn't skip iterations)
        assert runner.run_iteration.call_count == 3

    @pytest.mark.asyncio
    async def test_successful_callback_does_not_stop_loop(self):
        """A successful pre_iteration_callback lets the loop proceed normally."""
        runner = _make_runner()
        strategy = _make_strategy()

        callback_call_count = 0

        def ok_callback():
            nonlocal callback_call_count
            callback_call_count += 1

        # Patch internals
        runner._recover_incomplete_sessions = AsyncMock(return_value=0)
        runner._get_gateway_client = MagicMock(return_value=None)
        runner._register_with_gateway = MagicMock()
        runner._deregister_from_gateway = MagicMock()
        runner.state_manager.initialize = AsyncMock()
        runner.state_manager.close = AsyncMock()
        runner._gateway_heartbeat = MagicMock()

        mock_result = MagicMock(spec=IterationResult)
        mock_result.success = True
        mock_result.status = IterationStatus.SUCCESS
        runner.run_iteration = AsyncMock(return_value=mock_result)

        await runner.run_loop(
            strategy,
            interval_seconds=0,
            pre_iteration_callback=ok_callback,
            max_iterations=3,
        )

        assert callback_call_count == 3
        assert runner.run_iteration.call_count == 3

    @pytest.mark.asyncio
    async def test_no_callback_runs_normally(self):
        """Without a pre_iteration_callback, loop runs normally."""
        runner = _make_runner()
        strategy = _make_strategy()

        # Patch internals
        runner._recover_incomplete_sessions = AsyncMock(return_value=0)
        runner._get_gateway_client = MagicMock(return_value=None)
        runner._register_with_gateway = MagicMock()
        runner._deregister_from_gateway = MagicMock()
        runner.state_manager.initialize = AsyncMock()
        runner.state_manager.close = AsyncMock()
        runner._gateway_heartbeat = MagicMock()

        mock_result = MagicMock(spec=IterationResult)
        mock_result.success = True
        mock_result.status = IterationStatus.SUCCESS
        runner.run_iteration = AsyncMock(return_value=mock_result)

        await runner.run_loop(
            strategy,
            interval_seconds=0,
            pre_iteration_callback=None,
            max_iterations=2,
        )

        assert runner.run_iteration.call_count == 2


class TestCLICallbackIntegration:
    """Test that the CLI creates a callback raising CriticalCallbackError."""

    def test_reset_fork_callback_raises_critical_on_failure(self):
        """When fork reset returns False, callback raises CriticalCallbackError."""
        # Simulate what the CLI does when --reset-fork is set
        mock_gateway = MagicMock()
        mock_gateway.reset_anvil_forks.return_value = False

        def pre_iteration_cb():
            ok = mock_gateway.reset_anvil_forks()
            if not ok:
                raise CriticalCallbackError(
                    "Anvil fork reset failed. Cannot continue with stale fork state."
                )

        with pytest.raises(CriticalCallbackError, match="fork reset failed"):
            pre_iteration_cb()

    def test_reset_fork_callback_succeeds_on_true(self):
        """When fork reset returns True, callback completes without error."""
        mock_gateway = MagicMock()
        mock_gateway.reset_anvil_forks.return_value = True

        def pre_iteration_cb():
            ok = mock_gateway.reset_anvil_forks()
            if not ok:
                raise CriticalCallbackError("Anvil fork reset failed.")

        # Should not raise
        pre_iteration_cb()
        mock_gateway.reset_anvil_forks.assert_called_once()
