"""Tests for signal-based exit code contract.

Exit-code semantics (enforced in CLI run.py after run_loop completes):
  - _signal_received=True   -> exit 2  (K8s retries preempted pods)
  - All iterations failed    -> exit 1  (max_iterations with 0 successes)
  - Otherwise               -> exit 0  (graceful stop)
  - Signal takes precedence over max-iterations failure
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


def _make_runner() -> StrategyRunner:
    """Create a minimal StrategyRunner with mocked dependencies."""
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=AsyncMock(),
        alert_manager=MagicMock(),
        config=config,
    )
    runner._register_with_gateway = MagicMock()
    runner._deregister_from_gateway = MagicMock()
    runner._gateway_heartbeat = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    return runner


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = "test-strategy"
    strategy.config = {}
    strategy._wallet_activity_provider = None
    return strategy


def _make_result(status: IterationStatus = IterationStatus.SUCCESS) -> IterationResult:
    return IterationResult(
        status=status,
        strategy_id="test-strategy",
        duration_ms=10,
    )


class TestSignalReceived:
    """Verify _signal_received flag behaviour."""

    def test_signal_received_defaults_false(self):
        runner = _make_runner()
        assert runner._signal_received is False

    @pytest.mark.asyncio
    async def test_signal_received_reset_on_run_loop_start(self):
        """_signal_received should be cleared when run_loop starts,
        so a reused runner doesn't carry stale state."""
        runner = _make_runner()
        strategy = _make_strategy()

        # Simulate a prior signal
        runner._signal_received = True

        async def mock_run_iteration(s):
            return _make_result()

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        # After a clean run_loop, _signal_received should be False
        assert runner._signal_received is False

    @pytest.mark.asyncio
    async def test_signal_sets_flag_and_stops(self):
        """Calling the signal handler should set _signal_received and
        trigger shutdown."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_run_iteration(s):
            # Simulate receiving a signal mid-run
            runner._signal_received = True
            runner.request_shutdown()
            return _make_result()

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0),
            timeout=10,
        )

        assert runner._signal_received is True


class TestExitCodePrecedence:
    """Verify the runner state that drives CLI exit codes.

    The CLI reads these runner attributes after run_loop completes:
      - _signal_received -> exit 2
      - _successful_iterations == 0 && _total_iterations > 0 -> exit 1
      - otherwise -> exit 0
    Signal should take precedence over max-iterations failure.
    """

    @pytest.mark.asyncio
    async def test_graceful_stop_state(self):
        """After a graceful stop, runner state implies exit 0."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_run_iteration(s):
            # Manually track metrics since we bypass _record_success
            runner._total_iterations += 1
            runner._successful_iterations += 1
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=2),
            timeout=10,
        )

        assert runner._signal_received is False
        assert runner._successful_iterations == 2

    @pytest.mark.asyncio
    async def test_all_iterations_failed_state(self):
        """When all iterations fail, runner state implies exit 1."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_run_iteration(s):
            runner._total_iterations += 1
            return _make_result(IterationStatus.STRATEGY_ERROR)

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=3),
            timeout=10,
        )

        assert runner._signal_received is False
        assert runner._successful_iterations == 0
        assert runner._total_iterations == 3

    @pytest.mark.asyncio
    async def test_signal_during_failed_iterations_state(self):
        """Signal takes precedence: even if all iterations failed,
        _signal_received should be True so CLI exits 2."""
        runner = _make_runner()
        strategy = _make_strategy()
        iteration_count = 0

        async def mock_run_iteration(s):
            nonlocal iteration_count
            iteration_count += 1
            runner._total_iterations += 1
            if iteration_count >= 2:
                runner._signal_received = True
                runner.request_shutdown()
            return _make_result(IterationStatus.STRATEGY_ERROR)

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=5),
            timeout=10,
        )

        # Signal flag should be set even though all iterations failed
        assert runner._signal_received is True
        assert runner._successful_iterations == 0
        assert runner._total_iterations > 0
