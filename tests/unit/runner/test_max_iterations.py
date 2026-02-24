"""Tests for --max-iterations in StrategyRunner.run_loop (VIB-142)."""

import asyncio
from datetime import UTC, datetime
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
    state_mgr = AsyncMock()
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_mgr,
        alert_manager=MagicMock(),
        config=config,
    )
    # Patch out gateway and internal async methods
    runner._register_with_gateway = MagicMock()
    runner._deregister_from_gateway = MagicMock()
    runner._gateway_heartbeat = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    return runner


def _make_strategy() -> MagicMock:
    """Create a mock strategy that avoids triggering copy-trading paths."""
    strategy = MagicMock()
    strategy.strategy_id = "test-strategy"
    strategy.config = {}
    # Explicitly set to None to prevent copy-trading code paths
    strategy._wallet_activity_provider = None
    return strategy


def _make_result(status: IterationStatus = IterationStatus.SUCCESS) -> IterationResult:
    return IterationResult(
        status=status,
        strategy_id="test-strategy",
        duration_ms=10,
    )


class TestMaxIterations:
    """Tests for max_iterations parameter in run_loop."""

    @pytest.mark.asyncio
    async def test_max_iterations_stops_after_n(self):
        """Runner should stop after exactly max_iterations iterations."""
        runner = _make_runner()
        strategy = _make_strategy()
        iteration_count = 0

        async def mock_run_iteration(s):
            nonlocal iteration_count
            iteration_count += 1
            return _make_result()

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=3),
            timeout=10,
        )

        assert iteration_count == 3

    @pytest.mark.asyncio
    async def test_no_max_iterations_runs_until_shutdown(self):
        """Without max_iterations, runner loops until shutdown is requested."""
        runner = _make_runner()
        strategy = _make_strategy()
        iteration_count = 0

        async def mock_run_iteration(s):
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 5:
                runner.request_shutdown()
            return _make_result()

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=None),
            timeout=10,
        )

        assert iteration_count == 5

    @pytest.mark.asyncio
    async def test_max_iterations_one(self):
        """max_iterations=1 should run exactly one iteration."""
        runner = _make_runner()
        strategy = _make_strategy()
        iteration_count = 0

        async def mock_run_iteration(s):
            nonlocal iteration_count
            iteration_count += 1
            return _make_result()

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        assert iteration_count == 1

    @pytest.mark.asyncio
    async def test_callback_called_for_each_iteration(self):
        """Iteration callback should be called for every iteration including the last."""
        runner = _make_runner()
        strategy = _make_strategy()
        callback_count = 0

        async def mock_run_iteration(s):
            return _make_result()

        def on_iteration(result):
            nonlocal callback_count
            callback_count += 1

        runner.run_iteration = mock_run_iteration

        await asyncio.wait_for(
            runner.run_loop(
                strategy=strategy,
                interval_seconds=0,
                iteration_callback=on_iteration,
                max_iterations=3,
            ),
            timeout=10,
        )

        assert callback_count == 3
