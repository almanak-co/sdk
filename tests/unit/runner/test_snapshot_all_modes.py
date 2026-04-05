"""Tests for portfolio snapshot capture across all execution modes (VIB-2399).

Verifies that _capture_portfolio_snapshot is called after every iteration
regardless of iteration success/failure status, ensuring no gaps in the
equity curve for dashboard and PnL tracking.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


def _make_runner(enable_state_persistence: bool = True, dry_run: bool = False) -> StrategyRunner:
    """Create a minimal StrategyRunner with mocked dependencies."""
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=enable_state_persistence,
        enable_alerting=False,
        dry_run=dry_run,
    )
    state_mgr = AsyncMock()
    state_mgr.initialize = AsyncMock()
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_mgr,
        alert_manager=MagicMock(),
        config=config,
    )
    runner._register_with_gateway = MagicMock()
    runner._deregister_from_gateway = MagicMock()
    runner._gateway_heartbeat = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    # Mock _capture_portfolio_snapshot to track calls without side effects
    runner._capture_portfolio_snapshot = AsyncMock(return_value=None)
    return runner


def _make_strategy() -> MagicMock:
    """Create a mock strategy."""
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


class TestSnapshotCaptureAllModes:
    """Verify snapshot capture is called in every execution mode."""

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_success(self):
        """Snapshot captured after a successful iteration."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_hold(self):
        """Snapshot captured when strategy returns HOLD."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.HOLD)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_dry_run(self):
        """Snapshot captured in --dry-run mode."""
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.DRY_RUN)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_failure(self):
        """Snapshot captured even when iteration FAILS (equity curve continuity)."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.EXECUTION_FAILED)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        # This is the key assertion: snapshot is captured even on failure
        runner._capture_portfolio_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_compilation_failed(self):
        """Snapshot captured after compilation failure."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.COMPILATION_FAILED)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_teardown(self):
        """Snapshot captured after teardown iteration."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.TEARDOWN)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_snapshot_skipped_when_persistence_disabled(self):
        """Snapshot NOT captured when enable_state_persistence=False."""
        runner = _make_runner(enable_state_persistence=False)
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_snapshot_captured_every_iteration_in_continuous_mode(self):
        """Snapshot captured on each iteration in continuous mode (--interval)."""
        runner = _make_runner()
        strategy = _make_strategy()
        call_count = 0

        async def mock_iteration(s):
            nonlocal call_count
            call_count += 1
            # Alternate success and failure to prove both capture
            if call_count % 2 == 0:
                return _make_result(IterationStatus.EXECUTION_FAILED)
            return _make_result(IterationStatus.SUCCESS)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=4),
            timeout=10,
        )

        assert runner._capture_portfolio_snapshot.call_count == 4

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_circuit_breaker_open(self):
        """Snapshot captured even when circuit breaker is open."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.CIRCUIT_BREAKER_OPEN)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_snapshot_captured_on_strategy_error(self):
        """Snapshot captured when strategy.decide() raises an error."""
        runner = _make_runner()
        strategy = _make_strategy()

        async def mock_iteration(s):
            return _make_result(IterationStatus.STRATEGY_ERROR)

        runner.run_iteration = mock_iteration

        await asyncio.wait_for(
            runner.run_loop(strategy=strategy, interval_seconds=0, max_iterations=1),
            timeout=10,
        )

        runner._capture_portfolio_snapshot.assert_called_once()
