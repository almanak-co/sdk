"""Unit tests for the available_cash_usd fallback in _build_metrics_for_snapshot.

VIB-3614: total_value_usd is strategy-scoped (positions only).
On first snapshot (no open positions) or after all positions are closed,
total_value_usd is 0 while the capital sits in available_cash_usd.
The fallback ensures initial_value_usd and total_value_usd never start at 0
so PnL stays meaningful from the first iteration.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.portfolio import PortfolioMetrics, PortfolioSnapshot, ValueConfidence
from almanak.framework.runner.runner_state import _build_metrics_for_snapshot


def _make_snapshot(total_value_usd: Decimal, available_cash_usd: Decimal) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        strategy_id="test-strategy",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        total_value_usd=total_value_usd,
        available_cash_usd=available_cash_usd,
        positions=[],
        value_confidence=ValueConfidence.HIGH,
        error=None,
    )


def _make_runner(existing_metrics: PortfolioMetrics | None = None) -> MagicMock:
    runner = MagicMock()
    runner.state_manager = AsyncMock()
    runner.state_manager.get_portfolio_metrics = AsyncMock(return_value=existing_metrics)
    runner.config = MagicMock()
    runner.config.execution_mode = "paper"
    runner._last_cycle_id = "cycle-test"
    runner.deployment_id = "deploy-test"
    return runner


class TestMetricsFallbackInitialCreation:
    """First-ever snapshot: existing_metrics is None."""

    @pytest.mark.asyncio
    async def test_cash_only_start_uses_available_cash_as_baseline(self):
        """When total_value_usd=0 (no positions), initial_value_usd and total_value_usd
        both fall back to available_cash_usd so PnL starts at zero."""
        snapshot = _make_snapshot(
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("1000"),
        )
        runner = _make_runner(existing_metrics=None)

        result = await _build_metrics_for_snapshot(runner, "test-strategy", snapshot)

        assert result is not None
        assert result.initial_value_usd == Decimal("1000")
        assert result.total_value_usd == Decimal("1000")

    @pytest.mark.asyncio
    async def test_positions_open_at_first_snapshot_uses_total_value(self):
        """When positions are already open on first snapshot, total_value_usd is used directly."""
        snapshot = _make_snapshot(
            total_value_usd=Decimal("900"),
            available_cash_usd=Decimal("100"),
        )
        runner = _make_runner(existing_metrics=None)

        result = await _build_metrics_for_snapshot(runner, "test-strategy", snapshot)

        assert result is not None
        assert result.initial_value_usd == Decimal("900")
        assert result.total_value_usd == Decimal("900")


class TestMetricsFallbackUpdatePath:
    """Subsequent snapshots: existing_metrics is present, total_value_usd is updated."""

    @pytest.mark.asyncio
    async def test_all_positions_closed_falls_back_to_cash(self):
        """After all positions are closed, total_value_usd drops to 0 while
        available_cash_usd holds the returned capital. The update path should
        use available_cash_usd so PnL doesn't cliff to -initial."""
        snapshot = _make_snapshot(
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("1050"),
        )
        existing = PortfolioMetrics(
            strategy_id="test-strategy",
            timestamp=datetime(2025, 12, 31, tzinfo=UTC),
            total_value_usd=Decimal("950"),
            initial_value_usd=Decimal("1000"),
            deployment_id="deploy-test",
            execution_mode="paper",
            cycle_id="cycle-0",
        )
        runner = _make_runner(existing_metrics=existing)

        result = await _build_metrics_for_snapshot(runner, "test-strategy", snapshot)

        assert result is not None
        assert result.total_value_usd == Decimal("1050")
        # initial_value_usd must be preserved (not changed on update)
        assert result.initial_value_usd == Decimal("1000")

    @pytest.mark.asyncio
    async def test_positions_open_uses_total_value_directly(self):
        """Normal update: positions are open, total_value_usd > 0, no fallback needed."""
        snapshot = _make_snapshot(
            total_value_usd=Decimal("1100"),
            available_cash_usd=Decimal("50"),
        )
        existing = PortfolioMetrics(
            strategy_id="test-strategy",
            timestamp=datetime(2025, 12, 31, tzinfo=UTC),
            total_value_usd=Decimal("1000"),
            initial_value_usd=Decimal("1000"),
            deployment_id="deploy-test",
            execution_mode="paper",
            cycle_id="cycle-0",
        )
        runner = _make_runner(existing_metrics=existing)

        result = await _build_metrics_for_snapshot(runner, "test-strategy", snapshot)

        assert result is not None
        assert result.total_value_usd == Decimal("1100")
