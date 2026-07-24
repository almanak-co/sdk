"""Branch coverage for CanaryDeployment lifecycle plumbing.

Complements test_canary_compare_performance.py (which owns the decision
cascade) with the monitoring / action surfaces:

- ``_update_metrics``: provider-absent short-circuit, per-side metric
  refresh, missing-side placeholders in the emitted event, provider errors
  swallowed;
- ``_monitor_loop``: not-RUNNING exit, observation-complete handoff,
  sleep-and-continue, CancelledError break, generic-exception recovery;
- ``promote_canary`` / ``rollback_canary``: status guards, monitoring-task
  cancellation, callback invocation and callback-failure tolerance;
- ``update_canary_metrics`` / ``update_stable_metrics``: uninitialised-side
  no-op and per-field selective updates.

All timing is simulated (started_at pushed into the past, asyncio.sleep
monkeypatched); no real waiting, no network.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.deployment.canary import (
    CanaryConfig,
    CanaryDecision,
    CanaryDeployment,
    CanaryMetrics,
    CanaryStatus,
)
from almanak.framework.models.strategy_version import PerformanceMetrics


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _perf(pnl: str = "0", trades: int = 0) -> PerformanceMetrics:
    return PerformanceMetrics(net_pnl_usd=Decimal(pnl), total_trades=trades)


def _metrics(version_id: str, *, is_canary: bool) -> CanaryMetrics:
    return CanaryMetrics(
        version_id=version_id,
        capital_allocated_usd=Decimal("1000"),
        metrics=PerformanceMetrics(),
        is_canary=is_canary,
    )


def _make_deployment(
    *,
    with_metrics: bool = True,
    metrics_provider=None,
    on_promote=None,
    on_rollback=None,
) -> CanaryDeployment:
    deployment = CanaryDeployment(
        deployment_id="s1",
        stable_version_id="v_stable",
        canary_version_id="v_canary",
        config=CanaryConfig(emit_events=False),
        metrics_provider=metrics_provider,
        on_promote=on_promote,
        on_rollback=on_rollback,
    )
    if with_metrics:
        deployment.state.canary_metrics = _metrics("v_canary", is_canary=True)
        deployment.state.stable_metrics = _metrics("v_stable", is_canary=False)
    return deployment


# ---------------------------------------------------------------------------
# _update_metrics
# ---------------------------------------------------------------------------


class TestUpdateMetricsFromProvider:
    def test_no_provider_is_a_no_op(self):
        deployment = _make_deployment(metrics_provider=None)
        before = deployment.state.canary_metrics.metrics

        asyncio.run(deployment._update_metrics())

        assert deployment.state.canary_metrics.metrics is before

    def test_refreshes_both_sides_from_provider(self):
        by_version = {
            "v_canary": _perf(pnl="150", trades=7),
            "v_stable": _perf(pnl="90", trades=12),
        }
        deployment = _make_deployment(metrics_provider=lambda version: by_version[version])

        asyncio.run(deployment._update_metrics())

        assert deployment.state.canary_metrics.metrics.net_pnl_usd == Decimal("150")
        assert deployment.state.canary_metrics.trade_count == 7
        assert deployment.state.stable_metrics.metrics.net_pnl_usd == Decimal("90")
        assert deployment.state.stable_metrics.trade_count == 12

    def test_missing_sides_are_skipped(self):
        provider = MagicMock(side_effect=AssertionError("provider must not be called"))
        deployment = _make_deployment(with_metrics=False, metrics_provider=provider)

        asyncio.run(deployment._update_metrics())

        provider.assert_not_called()
        assert deployment.state.canary_metrics is None
        assert deployment.state.stable_metrics is None

    def test_only_canary_side_present_updates_it(self):
        deployment = _make_deployment(with_metrics=False, metrics_provider=lambda _v: _perf(pnl="5", trades=1))
        deployment.state.canary_metrics = _metrics("v_canary", is_canary=True)

        asyncio.run(deployment._update_metrics())

        assert deployment.state.canary_metrics.metrics.net_pnl_usd == Decimal("5")
        assert deployment.state.stable_metrics is None

    def test_provider_error_is_swallowed(self):
        deployment = _make_deployment(metrics_provider=MagicMock(side_effect=RuntimeError("db down")))

        asyncio.run(deployment._update_metrics())  # must not raise

        assert deployment.state.canary_metrics.trade_count == 0


# ---------------------------------------------------------------------------
# _monitor_loop
# ---------------------------------------------------------------------------


class TestMonitorLoop:
    def test_exits_immediately_when_not_running(self):
        deployment = _make_deployment(metrics_provider=MagicMock())
        deployment._update_metrics = AsyncMock()
        assert deployment.state.status == CanaryStatus.PENDING

        asyncio.run(deployment._monitor_loop())

        deployment._update_metrics.assert_not_awaited()

    def test_observation_complete_hands_off_and_breaks(self):
        deployment = _make_deployment(metrics_provider=MagicMock())
        deployment.state.status = CanaryStatus.RUNNING
        deployment.state.started_at = datetime.now(UTC) - timedelta(hours=3)
        deployment._update_metrics = AsyncMock()
        deployment._handle_observation_complete = AsyncMock()

        asyncio.run(deployment._monitor_loop())

        deployment._update_metrics.assert_awaited_once()
        deployment._handle_observation_complete.assert_awaited_once()

    def test_sleeps_and_loops_while_observing(self, monkeypatch):
        deployment = _make_deployment(metrics_provider=None)
        deployment.state.status = CanaryStatus.RUNNING
        deployment.state.started_at = datetime.now(UTC)  # observation still open
        sleeps: list[float] = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            deployment.state.status = CanaryStatus.CANCELLED  # end the loop

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        asyncio.run(deployment._monitor_loop())

        assert sleeps == [deployment.config.check_interval_seconds]

    def test_cancelled_error_breaks_loop(self):
        deployment = _make_deployment(metrics_provider=MagicMock())
        deployment.state.status = CanaryStatus.RUNNING
        deployment.state.started_at = datetime.now(UTC)
        deployment._update_metrics = AsyncMock(side_effect=asyncio.CancelledError)
        deployment._handle_observation_complete = AsyncMock()

        asyncio.run(deployment._monitor_loop())

        deployment._handle_observation_complete.assert_not_awaited()

    def test_generic_error_sleeps_and_retries(self, monkeypatch):
        deployment = _make_deployment(metrics_provider=MagicMock())
        deployment.state.status = CanaryStatus.RUNNING
        deployment.state.started_at = datetime.now(UTC)
        deployment._update_metrics = AsyncMock(side_effect=RuntimeError("boom"))
        sleeps: list[float] = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            deployment.state.status = CanaryStatus.CANCELLED  # end the loop

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        asyncio.run(deployment._monitor_loop())

        # The error-path sleep ran (not the happy-path one: _update_metrics raised).
        assert sleeps == [deployment.config.check_interval_seconds]


# ---------------------------------------------------------------------------
# promote_canary / rollback_canary
# ---------------------------------------------------------------------------


class TestPromoteCanary:
    def test_rejected_outside_running_states(self):
        deployment = _make_deployment()
        assert deployment.state.status == CanaryStatus.PENDING

        result = asyncio.run(deployment.promote_canary())

        assert result.success is False
        assert "Cannot promote canary in status PENDING" in result.error
        assert deployment.state.status == CanaryStatus.PENDING

    def test_promotes_cancels_monitor_and_calls_callback(self):
        promoted: list[str] = []
        deployment = _make_deployment(on_promote=lambda state: promoted.append(state.status.value))
        deployment.state.status = CanaryStatus.RUNNING

        async def scenario():
            deployment._monitoring_task = asyncio.create_task(asyncio.sleep(60))
            return await deployment.promote_canary()

        result = asyncio.run(scenario())

        assert result.success is True
        assert result.decision == CanaryDecision.PROMOTE
        assert deployment.state.status == CanaryStatus.PROMOTED
        assert deployment.state.ended_at is not None
        assert deployment._monitoring_task.cancelled()
        assert promoted == ["PROMOTED"]
        assert deployment.state.decision_history[-1]["action"] == "PROMOTED"
        assert deployment.state.decision_history[-1]["canary_version"] == "v_canary"

    def test_promote_callback_failure_does_not_fail_promotion(self):
        deployment = _make_deployment(on_promote=MagicMock(side_effect=RuntimeError("webhook down")))
        deployment.state.status = CanaryStatus.OBSERVATION_COMPLETE

        result = asyncio.run(deployment.promote_canary())

        assert result.success is True
        assert deployment.state.status == CanaryStatus.PROMOTED


class TestRollbackCanary:
    def test_rejected_outside_running_states(self):
        deployment = _make_deployment()
        deployment.state.status = CanaryStatus.PROMOTED

        result = asyncio.run(deployment.rollback_canary())

        assert result.success is False
        assert "Cannot rollback canary in status PROMOTED" in result.error

    def test_rolls_back_cancels_monitor_and_calls_callback(self):
        rolled_back: list[str] = []
        deployment = _make_deployment(on_rollback=lambda state: rolled_back.append(state.status.value))
        deployment.state.status = CanaryStatus.RUNNING

        async def scenario():
            deployment._monitoring_task = asyncio.create_task(asyncio.sleep(60))
            return await deployment.rollback_canary()

        result = asyncio.run(scenario())

        assert result.success is True
        assert result.decision == CanaryDecision.ROLLBACK
        assert deployment.state.status == CanaryStatus.ROLLED_BACK
        assert deployment.state.ended_at is not None
        assert deployment._monitoring_task.cancelled()
        assert rolled_back == ["ROLLED_BACK"]
        assert deployment.state.decision_history[-1]["action"] == "ROLLED_BACK"

    def test_rollback_callback_failure_does_not_fail_rollback(self):
        deployment = _make_deployment(on_rollback=MagicMock(side_effect=RuntimeError("pager down")))
        deployment.state.status = CanaryStatus.OBSERVATION_COMPLETE

        result = asyncio.run(deployment.rollback_canary())

        assert result.success is True
        assert deployment.state.status == CanaryStatus.ROLLED_BACK


# ---------------------------------------------------------------------------
# update_canary_metrics / update_stable_metrics
# ---------------------------------------------------------------------------


class TestManualMetricUpdates:
    def test_canary_update_before_deploy_is_a_no_op(self):
        deployment = _make_deployment(with_metrics=False)

        deployment.update_canary_metrics(pnl_usd=Decimal("10"), trades=1, errors=1, drawdown=Decimal("0.1"))

        assert deployment.state.canary_metrics is None

    def test_canary_update_sets_all_fields(self):
        deployment = _make_deployment()

        deployment.update_canary_metrics(
            pnl_usd=Decimal("42"), trades=5, errors=2, drawdown=Decimal("0.07")
        )

        canary = deployment.state.canary_metrics
        assert canary.metrics.net_pnl_usd == Decimal("42")
        assert canary.trade_count == 5
        assert canary.metrics.total_trades == 5
        assert canary.error_count == 2
        assert canary.metrics.max_drawdown == Decimal("0.07")

    def test_canary_update_none_fields_leave_values_untouched(self):
        deployment = _make_deployment()
        deployment.update_canary_metrics(pnl_usd=Decimal("42"), trades=5, errors=2, drawdown=Decimal("0.07"))

        deployment.update_canary_metrics()

        canary = deployment.state.canary_metrics
        assert canary.metrics.net_pnl_usd == Decimal("42")
        assert canary.trade_count == 5
        assert canary.error_count == 2
        assert canary.metrics.max_drawdown == Decimal("0.07")

    def test_stable_update_before_deploy_is_a_no_op(self):
        deployment = _make_deployment(with_metrics=False)

        deployment.update_stable_metrics(pnl_usd=Decimal("10"), trades=1, errors=1, drawdown=Decimal("0.1"))

        assert deployment.state.stable_metrics is None

    def test_stable_update_sets_all_fields(self):
        deployment = _make_deployment()

        deployment.update_stable_metrics(
            pnl_usd=Decimal("-3"), trades=9, errors=1, drawdown=Decimal("0.2")
        )

        stable = deployment.state.stable_metrics
        assert stable.metrics.net_pnl_usd == Decimal("-3")
        assert stable.trade_count == 9
        assert stable.metrics.total_trades == 9
        assert stable.error_count == 1
        assert stable.metrics.max_drawdown == Decimal("0.2")

    def test_stable_update_none_fields_leave_values_untouched(self):
        deployment = _make_deployment()
        deployment.update_stable_metrics(pnl_usd=Decimal("-3"), trades=9, errors=1, drawdown=Decimal("0.2"))

        deployment.update_stable_metrics()

        stable = deployment.state.stable_metrics
        assert stable.metrics.net_pnl_usd == Decimal("-3")
        assert stable.trade_count == 9
        assert stable.error_count == 1
        assert stable.metrics.max_drawdown == Decimal("0.2")
