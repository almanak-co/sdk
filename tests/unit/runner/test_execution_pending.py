"""Unresolved submissions are neither execution failures nor successes."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner._run_loop_helpers import handle_iteration_failure
from almanak.framework.runner.runner_models import IterationResult, IterationStatus


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [IterationStatus.EXECUTION_FAILED, IterationStatus.EXECUTION_PENDING])
async def test_pending_preserves_error_streak_without_tripping_or_resetting_breaker(status):
    previous_error = datetime.now(UTC)
    runner = SimpleNamespace(
        _consecutive_errors=2,
        _first_error_at=previous_error,
        _circuit_breaker=MagicMock(),
        config=SimpleNamespace(max_consecutive_errors=5),
        _maybe_trigger_emergency=AsyncMock(),
        _alert_consecutive_errors=AsyncMock(),
        _alert_execution_pending=AsyncMock(),
        _lifecycle_write_state=MagicMock(),
    )
    result = IterationResult(status=status, error="same ambiguous text", execution_pending_since=datetime.now(UTC))
    await handle_iteration_failure(runner, SimpleNamespace(), "deployment:test", result)
    assert not result.success
    if status is IterationStatus.EXECUTION_PENDING:
        assert runner._consecutive_errors == 2
        assert runner._first_error_at is previous_error
        runner._circuit_breaker.record_failure.assert_not_called()
        runner._circuit_breaker.record_success.assert_not_called()
        runner._maybe_trigger_emergency.assert_not_awaited()
        runner._alert_consecutive_errors.assert_not_awaited()
        runner._lifecycle_write_state.assert_not_called()
    else:
        assert runner._consecutive_errors == 3
        runner._circuit_breaker.record_failure.assert_called_once()
        runner._maybe_trigger_emergency.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "age,reason,alerts", [(299, None, 0), (301, None, 1), (0, "Legacy checkpoint", 1), (0, "Canonical revert", 1)]
)
async def test_pending_escalates_without_authorizing_teardown(age, reason, alerts):
    runner = SimpleNamespace(
        _consecutive_errors=0,
        _first_error_at=None,
        _circuit_breaker=MagicMock(),
        _alert_execution_pending=AsyncMock(),
        _lifecycle_write_state=MagicMock(),
        _maybe_trigger_emergency=AsyncMock(),
    )
    result = IterationResult(
        status=IterationStatus.EXECUTION_PENDING,
        execution_pending_since=datetime.now(UTC) - timedelta(seconds=age),
        execution_pending_reason=reason,
    )
    for _ in range(2):
        await handle_iteration_failure(runner, SimpleNamespace(), "deployment:test", result)
    assert runner._alert_execution_pending.await_count == alerts
    assert runner._lifecycle_write_state.call_count == alerts
    assert runner._consecutive_errors == 0
    runner._circuit_breaker.record_failure.assert_not_called()
    runner._maybe_trigger_emergency.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_operator_card_is_dispatched():
    from almanak.framework.models.operator_card import EventType
    from almanak.framework.runner.runner_alerts import RunnerAlerter

    runner = SimpleNamespace(
        config=SimpleNamespace(enable_alerting=True), alert_manager=SimpleNamespace(send_alert=AsyncMock())
    )
    result = IterationResult(status=IterationStatus.EXECUTION_PENDING, execution_pending_since=datetime.now(UTC))
    await RunnerAlerter(runner).alert_execution_pending(SimpleNamespace(deployment_id="deployment:test"), result)
    card = runner.alert_manager.send_alert.call_args.args[0]
    assert card.event_type is EventType.STUCK
    assert not card.has_auto_remediation
    assert all(action.value not in {"RESUME", "EMERGENCY_UNWIND", "CANCEL_TX"} for action in card.available_actions)


def test_success_clears_pending_error_lifecycle_without_error_streak():
    from almanak.core.lifecycle import LifecycleState
    from almanak.framework.runner._run_loop_helpers import handle_iteration_success

    runner = SimpleNamespace(
        _execution_pending_alert_at=datetime.now(UTC),
        _shutdown_requested=False,
        _terminal_lifecycle_state=None,
        _lifecycle_write_state=MagicMock(),
        config=SimpleNamespace(max_consecutive_errors=5),
        _circuit_breaker=None,
    )
    handle_iteration_success(runner, "deployment:test", False)
    runner._lifecycle_write_state.assert_called_once_with("deployment:test", LifecycleState.RUNNING)
    assert runner._execution_pending_alert_at is None
