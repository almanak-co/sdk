"""Tests for StrategyRunner sadflow retry policy."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

from almanak.framework.intents.state_machine import IntentState, SadflowActionType, SadflowContext
from almanak.framework.runner.strategy_runner import StrategyRunner


def _make_runner() -> StrategyRunner:
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )


def _make_context(error_message: str, error_type: str | None) -> SadflowContext:
    return SadflowContext(
        intent_id="intent-123",
        intent_type="SWAP",
        error_message=error_message,
        error_type=error_type,
        attempt_number=1,
        max_attempts=3,
        state=IntentState.SADFLOW_SWAP,
        started_at=datetime.now(UTC),
        total_duration_seconds=0.25,
    )


def test_abort_retries_for_insufficient_funds() -> None:
    runner = _make_runner()
    context = _make_context(
        "Insufficient ETH: need 4811520000000, have 0 (deficit: 4811520000000)",
        "INSUFFICIENT_FUNDS",
    )

    action = runner._on_sadflow_enter("INSUFFICIENT_FUNDS", 1, context)

    assert action is not None
    assert action.action_type == SadflowActionType.ABORT
    assert action.reason == context.error_message


def test_keep_default_retry_for_timeout() -> None:
    runner = _make_runner()
    context = _make_context(
        "Transaction confirmation timeout after 60s",
        "TIMEOUT",
    )

    action = runner._on_sadflow_enter("TIMEOUT", 1, context)

    assert action is None
