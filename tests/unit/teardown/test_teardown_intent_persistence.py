"""Tests for per-intent state persistence during teardown.

Validates that after each successful teardown intent:
1. on_intent_executed is called with the correct args
2. save_state and flush_pending_saves are called
3. Exceptions during persistence are logged without halting the teardown loop
4. The loop continues to process remaining intents after a persistence failure
"""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
)
from almanak.framework.teardown.slippage_manager import ExecutionResult
from almanak.framework.teardown.teardown_manager import TeardownManager


def _make_successful_exec_result():
    """Create a successful ExecutionResult from the slippage manager."""
    return ExecutionResult(
        success=True,
        final_slippage=Decimal("0.005"),
        status="completed",
        attempts=[],
    )


def _make_strategy(intents, *, persistence_error=None):
    """Create a mock strategy with teardown and persistence methods."""
    strategy = MagicMock()
    strategy.deployment_id = "test_persist"
    strategy.name = "PersistTestStrategy"
    strategy.chain = "base"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()

    positions = MagicMock(spec=TeardownPositionSummary)
    positions.positions = [MagicMock()]
    positions.total_value_usd = Decimal("100")
    positions.has_liquidation_risk = False
    positions.chains_involved = {"base"}
    strategy.get_open_positions.return_value = positions

    strategy.generate_teardown_intents.return_value = intents

    # Wire persistence methods
    if persistence_error:
        strategy.on_intent_executed.side_effect = persistence_error
    else:
        strategy.on_intent_executed.return_value = None  # sync return
    strategy.save_state.return_value = None
    strategy.flush_pending_saves = AsyncMock()

    return strategy


def _make_intent(intent_type="SWAP"):
    """Create a minimal mock intent."""
    intent = MagicMock()
    intent.intent_type = intent_type
    # Return a JSON-serializable dict for _persist_state
    intent.to_dict.return_value = {"intent_type": intent_type}
    # No max_slippage so cloning is skipped
    del intent.max_slippage
    return intent


@pytest.mark.asyncio
async def test_persistence_called_after_successful_intent():
    """on_intent_executed, save_state, flush_pending_saves are called on success."""
    intent = _make_intent()
    strategy = _make_strategy([intent])

    manager = TeardownManager()

    with patch.object(
        manager.slippage_manager,
        "execute_with_escalation",
        new=AsyncMock(return_value=_make_successful_exec_result()),
    ):
        await manager.execute(strategy=strategy, mode="graceful")

    strategy.on_intent_executed.assert_called_once()
    call_args = strategy.on_intent_executed.call_args
    assert call_args[0][0] is intent  # intent arg
    assert call_args[0][1] is True  # success=True
    strategy.save_state.assert_called_once()
    strategy.flush_pending_saves.assert_awaited_once()


@pytest.mark.asyncio
async def test_persistence_error_does_not_halt_loop():
    """If persistence raises, the teardown loop continues with remaining intents."""
    intents = [_make_intent("REPAY"), _make_intent("SWAP")]
    strategy = _make_strategy(intents, persistence_error=RuntimeError("DB write failed"))

    manager = TeardownManager()

    with patch.object(
        manager.slippage_manager,
        "execute_with_escalation",
        new=AsyncMock(return_value=_make_successful_exec_result()),
    ):
        result = await manager.execute(strategy=strategy, mode="graceful")

    # Both intents should have been attempted despite first persistence failure
    assert strategy.on_intent_executed.call_count == 2


@pytest.mark.asyncio
async def test_persistence_error_is_logged():
    """Persistence failure is logged with intent index and total count."""
    intent = _make_intent()
    strategy = _make_strategy([intent], persistence_error=RuntimeError("flush timeout"))

    manager = TeardownManager()

    with (
        patch.object(
            manager.slippage_manager,
            "execute_with_escalation",
            new=AsyncMock(return_value=_make_successful_exec_result()),
        ),
        patch("almanak.framework.teardown.teardown_manager.logger") as mock_logger,
    ):
        await manager.execute(strategy=strategy, mode="graceful")

    mock_logger.error.assert_any_call(
        "Failed to persist strategy state after teardown intent %d/%d: %s "
        "(on-chain action succeeded but persisted state may be stale)",
        1,
        1,
        strategy.on_intent_executed.side_effect,
    )


@pytest.mark.asyncio
async def test_async_on_intent_executed_is_awaited():
    """If on_intent_executed returns a coroutine, it is awaited."""
    intent = _make_intent()
    strategy = _make_strategy([intent])
    # Make on_intent_executed return a coroutine
    callback_called = False

    async def async_callback(i, success, result):
        nonlocal callback_called
        callback_called = True

    strategy.on_intent_executed.side_effect = async_callback

    manager = TeardownManager()

    with patch.object(
        manager.slippage_manager,
        "execute_with_escalation",
        new=AsyncMock(return_value=_make_successful_exec_result()),
    ):
        await manager.execute(strategy=strategy, mode="graceful")

    assert callback_called
    strategy.save_state.assert_called_once()
    strategy.flush_pending_saves.assert_awaited_once()
