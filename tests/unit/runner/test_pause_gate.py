"""Tests for operator pause gate behavior in StrategyRunner."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import IterationStatus, StrategyRunner


@pytest.mark.asyncio
async def test_is_strategy_paused_reads_persisted_state() -> None:
    """_is_strategy_paused returns true and reason when state has pause flags."""
    state_manager = MagicMock()
    state_manager.load_state = AsyncMock(
        return_value=SimpleNamespace(
            state={
                "is_paused": True,
                "pause_reason": "Operator requested pause",
            }
        )
    )

    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
    )

    paused, reason = await runner._is_strategy_paused("test_strategy")
    assert paused is True
    assert reason == "Operator requested pause"


@pytest.mark.asyncio
async def test_run_iteration_returns_hold_when_paused() -> None:
    """run_iteration exits early with HOLD when strategy is paused."""
    state_manager = MagicMock()
    state_manager.load_state = AsyncMock(
        return_value=SimpleNamespace(
            state={
                "is_paused": True,
                "pause_reason": "Paused from dashboard",
            }
        )
    )

    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
    )

    strategy = SimpleNamespace(strategy_id="test_strategy")
    result = await runner.run_iteration(strategy)

    assert result.status == IterationStatus.HOLD
    assert result.intent is not None
    assert "Paused from dashboard" in result.intent.reason
