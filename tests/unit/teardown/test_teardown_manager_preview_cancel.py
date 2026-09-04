"""Direct contracts for teardown preview and cancellation."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


def _strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "deployment:preview"
    strategy.name = "Preview Strategy"
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=strategy.deployment_id,
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.LP,
                position_id="position-1",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("10000"),
            )
        ],
    )
    strategy.generate_teardown_intents.return_value = [object()]
    return strategy


def _preview_manager() -> TeardownManager:
    manager = TeardownManager()
    manager.safety_guard.calculate_estimated_return_range = MagicMock(return_value=(Decimal("9600"), Decimal("9900")))
    manager._estimate_duration = MagicMock(return_value=4)
    manager._generate_warnings = MagicMock(return_value=["warning"])
    manager._serialize_position = MagicMock(return_value={"position_id": "position-1"})
    manager._describe_intent = MagicMock(return_value="Close LP position")
    return manager


@pytest.mark.asyncio
async def test_preview_preserves_identity_protection_and_rendering_contract() -> None:
    strategy = _strategy()
    manager = _preview_manager()
    market = object()

    preview = await manager.preview(strategy, "graceful", market=market)

    assert preview.deployment_id == "deployment:preview"
    assert preview.strategy_name == "Preview Strategy"
    assert preview.mode == "graceful"
    assert preview.positions == [{"position_id": "position-1"}]
    assert preview.current_value_usd == Decimal("10000")
    assert preview.protected_minimum_usd == Decimal("9700")
    assert preview.max_loss_percent == Decimal("0.03")
    assert preview.max_loss_usd == Decimal("300")
    assert preview.estimated_return_min_usd == Decimal("9600")
    assert preview.estimated_return_max_usd == Decimal("9900")
    assert preview.estimated_duration_minutes == 4
    assert preview.steps == ["Close LP position"]
    assert preview.warnings == ["warning"]
    strategy.generate_teardown_intents.assert_called_once_with(TeardownMode.SOFT, market=market)


@pytest.mark.asyncio
async def test_preview_retries_only_the_legacy_missing_market_parameter() -> None:
    strategy = _strategy()
    manager = _preview_manager()
    intent = object()
    market = object()

    def generate(mode: TeardownMode, **kwargs: object) -> list[object]:
        if "market" in kwargs:
            raise TypeError("generate_teardown_intents() got an unexpected keyword argument 'market'")
        return [intent]

    strategy.generate_teardown_intents.side_effect = generate

    preview = await manager.preview(strategy, "graceful", market=market)

    assert preview.steps == ["Close LP position"]
    assert strategy.generate_teardown_intents.call_args_list == [
        call(TeardownMode.SOFT, market=market),
        call(TeardownMode.SOFT),
    ]


@pytest.mark.asyncio
async def test_preview_propagates_internal_typeerror_that_merely_mentions_market() -> None:
    strategy = _strategy()
    strategy.generate_teardown_intents.side_effect = TypeError("market snapshot is malformed")

    with pytest.raises(TypeError, match="market snapshot is malformed"):
        await _preview_manager().preview(strategy, "graceful", market=object())

    strategy.generate_teardown_intents.assert_called_once()


def _state(
    *,
    mode: TeardownMode = TeardownMode.SOFT,
    status: TeardownStatus = TeardownStatus.PENDING,
    completed_intents: int = 0,
    cancel_window_until: datetime | None = None,
) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="teardown-1",
        deployment_id="deployment:cancel",
        mode=mode,
        status=status,
        total_intents=2,
        completed_intents=completed_intents,
        current_intent_index=completed_intents,
        started_at=now,
        updated_at=now,
        cancel_window_until=cancel_window_until,
    )


def _cancel_manager(state: TeardownState | None) -> tuple[TeardownManager, MagicMock]:
    state_manager = MagicMock()
    state_manager.get_teardown_state = AsyncMock(return_value=state)
    state_manager.save_teardown_state = AsyncMock()
    return TeardownManager(state_manager=state_manager), state_manager


@pytest.mark.asyncio
async def test_cancel_without_state_manager_returns_false() -> None:
    assert await TeardownManager().cancel("deployment:cancel") is False


@pytest.mark.asyncio
async def test_cancel_missing_deployment_state_returns_false_without_persisting() -> None:
    manager, state_manager = _cancel_manager(None)

    assert await manager.cancel("deployment:cancel") is False
    state_manager.get_teardown_state.assert_awaited_once_with("deployment:cancel")
    state_manager.save_teardown_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_rejects_expired_emergency_window_without_persisting() -> None:
    state = _state(
        mode=TeardownMode.HARD,
        status=TeardownStatus.CANCEL_WINDOW,
        cancel_window_until=datetime.now(UTC) - timedelta(seconds=1),
    )
    manager, state_manager = _cancel_manager(state)

    with pytest.raises(ValueError, match="^Cancel window has expired for emergency teardown$"):
        await manager.cancel("deployment:cancel")

    state_manager.save_teardown_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_pauses_started_emergency_teardown_and_persists_same_state() -> None:
    state = _state(
        mode=TeardownMode.HARD,
        status=TeardownStatus.EXECUTING,
        completed_intents=1,
        cancel_window_until=datetime.now(UTC) + timedelta(minutes=1),
    )
    manager, state_manager = _cancel_manager(state)

    assert await manager.cancel("deployment:cancel") is True
    assert state.status == TeardownStatus.PAUSED
    state_manager.save_teardown_state.assert_awaited_once_with(state)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [TeardownStatus.PENDING, TeardownStatus.EXECUTING])
async def test_cancel_soft_teardown_before_any_completed_intent_persists_full_cancel(
    status: TeardownStatus,
) -> None:
    state = _state(status=status)
    manager, state_manager = _cancel_manager(state)

    assert await manager.cancel("deployment:cancel") is True
    assert state.status == TeardownStatus.CANCELLED
    state_manager.save_teardown_state.assert_awaited_once_with(state)
