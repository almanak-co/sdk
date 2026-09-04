"""IntentStrategy state persistence across local and gateway backends."""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.state.state_manager import (
    StateData,
    StateManager,
    StateManagerConfig,
    StateNotFoundError,
)
from almanak.framework.state.strategy_state import STRATEGY_USER_STATE_KEY
from almanak.framework.strategies.intent_strategy import IntentStrategy
from almanak.framework.strategies.lp_position_tracker import LPPositionTracker

LOGGER = "almanak.framework.strategies.intent_strategy"
DEPLOYMENT_ID = "deployment:state-persistence"
TRACKER_KEY = "__framework_lp_position_tracker__"


class _StatefulStrategy(IntentStrategy):
    def __init__(self, state_manager=None, deployment_id: str = DEPLOYMENT_ID) -> None:  # type: ignore[no-untyped-def]
        self._state_manager = state_manager
        self._deployment_id = deployment_id
        self._state_version = 0
        self._pending_save = None
        self._lp_position_tracker = LPPositionTracker()
        self.user_state = {}
        self.loaded_states: list[dict] = []

    def decide(self, market):  # pragma: no cover - abstract contract only
        return None

    def get_open_positions(self):  # pragma: no cover - abstract contract only
        return []

    def generate_teardown_intents(self, mode, market=None):  # pragma: no cover - abstract contract only
        return []

    def get_persistent_state(self):
        return None if self.user_state is None else dict(self.user_state)

    def load_persistent_state(self, state):
        self.loaded_states.append(state)


async def _sqlite_manager(tmp_path, name: str) -> StateManager:
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / name)))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    return manager


class _GatewayStateService:
    def __init__(self) -> None:
        self.version = 3
        self.row = {
            "execution_progress": {"execution_id": "landed"},
            "legacy_strategy_key": "remove-on-write",
        }
        self.load_ids: list[str] = []
        self.save_requests = []

    def LoadState(self, request, timeout=None):  # noqa: N802
        self.load_ids.append(request.deployment_id)
        return SimpleNamespace(
            deployment_id=request.deployment_id,
            version=self.version,
            data=json.dumps(self.row).encode(),
            schema_version=1,
            checksum="",
            created_at=0,
        )

    def SaveState(self, request, timeout=None):  # noqa: N802
        self.save_requests.append(request)
        if request.expected_version != self.version:
            return SimpleNamespace(success=False, error="version conflict", new_version=self.version, checksum="")
        self.row = json.loads(request.data.decode())
        self.version += 1
        return SimpleNamespace(success=True, error="", new_version=self.version, checksum="gateway-checksum")


@pytest.mark.parametrize(
    ("state_manager", "deployment_id"),
    [(None, DEPLOYMENT_ID), (object(), "")],
)
def test_save_state_guard_skips_serialization(state_manager, deployment_id) -> None:
    strategy = _StatefulStrategy(state_manager, deployment_id)
    strategy.get_persistent_state = MagicMock(side_effect=AssertionError("state must not be serialized"))

    strategy.save_state()

    strategy.get_persistent_state.assert_not_called()
    assert strategy._pending_save is None


def test_save_state_sync_serializes_empty_user_and_framework_state(caplog) -> None:
    manager = MagicMock()
    manager.load_state = AsyncMock(side_effect=StateNotFoundError(DEPLOYMENT_ID))
    manager.save_state = AsyncMock(return_value=StateData(deployment_id=DEPLOYMENT_ID, version=1, state={}))
    strategy = _StatefulStrategy(manager)
    strategy.user_state = None
    caplog.set_level(logging.DEBUG, logger=LOGGER)

    strategy.save_state()

    candidate = manager.save_state.await_args.args[0]
    assert candidate.deployment_id == DEPLOYMENT_ID
    assert candidate.state[STRATEGY_USER_STATE_KEY] == {}
    assert candidate.state[TRACKER_KEY] == {}
    assert strategy._state_version == 1
    assert strategy._pending_save is None
    assert "Scheduled state save" in caplog.text


def test_save_state_sync_logs_backend_failure_without_raising(caplog) -> None:
    manager = MagicMock()
    manager.load_state = AsyncMock(side_effect=RuntimeError("gateway unavailable"))
    strategy = _StatefulStrategy(manager)
    strategy.user_state = {"phase": "open"}
    caplog.set_level(logging.WARNING, logger=LOGGER)

    strategy.save_state()

    assert strategy._state_version == 0
    assert strategy._pending_save is None
    assert "Failed to save state: gateway unavailable" in caplog.text


@pytest.mark.asyncio
async def test_save_state_queues_snapshots_without_racing_or_losing_dirty_state() -> None:
    strategy = _StatefulStrategy(object())
    strategy._lp_position_tracker = MagicMock()
    strategy._lp_position_tracker.to_persistent_dict.side_effect = [
        {"pool": {"position_id": "1"}},
        {"pool": {"position_id": "2"}},
    ]
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls = []
    active = 0
    max_active = 0

    async def persist(state_manager, deployment_id, user_state, *, framework_state):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        calls.append((state_manager, deployment_id, dict(user_state), dict(framework_state)))
        call_number = len(calls)
        try:
            if call_number == 1:
                first_started.set()
                await release_first.wait()
            return SimpleNamespace(version=call_number)
        finally:
            active -= 1

    with patch(
        "almanak.framework.state.strategy_state.replace_strategy_persistent_state",
        new=persist,
    ):
        strategy.user_state = {"phase": "first", "remove_me": True}
        strategy.save_state()
        first_save = strategy._pending_save
        await first_started.wait()

        strategy.user_state = {"phase": "second"}
        strategy.save_state()
        final_save = strategy._pending_save
        await asyncio.sleep(0)

        assert final_save is not first_save
        assert [call[2] for call in calls] == [{"phase": "first", "remove_me": True}]

        release_first.set()
        await final_save

    assert [call[2] for call in calls] == [
        {"phase": "first", "remove_me": True},
        {"phase": "second"},
    ]
    assert [call[3][TRACKER_KEY] for call in calls] == [
        {"pool": {"position_id": "1"}},
        {"pool": {"position_id": "2"}},
    ]
    assert all(call[0] is strategy._state_manager and call[1] == DEPLOYMENT_ID for call in calls)
    assert max_active == 1
    assert strategy._state_version == 2
    assert strategy._pending_save is final_save

    await strategy.flush_pending_saves()
    assert strategy._pending_save is None


@pytest.mark.asyncio
async def test_flush_pending_saves_logs_deferred_failure_and_clears_task(caplog) -> None:
    strategy = _StatefulStrategy(object())

    async def fail(*args, **kwargs):
        await asyncio.sleep(0)
        raise RuntimeError("deferred write failed")

    caplog.set_level(logging.WARNING, logger=LOGGER)
    with patch(
        "almanak.framework.state.strategy_state.replace_strategy_persistent_state",
        new=fail,
    ):
        strategy.save_state()
        assert strategy._pending_save is not None
        await strategy.flush_pending_saves()

    assert strategy._pending_save is None
    assert "Pending save failed during flush: deferred write failed" in caplog.text


@pytest.mark.asyncio
async def test_flush_pending_saves_logs_failure_from_completed_task(caplog) -> None:
    strategy = _StatefulStrategy()

    async def fail() -> None:
        raise RuntimeError("completed write failed")

    task = asyncio.create_task(fail())
    await asyncio.sleep(0)
    assert task.done()
    strategy._pending_save = task
    caplog.set_level(logging.WARNING, logger=LOGGER)

    await strategy.flush_pending_saves()

    assert strategy._pending_save is None
    assert "Pending save had error: completed write failed" in caplog.text


@pytest.mark.parametrize(
    ("state_manager", "deployment_id"),
    [(None, DEPLOYMENT_ID), (MagicMock(), "")],
)
def test_load_state_guard_skips_backend(state_manager, deployment_id) -> None:
    strategy = _StatefulStrategy(state_manager, deployment_id)

    assert strategy.load_state() is False

    if state_manager is not None:
        state_manager.load_state.assert_not_called()


@pytest.mark.asyncio
async def test_load_state_sync_refuses_to_block_running_event_loop(caplog) -> None:
    manager = MagicMock()
    manager.load_state = AsyncMock()
    strategy = _StatefulStrategy(manager)
    caplog.set_level(logging.DEBUG, logger=LOGGER)

    assert strategy.load_state() is False

    manager.load_state.assert_not_called()
    assert "Cannot load state synchronously in async context" in caplog.text


@pytest.mark.asyncio
async def test_load_state_sync_restores_legacy_local_state_and_framework_tracker(tmp_path, caplog) -> None:
    manager = await _sqlite_manager(tmp_path, "intent-strategy-load.sqlite")
    tracker_state = {"uniswap_v3|ethereum|weth/usdc/30": {"position_id": "42"}}
    saved = await manager.save_state(
        StateData(
            deployment_id=DEPLOYMENT_ID,
            version=1,
            state={
                "phase": "open",
                "ratio": 1.23456789,
                "long_value": "x" * 100,
                "execution_progress": {"execution_id": "landed"},
                TRACKER_KEY: tracker_state,
            },
        )
    )
    strategy = _StatefulStrategy(manager)
    caplog.set_level(logging.INFO, logger=LOGGER)
    try:
        loaded = await asyncio.to_thread(strategy.load_state)

        assert loaded is True
        assert strategy.loaded_states == [{"phase": "open", "ratio": 1.23456789, "long_value": "x" * 100}]
        assert strategy.lp_position_tracker.to_persistent_dict() == tracker_state
        assert strategy._state_version == saved.version
        assert "'ratio': '1.23457'" in caplog.text
        assert "x" * 80 in caplog.text
        assert "x" * 81 not in caplog.text
    finally:
        await manager.close()


@pytest.mark.parametrize("state_data", [None, SimpleNamespace(state={}, version=9)])
def test_load_state_returns_false_for_absent_or_empty_row(state_data) -> None:
    manager = MagicMock()
    manager.load_state = AsyncMock(return_value=state_data)
    strategy = _StatefulStrategy(manager)

    assert strategy.load_state() is False
    assert strategy.loaded_states == []
    assert strategy._state_version == 0
    manager.load_state.assert_awaited_once_with(DEPLOYMENT_ID)


@pytest.mark.parametrize(
    ("error", "level", "message"),
    [
        (StateNotFoundError(DEPLOYMENT_ID), logging.DEBUG, f"No existing state for {DEPLOYMENT_ID}"),
        (RuntimeError("transport failed"), logging.WARNING, "Failed to load state: transport failed"),
    ],
)
def test_load_state_preserves_not_found_and_failure_logging(error, level, message, caplog) -> None:
    manager = MagicMock()
    manager.load_state = AsyncMock(side_effect=error)
    strategy = _StatefulStrategy(manager)
    caplog.set_level(level, logger=LOGGER)

    assert strategy.load_state() is False

    assert message in caplog.text
    assert strategy._state_version == 0


def test_load_state_logs_restore_failure_and_keeps_prior_version(caplog) -> None:
    manager = MagicMock()
    manager.load_state = AsyncMock(
        return_value=SimpleNamespace(
            state={STRATEGY_USER_STATE_KEY: {"phase": "invalid"}},
            version=8,
        )
    )
    strategy = _StatefulStrategy(manager)
    strategy._state_version = 2
    strategy.load_persistent_state = MagicMock(side_effect=ValueError("invalid strategy state"))
    caplog.set_level(logging.WARNING, logger=LOGGER)

    assert strategy.load_state() is False

    assert strategy._state_version == 2
    assert "Failed to load state: invalid strategy state" in caplog.text


@pytest.mark.asyncio
async def test_gateway_round_trip_serializes_state_and_preserves_identity_and_runner_overlay() -> None:
    service = _GatewayStateService()
    manager = GatewayStateManager(SimpleNamespace(state=service))
    strategy = _StatefulStrategy(manager)
    tracker_state = {"uniswap_v3|ethereum|weth/usdc/30": {"position_id": "42"}}
    strategy._lp_position_tracker.load_persistent_dict(tracker_state)
    strategy.user_state = {"phase": "open", "amount": Decimal("1.25")}

    strategy.save_state()
    await strategy.flush_pending_saves()

    assert service.row[STRATEGY_USER_STATE_KEY] == {"phase": "open", "amount": "1.25"}
    assert service.row[TRACKER_KEY] == tracker_state
    assert service.row["execution_progress"] == {"execution_id": "landed"}
    assert "legacy_strategy_key" not in service.row
    assert service.save_requests[0].deployment_id == DEPLOYMENT_ID
    assert service.save_requests[0].expected_version == 3
    assert strategy._state_version == 4

    restarted = _StatefulStrategy(manager)
    assert await restarted.load_state_async() is True
    assert restarted.loaded_states == [{"phase": "open", "amount": "1.25"}]
    assert restarted.lp_position_tracker.to_persistent_dict() == tracker_state
    assert restarted._state_version == 4
    assert service.load_ids == [DEPLOYMENT_ID, DEPLOYMENT_ID]
