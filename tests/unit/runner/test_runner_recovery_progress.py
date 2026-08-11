"""Durability contract for execution-progress replay markers."""

import asyncio
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.runner.runner_models import ExecutionProgress
from almanak.framework.runner.runner_recovery import (
    clear_execution_progress,
    load_execution_progress,
    save_execution_progress,
)
from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.state.state_manager import StateConflictError, StateData, StateManager, StateManagerConfig
from almanak.framework.state.strategy_state import STRATEGY_USER_STATE_KEY
from almanak.framework.strategies.intent_strategy import IntentStrategy
from almanak.framework.strategies.lp_position_tracker import PERSISTENT_STATE_KEY, LPPositionTracker


class _AccessorHidingIntentStrategy(IntentStrategy):
    """Production-shaped strategy proving framework state ignores overrides."""

    def decide(self, market):
        return None

    def generate_teardown_intents(self, mode, market=None):
        return []

    def get_open_positions(self):
        return []

    def get_persistent_state(self):
        return {"position_open": True}

    @property
    def lp_position_tracker(self):
        # A user override may hide the public tooling accessor, but it must not
        # hide framework-owned state from the runner's durability boundary.
        return None


def _progress(deployment_id: str = "deployment:replay-safe") -> ExecutionProgress:
    return ExecutionProgress(
        execution_id="tx-pending",
        deployment_id=deployment_id,
        intents_hash="broadcast-pending",
        total_steps=1,
        failure_error="BROADCAST_RECONCILIATION_REQUIRED: outcome pending",
        reconciliation_required_step_index=0,
    )


@pytest.mark.asyncio
async def test_execution_progress_round_trips_through_real_sqlite(tmp_path) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "progress.sqlite")))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    runner = SimpleNamespace(state_manager=manager)
    progress = _progress()
    try:
        await save_execution_progress(runner, progress.deployment_id, progress)
        loaded = await load_execution_progress(runner, progress.deployment_id)
        assert loaded is not None
        assert loaded.is_reconciliation_required

        await clear_execution_progress(runner, progress.deployment_id)
        assert await load_execution_progress(runner, progress.deployment_id) is None
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_execution_progress_cas_retry_preserves_concurrent_runner_overlay(tmp_path, monkeypatch) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "progress-conflict.sqlite")))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    deployment_id = "deployment:progress-conflict"
    await manager.save_state(StateData(deployment_id=deployment_id, version=1, state={}))
    real_save = manager.save_state
    injected = False

    async def _conflict_once(state, expected_version=None):
        nonlocal injected
        if not injected:
            injected = True
            concurrent = await manager.load_state(deployment_id)
            concurrent.state["recovered_sessions"] = ["concurrent-session"]
            await real_save(concurrent, expected_version=concurrent.version)
            raise StateConflictError(deployment_id, expected_version, concurrent.version + 1)
        return await real_save(state, expected_version=expected_version)

    monkeypatch.setattr(manager, "save_state", _conflict_once)
    progress = _progress(deployment_id)
    progress.execution_id = "0xknown-after-submit"
    try:
        await save_execution_progress(SimpleNamespace(state_manager=manager), deployment_id, progress)
        durable = await manager.load_state(deployment_id)
        assert durable.state["execution_progress"]["execution_id"] == "0xknown-after-submit"
        assert durable.state["recovered_sessions"] == ["concurrent-session"]
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_sqlite_marker_write_failure_propagates(tmp_path, monkeypatch) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "write-failure.sqlite")))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    monkeypatch.setattr(store, "save", AsyncMock(side_effect=sqlite3.OperationalError("disk full")))
    runner = SimpleNamespace(state_manager=manager)
    try:
        with pytest.raises(sqlite3.OperationalError, match="disk full"):
            await save_execution_progress(runner, "deployment:sqlite-write", _progress("deployment:sqlite-write"))
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_sqlite_marker_read_failure_is_not_misreported_as_absent(tmp_path, monkeypatch) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "read-failure.sqlite")))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    monkeypatch.setattr(store, "get", AsyncMock(side_effect=sqlite3.OperationalError("database locked")))
    runner = SimpleNamespace(state_manager=manager)
    try:
        with pytest.raises(sqlite3.OperationalError, match="database locked"):
            await load_execution_progress(runner, "deployment:sqlite-read")
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_hosted_gateway_marker_write_failure_propagates() -> None:
    client = MagicMock()
    client.state.LoadState.return_value = SimpleNamespace(deployment_id="")
    client.state.SaveState.return_value = SimpleNamespace(
        success=False,
        error="Postgres unavailable",
        new_version=0,
    )
    runner = SimpleNamespace(state_manager=GatewayStateManager(client))

    with pytest.raises(RuntimeError, match="Postgres unavailable"):
        await save_execution_progress(runner, "deployment:hosted-write", _progress("deployment:hosted-write"))


@pytest.mark.asyncio
async def test_hosted_gateway_aborted_conflict_reloads_overlay_and_persists_marker() -> None:
    class _Aborted(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.ABORTED

    deployment_id = "deployment:hosted-conflict"

    def _loaded(version: int, state: dict) -> SimpleNamespace:
        return SimpleNamespace(
            deployment_id=deployment_id,
            version=version,
            data=json.dumps(state).encode(),
            schema_version=1,
            checksum="",
            created_at=0,
        )

    client = MagicMock()
    client.state.LoadState.side_effect = [
        _loaded(1, {"recovered_sessions": ["old"]}),
        _loaded(2, {"recovered_sessions": ["concurrent-session"]}),
    ]
    client.state.SaveState.side_effect = [
        _Aborted(),
        SimpleNamespace(success=True, error="", new_version=3, checksum=""),
    ]
    progress = _progress(deployment_id)
    progress.execution_id = "0xknown-after-submit"

    await save_execution_progress(
        SimpleNamespace(state_manager=GatewayStateManager(client)),
        deployment_id,
        progress,
    )

    assert client.state.SaveState.call_count == 2
    final_request = client.state.SaveState.call_args_list[-1].args[0]
    durable = json.loads(final_request.data.decode())
    assert durable["execution_progress"]["execution_id"] == "0xknown-after-submit"
    assert durable["recovered_sessions"] == ["concurrent-session"]


@pytest.mark.asyncio
async def test_hosted_gateway_non_conflict_rpc_error_propagates_unchanged() -> None:
    class _Unavailable(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    transport_error = _Unavailable()
    client = MagicMock()
    client.state.LoadState.return_value = SimpleNamespace(deployment_id="")
    client.state.SaveState.side_effect = transport_error

    with pytest.raises(grpc.RpcError) as raised:
        await save_execution_progress(
            SimpleNamespace(state_manager=GatewayStateManager(client)),
            "deployment:hosted-unavailable",
            _progress("deployment:hosted-unavailable"),
        )

    assert raised.value is transport_error


@pytest.mark.asyncio
async def test_hosted_gateway_marker_read_failure_propagates() -> None:
    client = MagicMock()
    client.state.LoadState.side_effect = RuntimeError("gateway transport unavailable")
    runner = SimpleNamespace(state_manager=GatewayStateManager(client))

    with pytest.raises(RuntimeError, match="gateway transport unavailable"):
        await load_execution_progress(runner, "deployment:hosted-read")


@pytest.mark.asyncio
async def test_completion_tombstone_survives_post_commit_ack_loss(tmp_path, monkeypatch) -> None:
    """A lost save ACK leaves either the old blocker or a durable tombstone, never absence."""
    config = SQLiteConfig(db_path=str(tmp_path / "post-commit-ack.sqlite"))
    store = SQLiteStore(config)
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    deployment_id = "deployment:post-commit-ack"
    runner = SimpleNamespace(state_manager=manager)
    runner._load_execution_progress = lambda dep: load_execution_progress(runner, dep)
    runner._save_execution_progress = lambda dep, progress: save_execution_progress(runner, dep, progress)
    landed = _progress(deployment_id)
    landed.mark_landed_repair_pending(0, "LANDED_REPAIR_PENDING: downstream durability pending")
    await save_execution_progress(runner, deployment_id, landed)

    committed_save = store.save

    async def _commit_then_lose_ack(state, expected_version=None):
        await committed_save(state, expected_version)
        raise ConnectionError("ACK lost after commit")

    monkeypatch.setattr(store, "save", _commit_then_lose_ack)
    with pytest.raises(ConnectionError, match="ACK lost after commit"):
        await StrategyRunner._complete_single_chain_replay_barrier(runner, deployment_id)
    await manager.close()

    restarted_store = SQLiteStore(config)
    restarted = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=restarted_store)
    await restarted.initialize()
    restarted_runner = SimpleNamespace(state_manager=restarted)
    try:
        durable = await load_execution_progress(restarted_runner, deployment_id)
        assert durable is not None
        assert durable.completed_step_index == 0
        assert durable.is_reconciliation_required is False
        assert durable.intents_hash == "landed-complete"
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_landed_strategy_state_merge_preserves_replay_marker_in_real_sqlite(tmp_path) -> None:
    """Callback state and the replay marker share one awaited durable CAS write."""
    config = SQLiteConfig(db_path=str(tmp_path / "landed-state.sqlite"))
    store = SQLiteStore(config)
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    deployment_id = "deployment:landed-state"
    progress = _progress(deployment_id)
    runner = SimpleNamespace(
        state_manager=manager,
        _flush_strategy_pending_save_strict=StrategyRunner._flush_strategy_pending_save_strict,
    )
    tracker = LPPositionTracker()
    tracker.load_persistent_dict({"uniswap_v3|avalanche|weth/usdc/30": {"position_id": "42"}})
    strategy = object.__new__(_AccessorHidingIntentStrategy)
    strategy._state_manager = manager
    strategy._pending_save = None
    strategy._lp_position_tracker = tracker
    strategy._state_version = 0
    strategy._deployment_id = deployment_id
    try:
        await save_execution_progress(runner, deployment_id, progress)
        await StrategyRunner._persist_landed_strategy_state_strict(runner, strategy, lane="test")
    finally:
        await manager.close()

    restarted_store = SQLiteStore(config)
    restarted = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=restarted_store)
    await restarted.initialize()
    try:
        durable = await restarted.load_state(deployment_id)
        assert durable.state[STRATEGY_USER_STATE_KEY]["position_open"] is True
        assert durable.state[PERSISTENT_STATE_KEY] == {"uniswap_v3|avalanche|weth/usdc/30": {"position_id": "42"}}
        assert ExecutionProgress.from_dict(durable.state["execution_progress"]).is_reconciliation_required
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_landed_async_save_failure_propagates_before_barrier_completion() -> None:
    """IntentStrategy's fire-and-forget task is awaited; its error cannot be swallowed."""
    strategy = SimpleNamespace(_state_manager=object(), _pending_save=None)

    async def _fail_save() -> None:
        await asyncio.sleep(0)
        raise OSError("durable store unavailable")

    def _schedule_save() -> None:
        strategy._pending_save = asyncio.create_task(_fail_save())

    strategy.save_state = _schedule_save
    runner = SimpleNamespace(
        state_manager=object(),
        _flush_strategy_pending_save_strict=StrategyRunner._flush_strategy_pending_save_strict,
    )

    with pytest.raises(RuntimeError, match="replay barrier retained") as raised:
        await StrategyRunner._persist_landed_strategy_state_strict(runner, strategy, lane="test")
    assert isinstance(raised.value.__cause__, OSError)
    assert strategy._pending_save is None


@pytest.mark.asyncio
async def test_prior_async_save_failure_prevents_pre_broadcast_marker() -> None:
    """A failed older save is drained before a new transaction may be armed."""

    async def _fail_save() -> None:
        raise OSError("older state write failed")

    pending = asyncio.create_task(_fail_save())
    strategy = SimpleNamespace(_pending_save=pending)

    with pytest.raises(OSError, match="older state write failed"):
        await StrategyRunner._flush_strategy_pending_save_strict(strategy)
    assert strategy._pending_save is None
