"""Shared strategy/runner row durability contracts."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.state.state_manager import StateConflictError, StateData, StateManager, StateManagerConfig
from almanak.framework.state.strategy_state import (
    RUNNER_OWNED_STATE_KEYS,
    STATE_OWNERSHIP_VERSION,
    STATE_OWNERSHIP_VERSION_KEY,
    STRATEGY_USER_STATE_KEY,
    StateValuePreconditionError,
    compare_and_delete_state_value,
    compare_and_replace_state_value,
    replace_strategy_persistent_state,
    runner_state_value,
    split_strategy_persistent_state,
)
from almanak.framework.strategies.intent_strategy import IntentStrategy
from almanak.framework.strategies.lp_position_tracker import LPPositionTracker


class _PersistentStrategy(IntentStrategy):
    def decide(self, market):
        return None

    def generate_teardown_intents(self, mode, market=None):
        return []

    def get_open_positions(self):
        return []

    def get_persistent_state(self):
        return dict(self.user_state)

    @property
    def lp_position_tracker(self):
        # The public tooling accessor is deliberately hostile; persistence must
        # use the final framework method and private tracker slot.
        return None


async def _sqlite_manager(tmp_path, name: str) -> StateManager:
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / name)))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=store)
    await manager.initialize()
    return manager


@pytest.mark.asyncio
async def test_sqlite_user_replacement_preserves_runner_overlay_and_deletes_keys(tmp_path) -> None:
    manager = await _sqlite_manager(tmp_path, "replacement.sqlite")
    deployment_id = "deployment:replacement"
    marker = {"intents_hash": "landed-accounting-pending"}
    legacy = StateData(
        deployment_id=deployment_id,
        version=1,
        state={"legacy_position": "open", "execution_progress": marker, "total_iterations": 7},
    )
    try:
        await manager.save_state(legacy)
        legacy_user, _ = split_strategy_persistent_state(legacy.state)
        assert legacy_user == {"legacy_position": "open"}

        await replace_strategy_persistent_state(
            manager,
            deployment_id,
            {"keep": 1, "delete_me": 2},
        )
        await replace_strategy_persistent_state(manager, deployment_id, {"keep": 3})

        durable = await manager.load_state(deployment_id)
        assert durable.state[STRATEGY_USER_STATE_KEY] == {"keep": 3}
        assert durable.state["execution_progress"] == marker
        assert durable.state["total_iterations"] == 7
        assert "legacy_position" not in durable.state
        assert durable.state[STATE_OWNERSHIP_VERSION_KEY] == STATE_OWNERSHIP_VERSION
        user_state, _ = split_strategy_persistent_state(durable.state)
        assert user_state == {"keep": 3}
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_intent_strategy_saves_chain_without_wiping_replay_marker(tmp_path) -> None:
    manager = await _sqlite_manager(tmp_path, "ordinary-save.sqlite")
    deployment_id = "deployment:ordinary"
    marker = {"intents_hash": "landed-accounting-pending"}
    await manager.save_state(StateData(deployment_id=deployment_id, version=1, state={"execution_progress": marker}))
    strategy = object.__new__(_PersistentStrategy)
    strategy._state_manager = manager
    strategy._deployment_id = deployment_id
    strategy._state_version = 1
    strategy._pending_save = None
    strategy._lp_position_tracker = LPPositionTracker()
    strategy._lp_position_tracker.load_persistent_dict({"uniswap_v3|avalanche|weth/usdc/30": {"position_id": "42"}})
    strategy.user_state = {"keep": 1, "delete_me": True}
    try:
        strategy.save_state()
        strategy.user_state = {"keep": 2}
        strategy.save_state()
        await strategy._pending_save

        durable = await manager.load_state(deployment_id)
        assert durable.state[STRATEGY_USER_STATE_KEY] == {"keep": 2}
        assert durable.state["execution_progress"] == marker
        assert durable.state["__framework_lp_position_tracker__"] == {
            "uniswap_v3|avalanche|weth/usdc/30": {"position_id": "42"}
        }
        assert durable.version == 3
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_sqlite_cas_retry_reloads_concurrent_runner_overlay(tmp_path, monkeypatch) -> None:
    manager = await _sqlite_manager(tmp_path, "conflict.sqlite")
    deployment_id = "deployment:conflict"
    await manager.save_state(
        StateData(
            deployment_id=deployment_id,
            version=1,
            state={"execution_progress": {"intents_hash": "broadcast-pending"}},
        )
    )
    real_save = manager.save_state
    injected = False

    async def _conflict_once(state, expected_version=None):
        nonlocal injected
        if not injected:
            injected = True
            concurrent = await manager.load_state(deployment_id)
            concurrent.state["execution_progress"] = {"intents_hash": "landed-accounting-pending"}
            concurrent.state["recovered_sessions"] = ["new"]
            await real_save(concurrent, expected_version=concurrent.version)
            raise StateConflictError(deployment_id, expected_version, concurrent.version + 1)
        return await real_save(state, expected_version=expected_version)

    monkeypatch.setattr(manager, "save_state", _conflict_once)
    try:
        await replace_strategy_persistent_state(manager, deployment_id, {"phase": "complete"})
        durable = await manager.load_state(deployment_id)
        assert durable.state[STRATEGY_USER_STATE_KEY] == {"phase": "complete"}
        assert durable.state["execution_progress"] == {"intents_hash": "landed-accounting-pending"}
        assert durable.state["recovered_sessions"] == ["new"]
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_cas_retry_is_bounded() -> None:
    manager = MagicMock()

    async def _load(_deployment_id):
        return StateData(deployment_id="deployment:bounded", version=4, state={})

    calls = 0

    async def _save(_state, expected_version=None):
        nonlocal calls
        calls += 1
        raise StateConflictError("deployment:bounded", expected_version, expected_version + 1)

    manager.load_state = _load
    manager.save_state = _save
    with pytest.raises(StateConflictError):
        await replace_strategy_persistent_state(manager, "deployment:bounded", {}, max_attempts=3)
    assert calls == 3


class _HostedStateService:
    def __init__(self) -> None:
        self.version = 1
        self.row = {
            "execution_progress": {"intents_hash": "landed-accounting-pending"},
            "copy_trading_state": {"cursor": 7},
        }

    def LoadState(self, request, timeout=None):
        return SimpleNamespace(
            deployment_id=request.deployment_id,
            version=self.version,
            data=json.dumps(self.row).encode(),
            schema_version=1,
            checksum="",
            created_at=0,
        )

    def SaveState(self, request, timeout=None):
        if request.expected_version != self.version:
            return SimpleNamespace(success=False, error="version conflict", new_version=self.version, checksum="")
        self.row = json.loads(request.data.decode())
        self.version += 1
        return SimpleNamespace(success=True, error="", new_version=self.version, checksum="")


@pytest.mark.asyncio
async def test_gateway_boundary_preserves_hosted_runner_overlay() -> None:
    service = _HostedStateService()
    client = SimpleNamespace(state=service)
    manager = GatewayStateManager(client)

    saved = await replace_strategy_persistent_state(
        manager,
        "deployment:hosted",
        {"position": "open"},
        framework_state={"__framework_lp_position_tracker__": {"pool": {"position_id": "42"}}},
    )

    assert saved.version == 2
    assert service.row[STRATEGY_USER_STATE_KEY] == {"position": "open"}
    assert service.row["execution_progress"] == {"intents_hash": "landed-accounting-pending"}
    assert service.row["copy_trading_state"] == {"cursor": 7}
    assert service.row["__framework_lp_position_tracker__"] == {"pool": {"position_id": "42"}}

    released = await compare_and_delete_state_value(
        manager,
        "deployment:hosted",
        "execution_progress",
        {"intents_hash": "landed-accounting-pending"},
    )
    assert released.version == 3
    assert "execution_progress" not in service.row
    assert service.row[STRATEGY_USER_STATE_KEY] == {"position": "open"}
    assert service.row["copy_trading_state"] == {"cursor": 7}


@pytest.mark.asyncio
async def test_compare_replace_revalidates_exact_marker_and_preserves_overlay(tmp_path) -> None:
    manager = await _sqlite_manager(tmp_path, "replace-marker.sqlite")
    deployment_id = "deployment:replace-marker"
    marker = {"execution_id": "tx-1", "barrier_phase": "landed_repair_pending"}
    await manager.save_state(
        StateData(
            deployment_id=deployment_id,
            version=1,
            state={"execution_progress": marker, "copy_trading_state": {"cursor": 1}},
        )
    )
    try:
        sealed = {"execution_id": "tx-1", "barrier_phase": "completed"}
        await compare_and_replace_state_value(manager, deployment_id, "execution_progress", marker, sealed)
        durable = await manager.load_state(deployment_id)
        assert durable.state["execution_progress"] == sealed
        assert durable.state["copy_trading_state"] == {"cursor": 1}
        with pytest.raises(StateValuePreconditionError, match="changed"):
            await compare_and_replace_state_value(manager, deployment_id, "execution_progress", marker, {})
    finally:
        await manager.close()


def test_unversioned_dual_vault_prefers_envelope_then_v1_uses_top_level() -> None:
    row = {
        "vault_state": {"settlement_nonce": 1},
        STRATEGY_USER_STATE_KEY: {"vault_state": {"settlement_nonce": 2}},
    }
    assert runner_state_value(row, "vault_state") == {"settlement_nonce": 2}
    row[STATE_OWNERSHIP_VERSION_KEY] = STATE_OWNERSHIP_VERSION
    assert runner_state_value(row, "vault_state") == {"settlement_nonce": 1}


@pytest.mark.asyncio
async def test_migration_promotes_vault_and_removes_stale_user_duplicates(tmp_path) -> None:
    manager = await _sqlite_manager(tmp_path, "vault-migration.sqlite")
    deployment_id = "deployment:vault-migration"
    await manager.save_state(
        StateData(
            deployment_id=deployment_id,
            version=1,
            state={
                "vault_state": {"settlement_nonce": 1},
                "position": "legacy",
                STRATEGY_USER_STATE_KEY: {
                    "vault_state": {"settlement_nonce": 2},
                    "position": "current",
                },
            },
        )
    )
    try:
        await replace_strategy_persistent_state(
            manager,
            deployment_id,
            {"vault_state": {"settlement_nonce": 2}, "position": "current"},
        )
        durable = await manager.load_state(deployment_id)
        assert durable.state["vault_state"] == {"settlement_nonce": 2}
        assert durable.state[STRATEGY_USER_STATE_KEY] == {"position": "current"}
        assert "position" not in durable.state
        assert runner_state_value(durable.state, "vault_state") == {"settlement_nonce": 2}
    finally:
        await manager.close()


def test_runner_owned_registry_covers_shared_row_contract() -> None:
    assert RUNNER_OWNED_STATE_KEYS == {
        "execution_progress",
        "recovered_sessions",
        "vault_state",
        "copy_trading_state",
        "last_iteration",
        "total_iterations",
        "successful_iterations",
        "consecutive_errors",
        "total_value_usd",
        "value_confidence",
        "valuation_source",
        "external_provider",
        "external_total_value_usd",
        "framework_total_value_usd",
        "reconciliation_status",
        "capital_flows",
    }


@pytest.mark.asyncio
async def test_compare_delete_refuses_changed_marker_and_preserves_row(tmp_path) -> None:
    manager = await _sqlite_manager(tmp_path, "release.sqlite")
    deployment_id = "deployment:release"
    actual = {"execution_id": "tx-new", "intents_hash": "reconciliation-required"}
    await manager.save_state(
        StateData(
            deployment_id=deployment_id,
            version=1,
            state={
                "execution_progress": actual,
                STRATEGY_USER_STATE_KEY: {"position": "open"},
                "runner_only": 9,
            },
        )
    )
    try:
        with pytest.raises(StateValuePreconditionError, match="changed"):
            await compare_and_delete_state_value(
                manager,
                deployment_id,
                "execution_progress",
                {"execution_id": "tx-old", "intents_hash": "reconciliation-required"},
            )
        durable = await manager.load_state(deployment_id)
        assert durable.state["execution_progress"] == actual
        assert durable.state[STRATEGY_USER_STATE_KEY] == {"position": "open"}
        assert durable.state["runner_only"] == 9
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_compare_delete_retries_cas_without_clobbering_concurrent_overlay(tmp_path, monkeypatch) -> None:
    manager = await _sqlite_manager(tmp_path, "release-conflict.sqlite")
    deployment_id = "deployment:release-conflict"
    marker = {"execution_id": "tx-1", "intents_hash": "reconciliation-required"}
    await manager.save_state(
        StateData(
            deployment_id=deployment_id,
            version=1,
            state={"execution_progress": marker, STRATEGY_USER_STATE_KEY: {"position": "open"}},
        )
    )
    real_save = manager.save_state
    injected = False

    async def _conflict_once(state, expected_version=None):
        nonlocal injected
        if not injected:
            injected = True
            concurrent = await manager.load_state(deployment_id)
            concurrent.state["runner_only"] = "concurrent"
            await real_save(concurrent, expected_version=concurrent.version)
            raise StateConflictError(deployment_id, expected_version, concurrent.version + 1)
        return await real_save(state, expected_version=expected_version)

    monkeypatch.setattr(manager, "save_state", _conflict_once)
    try:
        await compare_and_delete_state_value(manager, deployment_id, "execution_progress", marker)
        durable = await manager.load_state(deployment_id)
        assert "execution_progress" not in durable.state
        assert durable.state[STRATEGY_USER_STATE_KEY] == {"position": "open"}
        assert durable.state["runner_only"] == "concurrent"
    finally:
        await manager.close()
