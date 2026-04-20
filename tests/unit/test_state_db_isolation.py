"""Test that ALMANAK_STATE_DB env var properly isolates state across parallel shards."""

import os
import tempfile

import pytest

from almanak.framework.state.state_manager import (
    SQLiteConfigLight,
    StateData,
    StateManager,
    StateManagerConfig,
    WarmBackendType,
)


@pytest.mark.asyncio
async def test_state_db_isolation_separate_paths():
    """Two StateManagers with different db_path values must not share state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db1 = os.path.join(tmpdir, "shard1.db")
        db2 = os.path.join(tmpdir, "shard2.db")

        config1 = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=db1),
        )
        config2 = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=db2),
        )

        sm1 = StateManager(config1)
        sm2 = StateManager(config2)
        await sm1.initialize()
        await sm2.initialize()

        try:
            # Save state in shard 1
            state1 = StateData(
                strategy_id="test_strategy",
                version=1,
                state={"shard": "1", "value": 42},
            )
            await sm1.save_state(state1)

            # Verify shard 1 sees its own state
            loaded1 = await sm1.load_state("test_strategy")
            assert loaded1 is not None
            assert loaded1.state["shard"] == "1"
            assert loaded1.state["value"] == 42

            # Verify shard 2 does NOT see shard 1's state
            from almanak.framework.state.state_manager import StateNotFoundError

            with pytest.raises(StateNotFoundError):
                await sm2.load_state("test_strategy")

            # Save different state in shard 2
            state2 = StateData(
                strategy_id="test_strategy",
                version=1,
                state={"shard": "2", "value": 99},
            )
            await sm2.save_state(state2)

            # Verify each shard sees only its own state
            loaded1_again = await sm1.load_state("test_strategy")
            loaded2 = await sm2.load_state("test_strategy")
            assert loaded1_again.state["shard"] == "1"
            assert loaded2.state["shard"] == "2"

            # Verify both DB files exist independently
            assert os.path.exists(db1)
            assert os.path.exists(db2)
        finally:
            await sm1.close()
            await sm2.close()


@pytest.mark.asyncio
async def test_sqlite_config_reads_env_var(monkeypatch):
    """SQLiteConfigLight should read ALMANAK_STATE_DB env var as default."""
    monkeypatch.setenv("ALMANAK_STATE_DB", "/tmp/custom_state.db")
    config = SQLiteConfigLight()
    assert config.db_path == "/tmp/custom_state.db"


@pytest.mark.asyncio
async def test_sqlite_config_default_without_env_var(monkeypatch):
    """SQLiteConfigLight should default to ./almanak_state.db without env var."""
    monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
    config = SQLiteConfigLight()
    assert config.db_path == "./almanak_state.db"
