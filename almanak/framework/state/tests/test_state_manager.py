"""Tests for StateManager with SQLite backend integration.

Tests the StateManager class with SQLite as the WARM tier backend,
including:
- Backend selection via configuration
- State CRUD operations
- CAS (optimistic locking) behavior
- HOT to WARM write-through
- WARM to HOT state loading on startup
- Dependency injection of custom backends
"""

import os
import tempfile
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from ..backends.sqlite import SQLiteConfig, SQLiteStore
from ..state_manager import (
    SQLiteConfigLight,
    StateConflictError,
    StateData,
    StateManager,
    StateManagerConfig,
    StateNotFoundError,
    StateTier,
    WarmBackendType,
)

# =============================================================================
# FIXTURES
# =============================================================================


@pytest_asyncio.fixture
async def temp_db_path():
    """Create a temporary database path and clean up after test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "test_state.db")


@pytest_asyncio.fixture
async def sqlite_config(temp_db_path):
    """Create SQLite configuration for tests."""
    return StateManagerConfig(
        warm_backend=WarmBackendType.SQLITE,
        sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=True),
        load_state_on_startup=False,  # Disable to control test flow
    )


@pytest_asyncio.fixture
async def state_manager(sqlite_config):
    """Create and initialize StateManager with SQLite backend."""
    manager = StateManager(sqlite_config)
    await manager.initialize()
    yield manager
    await manager.close()


@pytest_asyncio.fixture
async def injected_backend_manager(temp_db_path):
    """Create StateManager with injected SQLiteStore backend."""
    sqlite_store = SQLiteStore(SQLiteConfig(db_path=temp_db_path))

    config = StateManagerConfig(
        load_state_on_startup=False,
    )
    manager = StateManager(config, warm_backend=sqlite_store)
    await manager.initialize()
    yield manager
    await manager.close()


# =============================================================================
# CONFIGURATION TESTS
# =============================================================================


class TestStateManagerConfiguration:
    """Tests for StateManager configuration."""

    @pytest.mark.asyncio
    async def test_sqlite_backend_via_config(self, sqlite_config, temp_db_path):
        """Test that SQLite backend is created from config."""
        manager = StateManager(sqlite_config)
        await manager.initialize()

        assert manager.is_initialized
        assert manager.warm_backend_type == WarmBackendType.SQLITE
        assert StateTier.WARM in manager.enabled_tiers
        assert StateTier.HOT in manager.enabled_tiers

        await manager.close()

        # Verify database file was created
        assert os.path.exists(temp_db_path)

    @pytest.mark.asyncio
    async def test_default_backend_is_postgresql(self):
        """Test that default backend is PostgreSQL (requires asyncpg)."""
        config = StateManagerConfig(
            enable_warm=False,  # Disable to avoid connection attempt
        )
        manager = StateManager(config)
        await manager.initialize()

        # PostgreSQL is default but disabled
        assert config.warm_backend == WarmBackendType.POSTGRESQL
        assert StateTier.WARM not in manager.enabled_tiers

        await manager.close()

    @pytest.mark.asyncio
    async def test_dependency_injection(self, injected_backend_manager):
        """Test that custom backend can be injected."""
        manager = injected_backend_manager

        assert manager.is_initialized
        assert manager.warm_backend_type == WarmBackendType.SQLITE
        assert manager.warm_backend is not None

    @pytest.mark.asyncio
    async def test_in_memory_sqlite(self):
        """Test StateManager with in-memory SQLite database."""
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=":memory:", wal_mode=False),
            load_state_on_startup=False,
        )
        manager = StateManager(config)
        await manager.initialize()

        # Save and load state
        state = StateData(
            strategy_id="test-strategy",
            version=1,
            state={"key": "value"},
        )
        await manager.save_state(state)
        loaded = await manager.load_state("test-strategy")

        assert loaded.state == {"key": "value"}

        await manager.close()


# =============================================================================
# CRUD TESTS
# =============================================================================


class TestStateManagerCRUD:
    """Tests for state CRUD operations."""

    @pytest.mark.asyncio
    async def test_save_and_load_state(self, state_manager):
        """Test basic save and load operations."""
        state = StateData(
            strategy_id="strat-1",
            version=1,
            state={"position": 100, "balance": "1000.50"},
        )

        saved = await state_manager.save_state(state)
        assert saved.strategy_id == "strat-1"
        assert saved.version >= 1

        loaded = await state_manager.load_state("strat-1")
        assert loaded.strategy_id == "strat-1"
        assert loaded.state["position"] == 100
        assert loaded.state["balance"] == "1000.50"

    @pytest.mark.asyncio
    async def test_update_state(self, state_manager):
        """Test updating existing state."""
        # Create initial state
        state = StateData(
            strategy_id="strat-2",
            version=1,
            state={"count": 1},
        )
        saved = await state_manager.save_state(state)
        initial_version = saved.version

        # Update state
        saved.state["count"] = 2
        saved.version = initial_version + 1
        await state_manager.save_state(saved, expected_version=initial_version)

        # Verify update
        loaded = await state_manager.load_state("strat-2")
        assert loaded.state["count"] == 2
        assert loaded.version > initial_version

    @pytest.mark.asyncio
    async def test_delete_state(self, state_manager):
        """Test deleting state."""
        state = StateData(
            strategy_id="strat-delete",
            version=1,
            state={"to_delete": True},
        )
        await state_manager.save_state(state)

        # Verify state exists
        loaded = await state_manager.load_state("strat-delete")
        assert loaded is not None

        # Delete state
        deleted = await state_manager.delete_state("strat-delete")
        assert deleted is True

        # Verify state is gone
        with pytest.raises(StateNotFoundError):
            await state_manager.load_state("strat-delete")

    @pytest.mark.asyncio
    async def test_state_not_found(self, state_manager):
        """Test loading non-existent state raises error."""
        with pytest.raises(StateNotFoundError) as exc_info:
            await state_manager.load_state("non-existent")

        assert exc_info.value.strategy_id == "non-existent"

    @pytest.mark.asyncio
    async def test_multiple_strategies(self, state_manager):
        """Test managing state for multiple strategies."""
        strategies = ["strat-a", "strat-b", "strat-c"]

        # Create states for all strategies
        for idx, strategy_id in enumerate(strategies):
            state = StateData(
                strategy_id=strategy_id,
                version=1,
                state={"index": idx},
            )
            await state_manager.save_state(state)

        # Verify each strategy has correct state
        for idx, strategy_id in enumerate(strategies):
            loaded = await state_manager.load_state(strategy_id)
            assert loaded.state["index"] == idx


# =============================================================================
# CAS (COMPARE-AND-SWAP) TESTS
# =============================================================================


class TestStateManagerCAS:
    """Tests for CAS (optimistic locking) behavior."""

    @pytest.mark.asyncio
    async def test_cas_success(self, state_manager):
        """Test successful CAS update."""
        state = StateData(
            strategy_id="cas-test",
            version=1,
            state={"value": "initial"},
        )
        saved = await state_manager.save_state(state)

        # Update with correct expected version
        saved.state["value"] = "updated"
        saved.version = saved.version + 1
        await state_manager.save_state(saved, expected_version=saved.version - 1)

        loaded = await state_manager.load_state("cas-test")
        assert loaded.state["value"] == "updated"

    @pytest.mark.asyncio
    async def test_cas_conflict(self, state_manager):
        """Test CAS update with version mismatch raises error."""
        state = StateData(
            strategy_id="cas-conflict",
            version=1,
            state={"value": "initial"},
        )
        saved = await state_manager.save_state(state)

        # Update state normally
        saved.state["value"] = "first-update"
        saved.version = saved.version + 1
        await state_manager.save_state(saved, expected_version=saved.version - 1)

        # Try to update with stale version
        state.state["value"] = "conflicting-update"
        state.version = 2
        with pytest.raises(StateConflictError) as exc_info:
            await state_manager.save_state(state, expected_version=1)

        assert exc_info.value.strategy_id == "cas-conflict"
        assert exc_info.value.expected_version == 1


# =============================================================================
# TIERED STORAGE TESTS
# =============================================================================


class TestTieredStorage:
    """Tests for HOT/WARM tier interaction."""

    @pytest.mark.asyncio
    async def test_hot_cache_on_save(self, state_manager):
        """Test that saves populate HOT cache."""
        state = StateData(
            strategy_id="hot-test",
            version=1,
            state={"cached": True},
        )
        await state_manager.save_state(state)

        # Load should come from HOT tier
        loaded = await state_manager.load_state("hot-test")
        assert loaded.loaded_from == StateTier.HOT

    @pytest.mark.asyncio
    async def test_warm_fallback_on_cache_miss(self, state_manager):
        """Test that WARM tier is used on cache miss."""
        state = StateData(
            strategy_id="warm-fallback",
            version=1,
            state={"fallback": True},
        )
        await state_manager.save_state(state)

        # Clear HOT cache
        state_manager.invalidate_hot_cache("warm-fallback")

        # Load should come from WARM tier
        loaded = await state_manager.load_state("warm-fallback")
        assert loaded.loaded_from == StateTier.WARM
        assert loaded.state["fallback"] is True

    @pytest.mark.asyncio
    async def test_warm_populates_hot_on_load(self, state_manager):
        """Test that loading from WARM populates HOT cache."""
        state = StateData(
            strategy_id="populate-hot",
            version=1,
            state={"populate": True},
        )
        await state_manager.save_state(state)

        # Clear HOT cache
        state_manager.invalidate_hot_cache("populate-hot")

        # First load from WARM
        loaded1 = await state_manager.load_state("populate-hot")
        assert loaded1.loaded_from == StateTier.WARM

        # Second load should be from HOT (populated on first load)
        loaded2 = await state_manager.load_state("populate-hot")
        assert loaded2.loaded_from == StateTier.HOT

    @pytest.mark.asyncio
    async def test_invalidate_hot_cache(self, state_manager):
        """Test HOT cache invalidation."""
        states = ["inv-1", "inv-2", "inv-3"]
        for strategy_id in states:
            state = StateData(
                strategy_id=strategy_id,
                version=1,
                state={"id": strategy_id},
            )
            await state_manager.save_state(state)

        # Invalidate single entry
        state_manager.invalidate_hot_cache("inv-1")
        loaded = await state_manager.load_state("inv-1")
        assert loaded.loaded_from == StateTier.WARM

        # Other entries still in HOT
        loaded = await state_manager.load_state("inv-2")
        assert loaded.loaded_from == StateTier.HOT

        # Invalidate all
        state_manager.invalidate_hot_cache()
        loaded = await state_manager.load_state("inv-2")
        assert loaded.loaded_from == StateTier.WARM


# =============================================================================
# STARTUP LOADING TESTS
# =============================================================================


class TestStartupLoading:
    """Tests for loading states from WARM to HOT on startup."""

    @pytest.mark.asyncio
    async def test_load_on_startup(self, temp_db_path):
        """Test that states are loaded from WARM to HOT on startup."""
        # First, create a manager and save some states
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=temp_db_path),
            load_state_on_startup=False,
        )
        manager1 = StateManager(config)
        await manager1.initialize()

        for i in range(3):
            state = StateData(
                strategy_id=f"startup-{i}",
                version=1,
                state={"index": i},
            )
            await manager1.save_state(state)

        await manager1.close()

        # Create new manager with load_state_on_startup=True
        config_with_load = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=temp_db_path),
            load_state_on_startup=True,  # Enable loading
        )
        manager2 = StateManager(config_with_load)
        await manager2.initialize()

        # All states should be in HOT cache now
        for i in range(3):
            loaded = await manager2.load_state(f"startup-{i}")
            assert loaded.loaded_from == StateTier.HOT
            assert loaded.state["index"] == i

        await manager2.close()


# =============================================================================
# METRICS TESTS
# =============================================================================


class TestMetrics:
    """Tests for metrics tracking."""

    @pytest.mark.asyncio
    async def test_metrics_recorded(self, state_manager):
        """Test that metrics are recorded for operations."""
        state = StateData(
            strategy_id="metrics-test",
            version=1,
            state={"metric": True},
        )

        # Clear existing metrics
        state_manager.clear_metrics()

        await state_manager.save_state(state)
        await state_manager.load_state("metrics-test")

        metrics = state_manager.get_metrics()
        assert len(metrics) > 0

        # Should have WARM save and HOT save/load operations
        operations = [(m.tier, m.operation) for m in metrics]
        assert (StateTier.WARM, "save") in operations

    @pytest.mark.asyncio
    async def test_metrics_summary(self, state_manager):
        """Test metrics summary generation."""
        state = StateData(
            strategy_id="summary-test",
            version=1,
            state={"summary": True},
        )

        state_manager.clear_metrics()

        await state_manager.save_state(state)
        for _ in range(5):
            await state_manager.load_state("summary-test")

        summary = state_manager.get_metrics_summary()

        # Should have HOT tier stats
        assert "HOT" in summary
        assert summary["HOT"]["total_operations"] > 0
        assert summary["HOT"]["success_rate"] == 1.0


# =============================================================================
# DATA INTEGRITY TESTS
# =============================================================================


class TestDataIntegrity:
    """Tests for state data integrity."""

    @pytest.mark.asyncio
    async def test_checksum_verification(self, state_manager):
        """Test that checksums are calculated and stored."""
        state = StateData(
            strategy_id="checksum-test",
            version=1,
            state={"data": "important"},
        )

        saved = await state_manager.save_state(state)
        assert saved.checksum  # Checksum should be set

        loaded = await state_manager.load_state("checksum-test")
        assert loaded.verify_checksum()  # Checksum should verify

    @pytest.mark.asyncio
    async def test_complex_state_data(self, state_manager):
        """Test saving and loading complex nested state data."""
        complex_state = {
            "positions": [
                {"token": "WETH", "amount": "1.5"},
                {"token": "USDC", "amount": "1000"},
            ],
            "config": {
                "max_slippage": 0.01,
                "enabled": True,
                "pools": ["pool1", "pool2"],
            },
            "metadata": {
                "created_at": datetime.now(UTC).isoformat(),
                "version": "1.0.0",
            },
        }

        state = StateData(
            strategy_id="complex-state",
            version=1,
            state=complex_state,
        )

        await state_manager.save_state(state)
        loaded = await state_manager.load_state("complex-state")

        assert loaded.state["positions"][0]["token"] == "WETH"
        assert loaded.state["config"]["max_slippage"] == 0.01
        assert loaded.state["config"]["pools"] == ["pool1", "pool2"]

    @pytest.mark.asyncio
    async def test_state_persistence_across_restarts(self, temp_db_path):
        """Test that state persists across manager restarts."""
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=temp_db_path),
            load_state_on_startup=False,
        )

        # Create state with first manager
        manager1 = StateManager(config)
        await manager1.initialize()

        state = StateData(
            strategy_id="persist-test",
            version=1,
            state={"persist": "value"},
        )
        await manager1.save_state(state)
        await manager1.close()

        # Verify with new manager instance
        manager2 = StateManager(config)
        await manager2.initialize()

        # Clear HOT cache to force WARM load
        manager2.invalidate_hot_cache()

        loaded = await manager2.load_state("persist-test")
        assert loaded.state["persist"] == "value"
        assert loaded.loaded_from == StateTier.WARM

        await manager2.close()
