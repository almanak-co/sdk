"""Tests for the gateway InstanceRegistry.

Tests cover:
- InstanceRegistry initialization with SQLite
- Registering and retrieving instances
- Status updates and heartbeats
- Archiving and unarchiving
- Purging instances (single and with events)
- Listing with archive filtering
- Persistence and reload from SQLite
- Thread safety with concurrent operations
"""

import tempfile
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from almanak.gateway.registry.store import (
    InstanceRegistry,
    StrategyInstance,
    get_instance_registry,
    reset_instance_registry,
)


def _make_instance(
    strategy_id: str = "test_strat:abc123",
    strategy_name: str = "test_strat",
    status: str = "RUNNING",
    archived: bool = False,
    chain: str = "arbitrum",
) -> StrategyInstance:
    """Create a test instance with defaults."""
    now = datetime.now(UTC)
    return StrategyInstance(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        template_name="TestStrategy",
        chain=chain,
        protocol="Uniswap V3",
        wallet_address="0x1234",
        config_json="{}",
        chains=chain,
        chain_wallets="{}",
        status=status,
        archived=archived,
        created_at=now,
        updated_at=now,
        last_heartbeat_at=now,
        version="1.0.0",
    )


class TestStrategyInstance:
    """Tests for StrategyInstance dataclass."""

    def test_create_instance(self):
        """Test creating a strategy instance."""
        inst = _make_instance()
        assert inst.strategy_id == "test_strat:abc123"
        assert inst.strategy_name == "test_strat"
        assert inst.template_name == "TestStrategy"
        assert inst.chain == "arbitrum"
        assert inst.status == "RUNNING"
        assert inst.archived is False


class TestInstanceRegistryBasics:
    """Tests for basic InstanceRegistry operations."""

    def test_initialize(self):
        """Test initializing the registry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()
            assert db_path.exists()

    def test_register_and_get(self):
        """Test registering and retrieving an instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            inst = _make_instance()
            is_new = registry.register(inst)
            assert is_new is True

            retrieved = registry.get("test_strat:abc123")
            assert retrieved is not None
            assert retrieved.strategy_id == "test_strat:abc123"
            assert retrieved.strategy_name == "test_strat"
            assert retrieved.status == "RUNNING"

    def test_register_returns_false_on_reregistration(self):
        """Test that re-registering returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            inst = _make_instance()
            assert registry.register(inst) is True
            assert registry.register(inst) is False

    def test_get_nonexistent_returns_none(self):
        """Test getting a non-existent instance returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            assert registry.get("nonexistent") is None

    def test_list_all(self):
        """Test listing all instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance("strat1:aaa111"))
            registry.register(_make_instance("strat2:bbb222"))

            instances = registry.list_all()
            assert len(instances) == 2
            ids = {i.strategy_id for i in instances}
            assert "strat1:aaa111" in ids
            assert "strat2:bbb222" in ids


class TestInstanceRegistryStatus:
    """Tests for status updates and heartbeats."""

    def test_update_status(self):
        """Test updating instance status."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance())
            assert registry.update_status("test_strat:abc123", "INACTIVE") is True

            inst = registry.get("test_strat:abc123")
            assert inst.status == "INACTIVE"

    def test_update_status_nonexistent_returns_false(self):
        """Test updating status of non-existent instance returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            assert registry.update_status("nonexistent", "INACTIVE") is False

    def test_heartbeat(self):
        """Test heartbeat updates timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            inst = _make_instance()
            old_heartbeat = inst.last_heartbeat_at
            registry.register(inst)

            # Wait a tiny bit to ensure timestamp differs
            import time

            time.sleep(0.01)

            assert registry.heartbeat("test_strat:abc123") is True

            updated = registry.get("test_strat:abc123")
            assert updated.last_heartbeat_at >= old_heartbeat

    def test_heartbeat_nonexistent_returns_false(self):
        """Test heartbeat of non-existent instance returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            assert registry.heartbeat("nonexistent") is False


class TestInstanceRegistryArchive:
    """Tests for archive and unarchive."""

    def test_archive(self):
        """Test archiving an instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance())
            assert registry.archive("test_strat:abc123") is True

            inst = registry.get("test_strat:abc123")
            assert inst.archived is True

    def test_archived_excluded_from_list_by_default(self):
        """Test that archived instances are excluded from list by default."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance("strat1:aaa111"))
            registry.register(_make_instance("strat2:bbb222"))
            registry.archive("strat1:aaa111")

            # Default excludes archived
            instances = registry.list_all()
            assert len(instances) == 1
            assert instances[0].strategy_id == "strat2:bbb222"

            # Include archived
            all_instances = registry.list_all(include_archived=True)
            assert len(all_instances) == 2

    def test_unarchive(self):
        """Test unarchiving an instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance())
            registry.archive("test_strat:abc123")
            assert registry.unarchive("test_strat:abc123") is True

            inst = registry.get("test_strat:abc123")
            assert inst.archived is False


class TestInstanceRegistryPurge:
    """Tests for purge operations."""

    def test_purge(self):
        """Test purging an instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance())
            assert registry.purge("test_strat:abc123") is True
            assert registry.get("test_strat:abc123") is None

    def test_purge_nonexistent_returns_false(self):
        """Test purging non-existent instance returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            assert registry.purge("nonexistent") is False

    def test_purge_with_events_deletes_both_tables(self):
        """Test atomic purge deletes instance and timeline events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            import sqlite3

            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            # Create timeline_events table (normally created by TimelineStore)
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS timeline_events (
                        event_id TEXT PRIMARY KEY,
                        strategy_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        description TEXT,
                        tx_hash TEXT,
                        chain TEXT,
                        details_json TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                # Insert a test event
                conn.execute(
                    "INSERT INTO timeline_events (event_id, strategy_id, timestamp, event_type, description) "
                    "VALUES (?, ?, ?, ?, ?)",
                    ("evt1", "test_strat:abc123", datetime.now(UTC).isoformat(), "TRADE", "Test event"),
                )
                conn.commit()

            registry.register(_make_instance())
            assert registry.purge_with_events("test_strat:abc123") is True

            # Verify both tables are empty for this strategy_id
            with sqlite3.connect(str(db_path)) as conn:
                inst_count = conn.execute(
                    "SELECT COUNT(*) FROM strategy_instances WHERE strategy_id = ?",
                    ("test_strat:abc123",),
                ).fetchone()[0]
                evt_count = conn.execute(
                    "SELECT COUNT(*) FROM timeline_events WHERE strategy_id = ?",
                    ("test_strat:abc123",),
                ).fetchone()[0]

            assert inst_count == 0
            assert evt_count == 0


class TestInstanceRegistryPersistence:
    """Tests for SQLite persistence across registry instances."""

    def test_persistence_across_restarts(self):
        """Test that instances persist across registry instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            # First registry - register instance
            registry1 = InstanceRegistry(db_path=db_path)
            registry1.initialize()
            registry1.register(_make_instance())
            registry1.close()

            # Second registry - should load from DB
            registry2 = InstanceRegistry(db_path=db_path)
            registry2.initialize()

            inst = registry2.get("test_strat:abc123")
            assert inst is not None
            assert inst.strategy_name == "test_strat"
            assert inst.status == "RUNNING"

    def test_status_persists_across_restarts(self):
        """Test that status changes persist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            registry1 = InstanceRegistry(db_path=db_path)
            registry1.initialize()
            registry1.register(_make_instance())
            registry1.update_status("test_strat:abc123", "INACTIVE")
            registry1.close()

            registry2 = InstanceRegistry(db_path=db_path)
            registry2.initialize()

            inst = registry2.get("test_strat:abc123")
            assert inst.status == "INACTIVE"

    def test_archive_persists_across_restarts(self):
        """Test that archive state persists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            registry1 = InstanceRegistry(db_path=db_path)
            registry1.initialize()
            registry1.register(_make_instance())
            registry1.archive("test_strat:abc123")
            registry1.close()

            registry2 = InstanceRegistry(db_path=db_path)
            registry2.initialize()

            inst = registry2.get("test_strat:abc123")
            assert inst.archived is True


class TestInstanceRegistryThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_registrations(self):
        """Test concurrent instance registrations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            errors = []

            def register_instances(thread_id: int):
                try:
                    for i in range(20):
                        inst = _make_instance(f"strat_{thread_id}:{i:06d}")
                        registry.register(inst)
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=register_instances, args=(t,)) for t in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert errors == [], f"Errors during concurrent registrations: {errors}"

            all_instances = registry.list_all()
            assert len(all_instances) == 100  # 5 threads x 20 instances

    def test_concurrent_heartbeats(self):
        """Test concurrent heartbeat updates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            # Register some instances
            for i in range(5):
                registry.register(_make_instance(f"strat:{i:06d}"))

            errors = []

            def send_heartbeats(thread_id: int):
                try:
                    for i in range(5):
                        for j in range(20):
                            registry.heartbeat(f"strat:{i:06d}")
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=send_heartbeats, args=(t,)) for t in range(3)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert errors == [], f"Errors during concurrent heartbeats: {errors}"


class TestInstanceRegistryStartupReconciliation:
    """Tests for VIB-1279: startup reconciliation marks RUNNING entries as STALE."""

    def test_reconcile_stale_on_startup_marks_running_as_stale(self):
        """RUNNING instances become STALE after startup reconciliation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance("strat1:aaa", status="RUNNING"))
            registry.register(_make_instance("strat2:bbb", status="RUNNING"))
            registry.register(_make_instance("strat3:ccc", status="INACTIVE"))

            count = registry.reconcile_stale_on_startup()
            assert count == 2

            assert registry.get("strat1:aaa").status == "STALE"
            assert registry.get("strat2:bbb").status == "STALE"
            assert registry.get("strat3:ccc").status == "INACTIVE"  # unchanged

    def test_reconcile_persists_to_sqlite(self):
        """Startup reconciliation persists STALE status to SQLite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            registry1 = InstanceRegistry(db_path=db_path)
            registry1.initialize()
            registry1.register(_make_instance("strat1:aaa", status="RUNNING"))
            registry1.reconcile_stale_on_startup()
            registry1.close()

            # Reload from DB - should see STALE not RUNNING
            registry2 = InstanceRegistry(db_path=db_path)
            registry2.initialize()
            assert registry2.get("strat1:aaa").status == "STALE"

    def test_reconcile_returns_zero_when_no_running(self):
        """Returns 0 when no RUNNING instances exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance("strat1:aaa", status="INACTIVE"))
            count = registry.reconcile_stale_on_startup()
            assert count == 0

    def test_reconcile_empty_registry(self):
        """Returns 0 on empty registry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            count = registry.reconcile_stale_on_startup()
            assert count == 0

    def test_heartbeat_recovers_stale_to_running(self):
        """A heartbeat from a STALE instance recovers it back to RUNNING (VIB-1279)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            registry.register(_make_instance("strat1:aaa", status="RUNNING"))
            registry.reconcile_stale_on_startup()
            assert registry.get("strat1:aaa").status == "STALE"

            # Strategy sends a heartbeat — should recover to RUNNING
            result = registry.heartbeat("strat1:aaa")
            assert result is True
            assert registry.get("strat1:aaa").status == "RUNNING"

    def test_heartbeat_recovery_persists_to_sqlite(self):
        """STALE -> RUNNING recovery via heartbeat persists to SQLite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            registry1 = InstanceRegistry(db_path=db_path)
            registry1.initialize()
            registry1.register(_make_instance("strat1:aaa", status="RUNNING"))
            registry1.reconcile_stale_on_startup()
            registry1.heartbeat("strat1:aaa")
            registry1.close()

            registry2 = InstanceRegistry(db_path=db_path)
            registry2.initialize()
            assert registry2.get("strat1:aaa").status == "RUNNING"


class TestInstanceRegistryHeartbeatTTL:
    """Tests for VIB-1280: background heartbeat TTL enforcement."""

    def test_enforce_heartbeat_ttl_marks_stale_entries(self):
        """RUNNING instances with expired heartbeats are marked STALE."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            now = datetime.now(UTC)
            old_heartbeat = now - timedelta(seconds=400)  # 400s ago, threshold is 300s

            inst = _make_instance("strat1:aaa", status="RUNNING")
            inst.last_heartbeat_at = old_heartbeat
            registry.register(inst)

            count = registry.enforce_heartbeat_ttl(stale_threshold_seconds=300)
            assert count == 1
            assert registry.get("strat1:aaa").status == "STALE"

    def test_enforce_heartbeat_ttl_skips_fresh_entries(self):
        """RUNNING instances with fresh heartbeats are NOT marked STALE."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            inst = _make_instance("strat1:aaa", status="RUNNING")
            # last_heartbeat_at is set to now() by _make_instance -- well within threshold
            registry.register(inst)

            count = registry.enforce_heartbeat_ttl(stale_threshold_seconds=300)
            assert count == 0
            assert registry.get("strat1:aaa").status == "RUNNING"

    def test_enforce_heartbeat_ttl_skips_non_running(self):
        """INACTIVE/PAUSED/ERROR instances are never marked STALE by TTL enforcement."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            now = datetime.now(UTC)
            old_heartbeat = now - timedelta(seconds=600)

            for status in ("INACTIVE", "PAUSED", "ERROR"):
                inst = _make_instance(f"strat:{status}", status=status)
                inst.last_heartbeat_at = old_heartbeat
                registry.register(inst)

            count = registry.enforce_heartbeat_ttl(stale_threshold_seconds=300)
            assert count == 0

    def test_enforce_heartbeat_ttl_persists_to_sqlite(self):
        """TTL enforcement persists STALE status to SQLite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            registry1 = InstanceRegistry(db_path=db_path)
            registry1.initialize()

            now = datetime.now(UTC)
            inst = _make_instance("strat1:aaa", status="RUNNING")
            inst.last_heartbeat_at = now - timedelta(seconds=400)
            registry1.register(inst)
            registry1.enforce_heartbeat_ttl(stale_threshold_seconds=300)
            registry1.close()

            registry2 = InstanceRegistry(db_path=db_path)
            registry2.initialize()
            assert registry2.get("strat1:aaa").status == "STALE"

    def test_enforce_heartbeat_ttl_only_marks_expired(self):
        """Only expired instances are marked STALE, fresh ones stay RUNNING."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            registry = InstanceRegistry(db_path=db_path)
            registry.initialize()

            now = datetime.now(UTC)

            fresh = _make_instance("fresh:aaa", status="RUNNING")
            # last_heartbeat_at is now by default
            registry.register(fresh)

            stale = _make_instance("stale:bbb", status="RUNNING")
            stale.last_heartbeat_at = now - timedelta(seconds=400)
            registry.register(stale)

            count = registry.enforce_heartbeat_ttl(stale_threshold_seconds=300)
            assert count == 1
            assert registry.get("fresh:aaa").status == "RUNNING"
            assert registry.get("stale:bbb").status == "STALE"


class TestInstanceRegistrySingleton:
    """Tests for singleton accessor functions."""

    def test_get_instance_registry_singleton(self):
        """Test that get_instance_registry returns singleton."""
        reset_instance_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            reg1 = get_instance_registry(db_path=db_path)
            reg2 = get_instance_registry()
            assert reg1 is reg2

    def test_reset_instance_registry(self):
        """Test resetting the singleton."""
        reset_instance_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            reg1 = get_instance_registry(db_path=db_path)
            reg1.register(_make_instance())

            reset_instance_registry()

            # New registry at different path should be empty
            db_path2 = Path(tmpdir) / "test2.db"
            reg2 = get_instance_registry(db_path=db_path2)
            assert reg2.list_all() == []

    def teardown_method(self):
        """Reset singleton after each test."""
        reset_instance_registry()


class TestAgentIdResolution:
    """Tests for AGENT_ID normalization in deployed mode.

    When AGENT_ID env var is set, the registry should use it as the key
    for all operations, ensuring write/read consistency with the dashboard.
    """

    def test_register_uses_agent_id(self, tmp_path, monkeypatch):
        """Register stores instance under AGENT_ID when env var is set."""
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")
        registry = InstanceRegistry(db_path=tmp_path / "test.db")
        registry.initialize()

        instance = _make_instance(strategy_id="uniswap_rsi:abc123")
        registry.register(instance)

        # Stored under AGENT_ID, not the original strategy_id
        assert registry.get("platform-uuid-1234") is not None
        assert registry.get("platform-uuid-1234").strategy_name == "test_strat"

    def test_get_resolves_agent_id(self, tmp_path, monkeypatch):
        """Get resolves AGENT_ID so lookups match registered data."""
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")
        registry = InstanceRegistry(db_path=tmp_path / "test.db")
        registry.initialize()

        instance = _make_instance(strategy_id="uniswap_rsi:abc123")
        registry.register(instance)

        # Querying with SDK strategy_id also resolves to AGENT_ID
        result = registry.get("uniswap_rsi:abc123")
        assert result is not None
        assert result.strategy_id == "platform-uuid-1234"

    def test_heartbeat_resolves_agent_id(self, tmp_path, monkeypatch):
        """Heartbeat finds the instance via resolved AGENT_ID."""
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")
        registry = InstanceRegistry(db_path=tmp_path / "test.db")
        registry.initialize()

        instance = _make_instance(strategy_id="uniswap_rsi:abc123")
        registry.register(instance)

        # Heartbeat with SDK strategy_id resolves to AGENT_ID
        assert registry.heartbeat("uniswap_rsi:abc123") is True

    def test_update_status_resolves_agent_id(self, tmp_path, monkeypatch):
        """Status update finds the instance via resolved AGENT_ID."""
        monkeypatch.setenv("AGENT_ID", "platform-uuid-1234")
        registry = InstanceRegistry(db_path=tmp_path / "test.db")
        registry.initialize()

        instance = _make_instance(strategy_id="uniswap_rsi:abc123")
        registry.register(instance)

        assert registry.update_status("uniswap_rsi:abc123", "PAUSED") is True
        assert registry.get("platform-uuid-1234").status == "PAUSED"

    def test_no_env_var_passes_through(self, tmp_path, monkeypatch):
        """Without AGENT_ID, strategy_id passes through unchanged (local mode)."""
        monkeypatch.delenv("AGENT_ID", raising=False)
        registry = InstanceRegistry(db_path=tmp_path / "test.db")
        registry.initialize()

        instance = _make_instance(strategy_id="uniswap_rsi:abc123")
        registry.register(instance)

        assert registry.get("uniswap_rsi:abc123") is not None
        assert registry.get("platform-uuid-1234") is None
