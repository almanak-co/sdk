"""Tests for the teardown factory backend-selection logic (VIB-4049 PR2 §4).

Pins the contract for ``create_teardown_state_manager`` and
``create_teardown_state_adapter``:

- No ``database_url`` and local mode → SQLite backend.
- ``database_url`` set → Postgres plugin loaded via entry point.
- Hosted (``ALMANAK_IS_HOSTED`` set) without ``database_url`` → raise loud
  ``RuntimeError`` (the April 30 silent-failure guard).
- ``database_url`` set but the plugin isn't installed → raise loud
  ``RuntimeError``.

These tests also raise the CRAP-gate coverage on both factories — without
them, the entry-point error-handling branches (cc=8) have ~25% coverage
and the gate fails at CRAP=35. Each branch is now reachable from a test,
which is the load-bearing reason this file exists (the factory logic is
otherwise dead-simple).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.teardown import (
    SQLiteTeardownStateAdapter,
    SQLiteTeardownStateManager,
    create_teardown_state_adapter,
    create_teardown_state_manager,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def _local_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force local mode by clearing the hosted-mode flag."""
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)


@pytest.fixture
def _hosted_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force hosted mode."""
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "test-agent-uuid")


# ---------------------------------------------------------------------------
# create_teardown_state_manager
# ---------------------------------------------------------------------------
class TestCreateTeardownStateManager:
    def test_local_mode_no_url_returns_sqlite(self, _local_mode, tmp_path) -> None:
        """Default local path: no DB URL → SQLite manager."""
        mgr = create_teardown_state_manager(
            database_url=None,
            sqlite_path=tmp_path / "state.db",
        )
        assert isinstance(mgr, SQLiteTeardownStateManager)

    def test_hosted_mode_no_url_raises(self, _hosted_mode) -> None:
        """Hosted without a DB URL must NOT silently fall back to SQLite."""
        with pytest.raises(RuntimeError, match="ALMANAK_GATEWAY_DATABASE_URL is unset in hosted mode"):
            create_teardown_state_manager(database_url=None)

    def test_url_set_no_plugin_raises(self, _local_mode) -> None:
        """``database_url`` set but the platform plugin isn't installed:
        raise the load-bearing error rather than silently fall through."""
        with patch("importlib.metadata.entry_points", return_value=[]):
            with pytest.raises(RuntimeError, match="almanak.teardown:postgres' plugin is installed"):
                create_teardown_state_manager(database_url="postgresql://stub/db")

    def test_url_set_plugin_load_failure_raises(self, _local_mode) -> None:
        """Plugin entry exists but ``.load()`` raises → wrap into RuntimeError."""
        broken_ep = MagicMock()
        broken_ep.load.side_effect = ImportError("simulated plugin import failure")
        with patch("importlib.metadata.entry_points", return_value=[broken_ep]):
            with pytest.raises(RuntimeError, match="Failed to load 'almanak.teardown:postgres' plugin"):
                create_teardown_state_manager(database_url="postgresql://stub/db")

    def test_url_set_plugin_loaded_returns_postgres(self, _local_mode) -> None:
        """Happy path: plugin loads, factory returns the platform-side class."""
        stub_cls = MagicMock(return_value=MagicMock(name="PostgresTeardownStateManager"))
        stub_cls.__name__ = "PostgresTeardownStateManager"  # for the factory's logger.info line
        ep = MagicMock()
        ep.load.return_value = stub_cls
        with patch("importlib.metadata.entry_points", return_value=[ep]):
            result = create_teardown_state_manager(database_url="postgresql://stub/db")
        stub_cls.assert_called_once_with(database_url="postgresql://stub/db")
        assert result is stub_cls.return_value


# ---------------------------------------------------------------------------
# create_teardown_state_adapter
# ---------------------------------------------------------------------------
class TestCreateTeardownStateAdapter:
    def test_local_mode_no_url_returns_sqlite(self, _local_mode, tmp_path) -> None:
        adapter = create_teardown_state_adapter(
            database_url=None,
            sqlite_path=tmp_path / "state.db",
        )
        assert isinstance(adapter, SQLiteTeardownStateAdapter)

    def test_hosted_mode_no_url_raises(self, _hosted_mode) -> None:
        with pytest.raises(RuntimeError, match="ALMANAK_GATEWAY_DATABASE_URL is unset in hosted mode"):
            create_teardown_state_adapter(database_url=None)

    def test_url_set_no_plugin_raises(self, _local_mode) -> None:
        with patch("importlib.metadata.entry_points", return_value=[]):
            with pytest.raises(RuntimeError, match="almanak.teardown:postgres_adapter' plugin is installed"):
                create_teardown_state_adapter(database_url="postgresql://stub/db")

    def test_url_set_plugin_load_failure_raises(self, _local_mode) -> None:
        broken_ep = MagicMock()
        broken_ep.load.side_effect = ImportError("simulated plugin import failure")
        with patch("importlib.metadata.entry_points", return_value=[broken_ep]):
            with pytest.raises(RuntimeError, match="Failed to load 'almanak.teardown:postgres_adapter' plugin"):
                create_teardown_state_adapter(database_url="postgresql://stub/db")

    def test_url_set_plugin_loaded_returns_postgres(self, _local_mode) -> None:
        stub_cls = MagicMock(return_value=MagicMock(name="PostgresTeardownStateAdapter"))
        stub_cls.__name__ = "PostgresTeardownStateAdapter"
        ep = MagicMock()
        ep.load.return_value = stub_cls
        with patch("importlib.metadata.entry_points", return_value=[ep]):
            result = create_teardown_state_adapter(database_url="postgresql://stub/db")
        stub_cls.assert_called_once_with(database_url="postgresql://stub/db")
        assert result is stub_cls.return_value
