"""Tests for teardown API persistence alignment with TeardownStateManager."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.api import teardown as teardown_api
from almanak.framework.teardown.state_manager import TeardownStateManager


@pytest.mark.asyncio
async def test_start_close_persists_teardown_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """start_close should persist a TeardownRequest for StrategyRunner pickup."""
    strategy_id = "test_strategy"
    teardown_api._teardown_state.remove_teardown(strategy_id)

    monkeypatch.setattr(
        teardown_api,
        "_get_strategy_data",
        lambda _: {
            "strategy_id": strategy_id,
            "name": "Test",
            "chain": "arbitrum",
            "total_value_usd": 1000.0,
            "positions": [],
        },
    )

    manager = MagicMock()
    monkeypatch.setattr(teardown_api, "get_teardown_state_manager", lambda: manager)

    request = teardown_api.CloseRequest(mode="graceful")
    response = await teardown_api.start_close(strategy_id, request, api_key="test-key")

    assert response.status == "cancel_window"
    manager.create_request.assert_called_once()
    persisted_request = manager.create_request.call_args[0][0]
    assert persisted_request.strategy_id == strategy_id
    assert persisted_request.mode.value == "SOFT"
    assert persisted_request.status.value == "cancel_window"


@pytest.mark.asyncio
async def test_cancel_close_marks_persisted_request_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    """cancel_close should mark persisted teardown request as cancelled."""
    strategy_id = "test_strategy_cancel"
    teardown_api._teardown_state.set_teardown(
        strategy_id,
        {
            "teardown_id": "td_123",
            "strategy_id": strategy_id,
            "mode": "graceful",
            "status": "cancel_window",
            "cancel_until": "2100-01-01T00:00:00+00:00",
        },
    )

    manager = MagicMock()
    monkeypatch.setattr(teardown_api, "get_teardown_state_manager", lambda: manager)

    response = await teardown_api.cancel_close(strategy_id, api_key="test-key")

    assert response.success is True
    manager.mark_cancelled.assert_called_once_with(strategy_id)


class TestResolveDbPath:
    """Tests for TeardownStateManager._resolve_db_path fallback logic."""

    def test_none_returns_per_user_default_when_home_writable(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        """Default (None, no env, no XDG) resolves under the per-user utility dir.

        VIB-3761: ``TeardownStateManager._resolve_db_path`` delegates to
        ``almanak.framework.local_paths.local_db_path()``. With no env
        overrides set, the canonical location is
        ``~/.local/share/almanak/utility/almanak_state.db`` so the runner
        (launched from a strategy dir) and the API (launched from the repo
        root) converge on the same file regardless of cwd.
        """
        monkeypatch.delenv("AGENT_ID", raising=False)
        monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        # Path.home() caches; use monkeypatch to override resolution.
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        result = TeardownStateManager._resolve_db_path(None)

        assert result == tmp_path / ".local" / "share" / "almanak" / "utility" / "almanak_state.db"

    def test_none_uses_xdg_data_home_when_set(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        """``XDG_DATA_HOME`` is honoured ahead of ``~/.local/share``."""
        xdg_dir = tmp_path / "xdg"
        monkeypatch.delenv("AGENT_ID", raising=False)
        monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)
        monkeypatch.setenv("XDG_DATA_HOME", str(xdg_dir))

        result = TeardownStateManager._resolve_db_path(None)

        assert result == xdg_dir / "almanak" / "utility" / "almanak_state.db"

    def test_none_returns_per_user_default_when_mkdir_fails(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        """A read-only home is best-effort; ``local_db_path`` still returns
        the canonical per-user path so SQLite surfaces the failure at
        connect time.

        VIB-3761 explicitly removed the cwd-relative fallback (``./almanak_state.db``)
        and the implicit ``/tmp`` rescue — those silently masked
        misconfiguration. The new contract is "log the OSError, return
        the canonical path, let SQLite raise" so operators see the
        actual failure mode.
        """
        monkeypatch.delenv("AGENT_ID", raising=False)
        monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        with patch.object(Path, "mkdir", side_effect=OSError("Read-only file system")):
            result = TeardownStateManager._resolve_db_path(None)
        assert result == tmp_path / ".local" / "share" / "almanak" / "utility" / "almanak_state.db"

    def test_explicit_path_bypasses_fallback(self):
        """Explicit db_path is returned directly without fallback logic."""
        result = TeardownStateManager._resolve_db_path("/custom/state.db")
        assert result == Path("/custom/state.db")
