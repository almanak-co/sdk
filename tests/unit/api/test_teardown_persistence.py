"""Tests for teardown API persistence alignment with TeardownStateManager."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.api import teardown as teardown_api
from almanak.framework.local_paths import LocalPathError
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
    """Tests for ``TeardownStateManager._resolve_db_path``.

    VIB-3835 tightened the contract: ``_resolve_db_path`` now delegates to
    the strict, strategy-scoped resolver (``local_strategy_db_path``).
    There is no utility-DB fallback — without an explicit ``db_path``,
    ``ALMANAK_STATE_DB``, or ``ALMANAK_STRATEGY_FOLDER`` the resolver
    raises ``LocalPathError`` so misconfigured deployments fail loudly.
    """

    def test_explicit_path_bypasses_resolver(self):
        """Explicit db_path is returned directly without invoking the
        local-paths resolver. This is the test-and-tooling escape hatch
        — the contract above only governs the ``None`` argument case.
        """
        result = TeardownStateManager._resolve_db_path("/custom/state.db")
        assert result == Path("/custom/state.db")

    def test_explicit_state_db_env_wins(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        """``ALMANAK_STATE_DB`` is the explicit-override branch. With it set
        the strict resolver returns that path verbatim — no folder needed.
        """
        explicit = tmp_path / "explicit.db"
        monkeypatch.delenv("AGENT_ID", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)
        monkeypatch.setenv("ALMANAK_STATE_DB", str(explicit))

        assert TeardownStateManager._resolve_db_path(None) == explicit.resolve()

    def test_strategy_folder_env_wins(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        """With ``ALMANAK_STRATEGY_FOLDER`` set to a real directory, the
        resolver returns ``<folder>/almanak_state.db``.
        """
        folder = tmp_path / "strategy"
        folder.mkdir()
        monkeypatch.delenv("AGENT_ID", raising=False)
        monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)
        monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))

        assert TeardownStateManager._resolve_db_path(None) == folder.resolve() / "almanak_state.db"

    def test_no_folder_raises_local_path_error(self, monkeypatch: pytest.MonkeyPatch):
        """VIB-3835: the strict resolver raises ``LocalPathError`` instead of
        falling back to a per-user utility DB.

        The May 1 mainnet teardown surfaced the silent-fallback failure mode
        — ``teardown request`` ran from a second shell with no
        ``ALMANAK_STRATEGY_FOLDER``, fell through to
        ``~/.local/share/almanak/utility/almanak_state.db``, and the runner
        (polling the strategy-folder DB) never saw the request. The strict
        resolver eliminates that branch.
        """
        for var in ("AGENT_ID", "ALMANAK_STATE_DB", "ALMANAK_STRATEGY_FOLDER", "ALMANAK_GATEWAY_DB_PATH"):
            monkeypatch.delenv(var, raising=False)

        with pytest.raises(LocalPathError, match=r"no strategy folder resolved"):
            TeardownStateManager._resolve_db_path(None)

    def test_gateway_db_path_does_not_satisfy_strict_resolver(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        """``ALMANAK_GATEWAY_DB_PATH`` is a gateway-only override — strategy-
        scoped writers must not honour it (VIB-3835)."""
        for var in ("AGENT_ID", "ALMANAK_STATE_DB", "ALMANAK_STRATEGY_FOLDER"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("ALMANAK_GATEWAY_DB_PATH", str(tmp_path / "gateway-only.db"))

        with pytest.raises(LocalPathError, match=r"no strategy folder resolved"):
            TeardownStateManager._resolve_db_path(None)
