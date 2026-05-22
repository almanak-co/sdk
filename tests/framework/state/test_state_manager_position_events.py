"""Unit tests for StateManager PositionEvent delegation methods (VIB-3204).

CodeRabbit round-4 audit fix: the delegation methods
``save_position_event``, ``update_position_attribution``,
``get_position_events``, and ``get_position_history`` were added in
round-3 (commit 7f5596438) after CR flagged that every call to
``StateManager.save_position_event(...)`` was silently raising
``AttributeError`` and being swallowed by the runner's outer try/except,
leaving entry_state permanently unstamped in production.

These tests lock the contract for each method against five scenarios CR
explicitly called out:

    1. Successful delegation (backend returns truthy).
    2. Missing warm backend (``self._warm is None``) -> default return.
    3. Unsupported backend (method absent from backend) -> default return.
    4. Backend returns False / empty list -> correct pass-through.
    5. Backend raises -> ``logger.error`` + ``_record_metrics`` failure.

No SQLite round-trip is used — the warm backend is mocked with an
``AsyncMock`` so the tests stay fast and purely exercise the delegation
plumbing. A regression here would re-introduce the silent-AttributeError
failure mode that broke IL attribution in production.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.state.state_manager import (
    StateManager,
    StateManagerConfig,
    StateTier,
)


@pytest.fixture
def manager_with_mocked_warm() -> tuple[StateManager, AsyncMock]:
    """Return a StateManager with a fully-mocked warm backend.

    Bypasses ``initialize()`` (flips ``_initialized`` manually) so tests
    don't touch a real SQLite file. The warm mock is an AsyncMock so all
    method awaits return Mock objects unless the test overrides them.
    """
    cfg = StateManagerConfig(enable_warm=True, enable_hot=False)
    mgr = StateManager(cfg)
    mgr._initialized = True
    warm = AsyncMock()
    mgr._warm = warm
    return mgr, warm


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# save_position_event
# ---------------------------------------------------------------------------


class TestSavePositionEvent:
    def test_successful_delegation_returns_true(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.save_position_event = AsyncMock(return_value=True)
        event = MagicMock()

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.save_position_event(event))

        assert result is True
        warm.save_position_event.assert_awaited_once_with(event)
        rec.assert_called_once()
        call_args = rec.call_args[0]
        assert call_args[0] is StateTier.WARM
        assert call_args[1] == "save_position_event"
        assert call_args[3] is True  # ok flag

    def test_no_warm_backend_returns_false(self) -> None:
        cfg = StateManagerConfig(enable_warm=True, enable_hot=False)
        mgr = StateManager(cfg)
        mgr._initialized = True
        mgr._warm = None

        assert _run(mgr.save_position_event(MagicMock())) is False

    def test_unsupported_backend_returns_false(self) -> None:
        cfg = StateManagerConfig(enable_warm=True, enable_hot=False)
        mgr = StateManager(cfg)
        mgr._initialized = True
        # Backend without save_position_event — delattr can't work on
        # MagicMock so use spec= to restrict the surface.
        mgr._warm = MagicMock(spec=[])  # no attrs

        assert _run(mgr.save_position_event(MagicMock())) is False

    def test_backend_returns_false_records_failure_metric(self, manager_with_mocked_warm) -> None:
        """When the backend reports False (silent no-op), metrics must
        reflect that as a failure — not a false-positive success. This is
        the exact bug CR round-4 caught on my round-3 implementation."""
        mgr, warm = manager_with_mocked_warm
        warm.save_position_event = AsyncMock(return_value=False)

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.save_position_event(MagicMock()))

        assert result is False
        call_args = rec.call_args[0]
        assert call_args[3] is False  # ok flag
        assert call_args[4] == "backend_returned_false"  # error tag

    def test_exception_path_logs_error_and_records_failure(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.save_position_event = AsyncMock(side_effect=RuntimeError("db offline"))

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.save_position_event(MagicMock()))

        assert result is False
        call_args = rec.call_args[0]
        assert call_args[3] is False
        assert "db offline" in call_args[4]


# ---------------------------------------------------------------------------
# update_position_attribution
# ---------------------------------------------------------------------------


class TestUpdatePositionAttribution:
    def test_successful_delegation_returns_true(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.update_position_attribution = AsyncMock(return_value=True)

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.update_position_attribution("evt-1", '{"v": 1}', 2))

        assert result is True
        # PR #2018 CR audit: deployment_id is plumbed through StateManager so
        # the GSM client can forward it to the gateway proto request as
        # wire-level scope. Default empty when caller doesn't pass one.
        warm.update_position_attribution.assert_awaited_once_with(
            "evt-1", '{"v": 1}', 2, deployment_id=""
        )
        assert rec.call_args[0][1] == "update_position_attribution"
        assert rec.call_args[0][3] is True

    def test_no_warm_backend_returns_false(self) -> None:
        mgr = StateManager(StateManagerConfig(enable_warm=True, enable_hot=False))
        mgr._initialized = True
        mgr._warm = None
        assert _run(mgr.update_position_attribution("e", "{}", 1)) is False

    def test_unsupported_backend_returns_false(self) -> None:
        mgr = StateManager(StateManagerConfig(enable_warm=True, enable_hot=False))
        mgr._initialized = True
        mgr._warm = MagicMock(spec=[])
        assert _run(mgr.update_position_attribution("e", "{}", 1)) is False

    def test_backend_returns_false_records_failure_metric(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.update_position_attribution = AsyncMock(return_value=False)

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.update_position_attribution("e", "{}", 1))

        assert result is False
        assert rec.call_args[0][3] is False
        assert rec.call_args[0][4] == "backend_returned_false"

    def test_exception_path_logs_error_and_records_failure(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.update_position_attribution = AsyncMock(side_effect=RuntimeError("CAS conflict"))

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.update_position_attribution("e", "{}", 1))

        assert result is False
        assert rec.call_args[0][3] is False
        assert "CAS conflict" in rec.call_args[0][4]


# ---------------------------------------------------------------------------
# get_position_events
# ---------------------------------------------------------------------------


class TestGetPositionEvents:
    def test_forwards_with_keyword_args(self, manager_with_mocked_warm) -> None:
        """CR round-4 caught this: my round-3 forwarded positionally as
        (deployment_id, event_type, limit) which mis-bound event_type to
        position_id. Must use keyword args so event_type lands in the
        right slot."""
        mgr, warm = manager_with_mocked_warm
        warm.get_position_events = AsyncMock(return_value=[{"id": 1}])

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.get_position_events("strat-1", event_type="CLOSE", limit=50))

        assert result == [{"id": 1}]
        warm.get_position_events.assert_awaited_once_with(
            deployment_id="strat-1",
            position_id=None,
            event_type="CLOSE",
            limit=50,
        )
        rec.assert_called_once()
        assert rec.call_args[0][1] == "get_position_events"
        assert rec.call_args[0][3] is True

    def test_no_warm_backend_returns_empty_list(self) -> None:
        mgr = StateManager(StateManagerConfig(enable_warm=True, enable_hot=False))
        mgr._initialized = True
        mgr._warm = None
        assert _run(mgr.get_position_events("s")) == []

    def test_unsupported_backend_returns_empty_list(self) -> None:
        mgr = StateManager(StateManagerConfig(enable_warm=True, enable_hot=False))
        mgr._initialized = True
        mgr._warm = MagicMock(spec=[])
        assert _run(mgr.get_position_events("s")) == []

    def test_backend_returns_empty_list_ok(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.get_position_events = AsyncMock(return_value=[])
        assert _run(mgr.get_position_events("s")) == []

    def test_exception_path_returns_empty_list_and_logs(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.get_position_events = AsyncMock(side_effect=RuntimeError("query failed"))

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.get_position_events("s"))

        assert result == []
        assert rec.call_args[0][3] is False
        assert "query failed" in rec.call_args[0][4]


# ---------------------------------------------------------------------------
# get_position_history
# ---------------------------------------------------------------------------


class TestGetPositionHistory:
    def test_successful_delegation(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.get_position_history = AsyncMock(return_value=[{"event_type": "OPEN"}])

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.get_position_history("s", "pos-7"))

        assert result == [{"event_type": "OPEN"}]
        warm.get_position_history.assert_awaited_once_with("s", "pos-7")
        rec.assert_called_once()
        assert rec.call_args[0][1] == "get_position_history"
        assert rec.call_args[0][3] is True

    def test_no_warm_backend_returns_empty_list(self) -> None:
        mgr = StateManager(StateManagerConfig(enable_warm=True, enable_hot=False))
        mgr._initialized = True
        mgr._warm = None
        assert _run(mgr.get_position_history("s", "p")) == []

    def test_unsupported_backend_returns_empty_list(self) -> None:
        mgr = StateManager(StateManagerConfig(enable_warm=True, enable_hot=False))
        mgr._initialized = True
        mgr._warm = MagicMock(spec=[])
        assert _run(mgr.get_position_history("s", "p")) == []

    def test_backend_returns_empty_list_ok(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.get_position_history = AsyncMock(return_value=[])
        assert _run(mgr.get_position_history("s", "p")) == []

    def test_exception_path_returns_empty_list_and_logs(self, manager_with_mocked_warm) -> None:
        mgr, warm = manager_with_mocked_warm
        warm.get_position_history = AsyncMock(side_effect=RuntimeError("boom"))

        with patch.object(mgr, "_record_metrics") as rec:
            result = _run(mgr.get_position_history("s", "p"))

        assert result == []
        assert rec.call_args[0][3] is False
        assert "boom" in rec.call_args[0][4]
