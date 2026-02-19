"""Tests for teardown API persistence alignment with TeardownStateManager."""

from unittest.mock import MagicMock

import pytest

from almanak.framework.api import teardown as teardown_api


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
