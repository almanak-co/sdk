"""Unit tests for ``call_strategy_action`` (Phase 5a refactor).

Covers the two dispatch paths:
    * Gateway path (pause/resume) - success, failure (False return), exception.
    * REST fallback path (bump-gas, cancel-tx, other) - 200, 400, 404, 500,
      ``ConnectionError`` (with ``connection_error`` flag), ``Timeout``, and an
      unexpected exception.

Target: >= 85 % line coverage of ``call_strategy_action``. These tests do NOT
exercise the Streamlit render helpers (``render_action_row`` etc.) - those are
smoke-tested separately in Phase 5e via ``streamlit.testing``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from almanak.framework.dashboard.pages.detail import call_strategy_action

MODULE = "almanak.framework.dashboard.pages.detail"


# ---------------------------------------------------------------------------
# Gateway path (pause / resume)
# ---------------------------------------------------------------------------


def test_call_strategy_action_gateway_pause_success() -> None:
    """Gateway returns True -> success payload with capitalised action name."""
    with patch(f"{MODULE}.execute_strategy_action", return_value=True) as mock_exec:
        result = call_strategy_action("strat-1", "pause")

    assert result == {"success": True, "message": "Pause request submitted"}
    # Verify we call the gateway with PAUSE (upper) and a sensible default reason.
    args, _ = mock_exec.call_args
    assert args[0] == "strat-1"
    assert args[1] == "PAUSE"
    assert "Pause requested from dashboard detail page" in args[2]


def test_call_strategy_action_gateway_resume_success_with_custom_reason() -> None:
    """Custom string reason in payload is forwarded verbatim to the gateway."""
    with patch(f"{MODULE}.execute_strategy_action", return_value=True) as mock_exec:
        result = call_strategy_action("strat-2", "resume", {"reason": "manual override"})

    assert result["success"] is True
    assert result["message"] == "Resume request submitted"
    args, _ = mock_exec.call_args
    assert args[1] == "RESUME"
    assert args[2] == "manual override"


def test_call_strategy_action_gateway_rejects_request() -> None:
    """Gateway returns False -> structured failure without raising."""
    with patch(f"{MODULE}.execute_strategy_action", return_value=False):
        result = call_strategy_action("strat-3", "pause")

    assert result == {"success": False, "error": "Gateway rejected Pause request"}


def test_call_strategy_action_gateway_raises_exception() -> None:
    """Gateway exception is caught and surfaced as a failure result."""
    with patch(f"{MODULE}.execute_strategy_action", side_effect=RuntimeError("grpc down")):
        result = call_strategy_action("strat-4", "resume")

    assert result["success"] is False
    assert "grpc down" in result["error"]


# ---------------------------------------------------------------------------
# REST fallback path (bump-gas / cancel-tx / other)
# ---------------------------------------------------------------------------


def _mock_response(status_code: int, json_data: dict | None = None) -> MagicMock:
    """Build a requests.Response-like mock."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


def test_call_strategy_action_rest_200_returns_json_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy-path REST 200 returns whatever JSON the server sent."""
    monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "test-api-key")
    body = {"success": True, "message": "gas bumped", "tx_hash": "0xabc"}
    with patch(f"{MODULE}.requests.post", return_value=_mock_response(200, body)) as mock_post:
        result = call_strategy_action("strat-1", "bump-gas", {"gas_price_gwei": 3.5})

    assert result == body
    call = mock_post.call_args
    assert call.args[0].endswith("/api/strategies/strat-1/bump-gas")
    # Regression for #1711: the API key is sourced from the env var, not a
    # hard-coded ``demo-key`` string. The request must be authenticated with
    # whatever key the operator configured.
    assert call.kwargs["headers"]["X-API-Key"] == "test-api-key"
    assert call.kwargs["json"] == {"gas_price_gwei": 3.5}


def test_call_strategy_action_rest_missing_api_key_aborts_without_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for #1711: unset ``ALMANAK_DASHBOARD_API_KEY`` -> refuse to call."""
    monkeypatch.delenv("ALMANAK_DASHBOARD_API_KEY", raising=False)
    with patch(f"{MODULE}.requests.post") as mock_post:
        result = call_strategy_action("strat-1", "bump-gas", {"gas_price_gwei": 3.5})

    assert result["success"] is False
    assert "ALMANAK_DASHBOARD_API_KEY" in result["error"]
    # Critical: no HTTP call must be made when the key is missing. The
    # previous hard-coded ``demo-key`` silently authenticated every request.
    mock_post.assert_not_called()


def test_call_strategy_action_rest_400_includes_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    """400 response surfaces the server-provided ``detail`` field."""
    monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "test-api-key")
    with patch(
        f"{MODULE}.requests.post",
        return_value=_mock_response(400, {"detail": "invalid gas price"}),
    ):
        result = call_strategy_action("strat-1", "bump-gas", {"gas_price_gwei": -1})

    assert result == {"success": False, "error": "invalid gas price"}


def test_call_strategy_action_rest_404_uses_strategy_id_in_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """404 surfaces a friendly strategy-not-found error."""
    monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "test-api-key")
    with patch(f"{MODULE}.requests.post", return_value=_mock_response(404)):
        result = call_strategy_action("unknown-strat", "bump-gas")

    assert result == {"success": False, "error": "Strategy unknown-strat not found"}


def test_call_strategy_action_rest_500_generic_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unexpected status codes produce a generic ``API error: <code>`` message."""
    monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "test-api-key")
    with patch(f"{MODULE}.requests.post", return_value=_mock_response(500)):
        result = call_strategy_action("strat-1", "bump-gas")

    assert result == {"success": False, "error": "API error: 500"}


def test_call_strategy_action_rest_connection_error_sets_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """``ConnectionError`` sets the ``connection_error`` flag for UI branching."""
    monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "test-api-key")
    with patch(f"{MODULE}.requests.post", side_effect=requests.exceptions.ConnectionError()):
        result = call_strategy_action("strat-1", "bump-gas")

    assert result["success"] is False
    assert result.get("connection_error") is True
    assert "Cannot connect to API server" in result["error"]


def test_call_strategy_action_rest_timeout_no_connection_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Timeout`` exceptions do NOT set ``connection_error``."""
    monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "test-api-key")
    with patch(f"{MODULE}.requests.post", side_effect=requests.exceptions.Timeout()):
        result = call_strategy_action("strat-1", "bump-gas")

    assert result == {"success": False, "error": "API request timed out"}
    assert "connection_error" not in result


def test_call_strategy_action_rest_unexpected_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown exceptions are caught in the bare-except fallback."""
    monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "test-api-key")
    with patch(f"{MODULE}.requests.post", side_effect=ValueError("boom")):
        result = call_strategy_action("strat-1", "bump-gas")

    assert result["success"] is False
    assert "boom" in result["error"]


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["Pause", " PAUSE ", "pause"])
def test_call_strategy_action_pause_action_is_case_and_space_insensitive(action: str) -> None:
    """Action name is normalised before dispatch decision."""
    with patch(f"{MODULE}.execute_strategy_action", return_value=True) as mock_exec:
        result = call_strategy_action("strat-1", action)

    assert result["success"] is True
    # Verify gateway path was taken (not REST).
    mock_exec.assert_called_once()
