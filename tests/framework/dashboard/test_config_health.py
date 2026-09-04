"""Focused tests for dashboard system-health feature gates."""

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from almanak.framework.dashboard import config


@pytest.fixture
def session_state(monkeypatch: pytest.MonkeyPatch) -> dict:
    state: dict = {}
    monkeypatch.setattr(config.st, "session_state", state)
    return state


@pytest.mark.parametrize(
    ("is_connected", "strategies", "expected_status", "actions_enabled"),
    [
        (
            False,
            [
                SimpleNamespace(status="RUNNING", deployment_id="active-strategy"),
                SimpleNamespace(status="STOPPED", deployment_id="stopped-strategy"),
            ],
            "healthy",
            True,
        ),
        (True, [], "degraded", False),
    ],
)
def test_gateway_health_maps_running_strategies_to_feature_gates(
    is_connected: bool,
    strategies: list[SimpleNamespace],
    expected_status: str,
    actions_enabled: bool,
) -> None:
    client = MagicMock(is_connected=is_connected)
    client.list_strategies.return_value = strategies

    with patch(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        return_value=client,
    ):
        health = config._check_health_via_gateway(config.SystemHealth(error="REST API unavailable"))

    assert health.api_available is True
    assert health.api_status == expected_status
    assert health.runners_active == int(actions_enabled)
    assert health.running_strategies == (["active-strategy"] if actions_enabled else [])
    assert health.features["view_strategies"] is True
    assert health.features["preview_teardown"] is True
    for feature in ("pause_resume", "bump_gas", "cancel_tx", "execute_teardown", "hot_reload_config"):
        assert health.features[feature] is actions_enabled
    expected_calls = [call.list_strategies()]
    if not is_connected:
        expected_calls.insert(0, call.connect())
    assert client.method_calls == expected_calls


def test_gateway_health_failure_keeps_original_error_and_disables_actions() -> None:
    client = MagicMock(is_connected=False)
    client.connect.side_effect = RuntimeError("gateway-token-must-not-reach-system-health")
    health = config.SystemHealth(error="REST API unavailable")

    with patch(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        return_value=client,
    ):
        result = config._check_health_via_gateway(health)

    assert result is health
    assert result.error == "REST API unavailable"
    assert result.api_available is False
    assert result.features["view_strategies"] is True
    assert result.features["view_timeline"] is True
    assert result.features["view_config"] is True
    for feature in (
        "preview_teardown",
        "api_available",
        "pause_resume",
        "bump_gas",
        "cancel_tx",
        "execute_teardown",
        "hot_reload_config",
    ):
        assert result.features[feature] is False
    client.list_strategies.assert_not_called()


def test_system_health_returns_fresh_cached_value(session_state: dict) -> None:
    cached = config.SystemHealth(api_available=True, api_status="healthy")
    session_state["_system_health_cache"] = cached
    session_state["_system_health_cache_time"] = 98

    with patch("time.time", return_value=100), patch.object(config.requests, "get") as get_mock:
        result = config.check_system_health()

    assert result is cached
    get_mock.assert_not_called()


def test_system_health_refreshes_stale_cache_from_rest_payload(session_state: dict) -> None:
    session_state["_system_health_cache"] = config.SystemHealth(error="stale")
    session_state["_system_health_cache_time"] = 90
    response = MagicMock(status_code=200)
    response.json.return_value = {
        "status": "degraded",
        "runners_active": 2,
        "running_strategies": [
            {"deployment_id": "strategy-a"},
            {"deployment_id": "strategy-b"},
        ],
        "features": {"pause_resume": True},
    }

    with (
        patch("time.time", side_effect=[100, 101]),
        patch.object(config.requests, "get", return_value=response) as get_mock,
    ):
        result = config.check_system_health()

    assert result.api_available is True
    assert result.api_status == "degraded"
    assert result.runners_active == 2
    assert result.running_strategies == ["strategy-a", "strategy-b"]
    assert result.features == {"pause_resume": True}
    get_mock.assert_called_once_with(f"{config.API_BASE_URL}/api/health", timeout=2)
    assert session_state["_system_health_cache"] is result
    assert session_state["_system_health_cache_time"] == 101


def test_system_health_uses_fail_closed_defaults_for_sparse_rest_payload(session_state: dict) -> None:
    response = MagicMock(status_code=200)
    response.json.return_value = {"running_strategies": [{}]}

    with patch("time.time", return_value=100), patch.object(config.requests, "get", return_value=response):
        result = config.check_system_health()

    assert result.api_available is True
    assert result.api_status == "healthy"
    assert result.runners_active == 0
    assert result.running_strategies == [""]
    assert result.features["view_strategies"] is True
    assert result.features["preview_teardown"] is True
    assert result.features["api_available"] is True
    for feature in ("pause_resume", "bump_gas", "cancel_tx", "execute_teardown", "hot_reload_config"):
        assert result.features[feature] is False


def test_system_health_records_non_success_rest_status_without_gateway_fallback(session_state: dict) -> None:
    response = MagicMock(status_code=503)

    with (
        patch("time.time", return_value=100),
        patch.object(config.requests, "get", return_value=response),
        patch.object(config, "_check_health_via_gateway") as gateway_health,
    ):
        result = config.check_system_health()

    assert result.api_available is False
    assert result.error == "API returned status 503"
    gateway_health.assert_not_called()


@pytest.mark.parametrize(
    "request_error",
    [
        config.requests.exceptions.ConnectionError("connection refused"),
        config.requests.exceptions.Timeout("request timed out"),
    ],
)
def test_system_health_uses_gateway_for_rest_transport_failures(
    session_state: dict,
    request_error: Exception,
) -> None:
    gateway_result = config.SystemHealth(api_available=True, api_status="degraded")

    with (
        patch("time.time", return_value=100),
        patch.object(config.requests, "get", side_effect=request_error) as get_mock,
        patch.object(config, "_check_health_via_gateway", return_value=gateway_result) as gateway_health,
    ):
        result = config.check_system_health()

    assert result is gateway_result
    get_mock.assert_called_once_with(f"{config.API_BASE_URL}/api/health", timeout=2)
    fallback_input = gateway_health.call_args.args[0]
    assert fallback_input.error == str(request_error)
    assert session_state["_system_health_cache"] is gateway_result


def test_system_health_records_unexpected_error_without_gateway_fallback(session_state: dict) -> None:
    with (
        patch("time.time", return_value=100),
        patch.object(config.requests, "get", side_effect=ValueError("invalid health response")),
        patch.object(config, "_check_health_via_gateway") as gateway_health,
    ):
        result = config.check_system_health()

    assert result.api_available is False
    assert result.features == {}
    assert result.error == "invalid health response"
    gateway_health.assert_not_called()
