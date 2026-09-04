"""Branch and golden-output tests for ``almanak strat pause``."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from almanak.framework.cli import status as status_mod
from almanak.framework.cli.status import strategy_pause
from almanak.gateway.proto import gateway_pb2


def _action_response(*, success: bool = True, error: str = "", action_id: str = "action-1") -> SimpleNamespace:
    return SimpleNamespace(success=success, error=error, action_id=action_id)


def _details(status: str) -> SimpleNamespace:
    return SimpleNamespace(summary=SimpleNamespace(status=status))


def _client(*, action_response=None, action_error: Exception | None = None) -> MagicMock:
    client = MagicMock()
    if action_error is not None:
        client.dashboard.ExecuteAction.side_effect = action_error
    else:
        client.dashboard.ExecuteAction.return_value = action_response or _action_response()
    return client


def _invoke(client: MagicMock, *args: str):
    with patch.object(status_mod, "_make_client", return_value=client):
        return CliRunner().invoke(strategy_pause, ["-s", "deployment:abc123", "--reason", "review", *args])


def test_strategy_pause_no_wait_golden_and_request() -> None:
    client = _client(action_response=_action_response(action_id="pause-42"))

    result = _invoke(client)

    assert result.exit_code == 0, result.output
    assert result.output == "Pause command issued for deployment:abc123 (action_id: pause-42)\n"
    assert client.dashboard.ExecuteAction.call_args.args[0] == gateway_pb2.ExecuteActionRequest(
        deployment_id="deployment:abc123",
        action="PAUSE",
        reason="review",
    )
    client.dashboard.GetStrategyDetails.assert_not_called()
    client.disconnect.assert_called_once_with()


def test_strategy_pause_rpc_error_exits_and_disconnects() -> None:
    client = _client(action_error=RuntimeError("rpc unavailable"))

    result = _invoke(client)

    assert result.exit_code == 1
    assert result.output == "Failed to pause strategy: rpc unavailable\n"
    client.disconnect.assert_called_once_with()


def test_strategy_pause_codifies_current_gateway_rejection() -> None:
    client = _client(
        action_response=_action_response(
            success=False,
            error="Action not implemented: PAUSE",
            action_id="",
        )
    )

    result = _invoke(client)

    assert result.exit_code == 1
    assert result.output == "Pause failed: Action not implemented: PAUSE\n"
    assert client.dashboard.ExecuteAction.call_args.args[0].action == "PAUSE"
    client.disconnect.assert_called_once_with()


def test_strategy_pause_waits_for_transition() -> None:
    client = _client(action_response=_action_response(action_id="pause-42"))
    client.dashboard.GetStrategyDetails.side_effect = [_details("RUNNING"), _details("PAUSED")]

    with patch("time.monotonic", return_value=100.0), patch("time.sleep") as sleep:
        result = _invoke(client, "--wait", "--timeout", "5")

    assert result.exit_code == 0, result.output
    assert result.output == (
        "Pause command issued for deployment:abc123 (action_id: pause-42)\nStrategy deployment:abc123 is now PAUSED.\n"
    )
    requests = [call.args[0] for call in client.dashboard.GetStrategyDetails.call_args_list]
    assert requests == [
        gateway_pb2.GetStrategyDetailsRequest(deployment_id="deployment:abc123"),
        gateway_pb2.GetStrategyDetailsRequest(deployment_id="deployment:abc123"),
    ]
    sleep.assert_not_called()
    client.disconnect.assert_called_once_with()


def test_strategy_pause_pre_status_error_is_best_effort() -> None:
    client = _client()
    client.dashboard.GetStrategyDetails.side_effect = [RuntimeError("sample failed"), _details("PAUSED")]

    with patch("time.monotonic", return_value=100.0), patch("time.sleep") as sleep:
        result = _invoke(client, "--wait", "--timeout", "5")

    assert result.exit_code == 0, result.output
    assert "sample failed" not in result.output
    assert "is now PAUSED" in result.output
    sleep.assert_not_called()
    client.disconnect.assert_called_once_with()


def test_strategy_pause_poll_error_then_timeout() -> None:
    client = _client()
    client.dashboard.GetStrategyDetails.side_effect = [_details("RUNNING"), RuntimeError("poll failed")]

    with patch("time.monotonic", side_effect=[100.0, 100.0, 102.0]), patch("time.sleep") as sleep:
        result = _invoke(client, "--wait", "--timeout", "1")

    assert result.exit_code == 1
    assert result.output == (
        "Pause command issued for deployment:abc123 (action_id: action-1)\n"
        "Poll error: poll failed\n"
        "Timed out waiting for deployment:abc123 to reach PAUSED status.\n"
    )
    sleep.assert_called_once_with(2)
    client.disconnect.assert_called_once_with()


def test_strategy_pause_non_paused_status_times_out() -> None:
    client = _client()
    client.dashboard.GetStrategyDetails.side_effect = [_details("RUNNING"), _details("RUNNING")]

    with patch("time.monotonic", side_effect=[100.0, 100.0, 102.0]), patch("time.sleep") as sleep:
        result = _invoke(client, "--wait", "--timeout", "1")

    assert result.exit_code == 1
    assert result.output.endswith("Timed out waiting for deployment:abc123 to reach PAUSED status.\n")
    sleep.assert_called_once_with(2)
    client.disconnect.assert_called_once_with()


def test_strategy_pause_preexisting_paused_status_does_not_false_positive() -> None:
    client = _client()
    client.dashboard.GetStrategyDetails.side_effect = [_details("PAUSED"), _details("PAUSED")]

    with patch("time.monotonic", side_effect=[100.0, 100.0, 102.0]), patch("time.sleep") as sleep:
        result = _invoke(client, "--wait", "--timeout", "1")

    assert result.exit_code == 1
    assert "is now PAUSED" not in result.output
    assert result.output.endswith("Timed out waiting for deployment:abc123 to reach PAUSED status.\n")
    sleep.assert_called_once_with(2)
    client.disconnect.assert_called_once_with()
