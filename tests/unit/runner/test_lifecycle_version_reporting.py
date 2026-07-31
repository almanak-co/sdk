from unittest.mock import MagicMock

import pytest

from almanak.core.lifecycle import LifecycleCommand, LifecycleState
from almanak.framework.runner import runner_gateway
from almanak.gateway.proto import gateway_pb2


def _runner_with_lifecycle_client():
    client = MagicMock()
    runner = MagicMock()
    runner._get_gateway_client.return_value = client
    return runner, client


def test_lifecycle_write_state_reports_loaded_almanak_version_once_per_agent(monkeypatch):
    monkeypatch.setattr(runner_gateway, "_REPORTED_ALMANAK_VERSION", "2.16.0rc1")
    runner_gateway._RUNNING_VERSION_REPORTED_DEPLOYMENT_IDS.clear()
    runner, client = _runner_with_lifecycle_client()

    runner_gateway.lifecycle_write_state(runner, "agent-1", LifecycleState.RUNNING)
    runner_gateway.lifecycle_write_state(runner, "agent-1", LifecycleState.RUNNING)

    first_request = client.lifecycle.WriteState.call_args_list[0].args[0]
    second_request = client.lifecycle.WriteState.call_args_list[1].args[0]
    assert first_request.running_almanak_version == "2.16.0rc1"
    assert first_request.HasField("running_almanak_version")
    assert not second_request.HasField("running_almanak_version")


def test_lifecycle_write_state_does_not_report_version_before_running(monkeypatch):
    monkeypatch.setattr(runner_gateway, "_REPORTED_ALMANAK_VERSION", "2.16.0rc1")
    runner_gateway._RUNNING_VERSION_REPORTED_DEPLOYMENT_IDS.clear()
    runner, client = _runner_with_lifecycle_client()

    runner_gateway.lifecycle_write_state(runner, "agent-1", LifecycleState.INITIALIZING)

    request = client.lifecycle.WriteState.call_args.args[0]
    assert not request.HasField("running_almanak_version")


@pytest.mark.parametrize("raw_command", ["STOP", "PAUSE", "RESUME"])
def test_lifecycle_poll_parses_known_current_and_historical_commands(raw_command: str):
    runner, client = _runner_with_lifecycle_client()
    client.lifecycle.ReadCommand.return_value = gateway_pb2.ReadAgentCommandResponse(
        found=True,
        command_id=42,
        deployment_id="agent-1",
        command=raw_command,
        issued_by="operator",
    )

    command = runner_gateway.lifecycle_poll_command(runner, "agent-1")

    assert command is LifecycleCommand(raw_command)
    ack_request = client.lifecycle.AckCommand.call_args.args[0]
    assert ack_request.command_id == 42


def test_lifecycle_poll_unknown_command_is_acked_without_dispatch(caplog):
    runner, client = _runner_with_lifecycle_client()
    client.lifecycle.ReadCommand.return_value = gateway_pb2.ReadAgentCommandResponse(
        found=True,
        command_id=43,
        deployment_id="agent-1",
        command="LIQUIDATE",
        issued_by="broken-client",
    )

    command = runner_gateway.lifecycle_poll_command(runner, "agent-1")

    assert command is None
    ack_request = client.lifecycle.AckCommand.call_args.args[0]
    assert ack_request.command_id == 43
    assert "acknowledging without dispatch" in caplog.text
