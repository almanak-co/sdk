from unittest.mock import MagicMock

from almanak.framework.runner import runner_gateway


def _runner_with_lifecycle_client():
    client = MagicMock()
    runner = MagicMock()
    runner._get_gateway_client.return_value = client
    return runner, client


def test_lifecycle_write_state_reports_loaded_almanak_version_once_per_agent(monkeypatch):
    monkeypatch.setattr(runner_gateway, "_REPORTED_ALMANAK_VERSION", "2.16.0rc1")
    runner_gateway._RUNNING_VERSION_REPORTED_AGENT_IDS.clear()
    runner, client = _runner_with_lifecycle_client()

    runner_gateway.lifecycle_write_state(runner, "agent-1", "RUNNING")
    runner_gateway.lifecycle_write_state(runner, "agent-1", "RUNNING")

    first_request = client.lifecycle.WriteState.call_args_list[0].args[0]
    second_request = client.lifecycle.WriteState.call_args_list[1].args[0]
    assert first_request.running_almanak_version == "2.16.0rc1"
    assert first_request.HasField("running_almanak_version")
    assert not second_request.HasField("running_almanak_version")


def test_lifecycle_write_state_does_not_report_version_before_running(monkeypatch):
    monkeypatch.setattr(runner_gateway, "_REPORTED_ALMANAK_VERSION", "2.16.0rc1")
    runner_gateway._RUNNING_VERSION_REPORTED_AGENT_IDS.clear()
    runner, client = _runner_with_lifecycle_client()

    runner_gateway.lifecycle_write_state(runner, "agent-1", "INITIALIZING")

    request = client.lifecycle.WriteState.call_args.args[0]
    assert not request.HasField("running_almanak_version")
