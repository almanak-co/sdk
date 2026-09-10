"""Startup confirmation must precede every strategy execution lane."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import _run_modes


@pytest.mark.parametrize("mode", ["continuous", "once", "lifecycle"])
@pytest.mark.parametrize("answer,allowed", [("y\n", True), ("n\n", False), ("\n", False), ("", False)])
def test_confirmation_gates_dispatch_and_cleans_up_on_decline(monkeypatch, mode, answer, allowed):
    cleanup = AsyncMock()
    lanes = {name: MagicMock(return_value=0) for name in ("continuous", "once", "lifecycle")}
    for name, mock in lanes.items():
        monkeypatch.setattr(_run_modes, "_run_test_lifecycle" if name == "lifecycle" else f"_run_{name}", mock)

    @click.command()
    def command():
        code = _run_modes._execute_run_mode(
            confirm_start=True,
            test_actions=["SWAP"] if mode == "lifecycle" else None,
            once=mode == "once",
            teardown_after=False,
            test_json=False,
            runner=SimpleNamespace(),
            strategy_instance=SimpleNamespace(deployment_id="deployment:example"),
            state_manager=SimpleNamespace(),
            cleanup_fn=cleanup,
            interval=15,
            max_iterations=None,
            reset_fork=False,
            managed_gateway=SimpleNamespace(),
        )
        raise SystemExit(code)

    result = CliRunner().invoke(command, input=answer)
    assert result.exit_code == (0 if allowed else 2), result.output
    assert "deployment:example" in result.output
    for name, mock in lanes.items():
        assert mock.call_count == int(allowed and name == mode)
    assert cleanup.await_count == int(not allowed)


@pytest.mark.parametrize("enabled", [False, True])
def test_public_cli_forwards_confirmation_without_changing_default(monkeypatch, tmp_path, enabled):
    import importlib

    cli = importlib.import_module("almanak.cli.cli")
    framework_run = MagicMock()
    monkeypatch.setattr(cli, "framework_run_cmd", framework_run)
    argv = ["strat", "run", "-d", str(tmp_path)]
    if enabled:
        argv.append("--confirm-start")
    result = CliRunner().invoke(cli.almanak, argv)
    assert result.exit_code == 0, result.output
    assert framework_run.call_args.kwargs["confirm_start"] is enabled
