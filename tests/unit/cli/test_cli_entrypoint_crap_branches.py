from __future__ import annotations

import importlib
import importlib.util
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import click
import pytest
from click.testing import CliRunner

cli_module = importlib.import_module("almanak.cli.cli")

_CLEAN_GATEWAY_ENV = {
    "ALMANAK_GATEWAY_HOST": "",
    "ALMANAK_GATEWAY_PORT": "",
    "DASHBOARD_PORT": "",
    "GATEWAY_HOST": "",
    "GATEWAY_PORT": "",
}


@pytest.fixture
def strategy_run_harness(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    events: list[tuple[object, ...]] = []
    framework = object()
    framework_run = Mock(side_effect=lambda **kwargs: events.append(("run", kwargs)))

    def load_dotenv(path: str) -> None:
        events.append(("dotenv", path))

    def emit_banner(*, strategy_name: str | None) -> None:
        events.append(("banner", strategy_name))

    def prime_config(ctx: click.Context) -> SimpleNamespace:
        events.append(("prime", ctx))
        return SimpleNamespace(framework=framework)

    def load_pyproject(working_dir: str) -> dict[str, int]:
        events.append(("pyproject", working_dir))
        return {}

    def install_redaction(*, framework_config: object) -> None:
        events.append(("redaction", framework_config))

    env_module = importlib.import_module("almanak.config.env")
    banner_module = importlib.import_module("almanak.framework.utils.deployment_banner")
    monkeypatch.setattr(env_module, "_load_dotenv_once", load_dotenv)
    monkeypatch.setattr(banner_module, "emit_cli_banner", emit_banner)
    monkeypatch.setattr(cli_module, "_prime_strategy_command_config", prime_config)
    monkeypatch.setattr(cli_module, "_load_pyproject_run_config", load_pyproject)
    monkeypatch.setattr(cli_module, "install_redaction", install_redaction)
    monkeypatch.setattr(cli_module, "framework_run_cmd", framework_run)
    return SimpleNamespace(
        events=events,
        framework=framework,
        framework_run=framework_run,
        banner_module=banner_module,
    )


def _invoke_strategy_run(tmp_path: Path, *args: str):
    return CliRunner().invoke(
        cli_module.strategy_run,
        ["--working-dir", str(tmp_path), *args],
        env=_CLEAN_GATEWAY_ENV,
    )


def test_strategy_run_forwards_exact_defaults(tmp_path: Path, strategy_run_harness: SimpleNamespace) -> None:
    result = _invoke_strategy_run(tmp_path)

    assert result.exit_code == 0
    assert result.output == ""
    assert strategy_run_harness.framework_run.call_args.kwargs == {
        "config_file": None,
        "once": False,
        "interval": 60,
        "dry_run": False,
        "fresh": False,
        "list_all": False,
        "verbose": False,
        "debug": False,
        "dashboard": False,
        "dashboard_port": 8501,
        "dashboard_mode": "hosted-parity",
        "simulate_tx": None,
        "network": None,
        "gateway_host": "127.0.0.1",
        "gateway_port": 50051,
        "no_gateway": False,
        "copy_mode": None,
        "copy_shadow": False,
        "copy_replay_file": None,
        "copy_strict": False,
        "wallet": "default",
        "log_file": None,
        "reset_fork": False,
        "max_iterations": None,
        "teardown_after": False,
        "working_dir": str(tmp_path),
        "anvil_ports": (),
        "keep_anvil": False,
    }


def test_strategy_run_forwards_every_explicit_option(
    tmp_path: Path,
    strategy_run_harness: SimpleNamespace,
) -> None:
    config_file = tmp_path / "custom.json"
    config_file.write_text("{}")
    replay_file = tmp_path / "replay.jsonl"
    log_file = tmp_path / "run.jsonl"

    result = _invoke_strategy_run(
        tmp_path,
        "--config",
        str(config_file),
        "--once",
        "--interval",
        "5",
        "--dry-run",
        "--list",
        "--verbose",
        "--debug",
        "--fresh",
        "--network",
        "anvil",
        "--gateway-host",
        "gateway.internal",
        "--gateway-port",
        "51051",
        "--no-gateway",
        "--copy-mode",
        "shadow",
        "--copy-shadow",
        "--copy-replay-file",
        str(replay_file),
        "--copy-strict",
        "--dashboard",
        "--dashboard-port",
        "8601",
        "--dashboard-mode",
        "command-center",
        "--no-simulate-tx",
        "--wallet",
        "isolated",
        "--log-file",
        str(log_file),
        "--reset-fork",
        "--max-iterations",
        "2",
        "--teardown-after",
        "--anvil-port",
        "arbitrum=8545",
        "--keep-anvil",
    )

    assert result.exit_code == 0
    assert result.output == ""
    assert strategy_run_harness.framework_run.call_args.kwargs == {
        "config_file": str(config_file),
        "once": True,
        "interval": 5,
        "dry_run": True,
        "fresh": True,
        "list_all": True,
        "verbose": True,
        "debug": True,
        "dashboard": True,
        "dashboard_port": 8601,
        "dashboard_mode": "command-center",
        "simulate_tx": False,
        "network": "anvil",
        "gateway_host": "gateway.internal",
        "gateway_port": 51051,
        "no_gateway": True,
        "copy_mode": "shadow",
        "copy_shadow": True,
        "copy_replay_file": str(replay_file),
        "copy_strict": True,
        "wallet": "isolated",
        "log_file": str(log_file),
        "reset_fork": True,
        "max_iterations": 2,
        "teardown_after": True,
        "working_dir": str(tmp_path),
        "anvil_ports": ("arbitrum=8545",),
        "keep_anvil": True,
    }


@pytest.mark.parametrize("filename", ["config.json", "config.yaml", "config.yml"])
def test_strategy_run_discovers_config_in_precedence_order(
    tmp_path: Path,
    strategy_run_harness: SimpleNamespace,
    filename: str,
) -> None:
    start = ["config.json", "config.yaml", "config.yml"].index(filename)
    for candidate in ["config.json", "config.yaml", "config.yml"][start:]:
        (tmp_path / candidate).write_text("{}")

    result = _invoke_strategy_run(tmp_path)

    expected = tmp_path / filename
    assert result.exit_code == 0
    assert result.output == f"Using config: {expected}\n"
    assert strategy_run_harness.framework_run.call_args.kwargs["config_file"] == str(expected)


def test_strategy_run_loads_dotenv_before_banner_and_runtime_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    strategy_run_harness: SimpleNamespace,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALMANAK_PRIVATE_KEY=test\n")

    def load_pyproject(working_dir: str) -> dict[str, int]:
        strategy_run_harness.events.append(("pyproject", working_dir))
        return {"interval": 30}

    monkeypatch.setattr(cli_module, "_load_pyproject_run_config", load_pyproject)
    result = _invoke_strategy_run(tmp_path)

    assert result.exit_code == 0
    assert result.output == f"Loaded environment from: {env_file}\nUsing interval from pyproject.toml: 30s\n"
    assert [event[0] for event in strategy_run_harness.events] == [
        "dotenv",
        "banner",
        "prime",
        "pyproject",
        "redaction",
        "run",
    ]
    assert strategy_run_harness.framework_run.call_args.kwargs["interval"] == 30


@pytest.mark.parametrize("interval", [4, 3601])
def test_strategy_run_rejects_out_of_range_explicit_interval_before_redaction_and_run(
    tmp_path: Path,
    strategy_run_harness: SimpleNamespace,
    interval: int,
) -> None:
    result = _invoke_strategy_run(tmp_path, "--interval", str(interval))

    assert result.exit_code == 1
    assert result.output == "Error: --interval must be between 5 and 3600 seconds\n"
    assert [event[0] for event in strategy_run_harness.events] == ["banner", "prime", "pyproject"]
    strategy_run_harness.framework_run.assert_not_called()


def test_strategy_run_banner_failure_is_non_fatal_and_root_name_falls_back_to_none(
    monkeypatch: pytest.MonkeyPatch,
    strategy_run_harness: SimpleNamespace,
) -> None:
    names: list[str | None] = []

    def fail_banner(*, strategy_name: str | None) -> None:
        names.append(strategy_name)
        raise RuntimeError("banner unavailable")

    monkeypatch.setattr(strategy_run_harness.banner_module, "emit_cli_banner", fail_banner)
    result = CliRunner().invoke(
        cli_module.strategy_run,
        ["--working-dir", "/"],
        env=_CLEAN_GATEWAY_ENV,
    )

    assert result.exit_code == 0
    assert result.output == "Failed to emit deployment-start banner: banner unavailable\n"
    assert names == [None]
    strategy_run_harness.framework_run.assert_called_once()


def test_strategy_run_translates_click_abort_to_silent_exit_one(
    tmp_path: Path,
    strategy_run_harness: SimpleNamespace,
) -> None:
    strategy_run_harness.framework_run.side_effect = click.Abort()

    result = _invoke_strategy_run(tmp_path)

    assert result.exit_code == 1
    assert result.output == ""


def test_strategy_run_formats_framework_failure(
    tmp_path: Path,
    strategy_run_harness: SimpleNamespace,
) -> None:
    strategy_run_harness.framework_run.side_effect = RuntimeError("run exploded")

    result = _invoke_strategy_run(tmp_path)

    assert result.exit_code == 1
    assert result.output == (
        "=========================================\n"
        "[ERROR]\n"
        "Strategy run failed\n"
        "-----------------------------------------\n"
        "Error             : run exploded\n"
        "=========================================\n"
    )


@pytest.fixture
def dashboard_harness(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    resolved = SimpleNamespace(timeout=12.5, auth_token="session-token")
    config = object()
    config_class = Mock(return_value=config)
    config_class.from_env.return_value = resolved
    client = Mock()
    client.wait_for_ready.return_value = True
    client_class = Mock(return_value=client)
    auto_detect = Mock()
    dashboard_env = {"dashboard": "env"}
    env_builder = Mock(return_value=dashboard_env)
    process_run = Mock()

    gateway_module = importlib.import_module("almanak.framework.gateway_client")
    local_paths_module = importlib.import_module("almanak.framework.local_paths")
    dashboard_package = importlib.import_module("almanak.framework.dashboard")
    monkeypatch.setattr(importlib.util, "find_spec", Mock(return_value=object()))
    monkeypatch.setattr(gateway_module, "GatewayClientConfig", config_class)
    monkeypatch.setattr(gateway_module, "GatewayClient", client_class)
    monkeypatch.setattr(local_paths_module, "auto_detect_strategy_folder", auto_detect)
    monkeypatch.setattr(
        dashboard_package,
        "app",
        SimpleNamespace(__file__="/tmp/almanak-dashboard.py"),
        raising=False,
    )
    monkeypatch.setattr(cli_module, "subprocess_env_with_overrides", env_builder)
    monkeypatch.setattr(subprocess, "run", process_run)
    return SimpleNamespace(
        resolved=resolved,
        config=config,
        config_class=config_class,
        client=client,
        client_class=client_class,
        auto_detect=auto_detect,
        dashboard_env=dashboard_env,
        env_builder=env_builder,
        process_run=process_run,
    )


def _invoke_dashboard(*args: str):
    return CliRunner().invoke(cli_module.dashboard, list(args), env=_CLEAN_GATEWAY_ENV)


def _dashboard_check_output(host: str = "127.0.0.1", port: int = 50051) -> str:
    return (
        "=========================================\n"
        "[INFO]\n"
        "Checking Gateway Connection\n"
        "-----------------------------------------\n"
        f"Gateway           : {host}:{port}\n"
        "=========================================\n"
    )


def _dashboard_start_output(host: str, gateway_port: int, dashboard_port: int) -> str:
    return (
        "=========================================\n"
        "[SUCCESS]\n"
        "Starting Almanak Dashboard\n"
        "-----------------------------------------\n"
        f"Dashboard Port    : {dashboard_port}\n"
        f"Gateway           : {host}:{gateway_port}\n"
        f"URL               : http://localhost:{dashboard_port}\n"
        "=========================================\n"
        "\n"
        "Press Ctrl+C to stop the dashboard.\n"
        "\n"
    )


def test_dashboard_missing_streamlit_fails_before_gateway_work(
    monkeypatch: pytest.MonkeyPatch,
    dashboard_harness: SimpleNamespace,
) -> None:
    monkeypatch.setattr(importlib.util, "find_spec", Mock(return_value=None))

    result = _invoke_dashboard()

    assert result.exit_code == 1
    assert result.output == (
        "=========================================\n"
        "[ERROR]\n"
        "Dashboard Requires Streamlit\n"
        "-----------------------------------------\n"
        "Error             : streamlit is not installed\n"
        "Solution          : pip install 'almanak[dashboard]'\n"
        "=========================================\n"
    )
    dashboard_harness.auto_detect.assert_not_called()
    dashboard_harness.client_class.assert_not_called()
    dashboard_harness.process_run.assert_not_called()


def test_dashboard_gateway_not_ready_preserves_failure_order(
    dashboard_harness: SimpleNamespace,
) -> None:
    dashboard_harness.client.wait_for_ready.return_value = False

    result = _invoke_dashboard()

    assert result.exit_code == 1
    assert result.output == _dashboard_check_output() + (
        "=========================================\n"
        "[ERROR]\n"
        "Gateway Not Available\n"
        "-----------------------------------------\n"
        "Error             : Cannot connect to gateway\n"
        "Solution          : Start gateway first with: almanak gateway\n"
        "=========================================\n"
    )
    dashboard_harness.client.connect.assert_called_once_with()
    dashboard_harness.client.wait_for_ready.assert_called_once_with(timeout=5.0)
    dashboard_harness.client.disconnect.assert_called_once_with()
    dashboard_harness.env_builder.assert_not_called()
    dashboard_harness.process_run.assert_not_called()


@pytest.mark.parametrize("failure_method", ["connect", "wait_for_ready", "disconnect"])
def test_dashboard_gateway_exception_is_formatted_before_process_setup(
    dashboard_harness: SimpleNamespace,
    failure_method: str,
) -> None:
    getattr(dashboard_harness.client, failure_method).side_effect = RuntimeError("gateway exploded")

    result = _invoke_dashboard()

    assert result.exit_code == 1
    assert result.output == _dashboard_check_output() + (
        "=========================================\n"
        "[ERROR]\n"
        "Gateway Connection Failed\n"
        "-----------------------------------------\n"
        "Error             : gateway exploded\n"
        "Solution          : Start gateway first with: almanak gateway\n"
        "=========================================\n"
    )
    dashboard_harness.client.disconnect.assert_called_once_with()
    dashboard_harness.env_builder.assert_not_called()
    dashboard_harness.process_run.assert_not_called()


@pytest.mark.parametrize(("browser_args", "headless"), [([], "false"), (["--no-browser"], "true")])
def test_dashboard_runs_exact_streamlit_command(
    dashboard_harness: SimpleNamespace,
    browser_args: list[str],
    headless: str,
) -> None:
    result = _invoke_dashboard(
        "--port",
        "8602",
        "--gateway-host",
        "gateway.internal",
        "--gateway-port",
        "51052",
        *browser_args,
    )

    assert result.exit_code == 0
    assert result.output == _dashboard_check_output("gateway.internal", 51052) + _dashboard_start_output(
        "gateway.internal", 51052, 8602
    )
    dashboard_harness.auto_detect.assert_called_once_with()
    dashboard_harness.config_class.from_env.assert_called_once_with()
    dashboard_harness.config_class.assert_called_once_with(
        host="gateway.internal",
        port=51052,
        timeout=12.5,
        auth_token="session-token",
    )
    dashboard_harness.client_class.assert_called_once_with(dashboard_harness.config)
    dashboard_harness.client.connect.assert_called_once_with()
    dashboard_harness.client.wait_for_ready.assert_called_once_with(timeout=5.0)
    dashboard_harness.client.disconnect.assert_called_once_with()
    dashboard_harness.env_builder.assert_called_once_with({"GATEWAY_HOST": "gateway.internal", "GATEWAY_PORT": "51052"})
    dashboard_harness.process_run.assert_called_once_with(
        [
            cli_module.sys.executable,
            "-m",
            "streamlit",
            "run",
            "/tmp/almanak-dashboard.py",
            "--server.port",
            "8602",
            "--server.headless",
            headless,
            "--browser.gatherUsageStats",
            "false",
        ],
        env=dashboard_harness.dashboard_env,
        check=True,
    )


def test_dashboard_keyboard_interrupt_is_a_clean_stop(dashboard_harness: SimpleNamespace) -> None:
    dashboard_harness.process_run.side_effect = KeyboardInterrupt()

    result = _invoke_dashboard()

    assert result.exit_code == 0
    assert result.output == _dashboard_check_output() + _dashboard_start_output("127.0.0.1", 50051, 8501) + (
        "\nDashboard stopped.\n"
    )


def test_dashboard_formats_streamlit_failure(dashboard_harness: SimpleNamespace) -> None:
    dashboard_harness.process_run.side_effect = subprocess.CalledProcessError(7, ["streamlit"])

    result = _invoke_dashboard()

    assert result.exit_code == 1
    assert result.output == _dashboard_check_output() + _dashboard_start_output("127.0.0.1", 50051, 8501) + (
        "=========================================\n"
        "[ERROR]\n"
        "Dashboard Failed\n"
        "-----------------------------------------\n"
        "Error             : Command '['streamlit']' returned non-zero exit status 7.\n"
        "=========================================\n"
    )
