from __future__ import annotations

import json
import os
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import click
import pytest
from click.testing import CliRunner

from almanak.cli import cli as cli_mod
from almanak.framework.cli._scenario import ScenarioOverrides

PRIVATE_KEY = "0x" + "1" * 64


def _install_runtime_fakes(
    monkeypatch: pytest.MonkeyPatch,
    callback: Callable[..., None],
    *,
    private_key: str | None = PRIVATE_KEY,
    skip_reason: str | None = None,
) -> None:
    monkeypatch.setattr(cli_mod, "framework_run_cmd", callback)
    monkeypatch.setattr(cli_mod, "install_redaction", lambda: None)
    monkeypatch.setattr(cli_mod, "_strat_test_skip_reason", lambda *_: skip_reason)
    boot_config = SimpleNamespace(gateway=SimpleNamespace(private_key=private_key))
    monkeypatch.setattr(cli_mod, "_prime_strategy_command_config", lambda _: boot_config)


def _invoke(tmp_path: Path, *args: str):
    return CliRunner().invoke(cli_mod.strat, ["test", "--working-dir", str(tmp_path), *args])


def test_requires_an_action_injection_or_teardown(tmp_path):
    result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert result.output == (
        "Error: strat test needs at least one of: --actions <csv>, --inject <json>, or --teardown\n"
    )


def test_invalid_injection_remains_a_click_error(tmp_path):
    result = _invoke(tmp_path, "--inject", "{not json}")

    assert result.exit_code == 1
    assert result.output.startswith("Error: --inject: invalid JSON")


def test_forwards_the_complete_framework_runtime_contract(monkeypatch, tmp_path):
    captured: dict[str, Any] = {}

    def capture_framework_run(**kwargs):
        captured.update(kwargs)

    _install_runtime_fakes(monkeypatch, capture_framework_run)
    config_file = tmp_path / "alternate.yaml"
    config_file.write_text("chain: arbitrum\n")

    result = _invoke(
        tmp_path,
        "--config",
        str(config_file),
        "--actions",
        " open, ,close ",
        "--teardown",
        "--asset-policy",
        "keep_outputs",
        "--inject",
        '{"prices": {"USDC": "0.95"}}',
        "--json",
        "--anvil-port",
        "arbitrum=8545",
        "--anvil-port",
        "base=8546",
        "--gateway-host",
        "gateway.internal",
        "--gateway-port",
        "60000",
        "--no-gateway",
    )

    assert result.exit_code == 0, result.output
    assert captured.pop("test_inject") == ScenarioOverrides(prices={"USDC": Decimal("0.95")})
    assert captured == {
        "config_file": str(config_file),
        "once": True,
        "interval": cli_mod.DEFAULT_STRAT_RUN_INTERVAL,
        "dry_run": False,
        "list_all": False,
        "verbose": False,
        "debug": False,
        "dashboard": False,
        "dashboard_port": 8501,
        "simulate_tx": None,
        "network": "anvil",
        "gateway_host": "gateway.internal",
        "gateway_port": 60000,
        "no_gateway": True,
        "copy_mode": None,
        "copy_shadow": False,
        "copy_replay_file": None,
        "copy_strict": False,
        "wallet": "default",
        "log_file": None,
        "reset_fork": False,
        "max_iterations": None,
        "teardown_after": True,
        "working_dir": str(tmp_path),
        "anvil_ports": ("arbitrum=8545", "base=8546"),
        "test_actions": ["open", "close"],
        "test_json": True,
        "test_asset_policy": "keep_outputs",
        "fresh": True,
    }


def test_injection_without_actions_runs_a_natural_decide_iteration(monkeypatch, tmp_path):
    captured: dict[str, Any] = {}
    _install_runtime_fakes(monkeypatch, lambda **kwargs: captured.update(kwargs))

    result = _invoke(tmp_path, "--inject", '{"balances": {"USDC": "100"}}')

    assert result.exit_code == 0, result.output
    assert captured["test_actions"] == [""]


def test_strategy_env_loads_before_config_without_overriding_shell_values(monkeypatch, tmp_path):
    shell_key = "0x" + "2" * 64
    dotenv_key = "0x" + "3" * 64
    monkeypatch.setenv("ALMANAK_PRIVATE_KEY", shell_key)
    (tmp_path / ".env").write_text(f"ALMANAK_PRIVATE_KEY={dotenv_key}\n")
    observed: dict[str, Any] = {}

    def prime_config(_):
        observed["config_private_key"] = os.environ["ALMANAK_PRIVATE_KEY"]
        return SimpleNamespace(gateway=SimpleNamespace(private_key=observed["config_private_key"]))

    def capture_framework_run(**_):
        from almanak.framework.cli.run_helpers import _runtime_private_key_override

        observed["runtime_private_key"] = _runtime_private_key_override.get()

    monkeypatch.setattr(cli_mod, "_prime_strategy_command_config", prime_config)
    monkeypatch.setattr(cli_mod, "framework_run_cmd", capture_framework_run)
    monkeypatch.setattr(cli_mod, "install_redaction", lambda: None)
    monkeypatch.setattr(cli_mod, "_strat_test_skip_reason", lambda *_: None)

    result = _invoke(tmp_path, "--actions", "open")

    assert result.exit_code == 0, result.output
    assert result.output == f"Loaded environment from: {tmp_path / '.env'}\n"
    assert observed == {"config_private_key": shell_key, "runtime_private_key": None}
    assert os.environ["ALMANAK_PRIVATE_KEY"] == shell_key


def test_setup_failure_emits_documented_json_result(monkeypatch, tmp_path):
    monkeypatch.setattr(
        cli_mod, "_prime_strategy_command_config", lambda _: (_ for _ in ()).throw(RuntimeError("bad config"))
    )
    monkeypatch.setattr(cli_mod, "install_redaction", lambda: None)
    monkeypatch.setattr(
        cli_mod,
        "_strat_test_skip_reason",
        lambda *_: pytest.fail("skip detection must not run after setup fails"),
    )
    monkeypatch.setattr(
        cli_mod,
        "framework_run_cmd",
        lambda **_: pytest.fail("framework runner must not run after setup fails"),
    )

    result = _invoke(tmp_path, "--actions", "open", "--teardown", "--json")

    assert result.exit_code == 1
    assert json.loads(result.output) == {
        "summary": {
            "all_passed": False,
            "skipped": False,
            "skip_reason": None,
            "steps_run": 0,
            "actions_passed": False,
            "teardown_passed": False,
            "coverage": {
                "requested_paths_exercised": False,
                "actions": [{"action": "open", "outcome": "not_run"}],
                "teardown": "not_run",
            },
            "error": "bad config",
        },
        "steps": [],
    }


def test_setup_failure_preserves_human_exception_behavior(monkeypatch, tmp_path):
    monkeypatch.setattr(
        cli_mod,
        "_prime_strategy_command_config",
        lambda _: (_ for _ in ()).throw(RuntimeError("bad config")),
    )

    result = _invoke(tmp_path, "--actions", "open")

    assert result.exit_code == 1
    assert result.output == ""
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "bad config"


def test_setup_abort_preserves_click_abort_behavior(monkeypatch, tmp_path):
    monkeypatch.setattr(
        cli_mod,
        "_prime_strategy_command_config",
        lambda _: (_ for _ in ()).throw(click.Abort()),
    )

    result = _invoke(tmp_path, "--actions", "open", "--json")

    assert result.exit_code == 1
    assert result.output == "Aborted!\n"


def test_runner_failure_preserves_human_error_output(monkeypatch, tmp_path):
    def fail_framework_run(**_):
        raise RuntimeError("gateway unavailable")

    _install_runtime_fakes(monkeypatch, fail_framework_run)

    result = _invoke(tmp_path, "--actions", "open")

    assert result.exit_code == 1
    assert result.output == (
        "=========================================\n"
        "[ERROR]\n"
        "Strategy test failed\n"
        "-----------------------------------------\n"
        "Error             : gateway unavailable\n"
        "=========================================\n"
    )


def test_runner_failure_preserves_json_error_output(monkeypatch, tmp_path):
    def fail_framework_run(**_):
        raise RuntimeError("gateway unavailable")

    _install_runtime_fakes(monkeypatch, fail_framework_run)

    result = _invoke(tmp_path, "--actions", "open", "--json")
    payload = json.loads(result.output)

    assert result.exit_code == 1
    assert payload["summary"]["error"] == "gateway unavailable"
    assert payload["summary"]["teardown_passed"] is None
    assert payload["summary"]["coverage"]["actions"] == [{"action": "open", "outcome": "not_run"}]
    assert payload["steps"] == []


def test_click_abort_preserves_silent_exit(monkeypatch, tmp_path):
    def abort_framework_run(**_):
        raise click.Abort()

    _install_runtime_fakes(monkeypatch, abort_framework_run)

    result = _invoke(tmp_path, "--teardown")

    assert result.exit_code == 1
    assert result.output == ""


def test_human_skip_preserves_message_and_success_exit(monkeypatch, tmp_path):
    _install_runtime_fakes(
        monkeypatch,
        lambda **_: pytest.fail("framework runner must not run for a skipped chain"),
        skip_reason="chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)",
    )

    result = _invoke(tmp_path, "--actions", "open")

    assert result.exit_code == 0
    assert result.output == "SKIP: chain 'solana' is not Anvil-forkable (not in CHAIN_IDS)\n"


def test_asset_policy_without_teardown_remains_forwarded(monkeypatch, tmp_path):
    captured: dict[str, Any] = {}
    _install_runtime_fakes(monkeypatch, lambda **kwargs: captured.update(kwargs))

    result = _invoke(tmp_path, "--actions", "open", "--asset-policy", "keep_outputs")

    assert result.exit_code == 0, result.output
    assert captured["teardown_after"] is False
    assert captured["test_asset_policy"] == "keep_outputs"
