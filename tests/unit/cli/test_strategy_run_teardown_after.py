"""Tests for `almanak strat run --teardown-after` passthrough."""

from __future__ import annotations

import importlib

import pytest
from click.testing import CliRunner

cli_module = importlib.import_module("almanak.cli.cli")


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


def test_strat_run_help_includes_teardown_after_option(cli_runner: CliRunner) -> None:
    """`almanak strat run --help` should expose --teardown-after."""
    result = cli_runner.invoke(cli_module.almanak, ["strat", "run", "--help"])

    assert result.exit_code == 0
    assert "--teardown-after" in result.output


def test_strat_run_passes_teardown_after_to_framework_run(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Wrapper CLI should passthrough teardown_after to framework run command."""
    captured_kwargs: dict[str, object] = {}

    def fake_framework_run_cmd(**kwargs):
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(cli_module, "framework_run_cmd", fake_framework_run_cmd)

    result = cli_runner.invoke(
        cli_module.almanak,
        [
            "strat",
            "run",
            "-d",
            str(tmp_path),
            "--once",
            "--teardown-after",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_kwargs["teardown_after"] is True
    assert captured_kwargs["once"] is True
    assert captured_kwargs["working_dir"] == str(tmp_path)


def test_strat_run_help_includes_keep_anvil_option(cli_runner: CliRunner) -> None:
    """`almanak strat run --help` should expose --keep-anvil (VIB-5846, completes VIB-5063).

    Regression test: the framework's `run` command (almanak/framework/cli/run.py)
    has always declared `--keep-anvil`, but the CLI-registered `strat run` command
    is a separate wrapper (`strategy_run` in almanak/cli/cli.py) that redeclares its
    own click options and forwards to the framework command via `ctx.invoke`. The
    wrapper never redeclared `--keep-anvil`, so it was unreachable from the actual
    CLI entrypoint even though unit tests against the framework helper passed.
    """
    result = cli_runner.invoke(cli_module.almanak, ["strat", "run", "--help"])

    assert result.exit_code == 0
    assert "--keep-anvil" in result.output


def test_strat_run_passes_keep_anvil_to_framework_run(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Wrapper CLI should passthrough --keep-anvil as keep_anvil=True."""
    captured_kwargs: dict[str, object] = {}

    def fake_framework_run_cmd(**kwargs):
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(cli_module, "framework_run_cmd", fake_framework_run_cmd)

    result = cli_runner.invoke(
        cli_module.almanak,
        [
            "strat",
            "run",
            "-d",
            str(tmp_path),
            "--once",
            "--network",
            "anvil",
            "--keep-anvil",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_kwargs["keep_anvil"] is True


def test_strat_run_defaults_keep_anvil_false(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Without --keep-anvil, the wrapper should still pass keep_anvil=False explicitly."""
    captured_kwargs: dict[str, object] = {}

    def fake_framework_run_cmd(**kwargs):
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(cli_module, "framework_run_cmd", fake_framework_run_cmd)

    result = cli_runner.invoke(
        cli_module.almanak,
        [
            "strat",
            "run",
            "-d",
            str(tmp_path),
            "--once",
            "--network",
            "anvil",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_kwargs["keep_anvil"] is False


def test_strat_run_uses_interval_from_pyproject_when_flag_omitted(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Wrapper CLI should default --interval from [tool.almanak.run] when present."""
    captured_kwargs: dict[str, object] = {}
    (tmp_path / "pyproject.toml").write_text(
        "\n".join(
            [
                "[tool.almanak]",
                'framework = "v2"',
                "",
                "[tool.almanak.run]",
                "interval = 30",
            ]
        )
    )

    def fake_framework_run_cmd(**kwargs):
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(cli_module, "framework_run_cmd", fake_framework_run_cmd)

    result = cli_runner.invoke(
        cli_module.almanak,
        [
            "strat",
            "run",
            "-d",
            str(tmp_path),
            "--once",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured_kwargs["interval"] == 30
    assert "Using interval from pyproject.toml: 30s" in result.output


# Options the wrapper (`strategy_run`) intentionally does NOT redeclare yet.
# This is the pre-existing wrapper/framework option-list drift documented in
# docs/internal/blueprints/16-cli-reference.md and tracked as a follow-up under
# VIB-5846. Removing an entry here means the wrapper gained that option (good) —
# delete the line. Adding a NEW entry means the framework grew an option the
# wrapper silently dropped: only add it if that drop is a conscious, tracked
# decision, never to make this test pass.
_KNOWN_WRAPPER_DRIFT: frozenset[str] = frozenset({"--dashboard-mode", "--debug", "--list", "--simulate-tx"})


def test_strat_run_options_do_not_drift_from_framework_run() -> None:
    """`strategy_run` must forward every framework `run` option except tracked drift.

    Guards the exact bug class VIB-5846 fixed: the CLI-registered `strat run`
    wrapper (`strategy_run`) hand-duplicates the framework `run` command's option
    list and forwards by name, so any framework option the wrapper forgets to
    redeclare becomes silently unreachable from the real CLI (as `--keep-anvil`
    was). This asserts the wrapper's options are a superset of the framework
    command's, modulo the explicitly-tracked `_KNOWN_WRAPPER_DRIFT` set.
    """
    framework_opts = {opt for param in cli_module.framework_run_cmd.params for opt in param.opts}
    wrapper_opts = {opt for param in cli_module.strategy_run.params for opt in param.opts}

    missing = framework_opts - wrapper_opts - _KNOWN_WRAPPER_DRIFT
    assert not missing, (
        "strat run wrapper is missing framework `run` options (new drift): "
        f"{sorted(missing)}. Either forward them in strategy_run or, if the drop "
        "is intentional and tracked, add them to _KNOWN_WRAPPER_DRIFT."
    )

    # Keep the drift allowlist honest: an entry that the wrapper has since gained,
    # or that the framework no longer declares, is stale and must be removed.
    stale = _KNOWN_WRAPPER_DRIFT - (framework_opts - wrapper_opts)
    assert not stale, f"_KNOWN_WRAPPER_DRIFT lists options that are no longer drifted: {sorted(stale)}. Remove them."
