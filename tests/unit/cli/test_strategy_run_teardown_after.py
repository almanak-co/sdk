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
