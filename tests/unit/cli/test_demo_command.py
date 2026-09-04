"""Golden and branch tests for ``almanak strat demo``."""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

import click.testing
import pytest
from click.testing import CliRunner, Result

from almanak import demo_strategies

demo_module = importlib.import_module("almanak.framework.cli.demo")

_STRATEGIES = [
    {"name": "alpha", "chain": "arbitrum", "description": "First strategy"},
    {"name": "beta_long", "chain": "base", "description": ""},
]
_TABLE = "  alpha      arbitrum  First strategy\n  beta_long  base      (no description)\n"


def _assert_result(
    result: Result,
    *,
    exit_code: int,
    output: str = "",
) -> None:
    assert result.exit_code == exit_code
    assert result.output == output
    if exit_code:
        assert isinstance(result.exception, SystemExit)
        assert result.exception.code == exit_code
    else:
        assert result.exception is None


def _install_catalog(monkeypatch: pytest.MonkeyPatch, source_root: Path) -> None:
    monkeypatch.setattr(demo_strategies, "DEMO_STRATEGY_NAMES", tuple(row["name"] for row in _STRATEGIES))
    monkeypatch.setattr(demo_strategies, "list_demo_strategies", lambda: list(_STRATEGIES))
    monkeypatch.setattr(demo_strategies, "get_demo_strategy_path", lambda name: source_root / name)


def _write_source(source_root: Path, name: str, config: object | None = None) -> Path:
    source = source_root / name
    source.mkdir(parents=True)
    (source / "strategy.py").write_text("STRATEGY = True\n", encoding="utf-8")
    if config is not None:
        (source / "config.json").write_text(json.dumps(config), encoding="utf-8")
    return source


def _force_interactive_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(click.testing._NamedTextIOWrapper, "isatty", lambda _self: True)


def test_demo_catalog_error_is_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_discovery() -> list[dict]:
        raise ValueError("duplicate demo name")

    monkeypatch.setattr(demo_strategies, "list_demo_strategies", fail_discovery)

    result = CliRunner().invoke(demo_module.demo, ["--list"])

    _assert_result(result, exit_code=1, output="Error: duplicate demo name\n")


def test_demo_empty_catalog_is_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(demo_strategies, "list_demo_strategies", lambda: [])

    result = CliRunner().invoke(demo_module.demo, ["--list"])

    _assert_result(result, exit_code=1, output="Error: no demo strategies found in package.\n")


def test_demo_list_is_golden(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_catalog(monkeypatch, tmp_path)

    result = CliRunner().invoke(demo_module.demo, ["--list"])

    _assert_result(result, exit_code=0, output=_TABLE)


def test_demo_unknown_name_is_exact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_catalog(monkeypatch, tmp_path)

    result = CliRunner().invoke(demo_module.demo, ["--name", "missing"])

    _assert_result(
        result,
        exit_code=1,
        output=("Error: unknown demo strategy 'missing'.\nAvailable: alpha, beta_long\n"),
    )


def test_demo_without_name_on_non_tty_lists_and_exits_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_catalog(monkeypatch, tmp_path)

    result = CliRunner().invoke(demo_module.demo)

    _assert_result(
        result,
        exit_code=0,
        output=(f"No --name provided and stdin is not a TTY. Available demo strategies:\n\n{_TABLE}"),
    )


def test_demo_interactive_cancellation_has_no_command_level_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_catalog(monkeypatch, tmp_path)
    _force_interactive_stdin(monkeypatch)
    selector = Mock(return_value=None)
    monkeypatch.setattr(demo_module, "_interactive_select", selector)

    result = CliRunner().invoke(demo_module.demo)

    _assert_result(result, exit_code=0)
    selector.assert_called_once_with(_STRATEGIES)


def test_demo_rejects_file_output_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source_root = tmp_path / "sources"
    _write_source(source_root, "alpha")
    _install_catalog(monkeypatch, source_root)
    output_file = tmp_path / "output.txt"
    output_file.write_text("occupied", encoding="utf-8")

    result = CliRunner().invoke(
        demo_module.demo,
        ["--name", "alpha", "--output-dir", str(output_file)],
    )

    _assert_result(
        result,
        exit_code=1,
        output=f"Error: output path is not a directory: {output_file}\n",
    )


def test_demo_rejects_existing_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source_root = tmp_path / "sources"
    _write_source(source_root, "alpha")
    _install_catalog(monkeypatch, source_root)
    output_root = tmp_path / "output"
    target = output_root / "alpha"
    target.mkdir(parents=True)

    result = CliRunner().invoke(
        demo_module.demo,
        ["--name", "alpha", "--output-dir", str(output_root)],
    )

    _assert_result(result, exit_code=1, output=f"Error: directory already exists: {target}\n")


def test_demo_copy_is_golden_and_preserves_runtime_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "sources"
    config = {
        "deployment_id": "deployment:packaged",
        "strategy_name": "PackagedStrategy",
        "description": "catalog copy",
        "protocol": "uniswap_v3",
        "network": "anvil",
        "chain": "arbitrum",
        "chains": ["arbitrum", "base"],
        "trade_amount": "1.25",
        "anvil_funding": {"0x0000000000000000000000000000000000000001": 2500},
        "token_funding": [
            {
                "chain": "arbitrum",
                "protocol": "uniswap_v3",
                "amount": "100.5",
            }
        ],
    }
    source = _write_source(source_root, "alpha", config)
    (source / ".env").write_text("DEMO_VALUE=preserved\n", encoding="utf-8")
    (source / "run_anvil.py").write_text("INTERNAL = True\n", encoding="utf-8")
    tests_dir = source / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_internal.py").write_text("def test_internal(): pass\n", encoding="utf-8")
    _install_catalog(monkeypatch, source_root)
    output_root = tmp_path / "new" / "output"
    monkeypatch.setenv("DEMO_PARENT_VALUE", "unchanged")

    with patch("subprocess.Popen", side_effect=AssertionError("demo copy must not start a process")):
        result = CliRunner().invoke(
            demo_module.demo,
            ["--name", "alpha", "--output-dir", str(output_root)],
        )

    target = output_root / "alpha"
    _assert_result(
        result,
        exit_code=0,
        output=(
            f"\nCopied demo strategy 'alpha' to {target}/\n"
            "\nNext steps:\n"
            f"  cd {target}\n"
            "  almanak strat run --network anvil --once\n"
        ),
    )
    assert os.environ["DEMO_PARENT_VALUE"] == "unchanged"
    assert (target / "strategy.py").is_file()
    assert (target / ".env").read_text(encoding="utf-8") == "DEMO_VALUE=preserved\n"
    assert not (target / "run_anvil.py").exists()
    assert not (target / "tests").exists()
    assert (source / "run_anvil.py").is_file()
    assert (source / "tests" / "test_internal.py").is_file()

    expected_config = {
        "network": "anvil",
        "chain": "arbitrum",
        "chains": ["arbitrum", "base"],
        "trade_amount": "1.25",
        "anvil_funding": {"0x0000000000000000000000000000000000000001": 2500},
        "token_funding": [
            {
                "chain": "arbitrum",
                "protocol": "uniswap_v3",
                "amount": "100.5",
            }
        ],
    }
    assert (target / "config.json").read_text(encoding="utf-8") == json.dumps(expected_config, indent=4) + "\n"


def test_demo_interactive_copy_without_optional_internal_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "sources"
    _write_source(source_root, "beta_long")
    _install_catalog(monkeypatch, source_root)
    _force_interactive_stdin(monkeypatch)
    selector = Mock(return_value="beta_long")
    monkeypatch.setattr(demo_module, "_interactive_select", selector)
    output_root = tmp_path / "output"
    output_root.mkdir()

    result = CliRunner().invoke(demo_module.demo, ["--output-dir", str(output_root)])

    target = output_root / "beta_long"
    _assert_result(
        result,
        exit_code=0,
        output=(
            f"\nCopied demo strategy 'beta_long' to {target}/\n"
            "\nNext steps:\n"
            f"  cd {target}\n"
            "  almanak strat run --network anvil --once\n"
        ),
    )
    selector.assert_called_once_with(_STRATEGIES)
    assert (target / "strategy.py").is_file()


def test_rewrite_config_leaves_runtime_only_config_byte_exact(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    original = '{"chain":"base","network":"mainnet","amount":"42"}\n'
    config_path.write_text(original, encoding="utf-8")

    demo_module._rewrite_config(tmp_path, "alpha")

    assert config_path.read_text(encoding="utf-8") == original


def test_rewrite_config_warns_and_leaves_malformed_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.json"
    original = "{broken\n"
    config_path.write_text(original, encoding="utf-8")

    demo_module._rewrite_config(tmp_path, "alpha")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "Warning: could not parse config.json "
        "(Expecting property name enclosed in double quotes: line 1 column 2 (char 1)); "
        "skipping config rewrite.\n"
    )
    assert config_path.read_text(encoding="utf-8") == original


def test_interactive_select_reports_missing_dependency(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setitem(sys.modules, "simple_term_menu", None)

    with pytest.raises(SystemExit) as exc_info:
        demo_module._interactive_select(_STRATEGIES)

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "Error: interactive selection requires 'simple-term-menu'. "
        "Install it with: pip install simple-term-menu\n"
        "Alternatively, use: almanak strat demo --name <strategy>\n"
    )


@pytest.mark.parametrize(
    ("index", "expected", "suffix"),
    [
        (1, "beta_long", ""),
        (None, None, "Cancelled.\n"),
    ],
)
def test_interactive_select_menu_is_golden(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    index: int | None,
    expected: str | None,
    suffix: str,
) -> None:
    calls: dict[str, object] = {}

    class FakeTerminalMenu:
        def __init__(self, entries: list[str], **kwargs: object) -> None:
            calls["entries"] = entries
            calls["kwargs"] = kwargs

        def show(self) -> int | None:
            return index

    menu_module = ModuleType("simple_term_menu")
    menu_module.TerminalMenu = FakeTerminalMenu  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "simple_term_menu", menu_module)

    selected = demo_module._interactive_select(_STRATEGIES)

    assert selected == expected
    assert calls == {
        "entries": [
            "alpha      [arbitrum]  First strategy",
            "beta_long  [base]  (no description)",
        ],
        "kwargs": {
            "title": "",
            "menu_cursor_style": ("fg_cyan", "bold"),
            "menu_highlight_style": ("bg_gray", "fg_cyan", "bold"),
        },
    }
    captured = capsys.readouterr()
    assert captured.out == f"Select a demo strategy:\n\n{suffix}"
    assert captured.err == ""
