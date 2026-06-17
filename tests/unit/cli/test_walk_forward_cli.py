"""Focused tests for the walk-forward backtest CLI helpers."""

from __future__ import annotations

import importlib
import json
from datetime import datetime
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli.backtest import backtest
from almanak.framework.cli.backtest.advanced import (
    _build_walk_forward_context,
    _format_walk_forward_param_range,
    _resolve_walk_forward_strategy_class,
)
from almanak.framework.cli.backtest.sweep import parse_param_ranges_from_config


def _write_walk_forward_config(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "param_ranges": {
                    "threshold": {"type": "continuous", "min": 0.01, "max": 0.1, "log": True},
                    "window": {"type": "discrete", "min": 10, "max": 30, "step": 10},
                    "mode": {"type": "categorical", "choices": ["fast", "slow"]},
                },
                "objective": "sortino_ratio",
                "n_trials": 7,
                "patience": 3,
            }
        )
    )
    return path


def test_walk_forward_context_uses_cli_overrides(tmp_path: Path) -> None:
    config_path = _write_walk_forward_config(tmp_path / "walk_forward.json")

    ctx = _build_walk_forward_context(
        strategy="demo_uniswap_lp",
        start=datetime(2024, 1, 1),
        end=datetime(2024, 5, 1),
        config_file=str(config_path),
        train_days=30,
        test_days=10,
        step_days=5,
        gap_days=2,
        min_windows=2,
        objective="sharpe_ratio",
        n_trials=11,
        patience=4,
        interval=1800,
        initial_capital=25000.0,
        chain="base",
        tokens="weth, usdc",
        output="wf.json",
        verbose=True,
    )

    assert ctx.settings.objective == "sharpe_ratio"
    assert ctx.settings.n_trials == 11
    assert ctx.settings.patience == 4
    assert ctx.token_list == ["WETH", "USDC"]
    assert ctx.output_label == "wf.json"
    assert ctx.output_path == Path("wf.json")
    assert ctx.window_summary.total_duration_days == 121
    assert ctx.window_summary.effective_step_days == 5
    assert ctx.window_summary.estimated_windows == 16


def test_walk_forward_param_range_formatting(tmp_path: Path) -> None:
    config_path = _write_walk_forward_config(tmp_path / "walk_forward.json")
    ranges = parse_param_ranges_from_config(json.loads(config_path.read_text()))

    assert _format_walk_forward_param_range("threshold", ranges["threshold"]) == (
        "  threshold: continuous [0.01, 0.1] (log)"
    )
    assert _format_walk_forward_param_range("window", ranges["window"]) == (
        "  window: discrete [10, 30, step=10]"
    )
    assert _format_walk_forward_param_range("mode", ranges["mode"]) == (
        "  mode: categorical ['fast', 'slow']"
    )
    assert _format_walk_forward_param_range("legacy", ["a", "b"]) == "  legacy: ['a', 'b']"


def test_walk_forward_fallback_strategy_uses_bound_mock(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing_strategy(_name: str) -> None:
        raise ValueError("missing")

    advanced_module = importlib.import_module("almanak.framework.cli.backtest.advanced")

    monkeypatch.setattr(advanced_module, "get_strategy", _missing_strategy)

    strategy_class = _resolve_walk_forward_strategy_class("missing")

    assert strategy_class.deployment_id == "mock-walk-forward"
    assert strategy_class({}).deployment_id == "mock-walk-forward"


def test_walk_forward_empty_param_ranges_is_usage_error(tmp_path: Path) -> None:
    config_path = tmp_path / "empty.json"
    config_path.write_text(json.dumps({"param_ranges": {}}))

    with pytest.raises(click.UsageError, match="No parameter ranges defined"):
        _build_walk_forward_context(
            strategy="demo_uniswap_lp",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 5, 1),
            config_file=str(config_path),
            train_days=30,
            test_days=10,
            step_days=None,
            gap_days=0,
            min_windows=2,
            objective=None,
            n_trials=None,
            patience=None,
            interval=3600,
            initial_capital=10000.0,
            chain="ethereum",
            tokens="WETH,USDC",
            output=None,
            verbose=False,
        )


def test_walk_forward_unknown_strategy_aborts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = _write_walk_forward_config(tmp_path / "walk_forward.json")

    advanced_module = importlib.import_module("almanak.framework.cli.backtest.advanced")

    monkeypatch.setattr(advanced_module, "list_strategies_fn", lambda: ["known_strategy"])

    with pytest.raises(click.Abort):
        _build_walk_forward_context(
            strategy="missing_strategy",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 5, 1),
            config_file=str(config_path),
            train_days=30,
            test_days=10,
            step_days=None,
            gap_days=0,
            min_windows=2,
            objective=None,
            n_trials=None,
            patience=None,
            interval=3600,
            initial_capital=10000.0,
            chain="ethereum",
            tokens="WETH,USDC",
            output=None,
            verbose=False,
        )

    captured = capsys.readouterr()
    assert "Error: Unknown strategy 'missing_strategy'" in captured.err
    assert "Available strategies: known_strategy" in captured.err


def test_walk_forward_dry_run_displays_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_walk_forward_config(tmp_path / "walk_forward.json")

    advanced_module = importlib.import_module("almanak.framework.cli.backtest.advanced")

    monkeypatch.setattr(advanced_module, "list_strategies_fn", lambda: [])

    result = CliRunner().invoke(
        backtest,
        [
            "walk-forward",
            "-s",
            "demo_uniswap_lp",
            "--start",
            "2024-01-01",
            "--end",
            "2024-05-01",
            "--config-file",
            str(config_path),
            "--train-days",
            "30",
            "--test-days",
            "10",
            "--step-days",
            "5",
            "--gap-days",
            "2",
            "--objective",
            "sharpe_ratio",
            "--n-trials",
            "11",
            "--patience",
            "4",
            "--tokens",
            "weth,usdc",
            "--output",
            "wf.json",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "WALK-FORWARD OPTIMIZATION CONFIGURATION" in result.output
    assert "Step Size: 5 days" in result.output
    assert "Optimization: sharpe_ratio" in result.output
    assert "Trials per Window: 11" in result.output
    assert "Early Stopping: patience=4" in result.output
    assert "Output: wf.json" in result.output
    assert "Dry run - walk-forward optimization not executed." in result.output
