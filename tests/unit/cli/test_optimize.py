"""Unit tests for Bayesian optimization CLI functionality.

Tests validate that the --optimize command correctly parses config files,
displays results, and integrates with OptunaTuner.
"""

from __future__ import annotations

import json
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from almanak.framework.cli.backtest import (
    backtest,
    load_optimization_config,
    parse_param_ranges_from_config,
    print_optimization_results,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


@pytest.fixture
def basic_optimization_config() -> dict[str, Any]:
    """Create a basic optimization config."""
    return {
        "param_ranges": {
            "threshold": {"type": "continuous", "min": 0.01, "max": 0.1},
            "window": {"type": "discrete", "min": 10, "max": 100, "step": 10},
            "mode": {"type": "categorical", "choices": ["aggressive", "conservative"]},
        },
        "objective": "sharpe_ratio",
        "n_trials": 10,
        "patience": 5,
    }


@pytest.fixture
def optimization_config_file(basic_optimization_config: dict[str, Any]) -> str:
    """Create a temporary config file and return its path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(basic_optimization_config, f)
        return f.name


@pytest.fixture
def mock_optimization_result() -> MagicMock:
    """Create a mock OptimizationResult."""
    result = MagicMock()
    result.best_params = {"threshold": 0.05, "window": 50, "mode": "aggressive"}
    result.best_value = 1.85
    result.best_trial_number = 7
    result.n_trials = 10
    result.direction = "maximize"
    result.stopped_early = False
    result.trials_without_improvement = 0
    return result


# =============================================================================
# Test Config Loading
# =============================================================================


class TestLoadOptimizationConfig:
    """Tests for load_optimization_config function."""

    def test_loads_valid_config(self, basic_optimization_config: dict[str, Any]) -> None:
        """Test loading a valid config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(basic_optimization_config, f)
            f.flush()

            config = load_optimization_config(Path(f.name))

            assert "param_ranges" in config
            assert config["objective"] == "sharpe_ratio"
            assert config["n_trials"] == 10

    def test_raises_on_missing_param_ranges(self) -> None:
        """Test error when param_ranges is missing."""
        import click

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"objective": "sharpe_ratio"}, f)
            f.flush()

            with pytest.raises(click.BadParameter, match="param_ranges"):
                load_optimization_config(Path(f.name))


class TestParseParamRangesFromConfig:
    """Tests for parse_param_ranges_from_config function."""

    def test_parses_continuous_param(self) -> None:
        """Test parsing continuous parameter."""
        config = {
            "param_ranges": {
                "threshold": {"type": "continuous", "min": 0.01, "max": 0.1}
            }
        }
        ranges = parse_param_ranges_from_config(config)

        assert "threshold" in ranges
        param = ranges["threshold"]
        assert param.param_type.value == "continuous"
        assert float(param.low) == 0.01
        assert float(param.high) == 0.1

    def test_parses_continuous_with_log(self) -> None:
        """Test parsing continuous parameter with log scale."""
        config = {
            "param_ranges": {
                "learning_rate": {"type": "continuous", "min": 0.0001, "max": 0.1, "log": True}
            }
        }
        ranges = parse_param_ranges_from_config(config)

        param = ranges["learning_rate"]
        assert param.log is True

    def test_parses_discrete_param(self) -> None:
        """Test parsing discrete parameter."""
        config = {
            "param_ranges": {
                "window": {"type": "discrete", "min": 10, "max": 100, "step": 10}
            }
        }
        ranges = parse_param_ranges_from_config(config)

        param = ranges["window"]
        assert param.param_type.value == "discrete"
        assert param.low == 10
        assert param.high == 100
        assert param.step == 10

    def test_parses_categorical_param(self) -> None:
        """Test parsing categorical parameter."""
        config = {
            "param_ranges": {
                "mode": {"type": "categorical", "choices": ["a", "b", "c"]}
            }
        }
        ranges = parse_param_ranges_from_config(config)

        param = ranges["mode"]
        assert param.param_type.value == "categorical"
        assert param.choices == ["a", "b", "c"]

    def test_parses_decimal_values(self) -> None:
        """Test parsing string values as Decimal for financial parameters."""
        config = {
            "param_ranges": {
                "capital": {"type": "continuous", "min": "1000", "max": "100000"}
            }
        }
        ranges = parse_param_ranges_from_config(config)

        param = ranges["capital"]
        assert isinstance(param.low, Decimal)
        assert isinstance(param.high, Decimal)

    def test_raises_on_missing_min_max(self) -> None:
        """Test error when min/max missing for continuous."""
        import click

        config = {
            "param_ranges": {
                "threshold": {"type": "continuous", "min": 0.01}  # missing max
            }
        }
        with pytest.raises(click.BadParameter, match="requires 'min' and 'max'"):
            parse_param_ranges_from_config(config)

    def test_raises_on_missing_choices(self) -> None:
        """Test error when choices missing for categorical."""
        import click

        config = {
            "param_ranges": {
                "mode": {"type": "categorical"}  # missing choices
            }
        }
        with pytest.raises(click.BadParameter, match="requires 'choices'"):
            parse_param_ranges_from_config(config)

    def test_raises_on_unknown_type(self) -> None:
        """Test error on unknown parameter type."""
        import click

        config = {
            "param_ranges": {
                "param": {"type": "unknown", "min": 0, "max": 1}
            }
        }
        with pytest.raises(click.BadParameter, match="Unknown parameter type"):
            parse_param_ranges_from_config(config)

    def test_supports_legacy_list_format(self) -> None:
        """Test legacy format with list for categorical."""
        config = {
            "param_ranges": {
                "mode": ["a", "b", "c"]
            }
        }
        ranges = parse_param_ranges_from_config(config)

        assert ranges["mode"] == ["a", "b", "c"]

    def test_supports_legacy_tuple_format(self) -> None:
        """Test legacy format with tuple for range."""
        config = {
            "param_ranges": {
                "threshold": (0.01, 0.1)
            }
        }
        ranges = parse_param_ranges_from_config(config)

        assert ranges["threshold"] == (0.01, 0.1)


# =============================================================================
# Test Results Display
# =============================================================================


class TestPrintOptimizationResults:
    """Tests for print_optimization_results function."""

    def test_prints_basic_results(
        self, mock_optimization_result: MagicMock, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that basic results are printed."""
        print_optimization_results(mock_optimization_result, "sharpe_ratio")

        captured = capsys.readouterr()
        assert "OPTIMIZATION RESULTS" in captured.out
        assert "sharpe_ratio" in captured.out
        assert "Best Trial: #7" in captured.out

    def test_prints_best_parameters(
        self, mock_optimization_result: MagicMock, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that best parameters are printed."""
        print_optimization_results(mock_optimization_result, "sharpe_ratio")

        captured = capsys.readouterr()
        assert "BEST PARAMETERS" in captured.out
        assert "threshold" in captured.out
        assert "window" in captured.out
        assert "mode" in captured.out

    def test_prints_early_stopping_info(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test early stopping info is printed."""
        result = MagicMock()
        result.best_params = {"a": 1}
        result.best_value = 1.5
        result.best_trial_number = 5
        result.n_trials = 10
        result.direction = "maximize"
        result.stopped_early = True
        result.trials_without_improvement = 3

        print_optimization_results(result, "sharpe_ratio")

        captured = capsys.readouterr()
        assert "Early Stopping: Yes" in captured.out
        assert "patience exhausted" in captured.out

    def test_prints_decimal_values(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that Decimal values are printed correctly."""
        result = MagicMock()
        result.best_params = {"capital": Decimal("50000.123456")}
        result.best_value = 1.5
        result.best_trial_number = 5
        result.n_trials = 10
        result.direction = "maximize"
        result.stopped_early = False
        result.trials_without_improvement = 0

        print_optimization_results(result, "sharpe_ratio")

        captured = capsys.readouterr()
        assert "50000.123456" in captured.out


# =============================================================================
# Test CLI Dry Run
# =============================================================================


class TestOptimizeDryRun:
    """Tests for optimize --dry-run functionality."""

    def test_dry_run_shows_config(
        self, cli_runner: CliRunner, optimization_config_file: str
    ) -> None:
        """Test that dry run displays configuration."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--config-file", optimization_config_file,
                "--dry-run",
            ],
        )
        assert "BAYESIAN OPTIMIZATION CONFIGURATION" in result.output
        assert "Dry run - optimization not executed" in result.output

    def test_dry_run_shows_parameters(
        self, cli_runner: CliRunner, optimization_config_file: str
    ) -> None:
        """Test that dry run shows parameter ranges."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--config-file", optimization_config_file,
                "--dry-run",
            ],
        )
        assert "Parameters to optimize:" in result.output
        assert "threshold" in result.output
        assert "window" in result.output
        assert "mode" in result.output


# =============================================================================
# Test CLI Options
# =============================================================================


class TestOptimizeCLIOptions:
    """Tests for optimize CLI options."""

    def test_strategy_required(self, cli_runner: CliRunner) -> None:
        """Test that --strategy is required."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
            ],
        )
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_config_file_required(
        self, cli_runner: CliRunner
    ) -> None:
        """Test that --config-file is required."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "test",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
            ],
        )
        assert result.exit_code != 0
        assert "config-file" in result.output.lower() or "required" in result.output.lower()

    def test_objective_choices(
        self, cli_runner: CliRunner, optimization_config_file: str
    ) -> None:
        """Test that objective must be valid choice."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--config-file", optimization_config_file,
                "--objective", "invalid_metric",
                "--dry-run",
            ],
        )
        # Click should reject invalid choice
        assert result.exit_code != 0 or "invalid" in result.output.lower() or "choice" in result.output.lower()

    def test_valid_objectives_accepted(
        self, cli_runner: CliRunner, optimization_config_file: str
    ) -> None:
        """Test that valid objectives are accepted."""
        for objective in ["sharpe_ratio", "sortino_ratio", "max_drawdown_pct"]:
            result = cli_runner.invoke(
                backtest,
                [
                    "optimize",
                    "-s", "demo_uniswap_lp",
                    "--start", "2024-01-01",
                    "--end", "2024-01-07",
                    "--config-file", optimization_config_file,
                    "--objective", objective,
                    "--dry-run",
                ],
            )
            assert f"Objective: {objective}" in result.output

    def test_n_trials_option(
        self, cli_runner: CliRunner, optimization_config_file: str
    ) -> None:
        """Test --n-trials option."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--config-file", optimization_config_file,
                "--n-trials", "100",
                "--dry-run",
            ],
        )
        assert "Trials: 100" in result.output

    def test_patience_option(
        self, cli_runner: CliRunner, optimization_config_file: str
    ) -> None:
        """Test --patience option."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--config-file", optimization_config_file,
                "--patience", "15",
                "--dry-run",
            ],
        )
        assert "patience=15" in result.output

    def test_seed_option(
        self, cli_runner: CliRunner, optimization_config_file: str
    ) -> None:
        """Test --seed option for reproducibility."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--config-file", optimization_config_file,
                "--seed", "42",
                "--dry-run",
            ],
        )
        assert "Random Seed: 42" in result.output


# =============================================================================
# Test Error Handling
# =============================================================================


class TestOptimizeErrorHandling:
    """Tests for optimize error handling."""

    def test_invalid_config_file(self, cli_runner: CliRunner) -> None:
        """Test error with non-existent config file."""
        result = cli_runner.invoke(
            backtest,
            [
                "optimize",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--config-file", "/nonexistent/path.json",
                "--dry-run",
            ],
        )
        assert result.exit_code != 0

    def test_invalid_json_config(self, cli_runner: CliRunner) -> None:
        """Test error with invalid JSON in config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }")
            f.flush()

            result = cli_runner.invoke(
                backtest,
                [
                    "optimize",
                    "-s", "demo_uniswap_lp",
                    "--start", "2024-01-01",
                    "--end", "2024-01-07",
                    "--config-file", f.name,
                    "--dry-run",
                ],
            )
            assert result.exit_code != 0

    def test_empty_param_ranges(self, cli_runner: CliRunner) -> None:
        """Test error with empty param_ranges."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"param_ranges": {}}, f)
            f.flush()

            result = cli_runner.invoke(
                backtest,
                [
                    "optimize",
                    "-s", "demo_uniswap_lp",
                    "--start", "2024-01-01",
                    "--end", "2024-01-07",
                    "--config-file", f.name,
                    "--dry-run",
                ],
            )
            assert result.exit_code != 0
            assert "No parameter ranges" in result.output


# =============================================================================
# Test CLI Help
# =============================================================================


class TestOptimizeHelp:
    """Tests for optimize CLI help text."""

    def test_help_available(self, cli_runner: CliRunner) -> None:
        """Test that --help works."""
        result = cli_runner.invoke(
            backtest,
            ["optimize", "--help"],
        )
        assert result.exit_code == 0
        assert "Bayesian optimization" in result.output

    def test_help_shows_examples(self, cli_runner: CliRunner) -> None:
        """Test that help shows examples."""
        result = cli_runner.invoke(
            backtest,
            ["optimize", "--help"],
        )
        assert "Examples:" in result.output

    def test_help_shows_parameter_types(self, cli_runner: CliRunner) -> None:
        """Test that help explains parameter types."""
        result = cli_runner.invoke(
            backtest,
            ["optimize", "--help"],
        )
        assert "continuous" in result.output
        assert "discrete" in result.output
        assert "categorical" in result.output

    def test_help_shows_objectives(self, cli_runner: CliRunner) -> None:
        """Test that help lists supported objectives."""
        result = cli_runner.invoke(
            backtest,
            ["optimize", "--help"],
        )
        assert "sharpe_ratio" in result.output
        assert "sortino_ratio" in result.output
        assert "max_drawdown_pct" in result.output
