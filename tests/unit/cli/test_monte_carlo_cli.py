"""Unit tests for Monte Carlo CLI command (US-027d).

Tests validate the CLI for Monte Carlo simulation:
- Command registration and help text
- Parameter parsing (n_paths, method, etc.)
- Dry run mode output
- Error handling for invalid inputs
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.cli.backtest import (
    backtest,
    print_monte_carlo_results,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


@pytest.fixture
def mock_monte_carlo_result() -> MagicMock:
    """Create a mock Monte Carlo simulation result."""
    result = MagicMock()
    result.n_paths = 100
    result.n_successful = 98
    result.n_failed = 2
    result.return_mean = Decimal("0.05")
    result.return_std = Decimal("0.15")
    result.return_percentile_5th = Decimal("-0.20")
    result.return_percentile_25th = Decimal("-0.05")
    result.return_percentile_50th = Decimal("0.04")
    result.return_percentile_75th = Decimal("0.12")
    result.return_percentile_95th = Decimal("0.30")
    result.max_drawdown_mean = Decimal("0.08")
    result.max_drawdown_worst = Decimal("0.25")
    result.max_drawdown_percentile_95th = Decimal("0.18")
    result.probability_negative_return = Decimal("0.35")
    result.probability_loss_exceeds_10pct = Decimal("0.12")
    result.probability_loss_exceeds_20pct = Decimal("0.05")
    result.probability_gain_exceeds_10pct = Decimal("0.40")
    result.probability_drawdown_exceeds_threshold = {
        "0.05": Decimal("0.80"),
        "0.10": Decimal("0.50"),
        "0.15": Decimal("0.25"),
        "0.20": Decimal("0.10"),
    }
    result.sharpe_mean = Decimal("0.85")
    result.sharpe_std = Decimal("0.30")
    result.to_dict.return_value = {
        "n_paths": 100,
        "n_successful": 98,
        "n_failed": 2,
        "return_mean": "0.05",
    }
    return result


# =============================================================================
# Test CLI Command Registration
# =============================================================================


class TestMonteCarloCommandRegistration:
    """Tests for Monte Carlo CLI command registration."""

    def test_monte_carlo_command_exists(self, cli_runner: CliRunner) -> None:
        """Test that monte-carlo command is registered."""
        result = cli_runner.invoke(backtest, ["monte-carlo", "--help"])
        assert result.exit_code == 0
        assert "Monte Carlo simulation" in result.output

    def test_help_shows_required_options(self, cli_runner: CliRunner) -> None:
        """Test that help shows required options."""
        result = cli_runner.invoke(backtest, ["monte-carlo", "--help"])
        assert "--strategy" in result.output
        assert "--start" in result.output
        assert "--end" in result.output

    def test_help_shows_n_paths_option(self, cli_runner: CliRunner) -> None:
        """Test that help shows n-paths option."""
        result = cli_runner.invoke(backtest, ["monte-carlo", "--help"])
        assert "--n-paths" in result.output
        assert "Number of price paths" in result.output

    def test_help_shows_method_option(self, cli_runner: CliRunner) -> None:
        """Test that help shows method option."""
        result = cli_runner.invoke(backtest, ["monte-carlo", "--help"])
        assert "--method" in result.output
        assert "gbm" in result.output
        assert "bootstrap" in result.output

    def test_help_shows_confidence_intervals_info(self, cli_runner: CliRunner) -> None:
        """Test that help mentions confidence intervals."""
        result = cli_runner.invoke(backtest, ["monte-carlo", "--help"])
        assert "Confidence intervals" in result.output or "confidence intervals" in result.output.lower()

    def test_help_shows_probability_info(self, cli_runner: CliRunner) -> None:
        """Test that help mentions probabilities."""
        result = cli_runner.invoke(backtest, ["monte-carlo", "--help"])
        assert "Probability" in result.output or "probability" in result.output.lower()


# =============================================================================
# Test CLI Parameter Parsing
# =============================================================================


class TestMonteCarloParameterParsing:
    """Tests for Monte Carlo CLI parameter parsing."""

    def test_missing_strategy_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that missing strategy shows error."""
        result = cli_runner.invoke(backtest, [
            "monte-carlo",
            "--start", "2024-01-01",
            "--end", "2024-06-01",
        ])
        assert result.exit_code != 0
        assert "strategy" in result.output.lower() or "required" in result.output.lower()

    def test_missing_start_date_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that missing start date shows error."""
        result = cli_runner.invoke(backtest, [
            "monte-carlo",
            "--strategy", "test",
            "--end", "2024-06-01",
        ])
        assert result.exit_code != 0

    def test_missing_end_date_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that missing end date shows error."""
        result = cli_runner.invoke(backtest, [
            "monte-carlo",
            "--strategy", "test",
            "--start", "2024-01-01",
        ])
        assert result.exit_code != 0

    def test_invalid_method_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that invalid method shows error."""
        result = cli_runner.invoke(backtest, [
            "monte-carlo",
            "--strategy", "test",
            "--start", "2024-01-01",
            "--end", "2024-06-01",
            "--method", "invalid_method",
        ])
        assert result.exit_code != 0
        assert "invalid" in result.output.lower() or "choice" in result.output.lower()


# =============================================================================
# Test Dry Run Mode
# =============================================================================


class TestMonteCarloDryRun:
    """Tests for Monte Carlo CLI dry run mode."""

    def test_dry_run_shows_configuration(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows configuration without running."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "monte-carlo",
                "--strategy", "test",
                "--start", "2024-01-01",
                "--end", "2024-06-01",
                "--n-paths", "100",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "MONTE CARLO SIMULATION CONFIGURATION" in result.output
        assert "Dry run" in result.output
        assert "Strategy: test" in result.output

    def test_dry_run_shows_n_paths(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows number of paths."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "monte-carlo",
                "--strategy", "test",
                "--start", "2024-01-01",
                "--end", "2024-06-01",
                "--n-paths", "500",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "500" in result.output

    def test_dry_run_shows_method(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows generation method."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "monte-carlo",
                "--strategy", "test",
                "--start", "2024-01-01",
                "--end", "2024-06-01",
                "--method", "gbm",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "GBM" in result.output

    def test_dry_run_shows_seed(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows seed when specified."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "monte-carlo",
                "--strategy", "test",
                "--start", "2024-01-01",
                "--end", "2024-06-01",
                "--seed", "42",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "42" in result.output


# =============================================================================
# Test Results Display
# =============================================================================


class TestMonteCarloResultsDisplay:
    """Tests for Monte Carlo results display function."""

    def test_print_results_shows_header(
        self,
        mock_monte_carlo_result: MagicMock,
    ) -> None:
        """Test that print_monte_carlo_results shows header."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_monte_carlo_results(mock_monte_carlo_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "MONTE CARLO SIMULATION RESULTS" in output

    def test_print_results_shows_path_counts(
        self,
        mock_monte_carlo_result: MagicMock,
    ) -> None:
        """Test that print_monte_carlo_results shows path counts."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_monte_carlo_results(mock_monte_carlo_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "100" in output  # n_paths
        assert "98" in output   # successful
        assert "2" in output    # failed

    def test_print_results_shows_confidence_intervals(
        self,
        mock_monte_carlo_result: MagicMock,
    ) -> None:
        """Test that print_monte_carlo_results shows confidence intervals."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_monte_carlo_results(mock_monte_carlo_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "5th percentile" in output
        assert "50th percentile" in output or "median" in output.lower()
        assert "95th percentile" in output

    def test_print_results_shows_probabilities(
        self,
        mock_monte_carlo_result: MagicMock,
    ) -> None:
        """Test that print_monte_carlo_results shows risk probabilities."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_monte_carlo_results(mock_monte_carlo_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Negative Return" in output
        assert "Loss > 10%" in output or "Loss" in output
        assert "Gain > 10%" in output or "Gain" in output

    def test_print_results_shows_drawdown_analysis(
        self,
        mock_monte_carlo_result: MagicMock,
    ) -> None:
        """Test that print_monte_carlo_results shows drawdown analysis."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_monte_carlo_results(mock_monte_carlo_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "DRAWDOWN" in output
        assert "Max Drawdown" in output or "Drawdown" in output

    def test_print_results_shows_sharpe_ratio(
        self,
        mock_monte_carlo_result: MagicMock,
    ) -> None:
        """Test that print_monte_carlo_results shows Sharpe ratio when available."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_monte_carlo_results(mock_monte_carlo_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Sharpe" in output


# =============================================================================
# Test Error Handling
# =============================================================================


class TestMonteCarloErrorHandling:
    """Tests for Monte Carlo CLI error handling."""

    def test_unknown_strategy_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that unknown strategy shows helpful error."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["real_strategy"]):
            result = cli_runner.invoke(backtest, [
                "monte-carlo",
                "--strategy", "unknown_strategy",
                "--start", "2024-01-01",
                "--end", "2024-06-01",
            ])
        assert result.exit_code != 0
        assert "Unknown strategy" in result.output or "unknown" in result.output.lower()

    def test_invalid_date_format_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that invalid date format shows error."""
        result = cli_runner.invoke(backtest, [
            "monte-carlo",
            "--strategy", "test",
            "--start", "invalid-date",
            "--end", "2024-06-01",
        ])
        assert result.exit_code != 0


# =============================================================================
# Test Statistics Calculation Integration
# =============================================================================


class TestMonteCarloStatisticsCalculation:
    """Integration tests for Monte Carlo statistics calculation.

    These tests verify that the statistics calculations work correctly
    with various input scenarios.
    """

    def test_percentile_calculation_accuracy(self) -> None:
        """Test that percentile calculations are accurate."""
        from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
            _calculate_percentile,
        )

        # Test with known values
        values = [Decimal(str(i)) for i in range(1, 101)]  # 1 to 100

        # 50th percentile should be around 50
        p50 = _calculate_percentile(values, 50)
        assert p50 >= Decimal("49") and p50 <= Decimal("51")

        # 5th percentile should be around 5
        p5 = _calculate_percentile(values, 5)
        assert p5 >= Decimal("4") and p5 <= Decimal("6")

        # 95th percentile should be around 95
        p95 = _calculate_percentile(values, 95)
        assert p95 >= Decimal("94") and p95 <= Decimal("96")

    def test_std_calculation_accuracy(self) -> None:
        """Test that standard deviation calculation is accurate."""
        from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
            _calculate_std,
        )

        # Test with known values where std = 1
        # For values 1, 2, 3 with mean 2: std = sqrt(((1-2)^2 + (2-2)^2 + (3-2)^2) / 2) = sqrt(1) = 1
        values = [Decimal("1"), Decimal("2"), Decimal("3")]
        mean = Decimal("2")
        std = _calculate_std(values, mean)

        # Standard deviation should be 1.0
        assert abs(float(std) - 1.0) < 0.01

    def test_probability_calculation_with_known_data(self) -> None:
        """Test probability calculations with known data."""
        # Create a simulation result with known probabilities
        from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
            MonteCarloSimulationResult,
        )

        result = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.10"),
            return_std=Decimal("0.05"),
            return_percentile_5th=Decimal("0.02"),
            return_percentile_25th=Decimal("0.06"),
            return_percentile_50th=Decimal("0.10"),
            return_percentile_75th=Decimal("0.14"),
            return_percentile_95th=Decimal("0.18"),
            max_drawdown_mean=Decimal("0.05"),
            max_drawdown_worst=Decimal("0.15"),
            max_drawdown_percentile_95th=Decimal("0.12"),
            probability_negative_return=Decimal("0.02"),  # 2%
            probability_loss_exceeds_10pct=Decimal("0.0"),
            probability_loss_exceeds_20pct=Decimal("0.0"),
            probability_gain_exceeds_10pct=Decimal("0.50"),  # 50%
            probability_drawdown_exceeds_threshold={
                "0.05": Decimal("0.50"),
                "0.10": Decimal("0.15"),
            },
        )

        # Verify the probabilities are stored correctly
        assert result.probability_negative_return == Decimal("0.02")
        assert result.probability_gain_exceeds_10pct == Decimal("0.50")
        assert result.probability_drawdown_exceeds_threshold["0.05"] == Decimal("0.50")
        assert result.probability_drawdown_exceeds_threshold["0.10"] == Decimal("0.15")
