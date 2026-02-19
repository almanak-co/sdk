"""Unit tests for Crisis Scenario CLI command (US-028d).

Tests validate the CLI for crisis scenario backtesting:
- Command registration and help text
- Parameter parsing (scenario name, custom dates, etc.)
- Dry run mode output
- List scenarios functionality
- Error handling for invalid inputs
- Crisis vs normal comparison output
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.cli.backtest import (
    backtest,
    print_crisis_backtest_results,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


@pytest.fixture
def mock_crisis_backtest_result() -> MagicMock:
    """Create a mock CrisisBacktestResult."""
    # Mock scenario
    scenario = MagicMock()
    scenario.name = "black_thursday"
    scenario.start_date.strftime.return_value = "2020-03-12"
    scenario.end_date.strftime.return_value = "2020-03-19"
    scenario.duration_days = 7
    scenario.description = "COVID-19 market crash - Bitcoin fell over 40%"

    # Mock metrics
    metrics = MagicMock()
    metrics.total_return_pct = Decimal("-0.15")
    metrics.max_drawdown_pct = Decimal("0.25")
    metrics.sharpe_ratio = Decimal("-0.50")
    metrics.sortino_ratio = Decimal("-0.45")
    metrics.volatility = Decimal("0.80")
    metrics.total_trades = 15
    metrics.winning_trades = 5
    metrics.losing_trades = 10
    metrics.win_rate = Decimal("0.333")

    # Mock result
    bt_result = MagicMock()
    bt_result.metrics = metrics

    result = MagicMock()
    result.scenario = scenario
    result.scenario_duration_days = 7
    result.total_return_during_crisis = Decimal("-0.15")
    result.max_drawdown_during_crisis = Decimal("0.25")
    result.result = bt_result
    result.crisis_metrics = {
        "days_to_trough": 3,
        "recovery_time_days": None,
        "recovery_pct": "0.35",
        "total_costs_usd": "150.50",
    }
    result.to_dict.return_value = {
        "scenario": {"name": "black_thursday"},
        "result": {"metrics": {}},
        "crisis_metrics": {},
    }
    return result


@pytest.fixture
def mock_crisis_result_with_comparison(mock_crisis_backtest_result: MagicMock) -> MagicMock:
    """Create mock result with normal period comparison."""
    result = mock_crisis_backtest_result
    result.crisis_metrics["normal_period_comparison"] = {
        "return_diff_pct": "-0.20",
        "volatility_ratio": "2.5",
        "drawdown_ratio": "3.0",
        "sharpe_diff": "-1.2",
        "win_rate_diff": "-0.15",
        "stress_resilience_score": "35",
    }
    return result


# =============================================================================
# Test CLI Command Registration
# =============================================================================


class TestScenarioCommandRegistration:
    """Tests for scenario CLI command registration."""

    def test_scenario_command_exists(self, cli_runner: CliRunner) -> None:
        """Test that scenario command is registered."""
        result = cli_runner.invoke(backtest, ["scenario", "--help"])
        assert result.exit_code == 0
        assert "crisis scenario" in result.output.lower()

    def test_help_shows_required_options(self, cli_runner: CliRunner) -> None:
        """Test that help shows required options."""
        result = cli_runner.invoke(backtest, ["scenario", "--help"])
        assert "--strategy" in result.output
        assert "--scenario" in result.output

    def test_help_shows_predefined_scenarios(self, cli_runner: CliRunner) -> None:
        """Test that help mentions predefined scenarios."""
        result = cli_runner.invoke(backtest, ["scenario", "--help"])
        assert "black_thursday" in result.output
        assert "terra_collapse" in result.output
        assert "ftx_collapse" in result.output

    def test_help_shows_custom_option(self, cli_runner: CliRunner) -> None:
        """Test that help shows custom scenario option."""
        result = cli_runner.invoke(backtest, ["scenario", "--help"])
        assert "custom" in result.output.lower()
        assert "--start" in result.output
        assert "--end" in result.output

    def test_help_shows_compare_normal_option(self, cli_runner: CliRunner) -> None:
        """Test that help shows compare-normal option."""
        result = cli_runner.invoke(backtest, ["scenario", "--help"])
        assert "--compare-normal" in result.output

    def test_help_shows_list_scenarios_option(self, cli_runner: CliRunner) -> None:
        """Test that help shows list-scenarios option."""
        result = cli_runner.invoke(backtest, ["scenario", "--help"])
        assert "--list-scenarios" in result.output


# =============================================================================
# Test List Scenarios
# =============================================================================


class TestListScenarios:
    """Tests for --list-scenarios functionality."""

    def test_list_scenarios_shows_all_predefined(self, cli_runner: CliRunner) -> None:
        """Test that --list-scenarios shows all predefined scenarios."""
        result = cli_runner.invoke(backtest, ["scenario", "--list-scenarios"])
        assert result.exit_code == 0
        assert "black_thursday" in result.output
        assert "terra_collapse" in result.output
        assert "ftx_collapse" in result.output

    def test_list_scenarios_shows_dates(self, cli_runner: CliRunner) -> None:
        """Test that --list-scenarios shows scenario date ranges."""
        result = cli_runner.invoke(backtest, ["scenario", "--list-scenarios"])
        assert "2020-03" in result.output  # Black Thursday
        assert "2022-05" in result.output  # Terra collapse
        assert "2022-11" in result.output  # FTX collapse

    def test_list_scenarios_shows_descriptions(self, cli_runner: CliRunner) -> None:
        """Test that --list-scenarios shows descriptions."""
        result = cli_runner.invoke(backtest, ["scenario", "--list-scenarios"])
        # Should have some descriptive text
        assert "COVID" in result.output or "crash" in result.output.lower()

    def test_list_scenarios_exits_without_running(self, cli_runner: CliRunner) -> None:
        """Test that --list-scenarios exits without requiring other params."""
        # Should work without --strategy
        result = cli_runner.invoke(backtest, ["scenario", "--list-scenarios"])
        assert result.exit_code == 0
        assert "AVAILABLE CRISIS SCENARIOS" in result.output


# =============================================================================
# Test CLI Parameter Parsing
# =============================================================================


class TestScenarioParameterParsing:
    """Tests for scenario CLI parameter parsing."""

    def test_missing_strategy_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that missing strategy shows error."""
        result = cli_runner.invoke(backtest, [
            "scenario",
            "--scenario", "black_thursday",
        ])
        assert result.exit_code != 0
        assert "strategy" in result.output.lower() or "required" in result.output.lower()

    def test_missing_scenario_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that missing scenario shows error."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
            ])
        assert result.exit_code != 0
        assert "scenario" in result.output.lower() or "required" in result.output.lower()

    def test_unknown_scenario_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that unknown scenario shows error with available options."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "invalid_scenario",
            ])
        assert result.exit_code != 0
        assert "Unknown scenario" in result.output or "unknown" in result.output.lower()

    def test_custom_scenario_requires_dates(self, cli_runner: CliRunner) -> None:
        """Test that custom scenario requires start and end dates."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "custom",
            ])
        assert result.exit_code != 0
        assert "start" in result.output.lower() and "end" in result.output.lower()

    def test_custom_scenario_with_dates_is_valid(self, cli_runner: CliRunner) -> None:
        """Test that custom scenario with dates passes validation."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "custom",
                "--start", "2023-03-10",
                "--end", "2023-03-15",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "CRISIS SCENARIO BACKTEST CONFIGURATION" in result.output


# =============================================================================
# Test Dry Run Mode
# =============================================================================


class TestScenarioDryRun:
    """Tests for scenario CLI dry run mode."""

    def test_dry_run_shows_configuration(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows configuration without running."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "black_thursday",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "CRISIS SCENARIO BACKTEST CONFIGURATION" in result.output
        assert "Dry run" in result.output
        assert "Strategy: test" in result.output

    def test_dry_run_shows_scenario_details(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows scenario details."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "terra_collapse",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "terra_collapse" in result.output
        assert "2022-05" in result.output  # Terra dates

    def test_dry_run_shows_mev_setting(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows MEV simulation setting."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "black_thursday",
                "--mev",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "MEV" in result.output
        assert "Enabled" in result.output

    def test_dry_run_no_mev_setting(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows MEV disabled when --no-mev."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "black_thursday",
                "--no-mev",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "MEV" in result.output
        assert "Disabled" in result.output

    def test_dry_run_shows_compare_normal_setting(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows compare-normal setting."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "black_thursday",
                "--compare-normal",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "Compare to Normal" in result.output
        assert "Yes" in result.output

    def test_dry_run_shows_custom_scenario_name(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows custom scenario name."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "custom",
                "--start", "2023-03-10",
                "--end", "2023-03-15",
                "--name", "svb_collapse",
                "--dry-run",
            ])
        assert result.exit_code == 0
        assert "svb_collapse" in result.output


# =============================================================================
# Test Results Display
# =============================================================================


class TestCrisisResultsDisplay:
    """Tests for crisis backtest results display function."""

    def test_print_results_shows_header(
        self,
        mock_crisis_backtest_result: MagicMock,
    ) -> None:
        """Test that print_crisis_backtest_results shows header."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_backtest_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "CRISIS SCENARIO BACKTEST RESULTS" in output

    def test_print_results_shows_scenario_name(
        self,
        mock_crisis_backtest_result: MagicMock,
    ) -> None:
        """Test that print_crisis_backtest_results shows scenario name."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_backtest_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "black_thursday" in output

    def test_print_results_shows_performance_metrics(
        self,
        mock_crisis_backtest_result: MagicMock,
    ) -> None:
        """Test that print_crisis_backtest_results shows performance metrics."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_backtest_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Total Return" in output
        assert "Max Drawdown" in output
        assert "Sharpe Ratio" in output

    def test_print_results_shows_trade_statistics(
        self,
        mock_crisis_backtest_result: MagicMock,
    ) -> None:
        """Test that print_crisis_backtest_results shows trade statistics."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_backtest_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "Total Trades" in output
        assert "Win Rate" in output

    def test_print_results_shows_crisis_specific_metrics(
        self,
        mock_crisis_backtest_result: MagicMock,
    ) -> None:
        """Test that print_crisis_backtest_results shows crisis-specific metrics."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_backtest_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "CRISIS-SPECIFIC METRICS" in output
        assert "Recovery" in output

    def test_print_results_shows_comparison_when_present(
        self,
        mock_crisis_result_with_comparison: MagicMock,
    ) -> None:
        """Test that print_crisis_backtest_results shows comparison when available."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_result_with_comparison)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        assert "CRISIS VS NORMAL PERIOD COMPARISON" in output
        assert "Stress Resilience" in output


# =============================================================================
# Test Error Handling
# =============================================================================


class TestScenarioErrorHandling:
    """Tests for error handling in scenario CLI."""

    def test_unknown_strategy_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that unknown strategy shows error with available options."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["valid_strat"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "unknown_strategy",
                "--scenario", "black_thursday",
            ])
        assert result.exit_code != 0
        # Should mention unknown strategy
        assert "Unknown strategy" in result.output or "unknown" in result.output.lower()

    def test_invalid_date_format_shows_error(self, cli_runner: CliRunner) -> None:
        """Test that invalid date format shows error."""
        with patch("almanak.framework.cli.backtest.list_strategies_fn", return_value=["test"]):
            result = cli_runner.invoke(backtest, [
                "scenario",
                "--strategy", "test",
                "--scenario", "custom",
                "--start", "not-a-date",
                "--end", "2023-03-15",
            ])
        assert result.exit_code != 0


# =============================================================================
# Test Statistics Calculation
# =============================================================================


class TestScenarioStatisticsCalculation:
    """Tests for scenario statistics calculation displayed in results."""

    def test_return_percentage_format(
        self,
        mock_crisis_backtest_result: MagicMock,
    ) -> None:
        """Test that returns are displayed as percentages."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_backtest_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        # Return of -0.15 should show as -15%
        assert "-15" in output or "-0.15" in output

    def test_drawdown_percentage_format(
        self,
        mock_crisis_backtest_result: MagicMock,
    ) -> None:
        """Test that drawdowns are displayed as percentages."""
        import sys
        from io import StringIO

        captured = StringIO()
        sys.stdout = captured
        try:
            print_crisis_backtest_results(mock_crisis_backtest_result)
        finally:
            sys.stdout = sys.__stdout__

        output = captured.getvalue()
        # Drawdown of 0.25 should show as 25%
        assert "25" in output
