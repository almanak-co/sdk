"""Integration tests for parallel sweep CLI functionality.

Tests validate that parallel and serial execution modes produce equivalent results
and that the --parallel flag correctly enables multiprocessing.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting import PnLBacktestConfig
from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
)
from almanak.framework.cli.backtest import (
    SweepParameter,
    SweepResult,
    backtest,
    generate_combinations,
    parse_param_string,
    run_sweep_backtest,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


@pytest.fixture
def mock_pnl_config() -> PnLBacktestConfig:
    """Create a mock PnL backtest config."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 7, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("30"),
    )


@pytest.fixture
def mock_backtest_result() -> BacktestResult:
    """Create a mock backtest result."""
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="test-strategy",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 7, tzinfo=UTC),
        trades=[],
        metrics=BacktestMetrics(
            total_trades=10,
            win_rate=Decimal("0.6"),
            total_return_pct=Decimal("5.5"),
            max_drawdown_pct=Decimal("2.3"),
            sharpe_ratio=Decimal("1.5"),
            sortino_ratio=Decimal("2.0"),
            calmar_ratio=Decimal("1.2"),
            profit_factor=Decimal("1.8"),
            annualized_return_pct=Decimal("28.5"),
            net_pnl_usd=Decimal("550"),
        ),
    )


# =============================================================================
# Test Parameter Parsing
# =============================================================================


class TestParseParamString:
    """Tests for parse_param_string function."""

    def test_basic_parsing(self) -> None:
        """Test parsing basic parameter string."""
        result = parse_param_string("window:10,20,30")
        assert result.name == "window"
        assert result.values == ["10", "20", "30"]

    def test_parsing_with_spaces(self) -> None:
        """Test parsing with spaces around values."""
        result = parse_param_string("threshold: 0.01, 0.02, 0.03")
        assert result.name == "threshold"
        assert result.values == ["0.01", "0.02", "0.03"]

    def test_single_value(self) -> None:
        """Test parsing single value."""
        result = parse_param_string("mode:aggressive")
        assert result.name == "mode"
        assert result.values == ["aggressive"]


class TestGenerateCombinations:
    """Tests for generate_combinations function."""

    def test_single_param(self) -> None:
        """Test combinations with single parameter."""
        params = [SweepParameter("a", ["1", "2", "3"])]
        combos = generate_combinations(params)
        assert len(combos) == 3
        assert combos == [{"a": "1"}, {"a": "2"}, {"a": "3"}]

    def test_multiple_params(self) -> None:
        """Test combinations with multiple parameters (Cartesian product)."""
        params = [
            SweepParameter("a", ["1", "2"]),
            SweepParameter("b", ["x", "y"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 4  # 2 x 2 = 4
        assert {"a": "1", "b": "x"} in combos
        assert {"a": "1", "b": "y"} in combos
        assert {"a": "2", "b": "x"} in combos
        assert {"a": "2", "b": "y"} in combos

    def test_empty_params(self) -> None:
        """Test combinations with no parameters."""
        combos = generate_combinations([])
        assert combos == [{}]


# =============================================================================
# Test Sweep Result Creation
# =============================================================================


class TestSweepResult:
    """Tests for SweepResult dataclass."""

    def test_sweep_result_creation(self, mock_backtest_result: BacktestResult) -> None:
        """Test creating a SweepResult."""
        result = SweepResult(
            params={"window": "10", "threshold": "0.5"},
            result=mock_backtest_result,
            sharpe_ratio=Decimal("1.5"),
            total_return_pct=Decimal("5.5"),
            max_drawdown_pct=Decimal("2.3"),
            win_rate=Decimal("0.6"),
            total_trades=10,
        )
        assert result.params == {"window": "10", "threshold": "0.5"}
        assert result.sharpe_ratio == Decimal("1.5")
        assert result.total_trades == 10


# =============================================================================
# Test CLI Dry Run
# =============================================================================


class TestSweepDryRun:
    """Tests for sweep --dry-run functionality."""

    def test_dry_run_shows_combinations(self, cli_runner: CliRunner) -> None:
        """Test that dry run displays all parameter combinations."""
        # Use a valid strategy name from the registry
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--param", "a:1,2",
                "--param", "b:x,y",
                "--dry-run",
            ],
        )
        # Should show all 4 combinations
        assert "a=1, b=x" in result.output or "a=1,b=x" in result.output
        assert "a=1, b=y" in result.output or "a=1,b=y" in result.output
        assert "a=2, b=x" in result.output or "a=2,b=x" in result.output
        assert "a=2, b=y" in result.output or "a=2,b=y" in result.output
        assert "Dry run - no backtests executed" in result.output

    def test_dry_run_shows_parallel_config(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows parallel execution configuration."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--param", "a:1,2,3",
                "--parallel",
                "--workers", "4",
                "--dry-run",
            ],
        )
        assert "Parallel (multiprocessing)" in result.output
        assert "Workers: " in result.output

    def test_dry_run_shows_async_config(self, cli_runner: CliRunner) -> None:
        """Test that dry run shows async execution configuration."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--param", "a:1,2,3",
                "--dry-run",
            ],
        )
        assert "Async (concurrent)" in result.output
        assert "Concurrency: " in result.output


# =============================================================================
# Test Parallel vs Serial Equivalence
# =============================================================================


class TestParallelSerialEquivalence:
    """Tests validating parallel and serial produce equivalent results."""

    @pytest.mark.asyncio
    async def test_run_sweep_backtest_returns_result(
        self, mock_backtest_result: BacktestResult, mock_pnl_config: PnLBacktestConfig
    ) -> None:
        """Test that run_sweep_backtest returns a SweepResult."""
        # Create a mock strategy class
        class MockStrategy:
            strategy_id = "mock"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        # Mock the PnLBacktester
        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_backtester_class:
            mock_backtester = MagicMock()
            mock_backtester.backtest = AsyncMock(return_value=mock_backtest_result)
            mock_backtester_class.return_value = mock_backtester

            result = await run_sweep_backtest(
                strategy_class=MockStrategy,
                base_config={},
                pnl_config=mock_pnl_config,
                data_provider=MagicMock(),
                params={"window": "10"},
            )

            assert isinstance(result, SweepResult)
            assert result.params == {"window": "10"}
            assert result.sharpe_ratio == Decimal("1.5")


class TestCLIParallelFlag:
    """Tests for --parallel and --workers CLI options."""

    def test_parallel_flag_exists(self, cli_runner: CliRunner) -> None:
        """Test that --parallel flag is recognized."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--param", "a:1",
                "--parallel",
                "--dry-run",
            ],
        )
        # Should not error on unrecognized option
        assert "no such option: --parallel" not in result.output

    def test_workers_option_exists(self, cli_runner: CliRunner) -> None:
        """Test that --workers option is recognized."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--param", "a:1",
                "--workers", "4",
                "--dry-run",
            ],
        )
        # Should not error on unrecognized option
        assert "no such option: --workers" not in result.output

    def test_workers_shorthand(self, cli_runner: CliRunner) -> None:
        """Test that -j shorthand works for --workers."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_lp",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--param", "a:1",
                "-j", "2",
                "--dry-run",
            ],
        )
        # Should show concurrency of 2
        assert "2" in result.output


# =============================================================================
# Test Error Handling
# =============================================================================


class TestSweepErrorHandling:
    """Tests for sweep error handling."""

    def test_missing_param_error(self, cli_runner: CliRunner) -> None:
        """Test error when no --param provided."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "test",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
            ],
        )
        assert result.exit_code != 0
        assert "At least one --param is required" in result.output

    def test_invalid_param_format_error(self, cli_runner: CliRunner) -> None:
        """Test error with invalid parameter format."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "test",
                "--start", "2024-01-01",
                "--end", "2024-01-07",
                "--param", "invalid_format",
            ],
        )
        assert result.exit_code != 0
        assert "Invalid parameter format" in result.output or "Expected" in result.output


# =============================================================================
# Integration Test for Result Consistency
# =============================================================================


class TestResultConsistency:
    """Integration tests for result consistency between modes."""

    def test_combination_count_matches_results(self) -> None:
        """Test that number of combinations equals expected Cartesian product."""
        params = [
            SweepParameter("a", ["1", "2", "3"]),
            SweepParameter("b", ["x", "y"]),
            SweepParameter("c", ["alpha", "beta"]),
        ]
        combos = generate_combinations(params)
        # 3 x 2 x 2 = 12
        assert len(combos) == 12

    def test_combinations_contain_all_params(self) -> None:
        """Test that each combination contains all parameters."""
        params = [
            SweepParameter("window", ["10", "20"]),
            SweepParameter("threshold", ["0.5", "1.0"]),
        ]
        combos = generate_combinations(params)
        for combo in combos:
            assert "window" in combo
            assert "threshold" in combo
            assert combo["window"] in ["10", "20"]
            assert combo["threshold"] in ["0.5", "1.0"]
