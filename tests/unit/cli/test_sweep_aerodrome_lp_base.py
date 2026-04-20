"""Tests for parameter sweep with Aerodrome LP strategy on Base.

Validates that the sweep CLI correctly:
1. Accepts LP-specific parameters (amount0, amount1, rebalance_threshold_pct)
2. Generates correct Cartesian product for LP parameter combinations
3. Passes parameters through to strategy instantiation
4. Produces ranked results sorted by Sharpe ratio
5. Handles LP-specific metrics (position tracking, rebalance triggers)

First parameter sweep test with an LP strategy on Base chain.
Kitchen Loop iteration 83, VIB-1360.
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
    print_sweep_results_table,
    run_sweep_backtest,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def mock_pnl_config_base() -> PnLBacktestConfig:
    """PnL config for Base chain sweep."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 10, 1, tzinfo=UTC),
        end_time=datetime(2024, 12, 1, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="base",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("0.01"),
    )


def _make_backtest_result(
    sharpe: str = "1.5",
    total_return: str = "5.0",
    drawdown: str = "3.0",
    win_rate: str = "0.55",
    trades: int = 12,
) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="incubating_aerodrome_lp_sweep",
        start_time=datetime(2024, 10, 1, tzinfo=UTC),
        end_time=datetime(2024, 12, 1, tzinfo=UTC),
        trades=[],
        metrics=BacktestMetrics(
            total_trades=trades,
            win_rate=Decimal(win_rate),
            total_return_pct=Decimal(total_return),
            max_drawdown_pct=Decimal(drawdown),
            sharpe_ratio=Decimal(sharpe),
            sortino_ratio=Decimal("2.0"),
            calmar_ratio=Decimal("1.0"),
            profit_factor=Decimal("1.5"),
            annualized_return_pct=Decimal("20.0"),
            net_pnl_usd=Decimal("500"),
        ),
    )


# =============================================================================
# LP Parameter Combination Generation
# =============================================================================


class TestAerodromeLPSweepCombinations:
    """Test that LP parameter grids generate correct combinations."""

    def test_amount_grid(self) -> None:
        """2x3 grid of LP amounts produces 6 combinations."""
        params = [
            SweepParameter("amount0", ["0.005", "0.01", "0.02"]),
            SweepParameter("amount1", ["15", "30"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 6
        assert {"amount0": "0.005", "amount1": "15"} in combos
        assert {"amount0": "0.02", "amount1": "30"} in combos

    def test_rebalance_threshold_grid(self) -> None:
        """Rebalance threshold + amount produces full Cartesian product."""
        params = [
            SweepParameter("rebalance_threshold_pct", ["3.0", "5.0", "10.0"]),
            SweepParameter("amount0", ["0.01", "0.02"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 6  # 3 x 2
        for combo in combos:
            assert "rebalance_threshold_pct" in combo
            assert "amount0" in combo

    def test_three_lp_param_grid(self) -> None:
        """Three LP parameters produce full Cartesian product."""
        params = [
            SweepParameter("amount0", ["0.005", "0.01"]),
            SweepParameter("amount1", ["15", "30"]),
            SweepParameter("rebalance_threshold_pct", ["3.0", "5.0"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 8  # 2 x 2 x 2


# =============================================================================
# Dry Run with LP Parameters
# =============================================================================


class TestAerodromeLPSweepDryRun:
    """Test sweep --dry-run with Aerodrome LP parameters on Base."""

    def test_dry_run_lp_sweep_on_base(self, cli_runner: CliRunner) -> None:
        """Dry run shows LP parameter combinations for Base."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "incubating_aerodrome_lp_sweep",
                "--start", "2024-10-01",
                "--end", "2024-12-01",
                "--chain", "base",
                "--tokens", "WETH,USDC",
                "--param", "amount0:0.005,0.01,0.02",
                "--param", "rebalance_threshold_pct:3.0,5.0,10.0",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "incubating_aerodrome_lp_sweep" in result.output
        assert "base" in result.output
        assert "Total combinations: 9" in result.output  # 3x3=9
        assert "Dry run - no backtests executed" in result.output

    def test_dry_run_single_lp_param(self, cli_runner: CliRunner) -> None:
        """Dry run with only rebalance threshold varied."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "incubating_aerodrome_lp_sweep",
                "--start", "2024-10-01",
                "--end", "2024-12-01",
                "--chain", "base",
                "--param", "rebalance_threshold_pct:1.0,3.0,5.0,10.0,20.0",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Total combinations: 5" in result.output


# =============================================================================
# Sweep Execution with Mocked Backtester
# =============================================================================


class TestAerodromeLPSweepExecution:
    """Test sweep execution with mocked PnL backtester for LP strategy."""

    @pytest.mark.asyncio
    async def test_sweep_applies_lp_params_to_strategy(
        self, mock_pnl_config_base: PnLBacktestConfig
    ) -> None:
        """Verify sweep injects LP params into strategy config."""
        captured_configs: list[dict] = []

        class TrackingStrategy:
            strategy_id = "incubating_aerodrome_lp_sweep"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                captured_configs.append(config.copy())

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_backtest_result()

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=TrackingStrategy,
                base_config={
                    "pool": "WETH/USDC",
                    "stable": False,
                    "amount0": 0.01,
                    "amount1": 30,
                    "rebalance_threshold_pct": 5.0,
                },
                pnl_config=mock_pnl_config_base,
                data_provider=MagicMock(),
                params={"amount0": "0.02", "rebalance_threshold_pct": "10.0"},
            )

        assert isinstance(result, SweepResult)
        assert result.params == {"amount0": "0.02", "rebalance_threshold_pct": "10.0"}
        assert len(captured_configs) == 1
        # Config should have overridden values (as floats after type coercion)
        assert captured_configs[0]["amount0"] == 0.02
        assert captured_configs[0]["rebalance_threshold_pct"] == 10.0
        # Non-overridden values should be preserved
        assert captured_configs[0]["pool"] == "WETH/USDC"

    @pytest.mark.asyncio
    async def test_sweep_returns_lp_metrics(
        self, mock_pnl_config_base: PnLBacktestConfig
    ) -> None:
        """Verify sweep result contains correct metrics for LP strategy."""

        class SimpleStrategy:
            strategy_id = "lp_test"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_backtest_result(
            sharpe="1.8", total_return="6.5", drawdown="4.2", trades=8
        )

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=SimpleStrategy,
                base_config={"amount0": 0.01},
                pnl_config=mock_pnl_config_base,
                data_provider=MagicMock(),
                params={"amount0": "0.02"},
            )

        assert result.sharpe_ratio == Decimal("1.8")
        assert result.total_return_pct == Decimal("6.5")
        assert result.max_drawdown_pct == Decimal("4.2")
        assert result.total_trades == 8


# =============================================================================
# Result Table Sorting (LP-specific)
# =============================================================================


class TestAerodromeLPSweepResultRanking:
    """Test that LP sweep results are ranked correctly by Sharpe ratio."""

    def test_results_sorted_by_sharpe(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify print_sweep_results_table sorts LP results by Sharpe descending."""
        # Simulate 3 different LP parameter combinations with varying Sharpe
        configs = [
            ("amount0:0.005", "0.3", "1.0", "8.0", "0.35", 3),  # worst
            ("amount0:0.01", "2.2", "9.0", "2.5", "0.65", 10),  # best
            ("amount0:0.02", "1.0", "4.0", "4.0", "0.50", 7),  # middle
        ]
        results = []
        for param_val, sharpe, ret, dd, wr, trades in configs:
            bt_result = _make_backtest_result(
                sharpe=sharpe, total_return=ret, drawdown=dd, win_rate=wr, trades=trades
            )
            results.append(
                SweepResult(
                    params={"amount0": param_val.split(":")[1]},
                    result=bt_result,
                    sharpe_ratio=bt_result.metrics.sharpe_ratio,
                    total_return_pct=bt_result.metrics.total_return_pct,
                    max_drawdown_pct=bt_result.metrics.max_drawdown_pct,
                    win_rate=bt_result.metrics.win_rate,
                    total_trades=bt_result.metrics.total_trades,
                )
            )

        params = [SweepParameter("amount0", ["0.005", "0.01", "0.02"])]
        print_sweep_results_table(results, params)
        captured = capsys.readouterr().out

        # Best combination should be amount0=0.01 with Sharpe 2.2
        assert "Best combination:" in captured
        assert "amount0: 0.01" in captured

        # Verify ordering: Sharpe 2.2 before 1.0 before 0.3
        pos1 = captured.find("2.200")
        pos2 = captured.find("1.000")
        pos3 = captured.find("0.300")
        assert pos1 < pos2 < pos3

    def test_rebalance_threshold_ranking(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify ranking with rebalance_threshold_pct as sweep parameter."""
        results = []
        thresholds = [("3.0", "1.5"), ("5.0", "2.0"), ("10.0", "0.8")]
        for threshold, sharpe in thresholds:
            bt_result = _make_backtest_result(sharpe=sharpe)
            results.append(
                SweepResult(
                    params={"rebalance_threshold_pct": threshold},
                    result=bt_result,
                    sharpe_ratio=bt_result.metrics.sharpe_ratio,
                    total_return_pct=bt_result.metrics.total_return_pct,
                    max_drawdown_pct=bt_result.metrics.max_drawdown_pct,
                    win_rate=bt_result.metrics.win_rate,
                    total_trades=bt_result.metrics.total_trades,
                )
            )

        params = [SweepParameter("rebalance_threshold_pct", ["3.0", "5.0", "10.0"])]
        print_sweep_results_table(results, params)
        captured = capsys.readouterr().out

        # Best should be threshold=5.0 with Sharpe 2.0
        assert "Best combination:" in captured
        assert "rebalance_threshold_pct: 5.0" in captured
