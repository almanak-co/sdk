"""Tests for parameter sweep with RSI strategy on Arbitrum.

Validates that the sweep CLI correctly:
1. Accepts RSI parameters (rsi_oversold, rsi_overbought, trade_size_usd)
2. Generates correct Cartesian product of RSI parameter combinations
3. Passes parameters through to strategy instantiation
4. Produces ranked results sorted by Sharpe ratio

These tests exercise the sweep pipeline end-to-end with mocked backtest
execution, verifying the integration between CLI, strategy config, and
result aggregation. This is the first test coverage for RSI-specific
parameter sweeping (Kitchen Loop iteration 52, VIB-579).
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
def mock_pnl_config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 3, 1, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("30"),
    )


def _make_backtest_result(
    sharpe: str = "1.5",
    total_return: str = "5.0",
    drawdown: str = "3.0",
    win_rate: str = "0.55",
    trades: int = 12,
) -> BacktestResult:
    """Create a BacktestResult with configurable metrics."""
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="demo_uniswap_rsi",
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 3, 1, tzinfo=UTC),
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
# RSI Parameter Combination Generation
# =============================================================================


class TestRSISweepCombinations:
    """Test that RSI parameter grids generate correct combinations."""

    def test_rsi_threshold_grid(self) -> None:
        """2x3 grid of RSI thresholds produces 6 combinations."""
        params = [
            SweepParameter("rsi_oversold", ["20", "30", "40"]),
            SweepParameter("rsi_overbought", ["60", "70"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 6
        # Verify a specific combination exists
        assert {"rsi_oversold": "20", "rsi_overbought": "60"} in combos
        assert {"rsi_oversold": "40", "rsi_overbought": "70"} in combos

    def test_three_param_grid(self) -> None:
        """RSI thresholds + trade size produces full Cartesian product."""
        params = [
            SweepParameter("rsi_oversold", ["25", "35"]),
            SweepParameter("rsi_overbought", ["65", "75"]),
            SweepParameter("trade_size_usd", ["5", "10"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 8  # 2 x 2 x 2
        for combo in combos:
            assert "rsi_oversold" in combo
            assert "rsi_overbought" in combo
            assert "trade_size_usd" in combo


# =============================================================================
# Dry Run with RSI Parameters
# =============================================================================


class TestRSISweepDryRun:
    """Test sweep --dry-run with RSI-specific parameters."""

    def test_dry_run_rsi_sweep_on_arbitrum(self, cli_runner: CliRunner) -> None:
        """Dry run shows RSI parameter combinations for Arbitrum."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_rsi",
                "--start", "2025-01-01",
                "--end", "2025-03-01",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--param", "rsi_oversold:20,30,40",
                "--param", "rsi_overbought:60,70,80",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_uniswap_rsi" in result.output
        assert "arbitrum" in result.output
        assert "Total combinations: 9" in result.output  # 3x3=9
        assert "Dry run - no backtests executed" in result.output

    def test_dry_run_single_rsi_param(self, cli_runner: CliRunner) -> None:
        """Dry run with only one RSI parameter varied."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_rsi",
                "--start", "2025-01-01",
                "--end", "2025-03-01",
                "--param", "rsi_oversold:20,25,30,35,40",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Total combinations: 5" in result.output  # 5 combinations


# =============================================================================
# Sweep Execution with Mocked Backtester
# =============================================================================


class TestRSISweepExecution:
    """Test sweep execution with mocked PnL backtester."""

    @pytest.mark.asyncio
    async def test_sweep_applies_rsi_params_to_strategy(
        self, mock_pnl_config: PnLBacktestConfig
    ) -> None:
        """Verify sweep injects RSI params into strategy config."""
        captured_configs: list[dict] = []

        class TrackingStrategy:
            strategy_id = "demo_uniswap_rsi"

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
                base_config={"rsi_oversold": 30, "rsi_overbought": 70},
                pnl_config=mock_pnl_config,
                data_provider=MagicMock(),
                params={"rsi_oversold": "25", "rsi_overbought": "75"},
            )

        assert isinstance(result, SweepResult)
        assert result.params == {"rsi_oversold": "25", "rsi_overbought": "75"}
        # Config should have the overridden values (as floats after type coercion)
        assert len(captured_configs) == 1
        assert captured_configs[0]["rsi_oversold"] == 25.0
        assert captured_configs[0]["rsi_overbought"] == 75.0

    @pytest.mark.asyncio
    async def test_sweep_returns_metrics(
        self, mock_pnl_config: PnLBacktestConfig
    ) -> None:
        """Verify sweep result contains correct metrics."""

        class SimpleStrategy:
            strategy_id = "rsi_test"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_backtest_result(
            sharpe="2.1", total_return="8.5", drawdown="1.2", trades=20
        )

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=SimpleStrategy,
                base_config={},
                pnl_config=mock_pnl_config,
                data_provider=MagicMock(),
                params={"rsi_oversold": "20"},
            )

        assert result.sharpe_ratio == Decimal("2.1")
        assert result.total_return_pct == Decimal("8.5")
        assert result.max_drawdown_pct == Decimal("1.2")
        assert result.total_trades == 20


# =============================================================================
# Result Table Sorting
# =============================================================================


class TestSweepResultRanking:
    """Test that sweep results are ranked by Sharpe ratio."""

    def test_results_sorted_by_sharpe(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify print_sweep_results_table sorts by Sharpe descending."""
        result1 = _make_backtest_result(sharpe="0.5", total_return="2.0", drawdown="5.0", win_rate="0.4", trades=5)
        result2 = _make_backtest_result(sharpe="2.5", total_return="10.0", drawdown="2.0", win_rate="0.7", trades=15)
        result3 = _make_backtest_result(sharpe="1.2", total_return="5.0", drawdown="3.0", win_rate="0.55", trades=10)

        results = [
            SweepResult(
                params={"rsi_oversold": "40"},
                result=result1,
                sharpe_ratio=result1.metrics.sharpe_ratio,
                total_return_pct=result1.metrics.total_return_pct,
                max_drawdown_pct=result1.metrics.max_drawdown_pct,
                win_rate=result1.metrics.win_rate,
                total_trades=result1.metrics.total_trades,
            ),
            SweepResult(
                params={"rsi_oversold": "20"},
                result=result2,
                sharpe_ratio=result2.metrics.sharpe_ratio,
                total_return_pct=result2.metrics.total_return_pct,
                max_drawdown_pct=result2.metrics.max_drawdown_pct,
                win_rate=result2.metrics.win_rate,
                total_trades=result2.metrics.total_trades,
            ),
            SweepResult(
                params={"rsi_oversold": "30"},
                result=result3,
                sharpe_ratio=result3.metrics.sharpe_ratio,
                total_return_pct=result3.metrics.total_return_pct,
                max_drawdown_pct=result3.metrics.max_drawdown_pct,
                win_rate=result3.metrics.win_rate,
                total_trades=result3.metrics.total_trades,
            ),
        ]

        params = [SweepParameter("rsi_oversold", ["20", "30", "40"])]

        # Exercise the print function and verify its output ranking
        print_sweep_results_table(results, params)
        captured = capsys.readouterr().out

        # Verify best combination is the highest Sharpe (rsi_oversold=20, sharpe=2.5)
        assert "Best combination:" in captured
        assert "rsi_oversold: 20" in captured

        # Verify table row ordering: rank 1 should appear before rank 2
        rank1_pos = captured.find("2.500")  # Sharpe 2.5
        rank2_pos = captured.find("1.200")  # Sharpe 1.2
        rank3_pos = captured.find("0.500")  # Sharpe 0.5
        assert rank1_pos < rank2_pos < rank3_pos
