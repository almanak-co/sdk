"""Tests for RSI period x trade_size_usd parameter sweep grid.

Validates the specific parameter grid from VIB-1701:
- RSI period: [10, 14, 20]
- Trade size USD: [500, 1000]
- Total: 6 combinations (3x2 Cartesian product)
- Window: 2025-01-01 to 2025-02-01
- Chain: Arbitrum
- Strategy: demo_uniswap_rsi_sweep

These tests exercise sweep pipeline paths not covered by existing tests:
1. rsi_period as a sweep parameter (previously only thresholds were swept)
2. trade_size_usd with large USD values (500, 1000 vs 3, 5, 10)
3. 31-day backtest window (vs 60-day in existing tests)
4. Full 6-combo grid execution with result ranking

Kitchen Loop iteration 119, VIB-1701.
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
# VIB-1701 Grid Constants
# =============================================================================

RSI_PERIODS = ["10", "14", "20"]
TRADE_SIZES = ["500", "1000"]
EXPECTED_COMBOS = 6  # 3 x 2


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def vib1701_pnl_config() -> PnLBacktestConfig:
    """31-day window: 2025-01-01 to 2025-02-01."""
    return PnLBacktestConfig(
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 2, 1, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("30"),
    )


def _make_result(
    sharpe: str = "1.0",
    total_return: str = "3.0",
    drawdown: str = "2.0",
    win_rate: str = "0.50",
    trades: int = 8,
    net_pnl: str = "300",
) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="demo_uniswap_rsi_sweep",
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 2, 1, tzinfo=UTC),
        trades=[],
        metrics=BacktestMetrics(
            total_trades=trades,
            win_rate=Decimal(win_rate),
            total_return_pct=Decimal(total_return),
            max_drawdown_pct=Decimal(drawdown),
            sharpe_ratio=Decimal(sharpe),
            sortino_ratio=Decimal("1.5"),
            calmar_ratio=Decimal("0.8"),
            profit_factor=Decimal("1.3"),
            annualized_return_pct=Decimal("15.0"),
            net_pnl_usd=Decimal(net_pnl),
        ),
    )


# =============================================================================
# Grid Generation
# =============================================================================


class TestVIB1701GridGeneration:
    """Test that the VIB-1701 specific grid produces exactly 6 combos."""

    def test_rsi_period_x_trade_size_grid(self):
        """3 RSI periods x 2 trade sizes = 6 combinations."""
        params = [
            SweepParameter("rsi_period", RSI_PERIODS),
            SweepParameter("trade_size_usd", TRADE_SIZES),
        ]
        combos = generate_combinations(params)
        assert len(combos) == EXPECTED_COMBOS

    def test_all_rsi_periods_present(self):
        """Each RSI period value appears in combinations."""
        params = [
            SweepParameter("rsi_period", RSI_PERIODS),
            SweepParameter("trade_size_usd", TRADE_SIZES),
        ]
        combos = generate_combinations(params)
        rsi_values = {c["rsi_period"] for c in combos}
        assert rsi_values == {"10", "14", "20"}

    def test_all_trade_sizes_present(self):
        """Each trade size value appears in combinations."""
        params = [
            SweepParameter("rsi_period", RSI_PERIODS),
            SweepParameter("trade_size_usd", TRADE_SIZES),
        ]
        combos = generate_combinations(params)
        trade_values = {c["trade_size_usd"] for c in combos}
        assert trade_values == {"500", "1000"}

    def test_specific_combo_exists(self):
        """Verify a specific expected combination."""
        params = [
            SweepParameter("rsi_period", RSI_PERIODS),
            SweepParameter("trade_size_usd", TRADE_SIZES),
        ]
        combos = generate_combinations(params)
        assert {"rsi_period": "14", "trade_size_usd": "1000"} in combos

    def test_no_duplicate_combos(self):
        """All combinations are unique."""
        params = [
            SweepParameter("rsi_period", RSI_PERIODS),
            SweepParameter("trade_size_usd", TRADE_SIZES),
        ]
        combos = generate_combinations(params)
        combo_tuples = [tuple(sorted(c.items())) for c in combos]
        assert len(combo_tuples) == len(set(combo_tuples))


# =============================================================================
# Dry Run
# =============================================================================


class TestVIB1701DryRun:
    """Test dry-run with the VIB-1701 grid."""

    def test_dry_run_shows_6_combinations(self, cli_runner: CliRunner):
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_rsi_sweep",
                "--start", "2025-01-01",
                "--end", "2025-02-01",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--param", "rsi_period:10,14,20",
                "--param", "trade_size_usd:500,1000",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Total combinations: 6" in result.output
        assert "Dry run" in result.output

    def test_dry_run_shows_strategy_name(self, cli_runner: CliRunner):
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_rsi_sweep",
                "--start", "2025-01-01",
                "--end", "2025-02-01",
                "--param", "rsi_period:10,14,20",
                "--param", "trade_size_usd:500,1000",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0
        assert "demo_uniswap_rsi_sweep" in result.output


# =============================================================================
# Sweep Execution (Mocked)
# =============================================================================


class TestVIB1701SweepExecution:
    """Test sweep execution with mocked backtester for VIB-1701 grid."""

    @pytest.mark.asyncio
    async def test_rsi_period_injected_into_config(
        self, vib1701_pnl_config: PnLBacktestConfig
    ):
        """Verify rsi_period is correctly injected into strategy config."""
        captured_configs: list[dict] = []

        class ConfigTracker:
            strategy_id = "demo_uniswap_rsi_sweep"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                captured_configs.append(config.copy())

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_result()
        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)
            await run_sweep_backtest(
                strategy_class=ConfigTracker,
                base_config={"rsi_period": 14, "trade_size_usd": 3},
                pnl_config=vib1701_pnl_config,
                data_provider=MagicMock(),
                params={"rsi_period": "20", "trade_size_usd": "1000"},
            )

        assert len(captured_configs) == 1
        assert captured_configs[0]["rsi_period"] == 20.0
        assert captured_configs[0]["trade_size_usd"] == 1000.0

    @pytest.mark.asyncio
    async def test_large_trade_size_accepted(
        self, vib1701_pnl_config: PnLBacktestConfig
    ):
        """Verify $500 and $1000 trade sizes work (larger than typical $3-10)."""

        class SimpleStrategy:
            strategy_id = "demo_uniswap_rsi_sweep"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_result(sharpe="1.8", total_return="12.0")
        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)
            result = await run_sweep_backtest(
                strategy_class=SimpleStrategy,
                base_config={"trade_size_usd": 3},
                pnl_config=vib1701_pnl_config,
                data_provider=MagicMock(),
                params={"trade_size_usd": "1000"},
            )

        assert result.sharpe_ratio == Decimal("1.8")
        assert result.params["trade_size_usd"] == "1000"


# =============================================================================
# Result Ranking for VIB-1701 Grid
# =============================================================================


class TestVIB1701ResultRanking:
    """Test ranking of 6 sweep results from VIB-1701 grid."""

    def _make_sweep_result(self, rsi_period: str, trade_size: str, sharpe: str) -> SweepResult:
        result = _make_result(sharpe=sharpe)
        return SweepResult(
            params={"rsi_period": rsi_period, "trade_size_usd": trade_size},
            result=result,
            sharpe_ratio=result.metrics.sharpe_ratio,
            total_return_pct=result.metrics.total_return_pct,
            max_drawdown_pct=result.metrics.max_drawdown_pct,
            win_rate=result.metrics.win_rate,
            total_trades=result.metrics.total_trades,
        )

    def test_6_results_ranked_by_sharpe(self, capsys: pytest.CaptureFixture[str]):
        """All 6 VIB-1701 combos ranked by Sharpe, best first."""
        results = [
            self._make_sweep_result("10", "500", "0.8"),
            self._make_sweep_result("10", "1000", "1.1"),
            self._make_sweep_result("14", "500", "1.5"),  # best
            self._make_sweep_result("14", "1000", "1.3"),
            self._make_sweep_result("20", "500", "0.9"),
            self._make_sweep_result("20", "1000", "0.6"),
        ]
        params = [
            SweepParameter("rsi_period", RSI_PERIODS),
            SweepParameter("trade_size_usd", TRADE_SIZES),
        ]
        print_sweep_results_table(results, params)
        captured = capsys.readouterr().out

        assert "Best combination:" in captured
        assert "rsi_period: 14" in captured
        assert "trade_size_usd: 500" in captured

        # Verify ordering: best Sharpe first
        pos_best = captured.find("1.500")
        pos_worst = captured.find("0.600")
        assert pos_best < pos_worst

    def test_all_6_results_appear_in_table(self, capsys: pytest.CaptureFixture[str]):
        """All 6 combinations appear in the output table."""
        results = [
            self._make_sweep_result(rsi, trade, f"{1.0 + i * 0.1:.1f}")
            for i, (rsi, trade) in enumerate(
                [(r, t) for r in RSI_PERIODS for t in TRADE_SIZES]
            )
        ]
        params = [
            SweepParameter("rsi_period", RSI_PERIODS),
            SweepParameter("trade_size_usd", TRADE_SIZES),
        ]
        print_sweep_results_table(results, params)
        captured = capsys.readouterr().out

        # All RSI periods should appear
        for rsi in RSI_PERIODS:
            assert rsi in captured, f"RSI period {rsi} missing from output"
