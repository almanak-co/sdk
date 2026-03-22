"""Tests for PnL backtest and parameter sweep with Uniswap RSI on Arbitrum.

Validates the specific parameter grid from VIB-1572:
- rsi_period: 7, 14, 21 (window sizes)
- rsi_oversold: 25, 30, 35 (buy thresholds)
- rsi_overbought: 65, 70, 75 (sell thresholds)

Tests verify:
1. Sweep generates 27 combinations (3x3x3 grid)
2. PnL dry-run with 30-day range on Arbitrum
3. Sweep dry-run with the ticket's exact parameter grid
4. Strategy accepts all parameter combinations without error
5. Different rsi_period values are passed through to market.rsi()

Kitchen Loop iteration 114, VIB-1572.
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
        end_time=datetime(2025, 1, 31, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("30"),
    )


def _make_backtest_result(
    sharpe: str = "1.0",
    total_return: str = "3.0",
    drawdown: str = "2.0",
    win_rate: str = "0.50",
    trades: int = 10,
) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="demo_uniswap_rsi_sweep",
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 1, 31, tzinfo=UTC),
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
            net_pnl_usd=Decimal("300"),
        ),
    )


# =============================================================================
# VIB-1572 Parameter Grid
# =============================================================================


class TestVIB1572ParameterGrid:
    """Test the specific parameter grid from VIB-1572."""

    def test_full_grid_generates_27_combinations(self) -> None:
        """3x3x3 grid of rsi_period x rsi_oversold x rsi_overbought = 27."""
        params = [
            SweepParameter("rsi_period", ["7", "14", "21"]),
            SweepParameter("rsi_oversold", ["25", "30", "35"]),
            SweepParameter("rsi_overbought", ["65", "70", "75"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 27

    def test_all_combinations_have_required_keys(self) -> None:
        """Every combination contains all three parameter keys."""
        params = [
            SweepParameter("rsi_period", ["7", "14", "21"]),
            SweepParameter("rsi_oversold", ["25", "30", "35"]),
            SweepParameter("rsi_overbought", ["65", "70", "75"]),
        ]
        combos = generate_combinations(params)
        for combo in combos:
            assert "rsi_period" in combo
            assert "rsi_oversold" in combo
            assert "rsi_overbought" in combo

    def test_extreme_combination_exists(self) -> None:
        """Verify boundary combinations exist."""
        params = [
            SweepParameter("rsi_period", ["7", "14", "21"]),
            SweepParameter("rsi_oversold", ["25", "30", "35"]),
            SweepParameter("rsi_overbought", ["65", "70", "75"]),
        ]
        combos = generate_combinations(params)
        # Tightest: short window, tight thresholds
        assert {"rsi_period": "7", "rsi_oversold": "35", "rsi_overbought": "65"} in combos
        # Widest: long window, wide thresholds
        assert {"rsi_period": "21", "rsi_oversold": "25", "rsi_overbought": "75"} in combos

    def test_minimum_10_combinations_with_2_params(self) -> None:
        """Even a 2-param grid exceeds 10 combinations per ticket AC."""
        params = [
            SweepParameter("rsi_oversold", ["25", "28", "30", "33", "35"]),
            SweepParameter("rsi_overbought", ["65", "70", "75"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) >= 10  # 5x3 = 15


# =============================================================================
# PnL Dry Run
# =============================================================================


class TestPnLDryRun:
    """Test PnL backtest dry-run for 30-day range on Arbitrum."""

    def test_pnl_dry_run_uniswap_rsi(self, cli_runner: CliRunner) -> None:
        """PnL dry-run with 30-day range shows correct configuration."""
        result = cli_runner.invoke(
            backtest,
            [
                "pnl",
                "-s", "demo_uniswap_rsi_sweep",
                "--start", "2025-01-01",
                "--end", "2025-01-31",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--interval", "3600",
                "--initial-capital", "10000",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_uniswap_rsi_sweep" in result.output
        assert "arbitrum" in result.output


# =============================================================================
# Sweep Dry Run with VIB-1572 Parameters
# =============================================================================


class TestSweepDryRunVIB1572:
    """Test sweep dry-run with the ticket's exact parameter ranges."""

    def test_sweep_dry_run_27_combinations(self, cli_runner: CliRunner) -> None:
        """Sweep dry-run shows 27 combinations for 3x3x3 grid."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_rsi_sweep",
                "--start", "2025-01-01",
                "--end", "2025-01-31",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--param", "rsi_period:7,14,21",
                "--param", "rsi_oversold:25,30,35",
                "--param", "rsi_overbought:65,70,75",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Total combinations: 27" in result.output
        assert "Dry run - no backtests executed" in result.output

    def test_sweep_dry_run_15_combinations(self, cli_runner: CliRunner) -> None:
        """Sweep dry-run with 5x3 = 15 combinations (>10 per AC)."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_uniswap_rsi_sweep",
                "--start", "2025-01-01",
                "--end", "2025-01-31",
                "--chain", "arbitrum",
                "--param", "rsi_oversold:25,28,30,33,35",
                "--param", "rsi_overbought:65,70,75",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Total combinations: 15" in result.output


# =============================================================================
# Strategy Parameter Injection
# =============================================================================


class TestStrategyParameterInjection:
    """Verify sweep injects VIB-1572 parameter values into strategy config."""

    @pytest.mark.asyncio
    async def test_rsi_period_injected(
        self, mock_pnl_config: PnLBacktestConfig
    ) -> None:
        """Sweep correctly overrides rsi_period in strategy config."""
        captured_configs: list[dict] = []

        class TrackingStrategy:
            strategy_id = "test_rsi"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                captured_configs.append(config.copy())

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_backtest_result()

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            await run_sweep_backtest(
                strategy_class=TrackingStrategy,
                base_config={"rsi_period": 14, "rsi_oversold": 30, "rsi_overbought": 70},
                pnl_config=mock_pnl_config,
                data_provider=MagicMock(),
                params={"rsi_period": "7", "rsi_oversold": "25", "rsi_overbought": "75"},
            )

        assert len(captured_configs) == 1
        assert captured_configs[0]["rsi_period"] == 7.0
        assert captured_configs[0]["rsi_oversold"] == 25.0
        assert captured_configs[0]["rsi_overbought"] == 75.0


# =============================================================================
# Strategy Accepts All VIB-1572 Parameter Values
# =============================================================================


class TestStrategyAcceptsAllParams:
    """Verify the sweep strategy accepts all parameter values without error."""

    @pytest.mark.parametrize("rsi_period", [7, 14, 21])
    def test_strategy_init_with_rsi_period(self, rsi_period: int) -> None:
        from strategies.demo.uniswap_rsi_sweep.strategy import UniswapRSISweepStrategy

        strat = UniswapRSISweepStrategy(
            config={
                "trade_size_usd": "3",
                "rsi_period": rsi_period,
                "rsi_oversold": "30",
                "rsi_overbought": "70",
                "max_slippage_bps": 100,
                "base_token": "WETH",
                "quote_token": "USDC",
            },
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
        )
        assert strat.rsi_period == rsi_period

    @pytest.mark.parametrize("rsi_oversold", [25, 30, 35])
    def test_strategy_init_with_rsi_oversold(self, rsi_oversold: int) -> None:
        from strategies.demo.uniswap_rsi_sweep.strategy import UniswapRSISweepStrategy

        strat = UniswapRSISweepStrategy(
            config={
                "trade_size_usd": "3",
                "rsi_period": 14,
                "rsi_oversold": str(rsi_oversold),
                "rsi_overbought": "70",
                "max_slippage_bps": 100,
                "base_token": "WETH",
                "quote_token": "USDC",
            },
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
        )
        assert strat.rsi_oversold == Decimal(str(rsi_oversold))

    @pytest.mark.parametrize("rsi_overbought", [65, 70, 75])
    def test_strategy_init_with_rsi_overbought(self, rsi_overbought: int) -> None:
        from strategies.demo.uniswap_rsi_sweep.strategy import UniswapRSISweepStrategy

        strat = UniswapRSISweepStrategy(
            config={
                "trade_size_usd": "3",
                "rsi_period": 14,
                "rsi_oversold": "30",
                "rsi_overbought": str(rsi_overbought),
                "max_slippage_bps": 100,
                "base_token": "WETH",
                "quote_token": "USDC",
            },
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
        )
        assert strat.rsi_overbought == Decimal(str(rsi_overbought))

    @pytest.mark.parametrize("rsi_period", [7, 21])
    def test_rsi_period_passed_to_market(self, rsi_period: int) -> None:
        """Verify non-default rsi_period values are passed to market.rsi()."""
        from strategies.demo.uniswap_rsi_sweep.strategy import UniswapRSISweepStrategy

        strat = UniswapRSISweepStrategy(
            config={
                "trade_size_usd": "3",
                "rsi_period": rsi_period,
                "rsi_oversold": "30",
                "rsi_overbought": "70",
                "max_slippage_bps": 100,
                "base_token": "WETH",
                "quote_token": "USDC",
            },
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
        )
        market = MagicMock()
        market.price.return_value = Decimal("3000")
        rsi_mock = MagicMock()
        rsi_mock.value = Decimal("50")
        market.rsi.return_value = rsi_mock
        quote_bal = MagicMock()
        quote_bal.balance = Decimal("10000")
        quote_bal.balance_usd = Decimal("10000")
        market.balance.return_value = quote_bal

        strat.decide(market)
        market.rsi.assert_called_once_with("WETH", period=rsi_period)
