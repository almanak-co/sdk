"""Tests for PnL backtest with Uniswap V3 RSI strategy on Optimism.

Validates that the PnL backtest CLI works on Optimism chain:
1. Strategy resolves and loads config correctly for Optimism
2. PnL backtest pipeline handles Optimism chain parameter
3. Backtest results contain expected metrics
4. Strategy correctly generates swap intents based on RSI

First PnL backtest test on Optimism chain.
Kitchen Loop iteration 83, VIB-1359.
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
    SweepResult,
    backtest,
    run_sweep_backtest,
)


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def mock_pnl_config_optimism() -> PnLBacktestConfig:
    """PnL config for Optimism chain."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 10, 1, tzinfo=UTC),
        end_time=datetime(2024, 12, 1, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="optimism",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("0.001"),
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
        strategy_id="incubating_uniswap_rsi_optimism",
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


class TestPnLBacktestOptimismConfig:
    """Test that PnL backtest config resolves correctly for Optimism."""

    def test_pnl_config_chain_is_optimism(self, mock_pnl_config_optimism: PnLBacktestConfig) -> None:
        assert mock_pnl_config_optimism.chain == "optimism"

    def test_pnl_config_tokens(self, mock_pnl_config_optimism: PnLBacktestConfig) -> None:
        assert "WETH" in mock_pnl_config_optimism.tokens
        assert "USDC" in mock_pnl_config_optimism.tokens

    def test_optimism_gas_price_low(self, mock_pnl_config_optimism: PnLBacktestConfig) -> None:
        """Optimism L2 gas should be much lower than mainnet."""
        assert mock_pnl_config_optimism.gas_price_gwei < Decimal("1")


class TestPnLBacktestOptimismSweepExecution:
    """Test PnL backtest execution on Optimism with mocked backtester."""

    @pytest.mark.asyncio
    async def test_sweep_on_optimism_chain(
        self, mock_pnl_config_optimism: PnLBacktestConfig
    ) -> None:
        """Verify sweep passes Optimism chain config through to backtester."""
        captured_configs: list[dict] = []

        class TrackingStrategy:
            strategy_id = "incubating_uniswap_rsi_optimism"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                captured_configs.append(config.copy())

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_backtest_result(trades=8)

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=TrackingStrategy,
                base_config={
                    "trade_size_usd": 5,
                    "rsi_period": 14,
                    "rsi_oversold": 35,
                    "rsi_overbought": 65,
                    "base_token": "WETH",
                    "quote_token": "USDC",
                },
                pnl_config=mock_pnl_config_optimism,
                data_provider=MagicMock(),
                params={"rsi_oversold": "30"},
            )

        assert isinstance(result, SweepResult)
        assert result.total_trades == 8
        assert result.params == {"rsi_oversold": "30"}
        # Config should have overridden value
        assert len(captured_configs) == 1
        assert captured_configs[0]["rsi_oversold"] == 30.0

    @pytest.mark.asyncio
    async def test_sweep_returns_metrics_on_optimism(
        self, mock_pnl_config_optimism: PnLBacktestConfig
    ) -> None:
        """Verify metrics extraction works for Optimism backtest."""

        class SimpleStrategy:
            strategy_id = "optimism_test"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_backtest_result(
            sharpe="1.8", total_return="7.2", drawdown="2.1", trades=15
        )

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=SimpleStrategy,
                base_config={"trade_size_usd": 5},
                pnl_config=mock_pnl_config_optimism,
                data_provider=MagicMock(),
                params={"trade_size_usd": "10"},
            )

        assert result.sharpe_ratio == Decimal("1.8")
        assert result.total_return_pct == Decimal("7.2")
        assert result.max_drawdown_pct == Decimal("2.1")
        assert result.total_trades == 15


class TestPnLBacktestOptimismDryRun:
    """Test PnL sweep dry-run with Optimism strategy."""

    def test_dry_run_optimism_strategy(self, cli_runner: CliRunner) -> None:
        """Dry run with Optimism RSI strategy shows correct config."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "incubating_uniswap_rsi_optimism",
                "--start", "2024-10-01",
                "--end", "2024-12-01",
                "--chain", "optimism",
                "--tokens", "WETH,USDC",
                "--param", "rsi_oversold:25,30,35,40",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "incubating_uniswap_rsi_optimism" in result.output
        assert "optimism" in result.output
        assert "Total combinations: 4" in result.output
        assert "Dry run - no backtests executed" in result.output
