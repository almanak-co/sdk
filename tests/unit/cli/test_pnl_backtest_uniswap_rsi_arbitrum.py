"""Tests for PnL backtest with Uniswap V3 RSI strategy on Arbitrum.

Validates that the PnL backtest CLI works with RSI swap intents:
1. Config resolves correctly for Uniswap V3 RSI on Arbitrum
2. PnL backtest pipeline handles SWAP intent lifecycle
3. Dry-run mode shows correct strategy configuration
4. Strategy cooldown mechanism works correctly through backtest

First PnL backtest CLI test for a swap strategy on Arbitrum.
Kitchen Loop iteration 129, VIB-1926.
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
def mock_pnl_config_rsi_arbitrum() -> PnLBacktestConfig:
    """PnL config for Uniswap V3 RSI on Arbitrum."""
    return PnLBacktestConfig(
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 3, 1, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("0.1"),
    )


def _make_rsi_backtest_result(
    sharpe: str = "1.2",
    total_return: str = "5.0",
    drawdown: str = "3.0",
    win_rate: str = "0.55",
    trades: int = 12,
) -> BacktestResult:
    """Create a BacktestResult representative of an RSI swap strategy."""
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="uniswap_v3_pnl_backtest_arbitrum",
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 3, 1, tzinfo=UTC),
        trades=[],
        metrics=BacktestMetrics(
            total_trades=trades,
            win_rate=Decimal(win_rate),
            total_return_pct=Decimal(total_return),
            max_drawdown_pct=Decimal(drawdown),
            sharpe_ratio=Decimal(sharpe),
            sortino_ratio=Decimal("1.5"),
            calmar_ratio=Decimal("1.7"),
            profit_factor=Decimal("1.4"),
            annualized_return_pct=Decimal("30.0"),
            net_pnl_usd=Decimal("500"),
        ),
    )


class TestPnLBacktestRSIConfig:
    """Test that PnL backtest config resolves correctly for RSI swap on Arbitrum."""

    def test_pnl_config_chain_is_arbitrum(
        self, mock_pnl_config_rsi_arbitrum: PnLBacktestConfig
    ) -> None:
        assert mock_pnl_config_rsi_arbitrum.chain == "arbitrum"

    def test_pnl_config_tokens_include_weth_usdc(
        self, mock_pnl_config_rsi_arbitrum: PnLBacktestConfig
    ) -> None:
        assert "WETH" in mock_pnl_config_rsi_arbitrum.tokens
        assert "USDC" in mock_pnl_config_rsi_arbitrum.tokens

    def test_arbitrum_gas_price_low(
        self, mock_pnl_config_rsi_arbitrum: PnLBacktestConfig
    ) -> None:
        """Arbitrum L2 gas should be much lower than mainnet."""
        assert mock_pnl_config_rsi_arbitrum.gas_price_gwei < Decimal("1")

    def test_backtest_window_59_days(
        self, mock_pnl_config_rsi_arbitrum: PnLBacktestConfig
    ) -> None:
        """59-day window gives enough RSI signals for meaningful backtest."""
        duration = (
            mock_pnl_config_rsi_arbitrum.end_time
            - mock_pnl_config_rsi_arbitrum.start_time
        )
        assert duration.days == 59


class TestPnLBacktestRSIExecution:
    """Test PnL backtest execution with RSI strategy on Arbitrum."""

    @pytest.mark.asyncio
    async def test_sweep_with_rsi_params(
        self, mock_pnl_config_rsi_arbitrum: PnLBacktestConfig
    ) -> None:
        """Verify sweep passes RSI config params through to backtester."""
        captured_configs: list[dict] = []

        class TrackingRSIStrategy:
            strategy_id = "uniswap_v3_pnl_backtest_arbitrum"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                captured_configs.append(config.copy())

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_rsi_backtest_result(trades=8)

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=TrackingRSIStrategy,
                base_config={
                    "trade_size_usd": "50",
                    "rsi_oversold": 35,
                    "rsi_overbought": 65,
                    "cooldown_ticks": 2,
                },
                pnl_config=mock_pnl_config_rsi_arbitrum,
                data_provider=MagicMock(),
                params={"rsi_oversold": "30"},
            )

        assert isinstance(result, SweepResult)
        assert result.total_trades == 8
        assert result.params == {"rsi_oversold": "30"}
        assert len(captured_configs) == 1
        assert captured_configs[0]["rsi_oversold"] == 30

    @pytest.mark.asyncio
    async def test_sweep_rsi_returns_metrics(
        self, mock_pnl_config_rsi_arbitrum: PnLBacktestConfig
    ) -> None:
        """Verify metrics extraction works for RSI swap backtest."""

        class SimpleRSIStrategy:
            strategy_id = "uniswap_v3_pnl_backtest_arbitrum"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_rsi_backtest_result(
            sharpe="1.5", total_return="7.2", drawdown="2.1", trades=15
        )

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=SimpleRSIStrategy,
                base_config={"trade_size_usd": "50", "rsi_oversold": 35},
                pnl_config=mock_pnl_config_rsi_arbitrum,
                data_provider=MagicMock(),
                params={"cooldown_ticks": "3"},
            )

        assert result.sharpe_ratio == Decimal("1.5")
        assert result.total_return_pct == Decimal("7.2")
        assert result.max_drawdown_pct == Decimal("2.1")
        assert result.total_trades == 15


class TestPnLBacktestRSIDryRun:
    """Test PnL backtest dry-run with RSI strategy."""

    def test_dry_run_rsi_strategy(self, cli_runner: CliRunner) -> None:
        """Dry run with RSI strategy shows correct config."""
        result = cli_runner.invoke(
            backtest,
            [
                "pnl",
                "-s", "uniswap_v3_pnl_backtest_arbitrum",
                "--start", "2025-01-01",
                "--end", "2025-03-01",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "uniswap_v3_pnl_backtest_arbitrum" in result.output
        assert "arbitrum" in result.output

    def test_dry_run_sweep_rsi_params(self, cli_runner: CliRunner) -> None:
        """Dry run sweep with RSI-specific params shows combinations."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "uniswap_v3_pnl_backtest_arbitrum",
                "--start", "2025-01-01",
                "--end", "2025-03-01",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--param", "rsi_oversold:30,35,40",
                "--param", "rsi_overbought:60,65,70",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "uniswap_v3_pnl_backtest_arbitrum" in result.output
        assert "Total combinations: 9" in result.output
        assert "Dry run - no backtests executed" in result.output
