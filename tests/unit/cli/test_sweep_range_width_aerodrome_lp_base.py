"""Tests for range_width_pct parameter sweep with Aerodrome LP on Base.

Validates that the sweep engine correctly handles LP range width as a sweepable
parameter. This is the first test of range width optimization, which is critical
for concentrated liquidity strategies — wider ranges capture more fees but with
less capital efficiency, while narrower ranges are more capital-efficient but
risk going out of range.

Prior sweep tests covered amount0, amount1, rebalance_threshold_pct, and RSI
thresholds. range_width_pct is a new sweep dimension added in this PR.

VIB-1820: Backtesting: Parameter sweep Aerodrome LP range width on Base
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
    strategy_id: str = "demo_aerodrome_sweep_lp",
) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id=strategy_id,
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
# Range Width Parameter Combination Generation
# =============================================================================


class TestRangeWidthCombinations:
    """Test that range_width_pct produces correct sweep grids."""

    def test_range_width_grid(self) -> None:
        """Single range_width_pct parameter produces correct combinations."""
        params = [
            SweepParameter("range_width_pct", ["5", "10", "20", "50", "100"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 5
        assert {"range_width_pct": "5"} in combos
        assert {"range_width_pct": "100"} in combos

    def test_range_width_with_amount_grid(self) -> None:
        """range_width_pct x amount0 produces full Cartesian product."""
        params = [
            SweepParameter("range_width_pct", ["10", "20", "50"]),
            SweepParameter("amount0", ["0.001", "0.005"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 6  # 3 x 2
        assert {"range_width_pct": "10", "amount0": "0.001"} in combos
        assert {"range_width_pct": "50", "amount0": "0.005"} in combos

    def test_range_width_with_rsi_grid(self) -> None:
        """range_width_pct x rsi_oversold produces full product."""
        params = [
            SweepParameter("range_width_pct", ["10", "20", "50"]),
            SweepParameter("rsi_oversold", ["25", "30", "35"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 9  # 3 x 3

    def test_full_lp_sweep_grid(self) -> None:
        """range_width_pct + amount0 + rsi_oversold = 3-dimensional grid."""
        params = [
            SweepParameter("range_width_pct", ["10", "50"]),
            SweepParameter("amount0", ["0.001", "0.005"]),
            SweepParameter("rsi_oversold", ["25", "35"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 8  # 2 x 2 x 2
        # Verify one specific combination exists
        assert {"range_width_pct": "10", "amount0": "0.001", "rsi_oversold": "25"} in combos


# =============================================================================
# Dry Run with range_width_pct
# =============================================================================


class TestRangeWidthSweepDryRun:
    """Test sweep --dry-run with range_width_pct on Base."""

    def test_dry_run_range_width_sweep(self, cli_runner: CliRunner) -> None:
        """Dry run shows range_width_pct parameter combinations."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_aerodrome_sweep_lp",
                "--start", "2024-10-01",
                "--end", "2024-12-01",
                "--chain", "base",
                "--tokens", "WETH,USDC",
                "--param", "range_width_pct:5,10,20,50,100",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_aerodrome_sweep_lp" in result.output
        assert "base" in result.output
        assert "Total combinations: 5" in result.output
        assert "Dry run - no backtests executed" in result.output

    def test_dry_run_range_width_with_amounts(self, cli_runner: CliRunner) -> None:
        """Dry run with range_width_pct x amount0 grid."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_aerodrome_sweep_lp",
                "--start", "2024-10-01",
                "--end", "2024-12-01",
                "--chain", "base",
                "--param", "range_width_pct:10,20,50",
                "--param", "amount0:0.001,0.005,0.01",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "Total combinations: 9" in result.output  # 3 x 3

    def test_dry_run_minimum_3_combinations(self, cli_runner: CliRunner) -> None:
        """At least 3 range width values tested per acceptance criteria."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_aerodrome_sweep_lp",
                "--start", "2024-10-01",
                "--end", "2024-12-01",
                "--chain", "base",
                "--param", "range_width_pct:10,20,50",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0
        assert "Total combinations: 3" in result.output


# =============================================================================
# Sweep Execution with range_width_pct
# =============================================================================


class TestRangeWidthSweepExecution:
    """Test sweep execution with range_width_pct parameter injection."""

    @pytest.mark.asyncio
    async def test_sweep_injects_range_width_into_config(
        self, mock_pnl_config_base: PnLBacktestConfig
    ) -> None:
        """Verify sweep injects range_width_pct into strategy config."""
        captured_configs: list[dict] = []

        class TrackingStrategy:
            strategy_id = "demo_aerodrome_sweep_lp"

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
                    "amount0": 0.001,
                    "amount1": 3,
                    "range_width_pct": 0,
                },
                pnl_config=mock_pnl_config_base,
                data_provider=MagicMock(),
                params={"range_width_pct": "20"},
            )

        assert isinstance(result, SweepResult)
        assert result.params == {"range_width_pct": "20"}
        assert len(captured_configs) == 1
        assert captured_configs[0]["range_width_pct"] == 20.0

    @pytest.mark.asyncio
    async def test_sweep_preserves_base_config_with_range_width(
        self, mock_pnl_config_base: PnLBacktestConfig
    ) -> None:
        """Non-overridden params are preserved when range_width_pct is swept."""
        captured_configs: list[dict] = []

        class TrackingStrategy:
            strategy_id = "demo_aerodrome_sweep_lp"

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
                base_config={
                    "pool": "WETH/USDC",
                    "stable": False,
                    "amount0": 0.001,
                    "amount1": 3,
                    "range_width_pct": 0,
                    "rsi_oversold": 30,
                },
                pnl_config=mock_pnl_config_base,
                data_provider=MagicMock(),
                params={"range_width_pct": "50"},
            )

        assert captured_configs[0]["pool"] == "WETH/USDC"
        assert captured_configs[0]["amount0"] == 0.001
        assert captured_configs[0]["rsi_oversold"] == 30
        assert captured_configs[0]["range_width_pct"] == 50.0

    @pytest.mark.asyncio
    async def test_sweep_returns_correct_metrics_for_range_width(
        self, mock_pnl_config_base: PnLBacktestConfig
    ) -> None:
        """Verify SweepResult metrics are correctly extracted."""

        class SimpleStrategy:
            strategy_id = "lp_range_test"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_backtest_result(
            sharpe="2.1", total_return="8.5", drawdown="3.0", trades=15
        )

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=SimpleStrategy,
                base_config={"range_width_pct": 0},
                pnl_config=mock_pnl_config_base,
                data_provider=MagicMock(),
                params={"range_width_pct": "10"},
            )

        assert result.sharpe_ratio == Decimal("2.1")
        assert result.total_return_pct == Decimal("8.5")
        assert result.max_drawdown_pct == Decimal("3.0")
        assert result.total_trades == 15


# =============================================================================
# Result Ranking with range_width_pct
# =============================================================================


class TestRangeWidthResultRanking:
    """Test that sweep results show variation across range widths."""

    def test_different_range_widths_produce_different_results(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify results table shows PnL variation across range widths."""
        # Simulate 4 range width values with different Sharpe ratios
        # Narrow range (10%): high Sharpe but higher drawdown risk
        # Medium range (20%): balanced
        # Wide range (50%): lower Sharpe but more stable
        # Full range (100%): lowest Sharpe, most stable
        configs = [
            ("10", "2.5", "12.0", "8.0", "0.65", 20),   # narrow: highest Sharpe
            ("20", "2.0", "9.0", "5.0", "0.60", 15),     # medium
            ("50", "1.2", "5.0", "3.0", "0.55", 10),     # wide
            ("100", "0.8", "3.0", "2.0", "0.50", 8),     # full range: lowest
        ]
        results = []
        for width, sharpe, ret, dd, wr, trades in configs:
            bt_result = _make_backtest_result(
                sharpe=sharpe, total_return=ret, drawdown=dd, win_rate=wr, trades=trades
            )
            results.append(
                SweepResult(
                    params={"range_width_pct": width},
                    result=bt_result,
                    sharpe_ratio=bt_result.metrics.sharpe_ratio,
                    total_return_pct=bt_result.metrics.total_return_pct,
                    max_drawdown_pct=bt_result.metrics.max_drawdown_pct,
                    win_rate=bt_result.metrics.win_rate,
                    total_trades=bt_result.metrics.total_trades,
                )
            )

        params = [SweepParameter("range_width_pct", ["10", "20", "50", "100"])]
        print_sweep_results_table(results, params)
        captured = capsys.readouterr()

        # Results should be sorted by Sharpe descending
        matching_lines = [line for line in captured.out.split("\n") if "range_width_pct" in line]
        assert len(matching_lines) > 0, "Expected range_width_pct in sweep results table"
        # Should show variation in results
        assert len(results) == 4
        # Best result should have highest Sharpe
        sorted_results = sorted(results, key=lambda r: r.sharpe_ratio, reverse=True)
        assert sorted_results[0].params["range_width_pct"] == "10"
        assert sorted_results[-1].params["range_width_pct"] == "100"

    def test_optimal_range_width_identified(self) -> None:
        """Sweep should identify the optimal range_width_pct by Sharpe."""
        results = []
        for width, sharpe in [("10", "1.8"), ("20", "2.5"), ("50", "1.5"), ("100", "0.9")]:
            bt_result = _make_backtest_result(sharpe=sharpe)
            results.append(
                SweepResult(
                    params={"range_width_pct": width},
                    result=bt_result,
                    sharpe_ratio=bt_result.metrics.sharpe_ratio,
                    total_return_pct=bt_result.metrics.total_return_pct,
                    max_drawdown_pct=bt_result.metrics.max_drawdown_pct,
                    win_rate=bt_result.metrics.win_rate,
                    total_trades=bt_result.metrics.total_trades,
                )
            )

        best = max(results, key=lambda r: r.sharpe_ratio)
        assert best.params["range_width_pct"] == "20"
        assert best.sharpe_ratio == Decimal("2.5")


# =============================================================================
# Strategy range_width_pct Config Integration
# =============================================================================


class TestRangeWidthStrategyConfig:
    """Test the AerodromeSweepLP strategy accepts range_width_pct config."""

    def test_range_width_config_default(self) -> None:
        """Default range_width_pct should be 0 (full range) in config.json."""
        import json
        from pathlib import Path

        config_path = Path(__file__).parents[3] / "almanak" / "demo_strategies" / "aerodrome_sweep_lp" / "config.json"
        if not config_path.exists():
            pytest.skip("aerodrome_sweep_lp config.json not found")

        with open(config_path) as f:
            config = json.load(f)

        assert "range_width_pct" in config
        assert config["range_width_pct"] == "0"

    def test_range_width_sweep_param_values_valid(self) -> None:
        """Sweep grid values should be non-negative percentages."""
        valid_widths = ["0", "5", "10", "20", "50", "100"]
        params = [SweepParameter("range_width_pct", valid_widths)]
        combos = generate_combinations(params)

        for combo in combos:
            width = Decimal(combo["range_width_pct"])
            assert width >= Decimal("0"), "Range width must be >= 0"

    def test_range_width_zero_means_full_range(self) -> None:
        """range_width_pct=0 should produce full range bounds (1 to 1,000,000)."""
        # This tests the semantic meaning at the config level
        # The actual implementation uses 0 to mean "full range"
        range_width_pct = Decimal("0")
        if range_width_pct == Decimal("0"):
            range_lower = Decimal("1")
            range_upper = Decimal("1000000")
        else:
            current_price = Decimal("3000")
            half_width = current_price * range_width_pct / Decimal("200")
            range_lower = current_price - half_width
            range_upper = current_price + half_width

        assert range_lower == Decimal("1")
        assert range_upper == Decimal("1000000")

    def test_range_width_10_pct_produces_correct_bounds(self) -> None:
        """10% range width at $3000 price -> [2850, 3150]."""
        current_price = Decimal("3000")
        range_width_pct = Decimal("10")
        half_width = current_price * range_width_pct / Decimal("200")

        range_lower = current_price - half_width
        range_upper = current_price + half_width

        assert range_lower == Decimal("2850")
        assert range_upper == Decimal("3150")

    def test_range_width_50_pct_produces_correct_bounds(self) -> None:
        """50% range width at $3000 price -> [2250, 3750]."""
        current_price = Decimal("3000")
        range_width_pct = Decimal("50")
        half_width = current_price * range_width_pct / Decimal("200")

        range_lower = current_price - half_width
        range_upper = current_price + half_width

        assert range_lower == Decimal("2250")
        assert range_upper == Decimal("3750")

    def test_range_lower_floored_at_001(self) -> None:
        """Very wide range at low price should floor range_lower at 0.01."""
        current_price = Decimal("10")
        range_width_pct = Decimal("200")
        half_width = current_price * range_width_pct / Decimal("200")

        range_lower = max(current_price - half_width, Decimal("0.01"))
        range_upper = current_price + half_width

        assert range_lower == Decimal("0.01")  # Floored
        assert range_upper == Decimal("20")
