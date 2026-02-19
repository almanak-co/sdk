"""Unit tests for Monte Carlo statistics calculations.

This module tests the Monte Carlo statistics features added for US-027c:
- Return percentiles (5th, 50th, 95th)
- Probability of max drawdown exceeding configurable thresholds
- Probability of negative returns
- monte_carlo_results field in BacktestResult

These tests focus on the statistical calculations and serialization,
without running full backtests (which are covered in integration tests).
"""

from datetime import datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult
from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
    MonteCarloConfig,
    MonteCarloPathBacktestResult,
    MonteCarloSimulationResult,
)


class TestMonteCarloConfigDrawdownThresholds:
    """Tests for drawdown threshold configuration in MonteCarloConfig."""

    def test_default_drawdown_thresholds(self):
        """Test that default drawdown thresholds are set correctly."""
        config = MonteCarloConfig()

        expected_thresholds = [
            Decimal("0.05"),
            Decimal("0.10"),
            Decimal("0.15"),
            Decimal("0.20"),
            Decimal("0.25"),
            Decimal("0.30"),
        ]

        assert config.drawdown_thresholds == expected_thresholds

    def test_custom_drawdown_thresholds(self):
        """Test that custom drawdown thresholds can be set."""
        custom_thresholds = [Decimal("0.03"), Decimal("0.07"), Decimal("0.15")]
        config = MonteCarloConfig(drawdown_thresholds=custom_thresholds)

        assert config.drawdown_thresholds == custom_thresholds

    def test_drawdown_threshold_validation_negative(self):
        """Test that negative threshold raises ValueError."""
        with pytest.raises(ValueError, match="between 0 and 1"):
            MonteCarloConfig(drawdown_thresholds=[Decimal("-0.05")])

    def test_drawdown_threshold_validation_greater_than_one(self):
        """Test that threshold > 1 raises ValueError."""
        with pytest.raises(ValueError, match="between 0 and 1"):
            MonteCarloConfig(drawdown_thresholds=[Decimal("1.5")])

    def test_drawdown_threshold_boundary_values(self):
        """Test boundary values 0 and 1 are allowed."""
        config = MonteCarloConfig(drawdown_thresholds=[Decimal("0"), Decimal("1")])
        assert Decimal("0") in config.drawdown_thresholds
        assert Decimal("1") in config.drawdown_thresholds

    def test_to_dict_includes_drawdown_thresholds(self):
        """Test that to_dict serializes drawdown thresholds."""
        config = MonteCarloConfig(
            drawdown_thresholds=[Decimal("0.10"), Decimal("0.20")]
        )
        data = config.to_dict()

        assert "drawdown_thresholds" in data
        assert data["drawdown_thresholds"] == ["0.10", "0.20"]


class TestMonteCarloSimulationResultDrawdownProbabilities:
    """Tests for drawdown probability calculations in MonteCarloSimulationResult."""

    def test_probability_drawdown_exceeds_threshold_field(self):
        """Test that probability_drawdown_exceeds_threshold field exists."""
        result = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.05"),
            return_std=Decimal("0.10"),
            return_percentile_5th=Decimal("-0.10"),
            return_percentile_25th=Decimal("0.01"),
            return_percentile_50th=Decimal("0.05"),
            return_percentile_75th=Decimal("0.10"),
            return_percentile_95th=Decimal("0.20"),
            max_drawdown_mean=Decimal("0.08"),
            max_drawdown_worst=Decimal("0.25"),
            max_drawdown_percentile_95th=Decimal("0.18"),
            probability_negative_return=Decimal("0.20"),
            probability_loss_exceeds_10pct=Decimal("0.05"),
            probability_loss_exceeds_20pct=Decimal("0.02"),
            probability_gain_exceeds_10pct=Decimal("0.30"),
            probability_drawdown_exceeds_threshold={
                "0.05": Decimal("0.80"),
                "0.10": Decimal("0.45"),
                "0.20": Decimal("0.10"),
            },
        )

        assert result.probability_drawdown_exceeds_threshold["0.05"] == Decimal("0.80")
        assert result.probability_drawdown_exceeds_threshold["0.10"] == Decimal("0.45")
        assert result.probability_drawdown_exceeds_threshold["0.20"] == Decimal("0.10")

    def test_probability_drawdown_to_dict(self):
        """Test that probability_drawdown_exceeds_threshold serializes correctly."""
        result = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.05"),
            return_std=Decimal("0.10"),
            return_percentile_5th=Decimal("-0.10"),
            return_percentile_25th=Decimal("0.01"),
            return_percentile_50th=Decimal("0.05"),
            return_percentile_75th=Decimal("0.10"),
            return_percentile_95th=Decimal("0.20"),
            max_drawdown_mean=Decimal("0.08"),
            max_drawdown_worst=Decimal("0.25"),
            max_drawdown_percentile_95th=Decimal("0.18"),
            probability_negative_return=Decimal("0.20"),
            probability_loss_exceeds_10pct=Decimal("0.05"),
            probability_loss_exceeds_20pct=Decimal("0.02"),
            probability_gain_exceeds_10pct=Decimal("0.30"),
            probability_drawdown_exceeds_threshold={
                "0.10": Decimal("0.45"),
                "0.20": Decimal("0.10"),
            },
        )

        data = result.to_dict()

        assert "probability_drawdown_exceeds_threshold" in data
        assert data["probability_drawdown_exceeds_threshold"]["0.10"] == "0.45"
        assert data["probability_drawdown_exceeds_threshold"]["0.20"] == "0.10"

    def test_probability_drawdown_from_dict(self):
        """Test that probability_drawdown_exceeds_threshold deserializes correctly."""
        data = {
            "n_paths": 100,
            "n_successful": 100,
            "n_failed": 0,
            "return_mean": "0.05",
            "return_std": "0.10",
            "return_percentile_5th": "-0.10",
            "return_percentile_25th": "0.01",
            "return_percentile_50th": "0.05",
            "return_percentile_75th": "0.10",
            "return_percentile_95th": "0.20",
            "max_drawdown_mean": "0.08",
            "max_drawdown_worst": "0.25",
            "max_drawdown_percentile_95th": "0.18",
            "probability_negative_return": "0.20",
            "probability_loss_exceeds_10pct": "0.05",
            "probability_loss_exceeds_20pct": "0.02",
            "probability_gain_exceeds_10pct": "0.30",
            "probability_drawdown_exceeds_threshold": {
                "0.10": "0.45",
                "0.20": "0.10",
            },
        }

        result = MonteCarloSimulationResult.from_dict(data)

        assert result.probability_drawdown_exceeds_threshold["0.10"] == Decimal("0.45")
        assert result.probability_drawdown_exceeds_threshold["0.20"] == Decimal("0.10")

    def test_empty_probability_drawdown_dict(self):
        """Test that empty probability_drawdown_exceeds_threshold works."""
        result = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.05"),
            return_std=Decimal("0.10"),
            return_percentile_5th=Decimal("-0.10"),
            return_percentile_25th=Decimal("0.01"),
            return_percentile_50th=Decimal("0.05"),
            return_percentile_75th=Decimal("0.10"),
            return_percentile_95th=Decimal("0.20"),
            max_drawdown_mean=Decimal("0.08"),
            max_drawdown_worst=Decimal("0.25"),
            max_drawdown_percentile_95th=Decimal("0.18"),
            probability_negative_return=Decimal("0.20"),
            probability_loss_exceeds_10pct=Decimal("0.05"),
            probability_loss_exceeds_20pct=Decimal("0.02"),
            probability_gain_exceeds_10pct=Decimal("0.30"),
            probability_drawdown_exceeds_threshold={},
        )

        assert result.probability_drawdown_exceeds_threshold == {}

        data = result.to_dict()
        assert data["probability_drawdown_exceeds_threshold"] == {}


class TestMonteCarloSimulationResultReturnPercentiles:
    """Tests for return percentile calculations in MonteCarloSimulationResult."""

    def test_return_percentiles_are_ordered(self):
        """Test that return percentiles are properly ordered."""
        result = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.05"),
            return_std=Decimal("0.10"),
            return_percentile_5th=Decimal("-0.15"),
            return_percentile_25th=Decimal("-0.02"),
            return_percentile_50th=Decimal("0.05"),
            return_percentile_75th=Decimal("0.12"),
            return_percentile_95th=Decimal("0.25"),
            max_drawdown_mean=Decimal("0.08"),
            max_drawdown_worst=Decimal("0.25"),
            max_drawdown_percentile_95th=Decimal("0.18"),
            probability_negative_return=Decimal("0.25"),
            probability_loss_exceeds_10pct=Decimal("0.10"),
            probability_loss_exceeds_20pct=Decimal("0.03"),
            probability_gain_exceeds_10pct=Decimal("0.35"),
        )

        # Percentiles should be in ascending order
        assert result.return_percentile_5th < result.return_percentile_25th
        assert result.return_percentile_25th < result.return_percentile_50th
        assert result.return_percentile_50th < result.return_percentile_75th
        assert result.return_percentile_75th < result.return_percentile_95th

    def test_return_percentiles_serialization_roundtrip(self):
        """Test return percentiles serialize and deserialize correctly."""
        original = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.05"),
            return_std=Decimal("0.10"),
            return_percentile_5th=Decimal("-0.1234"),
            return_percentile_25th=Decimal("0.0123"),
            return_percentile_50th=Decimal("0.0567"),
            return_percentile_75th=Decimal("0.1089"),
            return_percentile_95th=Decimal("0.2345"),
            max_drawdown_mean=Decimal("0.08"),
            max_drawdown_worst=Decimal("0.25"),
            max_drawdown_percentile_95th=Decimal("0.18"),
            probability_negative_return=Decimal("0.20"),
            probability_loss_exceeds_10pct=Decimal("0.05"),
            probability_loss_exceeds_20pct=Decimal("0.02"),
            probability_gain_exceeds_10pct=Decimal("0.30"),
        )

        data = original.to_dict()
        restored = MonteCarloSimulationResult.from_dict(data)

        assert restored.return_percentile_5th == original.return_percentile_5th
        assert restored.return_percentile_25th == original.return_percentile_25th
        assert restored.return_percentile_50th == original.return_percentile_50th
        assert restored.return_percentile_75th == original.return_percentile_75th
        assert restored.return_percentile_95th == original.return_percentile_95th


class TestMonteCarloSimulationResultProbabilities:
    """Tests for probability calculations in MonteCarloSimulationResult."""

    def test_probability_negative_return(self):
        """Test probability_negative_return calculation."""
        result = MonteCarloSimulationResult(
            n_paths=1000,
            n_successful=1000,
            n_failed=0,
            return_mean=Decimal("0.02"),
            return_std=Decimal("0.15"),
            return_percentile_5th=Decimal("-0.20"),
            return_percentile_25th=Decimal("-0.05"),
            return_percentile_50th=Decimal("0.02"),
            return_percentile_75th=Decimal("0.10"),
            return_percentile_95th=Decimal("0.25"),
            max_drawdown_mean=Decimal("0.10"),
            max_drawdown_worst=Decimal("0.35"),
            max_drawdown_percentile_95th=Decimal("0.25"),
            probability_negative_return=Decimal("0.35"),  # 35% chance of loss
            probability_loss_exceeds_10pct=Decimal("0.15"),
            probability_loss_exceeds_20pct=Decimal("0.05"),
            probability_gain_exceeds_10pct=Decimal("0.40"),
        )

        # Probability should be between 0 and 1
        assert Decimal("0") <= result.probability_negative_return <= Decimal("1")
        assert result.probability_negative_return == Decimal("0.35")

    def test_probability_bounds(self):
        """Test that all probability values are between 0 and 1."""
        result = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.05"),
            return_std=Decimal("0.10"),
            return_percentile_5th=Decimal("-0.10"),
            return_percentile_25th=Decimal("0.01"),
            return_percentile_50th=Decimal("0.05"),
            return_percentile_75th=Decimal("0.10"),
            return_percentile_95th=Decimal("0.20"),
            max_drawdown_mean=Decimal("0.08"),
            max_drawdown_worst=Decimal("0.25"),
            max_drawdown_percentile_95th=Decimal("0.18"),
            probability_negative_return=Decimal("0.20"),
            probability_loss_exceeds_10pct=Decimal("0.05"),
            probability_loss_exceeds_20pct=Decimal("0.02"),
            probability_gain_exceeds_10pct=Decimal("0.30"),
        )

        assert Decimal("0") <= result.probability_negative_return <= Decimal("1")
        assert Decimal("0") <= result.probability_loss_exceeds_10pct <= Decimal("1")
        assert Decimal("0") <= result.probability_loss_exceeds_20pct <= Decimal("1")
        assert Decimal("0") <= result.probability_gain_exceeds_10pct <= Decimal("1")


class TestBacktestResultMonteCarloResults:
    """Tests for monte_carlo_results field in BacktestResult."""

    def test_monte_carlo_results_field_exists(self):
        """Test that monte_carlo_results field can be set."""
        mc_results = MonteCarloSimulationResult(
            n_paths=100,
            n_successful=100,
            n_failed=0,
            return_mean=Decimal("0.05"),
            return_std=Decimal("0.10"),
            return_percentile_5th=Decimal("-0.10"),
            return_percentile_25th=Decimal("0.01"),
            return_percentile_50th=Decimal("0.05"),
            return_percentile_75th=Decimal("0.10"),
            return_percentile_95th=Decimal("0.20"),
            max_drawdown_mean=Decimal("0.08"),
            max_drawdown_worst=Decimal("0.25"),
            max_drawdown_percentile_95th=Decimal("0.18"),
            probability_negative_return=Decimal("0.20"),
            probability_loss_exceeds_10pct=Decimal("0.05"),
            probability_loss_exceeds_20pct=Decimal("0.02"),
            probability_gain_exceeds_10pct=Decimal("0.30"),
        )

        backtest_result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            metrics=BacktestMetrics(),
            monte_carlo_results=mc_results,
        )

        assert backtest_result.monte_carlo_results is not None
        assert backtest_result.monte_carlo_results.n_paths == 100

    def test_monte_carlo_results_default_none(self):
        """Test that monte_carlo_results defaults to None."""
        backtest_result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            metrics=BacktestMetrics(),
        )

        assert backtest_result.monte_carlo_results is None

    def test_monte_carlo_results_to_dict(self):
        """Test that monte_carlo_results serializes correctly in BacktestResult."""
        mc_results = MonteCarloSimulationResult(
            n_paths=50,
            n_successful=50,
            n_failed=0,
            return_mean=Decimal("0.08"),
            return_std=Decimal("0.12"),
            return_percentile_5th=Decimal("-0.12"),
            return_percentile_25th=Decimal("0.02"),
            return_percentile_50th=Decimal("0.08"),
            return_percentile_75th=Decimal("0.14"),
            return_percentile_95th=Decimal("0.28"),
            max_drawdown_mean=Decimal("0.10"),
            max_drawdown_worst=Decimal("0.30"),
            max_drawdown_percentile_95th=Decimal("0.22"),
            probability_negative_return=Decimal("0.18"),
            probability_loss_exceeds_10pct=Decimal("0.06"),
            probability_loss_exceeds_20pct=Decimal("0.01"),
            probability_gain_exceeds_10pct=Decimal("0.35"),
            probability_drawdown_exceeds_threshold={
                "0.10": Decimal("0.50"),
                "0.20": Decimal("0.15"),
            },
        )

        backtest_result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            metrics=BacktestMetrics(),
            monte_carlo_results=mc_results,
        )

        data = backtest_result.to_dict()

        assert "monte_carlo_results" in data
        assert data["monte_carlo_results"]["n_paths"] == 50
        assert data["monte_carlo_results"]["return_mean"] == "0.08"

    def test_monte_carlo_results_to_dict_when_none(self):
        """Test that monte_carlo_results serializes as None when not set."""
        backtest_result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            metrics=BacktestMetrics(),
        )

        data = backtest_result.to_dict()

        assert data["monte_carlo_results"] is None

    def test_monte_carlo_results_from_dict(self):
        """Test that monte_carlo_results deserializes correctly."""
        data = {
            "engine": "pnl",
            "strategy_id": "test_strategy",
            "start_time": "2024-01-01T00:00:00",
            "end_time": "2024-12-31T00:00:00",
            "metrics": {},
            "trades": [],
            "equity_curve": [],
            "monte_carlo_results": {
                "n_paths": 75,
                "n_successful": 75,
                "n_failed": 0,
                "return_mean": "0.06",
                "return_std": "0.11",
                "return_percentile_5th": "-0.11",
                "return_percentile_25th": "0.01",
                "return_percentile_50th": "0.06",
                "return_percentile_75th": "0.11",
                "return_percentile_95th": "0.22",
                "max_drawdown_mean": "0.09",
                "max_drawdown_worst": "0.28",
                "max_drawdown_percentile_95th": "0.20",
                "probability_negative_return": "0.22",
                "probability_loss_exceeds_10pct": "0.08",
                "probability_loss_exceeds_20pct": "0.02",
                "probability_gain_exceeds_10pct": "0.32",
                "probability_drawdown_exceeds_threshold": {
                    "0.10": "0.45",
                    "0.20": "0.12",
                },
            },
        }

        backtest_result = BacktestResult.from_dict(data)

        assert backtest_result.monte_carlo_results is not None
        assert backtest_result.monte_carlo_results.n_paths == 75
        assert backtest_result.monte_carlo_results.return_mean == Decimal("0.06")
        assert backtest_result.monte_carlo_results.probability_drawdown_exceeds_threshold["0.10"] == Decimal("0.45")

    def test_monte_carlo_results_from_dict_when_none(self):
        """Test that monte_carlo_results deserializes as None when not in data."""
        data = {
            "engine": "pnl",
            "strategy_id": "test_strategy",
            "start_time": "2024-01-01T00:00:00",
            "end_time": "2024-12-31T00:00:00",
            "metrics": {},
            "trades": [],
            "equity_curve": [],
        }

        backtest_result = BacktestResult.from_dict(data)

        assert backtest_result.monte_carlo_results is None

    def test_monte_carlo_results_roundtrip(self):
        """Test full serialization roundtrip with monte_carlo_results."""
        mc_results = MonteCarloSimulationResult(
            n_paths=200,
            n_successful=195,
            n_failed=5,
            return_mean=Decimal("0.0723"),
            return_std=Decimal("0.1456"),
            return_percentile_5th=Decimal("-0.1589"),
            return_percentile_25th=Decimal("0.0012"),
            return_percentile_50th=Decimal("0.0689"),
            return_percentile_75th=Decimal("0.1323"),
            return_percentile_95th=Decimal("0.3012"),
            max_drawdown_mean=Decimal("0.1123"),
            max_drawdown_worst=Decimal("0.3456"),
            max_drawdown_percentile_95th=Decimal("0.2567"),
            probability_negative_return=Decimal("0.2456"),
            probability_loss_exceeds_10pct=Decimal("0.1234"),
            probability_loss_exceeds_20pct=Decimal("0.0456"),
            probability_gain_exceeds_10pct=Decimal("0.4123"),
            probability_drawdown_exceeds_threshold={
                "0.05": Decimal("0.8567"),
                "0.10": Decimal("0.5234"),
                "0.15": Decimal("0.3123"),
                "0.20": Decimal("0.1567"),
                "0.25": Decimal("0.0789"),
                "0.30": Decimal("0.0234"),
            },
        )

        original = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="roundtrip_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            metrics=BacktestMetrics(),
            monte_carlo_results=mc_results,
        )

        data = original.to_dict()
        restored = BacktestResult.from_dict(data)

        assert restored.monte_carlo_results is not None
        assert restored.monte_carlo_results.n_paths == 200
        assert restored.monte_carlo_results.n_successful == 195
        assert restored.monte_carlo_results.n_failed == 5
        assert restored.monte_carlo_results.return_mean == Decimal("0.0723")
        assert restored.monte_carlo_results.return_percentile_5th == Decimal("-0.1589")
        assert restored.monte_carlo_results.probability_negative_return == Decimal("0.2456")
        assert restored.monte_carlo_results.probability_drawdown_exceeds_threshold["0.10"] == Decimal("0.5234")


class TestMonteCarloPathBacktestResult:
    """Tests for MonteCarloPathBacktestResult dataclass."""

    def test_path_backtest_result_fields(self):
        """Test MonteCarloPathBacktestResult has all required fields."""
        result = MonteCarloPathBacktestResult(
            path_index=5,
            final_return=Decimal("0.15"),
            final_value_usd=Decimal("11500"),
            max_drawdown=Decimal("0.08"),
            sharpe_ratio=Decimal("1.25"),
            total_trades=42,
            success=True,
        )

        assert result.path_index == 5
        assert result.final_return == Decimal("0.15")
        assert result.final_value_usd == Decimal("11500")
        assert result.max_drawdown == Decimal("0.08")
        assert result.sharpe_ratio == Decimal("1.25")
        assert result.total_trades == 42
        assert result.success is True

    def test_path_backtest_result_failed(self):
        """Test MonteCarloPathBacktestResult with failure."""
        result = MonteCarloPathBacktestResult(
            path_index=10,
            final_return=Decimal("0"),
            final_value_usd=Decimal("0"),
            max_drawdown=Decimal("0"),
            success=False,
            error="Backtest execution failed",
        )

        assert result.success is False
        assert result.error == "Backtest execution failed"

    def test_path_backtest_result_to_dict(self):
        """Test MonteCarloPathBacktestResult serialization."""
        result = MonteCarloPathBacktestResult(
            path_index=3,
            final_return=Decimal("0.12"),
            final_value_usd=Decimal("11200"),
            max_drawdown=Decimal("0.05"),
            sharpe_ratio=Decimal("1.50"),
            total_trades=35,
            success=True,
        )

        data = result.to_dict()

        assert data["path_index"] == 3
        assert data["final_return"] == "0.12"
        assert data["final_value_usd"] == "11200"
        assert data["max_drawdown"] == "0.05"
        assert data["sharpe_ratio"] == "1.50"
        assert data["total_trades"] == 35
        assert data["success"] is True
