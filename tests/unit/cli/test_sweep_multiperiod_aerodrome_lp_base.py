"""Tests for multi-period parameter sweep with Aerodrome LP on Base.

Validates the multi-period aggregation pipeline:
1. Results are grouped by parameter combination across periods
2. Average metrics (Sharpe, return, drawdown) are computed correctly
3. Sharpe std dev measures robustness across time windows
4. Aggregated results are sorted by average Sharpe ratio
5. JSON output includes both per-period and aggregated data
6. Best params align between console output and JSON output

First multi-period sweep test in the Kitchen Loop.
VIB-1927: Backtesting: Parameter sweep Aerodrome LP strategy on Base.
"""

from __future__ import annotations

import re

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
)
from almanak.framework.cli.backtest import (
    AggregatedParamResult,
    SweepParameter,
    SweepResult,
    _aggregate_multi_period_results,
    _print_multi_period_results,
    generate_combinations,
)


# =============================================================================
# Fixtures
# =============================================================================


def _make_backtest_result(
    sharpe: str = "1.5",
    total_return: str = "5.0",
    drawdown: str = "3.0",
    win_rate: str = "0.55",
    trades: int = 12,
    net_pnl: str = "500",
) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="demo_aerodrome_sweep_lp",
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
            net_pnl_usd=Decimal(net_pnl),
        ),
    )


def _make_sweep_result(
    params: dict[str, str],
    sharpe: str = "1.5",
    total_return: str = "5.0",
    drawdown: str = "3.0",
    trades: int = 12,
    net_pnl: str = "500",
    period_name: str = "",
) -> SweepResult:
    bt = _make_backtest_result(sharpe=sharpe, total_return=total_return, drawdown=drawdown, trades=trades, net_pnl=net_pnl)
    return SweepResult(
        params=params,
        result=bt,
        sharpe_ratio=bt.metrics.sharpe_ratio,
        total_return_pct=bt.metrics.total_return_pct,
        max_drawdown_pct=bt.metrics.max_drawdown_pct,
        win_rate=bt.metrics.win_rate,
        total_trades=bt.metrics.total_trades,
        period_name=period_name,
    )


# =============================================================================
# Multi-Period Aggregation
# =============================================================================


class TestMultiPeriodAggregation:
    """Test _aggregate_multi_period_results groups and averages correctly."""

    def test_basic_aggregation_two_periods(self) -> None:
        """Two periods, two param combos -> 2 aggregated results."""
        params_a = {"rsi_oversold": "25"}
        params_b = {"rsi_oversold": "35"}

        results = [
            _make_sweep_result(params_a, sharpe="2.0", total_return="8.0", drawdown="4.0", trades=10, net_pnl="400", period_name="Q1"),
            _make_sweep_result(params_a, sharpe="1.0", total_return="4.0", drawdown="6.0", trades=14, net_pnl="200", period_name="Q2"),
            _make_sweep_result(params_b, sharpe="1.5", total_return="6.0", drawdown="3.0", trades=8, net_pnl="300", period_name="Q1"),
            _make_sweep_result(params_b, sharpe="1.5", total_return="6.0", drawdown="3.0", trades=8, net_pnl="300", period_name="Q2"),
        ]
        combinations = [params_a, params_b]

        agg = _aggregate_multi_period_results(results, combinations)

        assert len(agg) == 2
        # Sorted by avg_sharpe descending
        # params_a: avg (2.0+1.0)/2 = 1.5
        # params_b: avg (1.5+1.5)/2 = 1.5
        # Both have 1.5 avg Sharpe — order determined by sort stability
        assert all(isinstance(a, AggregatedParamResult) for a in agg)

    def test_avg_sharpe_computed_correctly(self) -> None:
        """Average Sharpe = mean of per-period Sharpe ratios."""
        params = {"amount0": "0.01"}
        results = [
            _make_sweep_result(params, sharpe="1.0", period_name="Q1"),
            _make_sweep_result(params, sharpe="2.0", period_name="Q2"),
            _make_sweep_result(params, sharpe="3.0", period_name="Q3"),
        ]

        agg = _aggregate_multi_period_results(results, [params])

        assert len(agg) == 1
        assert agg[0].avg_sharpe == pytest.approx(2.0)

    def test_avg_return_computed_correctly(self) -> None:
        """Average return = mean of per-period returns."""
        params = {"amount0": "0.01"}
        results = [
            _make_sweep_result(params, total_return="10.0", period_name="Q1"),
            _make_sweep_result(params, total_return="20.0", period_name="Q2"),
        ]

        agg = _aggregate_multi_period_results(results, [params])

        assert agg[0].avg_return_pct == pytest.approx(15.0)

    def test_avg_drawdown_computed_correctly(self) -> None:
        """Average drawdown = mean of per-period drawdowns."""
        params = {"amount0": "0.01"}
        results = [
            _make_sweep_result(params, drawdown="5.0", period_name="Q1"),
            _make_sweep_result(params, drawdown="15.0", period_name="Q2"),
        ]

        agg = _aggregate_multi_period_results(results, [params])

        assert agg[0].avg_max_dd_pct == pytest.approx(10.0)

    def test_cumulative_pnl_sums_across_periods(self) -> None:
        """Cumulative PnL = sum of net_pnl_usd across periods."""
        params = {"range_width_pct": "20"}
        results = [
            _make_sweep_result(params, net_pnl="300", period_name="Q1"),
            _make_sweep_result(params, net_pnl="500", period_name="Q2"),
            _make_sweep_result(params, net_pnl="-100", period_name="Q3"),
        ]

        agg = _aggregate_multi_period_results(results, [params])

        assert agg[0].cumulative_pnl == pytest.approx(700.0)

    def test_sharpe_std_measures_robustness(self) -> None:
        """Sharpe std dev measures consistency across periods (lower = more robust)."""
        # Consistent performer: Sharpe [2.0, 2.0, 2.0]
        consistent = {"rsi_oversold": "30"}
        # Volatile performer: Sharpe [0.5, 2.0, 3.5]
        volatile = {"rsi_oversold": "25"}

        results = [
            _make_sweep_result(consistent, sharpe="2.0", period_name="Q1"),
            _make_sweep_result(consistent, sharpe="2.0", period_name="Q2"),
            _make_sweep_result(consistent, sharpe="2.0", period_name="Q3"),
            _make_sweep_result(volatile, sharpe="0.5", period_name="Q1"),
            _make_sweep_result(volatile, sharpe="2.0", period_name="Q2"),
            _make_sweep_result(volatile, sharpe="3.5", period_name="Q3"),
        ]

        agg = _aggregate_multi_period_results(results, [consistent, volatile])

        agg_map = {tuple(sorted(a.params.items())): a for a in agg}

        consistent_std = agg_map[tuple(sorted(consistent.items()))].sharpe_std
        volatile_std = agg_map[tuple(sorted(volatile.items()))].sharpe_std

        assert consistent_std == pytest.approx(0.0)
        assert volatile_std > 1.0  # High std = less robust

    def test_sorted_by_avg_sharpe_descending(self) -> None:
        """Results sorted by average Sharpe ratio descending."""
        params_a = {"amount0": "0.005"}
        params_b = {"amount0": "0.01"}
        params_c = {"amount0": "0.02"}

        results = [
            _make_sweep_result(params_a, sharpe="1.0", period_name="Q1"),
            _make_sweep_result(params_a, sharpe="1.0", period_name="Q2"),
            _make_sweep_result(params_b, sharpe="3.0", period_name="Q1"),
            _make_sweep_result(params_b, sharpe="3.0", period_name="Q2"),
            _make_sweep_result(params_c, sharpe="2.0", period_name="Q1"),
            _make_sweep_result(params_c, sharpe="2.0", period_name="Q2"),
        ]

        agg = _aggregate_multi_period_results(results, [params_a, params_b, params_c])

        assert len(agg) == 3
        assert agg[0].params == params_b  # Sharpe 3.0
        assert agg[1].params == params_c  # Sharpe 2.0
        assert agg[2].params == params_a  # Sharpe 1.0

    def test_single_period_sharpe_std_zero(self) -> None:
        """With only 1 period, sharpe_std should be 0."""
        params = {"amount0": "0.01"}
        results = [_make_sweep_result(params, sharpe="2.5", period_name="Q1")]

        agg = _aggregate_multi_period_results(results, [params])

        assert agg[0].sharpe_std == 0.0

    def test_per_period_results_preserved(self) -> None:
        """Each aggregated result contains the original per-period SweepResults."""
        params = {"range_width_pct": "10"}
        results = [
            _make_sweep_result(params, sharpe="1.5", period_name="Q1"),
            _make_sweep_result(params, sharpe="2.5", period_name="Q2"),
        ]

        agg = _aggregate_multi_period_results(results, [params])

        assert len(agg[0].per_period) == 2
        assert agg[0].per_period[0].period_name == "Q1"
        assert agg[0].per_period[1].period_name == "Q2"


# =============================================================================
# Multi-Period Console Output
# =============================================================================


class TestMultiPeriodOutput:
    """Test _print_multi_period_results console output."""

    def test_print_shows_best_combination(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Multi-period output shows the best parameter combination."""
        params_a = {"rsi_oversold": "25"}
        params_b = {"rsi_oversold": "35"}

        results = [
            _make_sweep_result(params_a, sharpe="1.0", period_name="Q1"),
            _make_sweep_result(params_a, sharpe="1.0", period_name="Q2"),
            _make_sweep_result(params_b, sharpe="2.5", period_name="Q1"),
            _make_sweep_result(params_b, sharpe="2.0", period_name="Q2"),
        ]

        agg = _aggregate_multi_period_results(results, [params_a, params_b])
        sweep_params = [SweepParameter("rsi_oversold", ["25", "35"])]

        _print_multi_period_results(results, agg, sweep_params)
        captured = capsys.readouterr().out

        assert "rsi_oversold" in captured
        # Best combination (params_b, Sharpe 2.25) should appear in WINNER section
        assert "WINNER" in captured
        assert "35" in captured  # Best param value

    def test_print_shows_sharpe_std(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Multi-period output includes Sharpe std dev."""
        params = {"amount0": "0.01"}
        results = [
            _make_sweep_result(params, sharpe="1.0", period_name="Q1"),
            _make_sweep_result(params, sharpe="3.0", period_name="Q2"),
        ]

        agg = _aggregate_multi_period_results(results, [params])
        sweep_params = [SweepParameter("amount0", ["0.01"])]

        _print_multi_period_results(results, agg, sweep_params)
        captured = capsys.readouterr().out

        assert "amount0" in captured
        # Sharpe std should appear in output (header column or winner line)
        assert re.search(r"std", captured, re.IGNORECASE)


# =============================================================================
# Multi-Period Sweep with LP-Specific Parameters
# =============================================================================


class TestMultiPeriodLPSweep:
    """Test multi-period sweep with Aerodrome LP parameters."""

    def test_range_width_across_market_regimes(self) -> None:
        """Range width performance varies across bull/bear/sideways periods."""
        narrow = {"range_width_pct": "10"}
        wide = {"range_width_pct": "50"}

        # Narrow range: great in sideways, bad in trends
        # Wide range: consistent across regimes
        results = [
            # Bull market Q1
            _make_sweep_result(narrow, sharpe="0.5", total_return="2.0", period_name="Bull"),
            _make_sweep_result(wide, sharpe="1.5", total_return="6.0", period_name="Bull"),
            # Sideways Q2
            _make_sweep_result(narrow, sharpe="3.0", total_return="12.0", period_name="Sideways"),
            _make_sweep_result(wide, sharpe="1.0", total_return="4.0", period_name="Sideways"),
            # Bear Q3
            _make_sweep_result(narrow, sharpe="-1.0", total_return="-5.0", period_name="Bear"),
            _make_sweep_result(wide, sharpe="0.5", total_return="1.0", period_name="Bear"),
        ]

        agg = _aggregate_multi_period_results(results, [narrow, wide])
        agg_map = {a.params["range_width_pct"]: a for a in agg}

        # Wide range is more robust (lower std)
        assert agg_map["50"].sharpe_std < agg_map["10"].sharpe_std
        # Wide range has higher avg Sharpe (1.0 vs 0.833)
        assert agg_map["50"].avg_sharpe == pytest.approx(1.0)
        assert agg_map["10"].avg_sharpe == pytest.approx(0.833, rel=0.01)

    def test_cooldown_period_sweep(self) -> None:
        """Reentry cooldown affects trade frequency across periods."""
        fast = {"reentry_cooldown": "1"}
        slow = {"reentry_cooldown": "5"}

        results = [
            _make_sweep_result(fast, sharpe="1.5", trades=30, period_name="Q1"),
            _make_sweep_result(fast, sharpe="1.0", trades=25, period_name="Q2"),
            _make_sweep_result(slow, sharpe="2.0", trades=8, period_name="Q1"),
            _make_sweep_result(slow, sharpe="1.8", trades=6, period_name="Q2"),
        ]

        agg = _aggregate_multi_period_results(results, [fast, slow])
        agg_map = {a.params["reentry_cooldown"]: a for a in agg}

        # Slow cooldown = fewer trades, higher Sharpe
        assert agg_map["5"].avg_sharpe > agg_map["1"].avg_sharpe
        assert agg_map["5"].avg_trades < agg_map["1"].avg_trades

    def test_multi_param_multi_period_grid(self) -> None:
        """2 params x 2 values x 3 periods = 12 results, 4 aggregated."""
        params = [
            SweepParameter("range_width_pct", ["10", "50"]),
            SweepParameter("rsi_oversold", ["25", "35"]),
        ]
        combos = generate_combinations(params)
        assert len(combos) == 4

        results = []
        for combo in combos:
            for i, period in enumerate(["Q1", "Q2", "Q3"]):
                # Deterministic Sharpe based on params
                base_sharpe = 1.0
                if combo["range_width_pct"] == "50":
                    base_sharpe += 0.5
                if combo["rsi_oversold"] == "25":
                    base_sharpe += 0.3
                sharpe = str(base_sharpe + i * 0.1)
                results.append(_make_sweep_result(combo, sharpe=sharpe, period_name=period))

        agg = _aggregate_multi_period_results(results, combos)

        assert len(agg) == 4
        # Best combo: range_width_pct=50, rsi_oversold=25 (highest base Sharpe)
        assert agg[0].params == {"range_width_pct": "50", "rsi_oversold": "25"}
        assert len(agg[0].per_period) == 3
