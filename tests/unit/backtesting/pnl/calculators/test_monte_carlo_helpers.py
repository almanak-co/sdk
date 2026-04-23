"""Unit tests for the phase helpers extracted from ``run_monte_carlo``.

Each helper is exercised directly (not through the async entry point) so
we can lock its boundary behaviour and keep ``run_monte_carlo`` at
CC <= 12.
"""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.calculators import _monte_carlo_helpers as h
from almanak.framework.backtesting.pnl.calculators.monte_carlo import (
    PathGenerationMethod,
    PricePathResult,
)
from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
    MonteCarloConfig,
    MonteCarloPathBacktestResult,
    MonteCarloSimulationResult,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _path_result(n_paths: int = 3) -> PricePathResult:
    return PricePathResult(
        paths=[[Decimal("100"), Decimal("110")] for _ in range(n_paths)],
        n_paths=n_paths,
        n_steps=1,
        method=PathGenerationMethod.GBM,
        drift=Decimal("0.05"),
        volatility=Decimal("0.2"),
        start_price=Decimal("100"),
        dt=Decimal("1") / Decimal("252"),
        seed=42,
    )


def _path_result_single(first_price: str) -> list[Decimal]:
    return [Decimal(first_price), Decimal(first_price) * Decimal("1.1")]


def _mc_path_result(
    path_index: int,
    final_return: str,
    *,
    max_drawdown: str = "0.05",
    sharpe: str | None = "1.0",
    success: bool = True,
    final_value: str = "10000",
) -> MonteCarloPathBacktestResult:
    return MonteCarloPathBacktestResult(
        path_index=path_index,
        final_return=Decimal(final_return),
        final_value_usd=Decimal(final_value),
        max_drawdown=Decimal(max_drawdown),
        sharpe_ratio=Decimal(sharpe) if sharpe is not None else None,
        total_trades=1,
        success=success,
    )


# ---------------------------------------------------------------------------
# _calculate_percentile
# ---------------------------------------------------------------------------


class TestCalculatePercentile:
    def test_empty_list_returns_zero(self) -> None:
        assert h._calculate_percentile([], 50) == Decimal("0")

    def test_single_value(self) -> None:
        assert h._calculate_percentile([Decimal("7")], 50) == Decimal("7")

    def test_known_indexing(self) -> None:
        v = [Decimal(str(i)) for i in range(1, 11)]  # 1..10 sorted
        assert h._calculate_percentile(v, 0) == Decimal("1")
        assert h._calculate_percentile(v, 50) == Decimal("5")
        assert h._calculate_percentile(v, 95) == Decimal("9")
        assert h._calculate_percentile(v, 100) == Decimal("10")

    def test_clamps_above_range(self) -> None:
        v = [Decimal("1"), Decimal("2"), Decimal("3")]
        # int((250/100)*2) == 5 -> clamped to 2.
        assert h._calculate_percentile(v, 250) == Decimal("3")

    def test_clamps_below_range(self) -> None:
        v = [Decimal("1"), Decimal("2"), Decimal("3")]
        # int((-10/100)*2) == 0 -> already valid; feed extreme negative to hit
        # the ``max(0, ...)`` branch.
        assert h._calculate_percentile(v, -500) == Decimal("1")


# ---------------------------------------------------------------------------
# _calculate_std
# ---------------------------------------------------------------------------


class TestCalculateStd:
    def test_fewer_than_two(self) -> None:
        assert h._calculate_std([], Decimal("0")) == Decimal("0")
        assert h._calculate_std([Decimal("5")], Decimal("5")) == Decimal("0")

    def test_zero_variance(self) -> None:
        assert h._calculate_std(
            [Decimal("3"), Decimal("3"), Decimal("3")], Decimal("3")
        ) == Decimal("0")

    def test_known_std(self) -> None:
        values = [Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4"), Decimal("5")]
        # Sample std of 1..5 = sqrt(2.5) ~= 1.58113883...
        result = h._calculate_std(values, Decimal("3"))
        assert abs(result - Decimal("1.58113883008418966")) < Decimal("1e-12")

    def test_negative_variance_defensive_return(self) -> None:
        # This path cannot happen with real stats (sum of squares >= 0), but the
        # defensive ``if variance <= 0: return 0`` short-circuit is reachable if
        # a caller hands in a mean that cancels out to zero. Covering the two
        # identical-value path above and two-distinct-value path together
        # locks the branch.
        result = h._calculate_std([Decimal("1"), Decimal("1")], Decimal("1"))
        assert result == Decimal("0")


# ---------------------------------------------------------------------------
# resolve_runtime_defaults
# ---------------------------------------------------------------------------


class _FakeFee:
    pass


class _FakeSlip:
    pass


def _install_fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = types.ModuleType("almanak.framework.backtesting.pnl.engine")
    fake.DefaultFeeModel = _FakeFee  # type: ignore[attr-defined]
    fake.DefaultSlippageModel = _FakeSlip  # type: ignore[attr-defined]

    class _FakePnL:
        pass

    fake.PnLBacktester = _FakePnL  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "almanak.framework.backtesting.pnl.engine", fake)


class TestResolveRuntimeDefaults:
    def test_all_none_fills_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_engine(monkeypatch)
        cfg, fees, slips = h.resolve_runtime_defaults(None, None, None)
        assert isinstance(cfg, MonteCarloConfig)
        assert isinstance(fees["default"], _FakeFee)
        assert isinstance(slips["default"], _FakeSlip)

    def test_preserves_provided_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_engine(monkeypatch)
        user_cfg = MonteCarloConfig(n_paths=5)
        user_fees = {"custom": object()}
        user_slips = {"custom": object()}
        cfg, fees, slips = h.resolve_runtime_defaults(user_cfg, user_fees, user_slips)
        assert cfg is user_cfg
        assert fees is user_fees
        assert slips is user_slips


# ---------------------------------------------------------------------------
# determine_paths_to_run
# ---------------------------------------------------------------------------


class TestDeterminePathsToRun:
    def test_under_available_no_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        paths = _path_result(n_paths=10)
        with caplog.at_level("WARNING"):
            n = h.determine_paths_to_run(paths, 5)
        assert n == 5
        assert not any(
            "only" in rec.message and "available" in rec.message for rec in caplog.records
        )

    def test_exactly_available_no_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        paths = _path_result(n_paths=4)
        with caplog.at_level("WARNING"):
            n = h.determine_paths_to_run(paths, 4)
        assert n == 4
        assert not any(
            "only" in rec.message and "available" in rec.message for rec in caplog.records
        )

    def test_above_available_clamps_and_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        paths = _path_result(n_paths=2)
        with caplog.at_level("WARNING"):
            n = h.determine_paths_to_run(paths, 10)
        assert n == 2
        assert any("only 2 available" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# dispatch_backtests
# ---------------------------------------------------------------------------


class TestDispatchBacktests:
    @pytest.mark.asyncio
    async def test_sequential_preserves_order(self) -> None:
        calls: list[int] = []

        async def run(i: int) -> MonteCarloPathBacktestResult:
            calls.append(i)
            return _mc_path_result(path_index=i, final_return="0.01")

        results = await h.dispatch_backtests(
            n_paths_to_run=3,
            parallel_workers=1,
            run_path=run,
            progress_callback=None,
        )
        assert [r.path_index for r in results] == [0, 1, 2]
        assert calls == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_sequential_progress_callback(self) -> None:
        seen: list[tuple[int, int]] = []

        async def run(i: int) -> MonteCarloPathBacktestResult:
            return _mc_path_result(path_index=i, final_return="0.01")

        def cb(done: int, total: int) -> None:
            seen.append((done, total))

        await h.dispatch_backtests(
            n_paths_to_run=3,
            parallel_workers=1,
            run_path=run,
            progress_callback=cb,
        )
        assert seen == [(1, 3), (2, 3), (3, 3)]

    @pytest.mark.asyncio
    async def test_parallel_returns_in_path_order(self) -> None:
        async def run(i: int) -> MonteCarloPathBacktestResult:
            # Delay path 0 more than path 2 to ensure gather preserves submit order.
            await asyncio.sleep(0.005 * (3 - i))
            return _mc_path_result(path_index=i, final_return="0.01")

        results = await h.dispatch_backtests(
            n_paths_to_run=3,
            parallel_workers=4,
            run_path=run,
            progress_callback=None,
        )
        # asyncio.gather preserves input ordering regardless of completion order.
        assert [r.path_index for r in results] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_parallel_progress_callback_fires_per_path(self) -> None:
        seen: list[tuple[int, int]] = []

        async def run(i: int) -> MonteCarloPathBacktestResult:
            return _mc_path_result(path_index=i, final_return="0.01")

        def cb(done: int, total: int) -> None:
            seen.append((done, total))

        await h.dispatch_backtests(
            n_paths_to_run=3,
            parallel_workers=2,
            run_path=run,
            progress_callback=cb,
        )
        assert len(seen) == 3
        assert {t for _, t in seen} == {3}
        assert {d for d, _ in seen} == {1, 2, 3}

    @pytest.mark.asyncio
    async def test_parallel_semaphore_limits_concurrency(self) -> None:
        """Semaphore must allow at most ``parallel_workers`` concurrent tasks."""
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def run(i: int) -> MonteCarloPathBacktestResult:
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.01)
            async with lock:
                active -= 1
            return _mc_path_result(path_index=i, final_return="0.01")

        await h.dispatch_backtests(
            n_paths_to_run=6,
            parallel_workers=2,
            run_path=run,
            progress_callback=None,
        )
        assert peak <= 2

    @pytest.mark.asyncio
    async def test_parallel_workers_of_one_falls_back_to_sequential(self) -> None:
        """``parallel_workers`` of 1 must take the sequential branch."""
        calls: list[int] = []

        async def run(i: int) -> MonteCarloPathBacktestResult:
            calls.append(i)
            return _mc_path_result(path_index=i, final_return="0.01")

        results = await h.dispatch_backtests(
            n_paths_to_run=2,
            parallel_workers=1,
            run_path=run,
            progress_callback=None,
        )
        assert [r.path_index for r in results] == [0, 1]
        assert calls == [0, 1]

    @pytest.mark.asyncio
    async def test_zero_paths_returns_empty(self) -> None:
        async def run(i: int) -> MonteCarloPathBacktestResult:
            raise AssertionError("should not be called")

        assert (
            await h.dispatch_backtests(
                n_paths_to_run=0,
                parallel_workers=1,
                run_path=run,
                progress_callback=None,
            )
            == []
        )


# ---------------------------------------------------------------------------
# build_empty_result
# ---------------------------------------------------------------------------


class TestBuildEmptyResult:
    def test_fields_pinned(self) -> None:
        paths = _path_result(n_paths=4)
        cfg = MonteCarloConfig(collect_individual_results=True)
        failed = [
            _mc_path_result(i, "0", success=False)
            for i in range(3)
        ]
        result = h.build_empty_result(
            n_paths_to_run=4,
            n_failed=3,
            results=failed,
            mc_config=cfg,
            paths=paths,
        )
        assert isinstance(result, MonteCarloSimulationResult)
        assert result.n_paths == 4
        assert result.n_successful == 0
        assert result.n_failed == 3
        assert result.return_mean == Decimal("0")
        assert result.return_std == Decimal("0")
        assert result.probability_negative_return == Decimal("1")
        assert result.probability_loss_exceeds_10pct == Decimal("0")
        assert result.probability_gain_exceeds_10pct == Decimal("0")
        # collect_individual_results=True => results kept
        assert result.individual_results == failed
        assert result.price_paths_config == paths.to_dict()
        assert result.monte_carlo_config == cfg.to_dict()

    def test_drops_individual_when_not_collecting(self) -> None:
        paths = _path_result(n_paths=2)
        cfg = MonteCarloConfig(collect_individual_results=False)
        failed = [_mc_path_result(0, "0", success=False)]
        result = h.build_empty_result(
            n_paths_to_run=2,
            n_failed=1,
            results=failed,
            mc_config=cfg,
            paths=paths,
        )
        assert result.individual_results == []


# ---------------------------------------------------------------------------
# aggregate_successful_results
# ---------------------------------------------------------------------------


class TestAggregateSuccessfulResults:
    def test_pinned_aggregation_math(self) -> None:
        paths = _path_result(n_paths=4)
        cfg = MonteCarloConfig(
            collect_individual_results=True,
            drawdown_thresholds=[Decimal("0.10"), Decimal("0.25")],
        )

        # Returns: +10%, -15%, +5%, -25%; DDs: 0.02, 0.18, 0.05, 0.30;
        # Sharpes: 1.0, -0.5, 0.7, None.
        raw = [
            _mc_path_result(0, "0.10", max_drawdown="0.02", sharpe="1.0"),
            _mc_path_result(1, "-0.15", max_drawdown="0.18", sharpe="-0.5"),
            _mc_path_result(2, "0.05", max_drawdown="0.05", sharpe="0.7"),
            _mc_path_result(3, "-0.25", max_drawdown="0.30", sharpe=None),
        ]

        result = h.aggregate_successful_results(
            results=raw,
            successful=raw,
            n_paths_to_run=4,
            n_failed=0,
            mc_config=cfg,
            paths=paths,
        )

        assert result.n_successful == 4
        assert result.n_failed == 0

        # Mean return = (-0.25 + -0.15 + 0.05 + 0.10) / 4 = -0.0625
        assert result.return_mean == Decimal("-0.0625")

        # Sorted returns: -0.25, -0.15, 0.05, 0.10 -> percentile indices:
        #   5   -> 0 -> -0.25
        #   25  -> 0 -> -0.25
        #   50  -> 1 -> -0.15
        #   75  -> 2 -> 0.05
        #   95  -> 2 -> 0.05
        assert result.return_percentile_5th == Decimal("-0.25")
        assert result.return_percentile_25th == Decimal("-0.25")
        assert result.return_percentile_50th == Decimal("-0.15")
        assert result.return_percentile_75th == Decimal("0.05")
        assert result.return_percentile_95th == Decimal("0.05")

        # Drawdowns sorted: 0.02, 0.05, 0.18, 0.30
        assert result.max_drawdown_mean == Decimal("0.1375")
        assert result.max_drawdown_worst == Decimal("0.30")
        assert result.max_drawdown_percentile_95th == Decimal("0.18")

        # Probabilities (strict comparisons, same as original code)
        assert result.probability_negative_return == Decimal("0.5")
        assert result.probability_loss_exceeds_10pct == Decimal("0.5")
        assert result.probability_loss_exceeds_20pct == Decimal("0.25")
        assert result.probability_gain_exceeds_10pct == Decimal("0")

        # Drawdown threshold probabilities: > 0.10 => {0.18, 0.30} => 2/4;
        # > 0.25 => {0.30} => 1/4
        assert result.probability_drawdown_exceeds_threshold["0.10"] == Decimal("0.5")
        assert result.probability_drawdown_exceeds_threshold["0.25"] == Decimal("0.25")

        # Sharpes (3 non-None): 1.0, -0.5, 0.7 -> mean = 0.4
        assert result.sharpe_mean == Decimal("0.4")
        # Std sample of (1.0, -0.5, 0.7): sqrt(0.63) ~= 0.7937...
        assert result.sharpe_std is not None
        assert abs(result.sharpe_std - Decimal("0.79372539331937721")) < Decimal("1e-12")

        # Individual results retained (collect_individual_results=True).
        assert result.individual_results == raw

        # Configs captured.
        assert result.price_paths_config == paths.to_dict()
        assert result.monte_carlo_config == cfg.to_dict()

    def test_sharpes_all_none_yields_none(self) -> None:
        paths = _path_result(n_paths=2)
        cfg = MonteCarloConfig()
        raw = [
            _mc_path_result(0, "0.05", sharpe=None),
            _mc_path_result(1, "0.10", sharpe=None),
        ]
        result = h.aggregate_successful_results(
            results=raw,
            successful=raw,
            n_paths_to_run=2,
            n_failed=0,
            mc_config=cfg,
            paths=paths,
        )
        assert result.sharpe_mean is None
        assert result.sharpe_std is None

    def test_drops_individual_when_not_collecting(self) -> None:
        paths = _path_result(n_paths=2)
        cfg = MonteCarloConfig(collect_individual_results=False)
        raw = [_mc_path_result(0, "0.05"), _mc_path_result(1, "0.10")]
        result = h.aggregate_successful_results(
            results=raw,
            successful=raw,
            n_paths_to_run=2,
            n_failed=0,
            mc_config=cfg,
            paths=paths,
        )
        assert result.individual_results == []

    def test_empty_drawdown_thresholds(self) -> None:
        paths = _path_result(n_paths=2)
        cfg = MonteCarloConfig(drawdown_thresholds=[])
        raw = [_mc_path_result(0, "0.05"), _mc_path_result(1, "0.10")]
        result = h.aggregate_successful_results(
            results=raw,
            successful=raw,
            n_paths_to_run=2,
            n_failed=0,
            mc_config=cfg,
            paths=paths,
        )
        assert result.probability_drawdown_exceeds_threshold == {}


# ---------------------------------------------------------------------------
# Private stats helpers (targeted branch coverage)
# ---------------------------------------------------------------------------


class TestPrivateStatsHelpers:
    def test_return_statistics(self) -> None:
        raw = [
            _mc_path_result(0, "0.10"),
            _mc_path_result(1, "-0.05"),
            _mc_path_result(2, "0.02"),
        ]
        returns, mean, std = h._return_statistics(raw)
        assert returns == sorted(Decimal(v) for v in ["0.10", "-0.05", "0.02"])
        assert mean == (Decimal("0.10") - Decimal("0.05") + Decimal("0.02")) / Decimal("3")
        assert std > Decimal("0")

    def test_drawdown_statistics(self) -> None:
        raw = [
            _mc_path_result(0, "0", max_drawdown="0.05"),
            _mc_path_result(1, "0", max_drawdown="0.15"),
            _mc_path_result(2, "0", max_drawdown="0.10"),
        ]
        sorted_dd, mean = h._drawdown_statistics(raw)
        assert sorted_dd == [Decimal("0.05"), Decimal("0.10"), Decimal("0.15")]
        assert mean == Decimal("0.10")

    def test_return_probabilities_all_positive(self) -> None:
        raw = [_mc_path_result(i, "0.20") for i in range(3)]
        p_neg, p_loss10, p_loss20, p_gain10 = h._return_probabilities(raw)
        assert p_neg == Decimal("0")
        assert p_loss10 == Decimal("0")
        assert p_loss20 == Decimal("0")
        assert p_gain10 == Decimal("1")

    def test_return_probabilities_all_catastrophic(self) -> None:
        raw = [_mc_path_result(i, "-0.50") for i in range(4)]
        p_neg, p_loss10, p_loss20, p_gain10 = h._return_probabilities(raw)
        assert p_neg == Decimal("1")
        assert p_loss10 == Decimal("1")
        assert p_loss20 == Decimal("1")
        assert p_gain10 == Decimal("0")

    def test_drawdown_threshold_probabilities_no_exceedances(self) -> None:
        raw = [_mc_path_result(i, "0", max_drawdown="0.01") for i in range(5)]
        probs = h._drawdown_threshold_probabilities(raw, [Decimal("0.05"), Decimal("0.10")])
        assert probs == {"0.05": Decimal("0"), "0.10": Decimal("0")}

    def test_sharpe_statistics_empty_and_nonempty(self) -> None:
        empty = [_mc_path_result(0, "0", sharpe=None), _mc_path_result(1, "0", sharpe=None)]
        assert h._sharpe_statistics(empty) == (None, None)

        mixed = [
            _mc_path_result(0, "0", sharpe="1.0"),
            _mc_path_result(1, "0", sharpe=None),
            _mc_path_result(2, "0", sharpe="2.0"),
        ]
        mean, std = h._sharpe_statistics(mixed)
        assert mean == Decimal("1.5")
        assert std is not None
        # Std sample of (1.0, 2.0) = sqrt(0.5) ~= 0.7071...
        assert abs(std - Decimal("0.70710678118654752")) < Decimal("1e-12")
