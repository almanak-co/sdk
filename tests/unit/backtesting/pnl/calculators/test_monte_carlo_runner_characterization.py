"""Characterization tests for ``run_monte_carlo``.

These tests lock the observable behaviour of
``almanak.framework.backtesting.pnl.calculators.monte_carlo_runner.run_monte_carlo``
before any refactor. They cover:

* Sequential and parallel execution branches
* Progress callback emission in both modes
* Aggregation math (mean / std / percentiles / probabilities / drawdown
  thresholds / Sharpe) pinned to exact ``Decimal`` values
* All-failed early return path
* Partial failure path (a subset of paths raise)
* ``n_paths`` larger than available truncates + emits warning
* Default ``MonteCarloConfig`` / ``fee_models`` / ``slippage_models`` wiring
* ``collect_individual_results=False`` drops per-path results
* ``SimulatedPricePathProvider`` passed into backtester matches
  ``(base_token, quote_token, start_time, interval_seconds)``
* ``run_monte_carlo_sync`` delegates correctly

The tests install fake ``PnLBacktester`` / ``DefaultFeeModel`` /
``DefaultSlippageModel`` classes into the ``pnl.engine`` module so no real
backtester is constructed. That is essential because ``run_monte_carlo``
imports them lazily inside the function.
"""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult
from almanak.framework.backtesting.pnl.calculators.monte_carlo import (
    PathGenerationMethod,
    PricePathResult,
)
from almanak.framework.backtesting.pnl.calculators import monte_carlo_runner as runner_mod
from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
    MonteCarloConfig,
    MonteCarloPathBacktestResult,
    MonteCarloSimulationResult,
    SimulatedPricePathProvider,
    _calculate_percentile,
    _calculate_std,
    _run_single_path_backtest,
    run_monte_carlo,
    run_monte_carlo_sync,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeMetrics:
    """Stand-in for ``BacktestMetrics`` from the view of ``run_monte_carlo``."""

    max_drawdown_pct: Decimal = Decimal("0")
    sharpe_ratio: Decimal | None = None
    total_trades: int = 0


@dataclass
class _FakeBacktestResult:
    initial_portfolio_value_usd: Decimal
    final_capital_usd: Decimal
    metrics: _FakeMetrics


class _FakeBacktester:
    """Captures construction kwargs + returns a pre-seeded result per path.

    ``_results`` is a class-level list indexed by ``path_index`` (from the
    data provider's price path identity) — but because tests control the
    price-paths list, we use a module-level dispatcher to look up by path
    index instead. Here we only need to construct cleanly and return the
    result the test wired via ``_result_for(path_key)``.
    """

    _result_by_key: dict[tuple, _FakeBacktestResult | Exception] = {}
    constructed: list[dict[str, Any]] = []

    def __init__(
        self,
        *,
        data_provider: SimulatedPricePathProvider,
        fee_models: dict[str, Any],
        slippage_models: dict[str, Any],
    ) -> None:
        self.data_provider = data_provider
        self.fee_models = fee_models
        self.slippage_models = slippage_models
        type(self).constructed.append(
            {
                "data_provider": data_provider,
                "fee_models": fee_models,
                "slippage_models": slippage_models,
            }
        )

    async def backtest(
        self,
        strategy: Any,
        config: PnLBacktestConfig,
    ) -> _FakeBacktestResult:
        # Key by the first price of the path (unique in test fixtures).
        first_price = self.data_provider.price_path[0]
        key = (first_price,)
        entry = type(self)._result_by_key.get(key)
        if entry is None:
            raise AssertionError(f"No fake result configured for key={key}")
        if isinstance(entry, Exception):
            raise entry
        return entry


class _FakeFeeModel:
    pass


class _FakeSlippageModel:
    pass


def _install_fake_engine_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a fake ``almanak.framework.backtesting.pnl.engine`` module.

    ``run_monte_carlo`` does ``from almanak.framework.backtesting.pnl.engine
    import DefaultFeeModel, DefaultSlippageModel, PnLBacktester`` at call time.
    We pre-seed ``sys.modules`` so that import resolves to our fakes.
    """

    fake = types.ModuleType("almanak.framework.backtesting.pnl.engine")
    fake.PnLBacktester = _FakeBacktester  # type: ignore[attr-defined]
    fake.DefaultFeeModel = _FakeFeeModel  # type: ignore[attr-defined]
    fake.DefaultSlippageModel = _FakeSlippageModel  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "almanak.framework.backtesting.pnl.engine", fake)

    # Reset per-test state on the fake backtester.
    _FakeBacktester._result_by_key = {}
    _FakeBacktester.constructed = []


def _make_paths(price_lists: list[list[str]], *, seed: int | None = 7) -> PricePathResult:
    paths = [[Decimal(p) for p in row] for row in price_lists]
    start_price = paths[0][0] if paths else Decimal("0")
    return PricePathResult(
        paths=paths,
        n_paths=len(paths),
        n_steps=len(paths[0]) - 1 if paths else 0,
        method=PathGenerationMethod.GBM,
        drift=Decimal("0.05"),
        volatility=Decimal("0.2"),
        start_price=start_price,
        dt=Decimal("1") / Decimal("252"),
        seed=seed,
    )


def _make_backtest_config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 2),
        interval_seconds=3600,
        token_funding=_pnl_token_funding("10000"),
    )


def _configure_result(first_price: str, result: _FakeBacktestResult | Exception) -> None:
    _FakeBacktester._result_by_key[(Decimal(first_price),)] = result


# ---------------------------------------------------------------------------
# Helper function tests (pure, no engine needed)
# ---------------------------------------------------------------------------


class TestCalculatePercentile:
    def test_empty_returns_zero(self) -> None:
        assert _calculate_percentile([], 50) == Decimal("0")

    def test_basic_percentiles(self) -> None:
        values = [Decimal(str(i)) for i in range(1, 11)]  # 1..10 sorted
        assert _calculate_percentile(values, 0) == Decimal("1")
        assert _calculate_percentile(values, 50) == Decimal("5")
        assert _calculate_percentile(values, 95) == Decimal("9")
        assert _calculate_percentile(values, 100) == Decimal("10")

    def test_clamps_index(self) -> None:
        values = [Decimal("1"), Decimal("2")]
        # Large percentile clamps to final index
        assert _calculate_percentile(values, 200) == Decimal("2")


class TestCalculateStd:
    def test_fewer_than_two_returns_zero(self) -> None:
        assert _calculate_std([], Decimal("0")) == Decimal("0")
        assert _calculate_std([Decimal("1")], Decimal("1")) == Decimal("0")

    def test_known_std(self) -> None:
        values = [Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4"), Decimal("5")]
        mean = Decimal("3")
        result = _calculate_std(values, mean)
        # Sample std of 1..5 = sqrt(2.5) ~= 1.58113883...
        assert abs(result - Decimal("1.58113883008418966")) < Decimal("1e-12")

    def test_zero_variance(self) -> None:
        values = [Decimal("3"), Decimal("3"), Decimal("3")]
        assert _calculate_std(values, Decimal("3")) == Decimal("0")


# ---------------------------------------------------------------------------
# SimulatedPricePathProvider tests
# ---------------------------------------------------------------------------


class TestSimulatedPricePathProvider:
    @pytest.mark.asyncio
    async def test_get_price_quote_is_one(self) -> None:
        provider = SimulatedPricePathProvider(
            price_path=[Decimal("100"), Decimal("110")],
            base_token="weth",
            quote_token="usdc",
            start_time=datetime(2024, 1, 1),
            interval_seconds=3600,
        )
        assert await provider.get_price("USDC", datetime(2024, 1, 1)) == Decimal("1")

    @pytest.mark.asyncio
    async def test_get_price_base_step_indexing(self) -> None:
        provider = SimulatedPricePathProvider(
            price_path=[Decimal("100"), Decimal("110"), Decimal("120")],
            base_token="WETH",
            quote_token="USDC",
            start_time=datetime(2024, 1, 1),
            interval_seconds=3600,
        )
        # t=0 -> index 0
        assert await provider.get_price("weth", datetime(2024, 1, 1)) == Decimal("100")
        # t=1h -> index 1
        assert await provider.get_price("WETH", datetime(2024, 1, 1, 1)) == Decimal("110")
        # t=2h -> index 2
        assert await provider.get_price("WETH", datetime(2024, 1, 1, 2)) == Decimal("120")
        # Past end clamps to last
        assert await provider.get_price("WETH", datetime(2024, 1, 2)) == Decimal("120")

    @pytest.mark.asyncio
    async def test_get_price_unknown_token_raises(self) -> None:
        provider = SimulatedPricePathProvider(price_path=[Decimal("1")])
        with pytest.raises(ValueError, match="Price not available"):
            await provider.get_price("UNKNOWN", datetime(2024, 1, 1))

    @pytest.mark.asyncio
    async def test_ohlcv_returns_empty(self) -> None:
        provider = SimulatedPricePathProvider(price_path=[Decimal("1")])
        result = await provider.get_ohlcv(
            "WETH", datetime(2024, 1, 1), datetime(2024, 1, 2), 3600
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_iterate_yields_market_states(self) -> None:
        provider = SimulatedPricePathProvider(
            price_path=[Decimal("100"), Decimal("110"), Decimal("120")],
            base_token="WETH",
            quote_token="USDC",
            start_time=datetime(2024, 1, 1),
            interval_seconds=3600,
        )
        cfg = HistoricalDataConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 1, 2),
            interval_seconds=3600,
            chains=["base"],
        )
        emitted = [(ts, ms) async for ts, ms in provider.iterate(cfg)]
        # 3 steps: t0, t0+1h, t0+2h.
        assert len(emitted) == 3
        prices = [ms.prices for _, ms in emitted]
        assert prices[0]["WETH"] == Decimal("100")
        assert prices[1]["WETH"] == Decimal("110")
        assert prices[2]["WETH"] == Decimal("120")
        # Chain honoured from config
        assert emitted[0][1].chain == "base"

    @pytest.mark.asyncio
    async def test_iterate_default_chain_when_empty(self) -> None:
        provider = SimulatedPricePathProvider(
            price_path=[Decimal("100")],
            start_time=datetime(2024, 1, 1),
            interval_seconds=3600,
        )
        cfg = HistoricalDataConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 1, 1),
            interval_seconds=3600,
            chains=["ethereum"],
        )
        # We can't construct HistoricalDataConfig with empty chains (validator),
        # so assert the default ``arbitrum`` fallback path via monkeypatching
        # ``config.chains`` after construction.
        cfg.chains = []  # type: ignore[assignment]
        emitted = [(ts, ms) async for ts, ms in provider.iterate(cfg)]
        assert emitted[0][1].chain == "arbitrum"


# ---------------------------------------------------------------------------
# _run_single_path_backtest tests
# ---------------------------------------------------------------------------


class TestRunSinglePathBacktest:
    @pytest.mark.asyncio
    async def test_success_extracts_metrics(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result(
            "100",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("10000"),
                final_capital_usd=Decimal("11000"),
                metrics=_FakeMetrics(
                    max_drawdown_pct=Decimal("0.05"),
                    sharpe_ratio=Decimal("1.2"),
                    total_trades=7,
                ),
            ),
        )

        result = await _run_single_path_backtest(
            strategy=object(),
            price_path=[Decimal("100"), Decimal("110")],
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(collect_individual_results=True),
            path_index=3,
            backtester_class=_FakeBacktester,
            fee_models={"default": _FakeFeeModel()},
            slippage_models={"default": _FakeSlippageModel()},
        )

        assert result.success is True
        assert result.path_index == 3
        assert result.final_value_usd == Decimal("11000")
        assert result.final_return == Decimal("0.1")
        assert result.max_drawdown == Decimal("0.05")
        assert result.sharpe_ratio == Decimal("1.2")
        assert result.total_trades == 7
        # collect_individual_results=True keeps metrics
        assert result.metrics is not None

    @pytest.mark.asyncio
    async def test_collect_results_false_drops_metrics(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result(
            "100",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("10000"),
                final_capital_usd=Decimal("10500"),
                metrics=_FakeMetrics(),
            ),
        )
        result = await _run_single_path_backtest(
            strategy=object(),
            price_path=[Decimal("100")],
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(collect_individual_results=False),
            path_index=0,
            backtester_class=_FakeBacktester,
            fee_models={},
            slippage_models={},
        )
        assert result.metrics is None

    @pytest.mark.asyncio
    async def test_zero_initial_capital_yields_zero_return(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result(
            "100",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("0"),
                final_capital_usd=Decimal("500"),
                metrics=_FakeMetrics(),
            ),
        )
        result = await _run_single_path_backtest(
            strategy=object(),
            price_path=[Decimal("100")],
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(),
            path_index=0,
            backtester_class=_FakeBacktester,
            fee_models={},
            slippage_models={},
        )
        assert result.success is True
        assert result.final_return == Decimal("0")

    @pytest.mark.asyncio
    async def test_exception_recorded_as_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result("100", RuntimeError("boom"))
        result = await _run_single_path_backtest(
            strategy=object(),
            price_path=[Decimal("100")],
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(),
            path_index=2,
            backtester_class=_FakeBacktester,
            fee_models={},
            slippage_models={},
        )
        assert result.success is False
        assert result.error == "boom"
        assert result.path_index == 2
        assert result.final_return == Decimal("0")
        assert result.final_value_usd == Decimal("0")
        assert result.max_drawdown == Decimal("0")


# ---------------------------------------------------------------------------
# run_monte_carlo end-to-end tests (with fake engine)
# ---------------------------------------------------------------------------


def _results_for(
    pairs: list[tuple[str, str, str]],
    *,
    max_drawdowns: list[str] | None = None,
    sharpes: list[str | None] | None = None,
) -> None:
    """Helper: wire fake backtester results for paths whose first price is ``pairs[i][0]``.

    ``pairs`` is a list of ``(first_price, initial_capital, final_capital)`` triples.
    ``max_drawdowns`` / ``sharpes``, if given, must be the same length.
    """
    for i, (first, initial, final) in enumerate(pairs):
        md = Decimal(max_drawdowns[i]) if max_drawdowns else Decimal("0.05")
        sr_raw = sharpes[i] if sharpes else "1.0"
        sr = Decimal(sr_raw) if sr_raw is not None else None
        _configure_result(
            first,
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal(initial),
                final_capital_usd=Decimal(final),
                metrics=_FakeMetrics(
                    max_drawdown_pct=md,
                    sharpe_ratio=sr,
                    total_trades=10 + i,
                ),
            ),
        )


class TestRunMonteCarloSequential:
    @pytest.mark.asyncio
    async def test_aggregation_math_pinned(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Lock exact aggregation values for a hand-picked 4-path run."""
        _install_fake_engine_module(monkeypatch)

        # Returns: +10%, -15%, +5%, -25%.
        _results_for(
            [
                ("100", "10000", "11000"),  # +10%
                ("200", "10000", "8500"),   # -15%
                ("300", "10000", "10500"),  # +5%
                ("400", "10000", "7500"),   # -25%
            ],
            max_drawdowns=["0.02", "0.18", "0.05", "0.30"],
            sharpes=["1.0", "-0.5", "0.7", None],
        )
        paths = _make_paths(
            [
                ["100", "110"],
                ["200", "190"],
                ["300", "310"],
                ["400", "380"],
            ]
        )

        mc_cfg = MonteCarloConfig(
            n_paths=4,
            parallel_workers=1,  # sequential branch
            drawdown_thresholds=[Decimal("0.10"), Decimal("0.25")],
        )
        result = await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=mc_cfg,
        )

        # Counts
        assert result.n_paths == 4
        assert result.n_successful == 4
        assert result.n_failed == 0

        # Return mean = (0.1 + (-0.15) + 0.05 + (-0.25)) / 4 = -0.0625
        assert result.return_mean == Decimal("-0.0625")
        # Sorted returns: -0.25, -0.15, 0.05, 0.10
        assert result.return_percentile_5th == Decimal("-0.25")  # idx 0
        assert result.return_percentile_25th == Decimal("-0.25")  # idx 0
        assert result.return_percentile_50th == Decimal("-0.15")  # idx 1
        assert result.return_percentile_75th == Decimal("0.05")   # idx 2
        assert result.return_percentile_95th == Decimal("0.05")   # idx 2 (int((95/100)*3)=2)

        # Drawdowns sorted: 0.02, 0.05, 0.18, 0.30; mean = 0.1375; worst = 0.30
        assert result.max_drawdown_mean == Decimal("0.1375")
        assert result.max_drawdown_worst == Decimal("0.30")
        assert result.max_drawdown_percentile_95th == Decimal("0.18")

        # Probabilities: paths with final_return < 0 => 2 out of 4 => 0.5
        assert result.probability_negative_return == Decimal("0.5")
        # final_return < -0.1 => 2 out of 4 (-0.15, -0.25)
        assert result.probability_loss_exceeds_10pct == Decimal("0.5")
        # final_return < -0.2 => 1 out of 4 (-0.25)
        assert result.probability_loss_exceeds_20pct == Decimal("0.25")
        # final_return > 0.1 => 0 of 4 (strict >)
        assert result.probability_gain_exceeds_10pct == Decimal("0")

        # Drawdown threshold probabilities: > 0.10 => paths with dd 0.18, 0.30 => 2/4
        assert result.probability_drawdown_exceeds_threshold["0.10"] == Decimal("0.5")
        # > 0.25 => only 0.30 => 1/4
        assert result.probability_drawdown_exceeds_threshold["0.25"] == Decimal("0.25")

        # Sharpe: only 3 non-None: 1.0, -0.5, 0.7 => mean = 0.4
        assert result.sharpe_mean == Decimal("0.4")
        # std of (1.0, -0.5, 0.7) with mean 0.4 -> sqrt( ((0.6)^2 + (-0.9)^2 + (0.3)^2)/2 )
        # = sqrt((0.36+0.81+0.09)/2) = sqrt(0.63) ~= 0.7937...
        assert result.sharpe_std is not None
        assert abs(result.sharpe_std - Decimal("0.79372539331937721")) < Decimal("1e-12")

        # Individual results collected by default
        assert len(result.individual_results) == 4

        # Configs stored
        assert result.price_paths_config == paths.to_dict()
        assert result.monte_carlo_config == mc_cfg.to_dict()

    @pytest.mark.asyncio
    async def test_progress_callback_fires_sequential(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _results_for(
            [("100", "10000", "11000"), ("200", "10000", "9000")],
        )
        seen: list[tuple[int, int]] = []

        def cb(completed: int, total: int) -> None:
            seen.append((completed, total))

        paths = _make_paths([["100", "110"], ["200", "190"]])
        await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(
                n_paths=2, parallel_workers=1, progress_callback=cb
            ),
        )
        assert seen == [(1, 2), (2, 2)]


class TestRunMonteCarloParallel:
    @pytest.mark.asyncio
    async def test_parallel_produces_same_aggregates_as_sequential(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _results_for(
            [
                ("100", "10000", "11000"),
                ("200", "10000", "9500"),
                ("300", "10000", "10200"),
            ],
            max_drawdowns=["0.03", "0.12", "0.06"],
            sharpes=["1.1", "0.2", "0.8"],
        )
        paths = _make_paths(
            [["100", "110"], ["200", "190"], ["300", "305"]]
        )
        mc_cfg = MonteCarloConfig(
            n_paths=3,
            parallel_workers=4,
            drawdown_thresholds=[Decimal("0.10")],
        )
        result = await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=mc_cfg,
        )

        assert result.n_successful == 3
        # returns: 0.10, -0.05, 0.02 -> mean = 0.0233... Decimal division
        expected_mean = (
            Decimal("0.10") + Decimal("-0.05") + Decimal("0.02")
        ) / Decimal("3")
        assert result.return_mean == expected_mean
        # DD > 0.10 => only 0.12 => 1/3
        assert result.probability_drawdown_exceeds_threshold["0.10"] == Decimal("1") / Decimal("3")

    @pytest.mark.asyncio
    async def test_parallel_progress_callback_fires_each_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _results_for(
            [
                ("100", "10000", "11000"),
                ("200", "10000", "9000"),
                ("300", "10000", "10500"),
            ]
        )
        seen: list[tuple[int, int]] = []

        def cb(done: int, total: int) -> None:
            seen.append((done, total))

        paths = _make_paths(
            [["100", "110"], ["200", "190"], ["300", "305"]]
        )
        await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(
                n_paths=3, parallel_workers=2, progress_callback=cb
            ),
        )
        # Each of the 3 paths fires exactly once.
        assert len(seen) == 3
        assert {total for _, total in seen} == {3}
        assert {done for done, _ in seen} == {1, 2, 3}


class TestRunMonteCarloBranches:
    @pytest.mark.asyncio
    async def test_all_failed_returns_empty_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result("100", RuntimeError("x"))
        _configure_result("200", ValueError("y"))
        paths = _make_paths([["100", "110"], ["200", "190"]])

        result = await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(n_paths=2, parallel_workers=1),
        )

        assert result.n_paths == 2
        assert result.n_successful == 0
        assert result.n_failed == 2
        assert result.return_mean == Decimal("0")
        assert result.return_std == Decimal("0")
        assert result.probability_negative_return == Decimal("1")
        assert result.probability_loss_exceeds_10pct == Decimal("0")
        assert result.probability_loss_exceeds_20pct == Decimal("0")
        assert result.probability_gain_exceeds_10pct == Decimal("0")
        assert result.sharpe_mean is None
        assert result.sharpe_std is None
        # Individual results still collected (collect_individual_results=True default)
        assert len(result.individual_results) == 2

    @pytest.mark.asyncio
    async def test_partial_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result("100", RuntimeError("bad"))
        _configure_result(
            "200",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("10000"),
                final_capital_usd=Decimal("11000"),
                metrics=_FakeMetrics(max_drawdown_pct=Decimal("0.04"), sharpe_ratio=Decimal("1.5")),
            ),
        )
        paths = _make_paths([["100", "110"], ["200", "220"]])

        result = await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(n_paths=2, parallel_workers=1),
        )
        assert result.n_successful == 1
        assert result.n_failed == 1
        assert result.return_mean == Decimal("0.1")
        # One successful path -> std is 0
        assert result.return_std == Decimal("0")

    @pytest.mark.asyncio
    async def test_requested_more_paths_than_available_truncates_and_warns(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _results_for([("100", "10000", "10500"), ("200", "10000", "11000")])
        paths = _make_paths([["100", "110"], ["200", "220"]])

        with caplog.at_level("WARNING"):
            result = await run_monte_carlo(
                strategy=object(),
                paths=paths,
                backtest_config=_make_backtest_config(),
                mc_config=MonteCarloConfig(n_paths=10, parallel_workers=1),
            )

        assert result.n_paths == 2  # clamped to available
        assert any("only 2 available" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_defaults_when_mc_config_and_models_are_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result(
            "100",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("10000"),
                final_capital_usd=Decimal("10500"),
                metrics=_FakeMetrics(),
            ),
        )
        paths = _make_paths([["100", "110"]])

        # mc_config=None means default MonteCarloConfig() which asks for 1000 paths
        # but we only supply 1, so it clamps to 1 - exactly the behaviour we pin.
        result = await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
        )
        assert result.n_paths == 1
        # Default fee/slippage models were constructed and passed through
        assert _FakeBacktester.constructed, "backtester must have been constructed"
        assert "default" in _FakeBacktester.constructed[0]["fee_models"]
        assert "default" in _FakeBacktester.constructed[0]["slippage_models"]
        assert isinstance(
            _FakeBacktester.constructed[0]["fee_models"]["default"], _FakeFeeModel
        )
        assert isinstance(
            _FakeBacktester.constructed[0]["slippage_models"]["default"],
            _FakeSlippageModel,
        )

    @pytest.mark.asyncio
    async def test_collect_individual_results_false_drops_list(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result(
            "100",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("10000"),
                final_capital_usd=Decimal("11000"),
                metrics=_FakeMetrics(),
            ),
        )
        paths = _make_paths([["100", "110"]])
        result = await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(
                n_paths=1, parallel_workers=1, collect_individual_results=False
            ),
        )
        assert result.individual_results == []

    @pytest.mark.asyncio
    async def test_data_provider_uses_backtest_config_timing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result(
            "100",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("10000"),
                final_capital_usd=Decimal("10000"),
                metrics=_FakeMetrics(),
            ),
        )
        cfg = PnLBacktestConfig(
            start_time=datetime(2024, 6, 15, 12, 0, 0),
            end_time=datetime(2024, 6, 16),
            interval_seconds=1800,
            token_funding=_pnl_token_funding("10000"),
        )
        paths = _make_paths([["100", "110"]])
        await run_monte_carlo(
            strategy=object(),
            paths=paths,
            backtest_config=cfg,
            mc_config=MonteCarloConfig(
                n_paths=1, parallel_workers=1, base_token="wBTC", quote_token="DAI"
            ),
        )
        dp = _FakeBacktester.constructed[0]["data_provider"]
        assert isinstance(dp, SimulatedPricePathProvider)
        assert dp.start_time == datetime(2024, 6, 15, 12, 0, 0)
        assert dp.interval_seconds == 1800
        assert dp.base_token == "WBTC"
        assert dp.quote_token == "DAI"


# ---------------------------------------------------------------------------
# run_monte_carlo_sync
# ---------------------------------------------------------------------------


class TestRunMonteCarloSync:
    def test_sync_wrapper_returns_same_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_engine_module(monkeypatch)
        _configure_result(
            "100",
            _FakeBacktestResult(
                initial_portfolio_value_usd=Decimal("10000"),
                final_capital_usd=Decimal("12000"),
                metrics=_FakeMetrics(),
            ),
        )
        paths = _make_paths([["100", "120"]])
        result = run_monte_carlo_sync(
            strategy=object(),
            paths=paths,
            backtest_config=_make_backtest_config(),
            mc_config=MonteCarloConfig(n_paths=1, parallel_workers=1),
        )
        assert isinstance(result, MonteCarloSimulationResult)
        assert result.n_successful == 1
        assert result.return_mean == Decimal("0.2")
