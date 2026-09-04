"""Pre-materialization backtest window feasibility gate (ALM-3385)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import HistoricalDataCapability, MarketState, token_ref_display
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.backtesting.pnl.feasibility import (
    ENV_BUDGET_SECONDS,
    ENV_OHLCV_PAGE_LATENCY_SECONDS,
    ENV_PAGE_LATENCY_SECONDS,
    ENV_SAFETY_MARGIN,
    ENV_TICKS_PER_SECOND,
    BacktestWindowTooLongError,
    ExactPoolOHLCVCost,
    FeasibilityKnobs,
    enforce_window_feasibility,
    estimate_config_cost,
    estimate_cost,
    expected_ohlcv_pages,
    expected_pages,
    expected_points,
    max_feasible_ticks,
)
from almanak.framework.backtesting.pnl.providers import snapshot_pool_state
from almanak.framework.backtesting.pnl.providers.snapshot_pool_analytics import HistoricalPoolAnalyticsTarget
from almanak.framework.backtesting.pnl.providers.snapshot_pool_ohlcv import HistoricalPoolOHLCVTarget
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import (
    _MAX_POINTS_PER_REQUEST,
    HistoricalPoolStatePoint,
    HistoricalPoolStateTarget,
)
from almanak.framework.data.timeframes import OHLCVTimeframe
from tests.backtesting_funding import pnl_token_funding

_POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
_TOKEN0 = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_TOKEN1 = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_HOUR = 3600
_DAY = 86400


def _knobs(**overrides: float) -> FeasibilityKnobs:
    defaults: dict[str, float] = {
        "page_latency_seconds": 20.0,
        "ticks_per_second": 3.5,
        "budget_seconds": 900.0,
        "safety_margin": 0.8,
    }
    defaults.update(overrides)
    return FeasibilityKnobs(**defaults)


def _config(days: float, *, interval_seconds: int = _HOUR) -> PnLBacktestConfig:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=days),
        interval_seconds=interval_seconds,
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        token_funding=pnl_token_funding(Decimal("100"), chain="arbitrum"),
        include_gas_costs=False,
        preflight_validation=False,
    )


class _Strategy:
    deployment_id = "feasibility_probe"
    config = {
        "chain": "arbitrum",
        "token_funding": pnl_token_funding(Decimal("100"), chain="arbitrum"),
    }

    backtest_pool_state_targets = (HistoricalPoolStateTarget("arbitrum", "uniswap_v3", _POOL, (_TOKEN0, _TOKEN1), 500),)

    def decide(self, market: Any) -> None:
        return None


_OHLCV_TARGET = HistoricalPoolOHLCVTarget(
    "arbitrum",
    "uniswap_v3",
    _POOL,
    _TOKEN0,
    _TOKEN1,
    OHLCVTimeframe.FIFTEEN_MINUTES,
    100,
)
_OHLCV_COST = ExactPoolOHLCVCost(
    lane_key=_OHLCV_TARGET.manifest_key,
    timeframe=_OHLCV_TARGET.timeframe,
    lookback_candles=_OHLCV_TARGET.lookback_candles,
)
_ANALYTICS_TARGET = HistoricalPoolAnalyticsTarget("arbitrum", "uniswap_v3", _POOL)


class _ReadinessProvider:
    provider_name = "feasibility_fixture"
    historical_capability = HistoricalDataCapability.FULL

    async def get_price(self, token: Any, timestamp: datetime) -> Decimal:
        return Decimal("1") if "USDC" in token_ref_display(token).upper() else Decimal("2000")

    async def iterate(self, config: Any):
        for index in range(2):
            timestamp = config.start_time + timedelta(hours=index)
            prices = {token: await self.get_price(token, timestamp) for token in config.tokens}
            yield timestamp, MarketState(timestamp=timestamp, prices=prices, chain="arbitrum")


def _spy_fetcher(calls: list[dict[str, Any]]):
    def fetch(**kwargs: Any) -> list[HistoricalPoolStatePoint]:
        calls.append(kwargs)
        samples = range(kwargs["start_ts"], kwargs["end_ts"] + 1, kwargs["interval_secs"])
        return [
            HistoricalPoolStatePoint(
                sample,
                100 + index,
                2**96,
                0,
                9_000,
                _TOKEN0,
                _TOKEN1,
                18,
                6,
                500,
                2 * 10**18,
                4 * 10**6,
                "on_chain_archive",
            )
            for index, sample in enumerate(samples)
        ]

    return fetch


# --- page / cost math -------------------------------------------------------


@pytest.mark.parametrize(
    ("duration_seconds", "interval_seconds", "points"),
    [
        (0, _HOUR, 1),
        (_HOUR, _HOUR, 2),
        (_DAY, _HOUR, 25),
        (180 * _DAY, _HOUR, 4321),
        (180 * _DAY, 4 * _HOUR, 1081),
    ],
)
def test_expected_points_mirrors_materialize_history_grid(
    duration_seconds: int, interval_seconds: int, points: int
) -> None:
    start = 1_000
    grid = range(start, start + duration_seconds + 1, interval_seconds)
    assert expected_points(duration_seconds, interval_seconds) == points == len(grid)


def test_expected_points_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError):
        expected_points(_DAY, 0)


@pytest.mark.parametrize(
    ("points", "pages"),
    [(0, 0), (1, 1), (_MAX_POINTS_PER_REQUEST, 1), (_MAX_POINTS_PER_REQUEST + 1, 2), (4321, 34)],
)
def test_expected_pages_uses_provider_page_size(points: int, pages: int) -> None:
    assert expected_pages(points) == pages


def test_estimate_cost_sums_paged_data_and_simulation() -> None:
    estimate = estimate_cost(
        duration_seconds=180 * _DAY,
        interval_seconds=_HOUR,
        target_count=1,
        knobs=_knobs(),
    )

    assert estimate.points_per_target == 4321
    assert estimate.pages_per_target == 34
    assert estimate.ticks == 4320
    assert estimate.data_seconds == pytest.approx(34 * 20.0)
    assert estimate.simulation_seconds == pytest.approx(4320 / 3.5)
    assert estimate.total_seconds == pytest.approx(680 + 4320 / 3.5)
    assert estimate.usable_budget_seconds == pytest.approx(720.0)
    assert not estimate.feasible


def test_estimate_data_cost_scales_with_target_count() -> None:
    knobs = _knobs()
    single = estimate_cost(duration_seconds=7 * _DAY, interval_seconds=_HOUR, target_count=1, knobs=knobs)
    triple = estimate_cost(duration_seconds=7 * _DAY, interval_seconds=_HOUR, target_count=3, knobs=knobs)

    assert triple.data_seconds == pytest.approx(single.data_seconds * 3)
    assert triple.simulation_seconds == pytest.approx(single.simulation_seconds)


def test_coarser_interval_reduces_pages_and_ticks() -> None:
    knobs = _knobs()
    hourly = estimate_cost(duration_seconds=30 * _DAY, interval_seconds=_HOUR, target_count=1, knobs=knobs)
    daily = estimate_cost(duration_seconds=30 * _DAY, interval_seconds=_DAY, target_count=1, knobs=knobs)

    assert daily.pages_per_target < hourly.pages_per_target
    assert daily.total_seconds < hourly.total_seconds


def test_exact_pool_ohlcv_cost_uses_bounded_pages_not_ticks() -> None:
    duration = 32 * _DAY
    costs = (_OHLCV_COST,)

    estimate = estimate_cost(
        duration_seconds=duration,
        interval_seconds=15 * 60,
        target_count=1,
        exact_pool_ohlcv_costs=costs,
        knobs=_knobs(),
    )

    assert expected_ohlcv_pages(duration, costs) == 4
    assert estimate.exact_pool_ohlcv_pages == 4
    assert estimate.exact_pool_ohlcv_targets == 1
    assert estimate.exact_pool_ohlcv_data_seconds == 40


def test_config_cost_matches_floor_aligned_materialization_at_page_boundary() -> None:
    duration = (30_000 - _OHLCV_TARGET.lookback_candles) * _OHLCV_TARGET.timeframe.seconds + 1
    config = _config(duration / _DAY, interval_seconds=15 * 60)

    estimate = estimate_config_cost(
        config,
        target_count=1,
        exact_pool_ohlcv_costs=(_OHLCV_COST,),
        knobs=_knobs(budget_seconds=100_000),
    )

    assert expected_ohlcv_pages(duration, (_OHLCV_COST,)) == 31
    assert estimate.exact_pool_ohlcv_pages == 30
    assert estimate.feasible


# --- env knobs --------------------------------------------------------------


def test_knobs_default_to_documented_values(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        ENV_PAGE_LATENCY_SECONDS,
        ENV_OHLCV_PAGE_LATENCY_SECONDS,
        ENV_TICKS_PER_SECOND,
        ENV_BUDGET_SECONDS,
        ENV_SAFETY_MARGIN,
    ):
        monkeypatch.delenv(name, raising=False)

    knobs = FeasibilityKnobs.from_env()

    assert knobs.page_latency_seconds == 20.0
    assert knobs.ohlcv_page_latency_seconds == 10.0
    assert knobs.ticks_per_second == 3.5
    assert knobs.budget_seconds == 7200.0
    assert knobs.safety_margin == 0.8


def test_env_overrides_every_knob(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_PAGE_LATENCY_SECONDS, "5")
    monkeypatch.setenv(ENV_OHLCV_PAGE_LATENCY_SECONDS, "2")
    monkeypatch.setenv(ENV_TICKS_PER_SECOND, "100")
    monkeypatch.setenv(ENV_BUDGET_SECONDS, "3600")
    monkeypatch.setenv(ENV_SAFETY_MARGIN, "0.5")

    knobs = FeasibilityKnobs.from_env()

    assert (knobs.page_latency_seconds, knobs.ohlcv_page_latency_seconds, knobs.ticks_per_second) == (5.0, 2.0, 100.0)
    assert (knobs.budget_seconds, knobs.safety_margin) == (3600.0, 0.5)
    assert knobs.usable_budget_seconds == pytest.approx(1800.0)


@pytest.mark.parametrize("raw", ["", "  ", "not-a-number", "0", "-3", "nan"])
def test_unusable_env_values_fall_back_to_defaults(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv(ENV_BUDGET_SECONDS, raw)

    assert FeasibilityKnobs.from_env().budget_seconds == 7200.0


def test_default_budget_admits_one_year_at_one_hour(monkeypatch: pytest.MonkeyPatch) -> None:
    """The product promise: a year-long hourly single-pool window runs on the default budget."""
    for name in (
        ENV_PAGE_LATENCY_SECONDS,
        ENV_OHLCV_PAGE_LATENCY_SECONDS,
        ENV_TICKS_PER_SECOND,
        ENV_BUDGET_SECONDS,
        ENV_SAFETY_MARGIN,
    ):
        monkeypatch.delenv(name, raising=False)

    for targets in (1, 2):
        estimate = enforce_window_feasibility(_config(365), target_count=targets)
        assert estimate is not None and estimate.feasible


def test_env_budget_can_admit_a_window_the_default_rejects(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(3 * 365)
    for name in (
        ENV_PAGE_LATENCY_SECONDS,
        ENV_OHLCV_PAGE_LATENCY_SECONDS,
        ENV_TICKS_PER_SECOND,
        ENV_BUDGET_SECONDS,
        ENV_SAFETY_MARGIN,
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(BacktestWindowTooLongError):
        enforce_window_feasibility(config, target_count=1)

    monkeypatch.setenv(ENV_BUDGET_SECONDS, "36000")

    estimate = enforce_window_feasibility(config, target_count=1)
    assert estimate is not None and estimate.feasible


def test_knobs_reject_non_positive_construction() -> None:
    with pytest.raises(ValueError):
        FeasibilityKnobs(page_latency_seconds=0)


# --- exception shape --------------------------------------------------------


def test_window_too_long_exception_shape() -> None:
    config = _config(180)

    with pytest.raises(BacktestWindowTooLongError) as excinfo:
        enforce_window_feasibility(config, target_count=1, knobs=_knobs())

    error = excinfo.value
    assert error.preflight_code == "WINDOW_TOO_LONG"
    assert isinstance(error.preflight_blockers, list)
    assert len(error.preflight_blockers) == 1
    blocker = error.preflight_blockers[0]
    assert set(blocker) == {"code", "message", "recommendations"}
    assert blocker["code"] == "WINDOW_TOO_LONG"
    assert "180.0-day window needs ~" in blocker["message"]
    assert "against a 720s budget" in blocker["message"]
    assert isinstance(blocker["recommendations"], list)
    assert blocker["recommendations"]
    assert any("shorten the window to ~" in item for item in blocker["recommendations"])
    assert all(isinstance(item, str) for item in blocker["recommendations"])


def test_window_too_long_is_caught_as_preflight_validation_error() -> None:
    with pytest.raises(PreflightValidationError) as excinfo:
        enforce_window_feasibility(_config(180), target_count=1, knobs=_knobs())

    error = excinfo.value
    assert error.code == "WINDOW_TOO_LONG"
    assert error.failed_checks == ["backtest_window_feasibility"]
    assert error.details["code"] == "WINDOW_TOO_LONG"
    assert error.details["pool_state_pages"] == 34


def test_recommendations_stay_user_actionable() -> None:
    """Recommendations render verbatim to end users, who cannot set job env vars."""
    with pytest.raises(BacktestWindowTooLongError) as excinfo:
        enforce_window_feasibility(_config(180), target_count=1, knobs=_knobs())

    recommendations = excinfo.value.preflight_blockers[0]["recommendations"]

    assert any("shorten the window to ~" in item for item in recommendations)
    assert any("hosted backtest runs are currently limited to ~15 min" in item for item in recommendations)
    joined = " ".join(recommendations)
    assert "ALMANAK_" not in joined
    for name in (ENV_BUDGET_SECONDS, ENV_PAGE_LATENCY_SECONDS, ENV_TICKS_PER_SECOND, ENV_SAFETY_MARGIN):
        assert name not in joined


def test_details_carry_operator_knob_env_vars() -> None:
    with pytest.raises(BacktestWindowTooLongError) as excinfo:
        enforce_window_feasibility(_config(180), target_count=1, knobs=_knobs())

    knob_env_vars = excinfo.value.details["knob_env_vars"]

    assert knob_env_vars == {
        ENV_BUDGET_SECONDS: 900.0,
        ENV_SAFETY_MARGIN: 0.8,
        ENV_PAGE_LATENCY_SECONDS: 20.0,
        ENV_OHLCV_PAGE_LATENCY_SECONDS: 10.0,
        ENV_TICKS_PER_SECOND: 3.5,
    }


def test_recommended_shorter_window_is_actually_feasible() -> None:
    knobs = _knobs()
    with pytest.raises(BacktestWindowTooLongError) as excinfo:
        enforce_window_feasibility(_config(180), target_count=1, knobs=knobs)

    feasible_days = excinfo.value.details["feasible_days"]
    assert 0 < feasible_days < 180
    retry = estimate_cost(
        duration_seconds=int(feasible_days * _DAY),
        interval_seconds=_HOUR,
        target_count=1,
        knobs=knobs,
    )
    assert retry.feasible

    assert enforce_window_feasibility(_config(feasible_days), target_count=1, knobs=knobs) is not None


def test_coarser_interval_is_never_beyond_the_strategy_cadence() -> None:
    """A 180d hourly window at the 900s budget needs a ~2.7h tick to fit."""

    def recommendations(cadence: int | None) -> list[str]:
        with pytest.raises(BacktestWindowTooLongError) as excinfo:
            enforce_window_feasibility(_config(180), target_count=1, knobs=_knobs(), strategy_cadence_seconds=cadence)
        assert excinfo.value.details["strategy_cadence_seconds"] == cadence
        return excinfo.value.preflight_blockers[0]["recommendations"]

    assert any("coarser interval" in item for item in recommendations(None))
    assert any("coarser interval" in item for item in recommendations(4 * _HOUR))
    assert not any("coarser interval" in item for item in recommendations(_HOUR))
    assert any("shorten the window" in item for item in recommendations(_HOUR))


def test_strategy_cadence_is_read_from_data_granularity() -> None:
    assert _engine_helpers._strategy_cadence_seconds({"data_granularity": "4h"}) == 4 * _HOUR
    assert _engine_helpers._strategy_cadence_seconds({}) is None
    assert _engine_helpers._strategy_cadence_seconds(None) is None
    assert _engine_helpers._strategy_cadence_seconds({"data_granularity": "sometimes"}) is None
    assert _engine_helpers._strategy_cadence_seconds({"data_granularity": 14400}) is None


def test_first_use_feasibility_forwards_strategy_cadence(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(1)
    calls: list[tuple[PnLBacktestConfig, int, int | None]] = []

    def capture(
        received_config: PnLBacktestConfig,
        *,
        target_count: int,
        strategy_cadence_seconds: int | None,
    ) -> None:
        calls.append((received_config, target_count, strategy_cadence_seconds))

    monkeypatch.setattr(_engine_helpers, "enforce_window_feasibility", capture)

    _engine_helpers._first_use_feasibility(config, {"data_granularity": "4h"})()

    assert calls == [(config, 1, 4 * _HOUR)]


def test_more_targets_shrink_the_recommended_window() -> None:
    knobs = _knobs()
    ticks_one = max_feasible_ticks(target_count=1, knobs=knobs)
    ticks_four = max_feasible_ticks(target_count=4, knobs=knobs)

    assert 0 < ticks_four < ticks_one


# --- gate behaviour ---------------------------------------------------------


def test_feasible_window_passes_untouched() -> None:
    estimate = enforce_window_feasibility(_config(7), target_count=1, knobs=_knobs())

    assert estimate is not None
    assert estimate.feasible
    assert estimate.pages_per_target == 2


def test_zero_targets_are_never_gated() -> None:
    assert enforce_window_feasibility(_config(3650), target_count=0, knobs=_knobs()) is None


def test_gate_makes_no_network_call(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        snapshot_pool_state,
        "fetch_historical_pool_state_points",
        lambda **_kwargs: pytest.fail("feasibility estimation must be pure computation"),
    )

    with pytest.raises(BacktestWindowTooLongError):
        enforce_window_feasibility(_config(180), target_count=1, knobs=_knobs())


def test_exact_pool_ohlcv_burst_limit_rejects_before_pool_state_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(snapshot_pool_state, "fetch_historical_pool_state_points", _spy_fetcher(calls))

    class OhlcvStrategy(_Strategy):
        backtest_pool_ohlcv_targets = (_OHLCV_TARGET,)

    with pytest.raises(BacktestWindowTooLongError) as excinfo:
        asyncio.run(
            _engine_helpers._prepare_declared_historical_pool_state(
                OhlcvStrategy(),
                OhlcvStrategy.config,
                _config(365, interval_seconds=15 * 60),
                None,
            )
        )

    assert calls == []
    assert excinfo.value.details["exact_pool_ohlcv_pages"] == 36
    assert excinfo.value.details["exact_pool_ohlcv_request_limit"] == 30
    assert "provider materialization limit: 30 requests" in str(excinfo.value)


# --- engine seam ------------------------------------------------------------


def test_engine_seam_rejects_long_window_before_any_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(snapshot_pool_state, "fetch_historical_pool_state_points", _spy_fetcher(calls))
    config = _config(3 * 365)

    with pytest.raises(BacktestWindowTooLongError) as excinfo:
        asyncio.run(
            _engine_helpers._prepare_declared_historical_pool_state(
                _Strategy(),
                _Strategy.config,
                config,
                None,
            )
        )

    assert calls == []
    assert excinfo.value.preflight_code == "WINDOW_TOO_LONG"


def test_engine_seam_materializes_feasible_window(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(snapshot_pool_state, "fetch_historical_pool_state_points", _spy_fetcher(calls))

    source = asyncio.run(
        _engine_helpers._prepare_declared_historical_pool_state(
            _Strategy(),
            _Strategy.config,
            _config(1),
            None,
        )
    )

    assert source is not None
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("state_targets", "ohlcv_targets", "analytics_targets", "failed_check", "lane"),
    [
        (
            _Strategy.backtest_pool_state_targets,
            (_OHLCV_TARGET,),
            (_ANALYTICS_TARGET,),
            "historical_exact_pool_state",
            "exact-pool state",
        ),
        ((), (_OHLCV_TARGET,), (_ANALYTICS_TARGET,), "historical_exact_pool_ohlcv", "exact-pool OHLCV identity"),
        ((), (), (_ANALYTICS_TARGET,), "historical_pool_analytics", "pool-analytics state"),
    ],
)
def test_pool_state_materialization_failure_attribution_precedence(
    monkeypatch: pytest.MonkeyPatch,
    state_targets: tuple[HistoricalPoolStateTarget, ...],
    ohlcv_targets: tuple[HistoricalPoolOHLCVTarget, ...],
    analytics_targets: tuple[HistoricalPoolAnalyticsTarget, ...],
    failed_check: str,
    lane: str,
) -> None:
    strategy = type(
        "Strategy",
        (),
        {
            "backtest_pool_state_targets": state_targets,
            "backtest_pool_ohlcv_targets": ohlcv_targets,
            "backtest_pool_analytics_targets": analytics_targets,
        },
    )()
    cause = ValueError("archive fixture failure")

    async def fail_materialization(_source: Any, _target: HistoricalPoolStateTarget) -> int:
        raise cause

    monkeypatch.setattr(snapshot_pool_state.SnapshotPoolStateSource, "materialize_history", fail_materialization)

    with pytest.raises(PreflightValidationError) as excinfo:
        asyncio.run(
            _engine_helpers._prepare_declared_historical_pool_state(
                strategy,
                _Strategy.config,
                _config(1, interval_seconds=15 * 60),
                None,
            )
        )

    assert excinfo.value.failed_checks == [failed_check]
    assert (
        excinfo.value.message
        == f"Historical {lane} preflight failed for arbitrum:uniswap_v3:{_POOL}: archive fixture failure"
    )
    assert excinfo.value.__cause__ is cause


@pytest.mark.asyncio
async def test_readiness_reports_window_too_long_blocker(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(snapshot_pool_state, "fetch_historical_pool_state_points", _spy_fetcher(calls))
    backtester = PnLBacktester(
        data_provider=_ReadinessProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    result = await backtester.check_readiness(_Strategy(), _config(3 * 365))

    assert calls == []
    assert not result.ready
    assert [blocker["code"] for blocker in result.blockers] == ["WINDOW_TOO_LONG"]
    assert any("shorten the window to ~" in item for item in result.blockers[0]["recommendations"])


@pytest.mark.asyncio
async def test_readiness_reports_exact_pool_ohlcv_request_cost_before_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(snapshot_pool_state, "fetch_historical_pool_state_points", _spy_fetcher(calls))

    class OhlcvStrategy(_Strategy):
        backtest_pool_ohlcv_targets = (_OHLCV_TARGET,)

    backtester = PnLBacktester(
        data_provider=_ReadinessProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    result = await backtester.check_readiness(OhlcvStrategy(), _config(365, interval_seconds=15 * 60))

    assert calls == []
    assert not result.ready
    assert result.blockers[0]["details"]["exact_pool_ohlcv_pages"] == 36
    assert result.blockers[0]["details"]["exact_pool_ohlcv_request_limit"] == 30


def test_engine_seam_skips_gate_without_declared_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        snapshot_pool_state,
        "fetch_historical_pool_state_points",
        lambda **_kwargs: pytest.fail("no declared target must not fetch pool state"),
    )

    class _NoTargets:
        config = _Strategy.config

    source = asyncio.run(
        _engine_helpers._prepare_declared_historical_pool_state(
            _NoTargets(),
            {"chain": "arbitrum"},
            _config(3650),
            None,
        )
    )

    assert source is None
