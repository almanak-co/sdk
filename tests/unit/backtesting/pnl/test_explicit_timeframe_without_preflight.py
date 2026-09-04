"""An explicit price timeframe is strict even when preflight is disabled.

The hosted runner turns ``preflight_validation`` off, which used to let an
explicit 15m request run on hourly candles with ``resolved_timeframe`` unset.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import HistoricalCoverage, token_ref_display
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from tests.backtesting_funding import pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

_START = datetime(2026, 7, 15, tzinfo=UTC)
_END = _START + timedelta(days=2)


class _CoverageProvider(MockDataProvider):
    """Mock ticks plus range coverage with a configurable native cadence."""

    provider_name = "coverage_fixture"

    def __init__(self, *, observed_interval_seconds: int | None, always_full: bool = False) -> None:
        super().__init__()
        self.observed_interval_seconds = observed_interval_seconds
        # A provider that grades the window, not the cadence: ``full`` even when its candles are coarser.
        self.always_full = always_full
        self.probed: list[tuple[str, int]] = []

    async def get_price_coverage(self, token: object, start: datetime, end: datetime, interval_seconds: int) -> Any:
        self.probed.append((token_ref_display(token), interval_seconds))
        observed = self.observed_interval_seconds
        coarser = observed is not None and observed > interval_seconds
        return HistoricalCoverage(
            status="partial" if coarser and not self.always_full else "full",
            requested_start=start,
            requested_end=end,
            first_available_at=start,
            last_available_at=end,
            earliest_contiguous_at=None if coarser else start,
            coverage_ratio=Decimal("0.25") if coarser else Decimal("1"),
            provider=self.provider_name,
            source_id=token_ref_display(token),
            interval_seconds=interval_seconds,
            observed_interval_seconds=observed,
            resolved_interval_seconds=observed,
            resolved_coverage_complete=True,
        )


class _HoldStrategy:
    deployment_id = "explicit-timeframe"

    def decide(self, market: Any) -> Any:
        return None


def _config(timeframe: str | None) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=_START,
        end_time=_END,
        interval_seconds=3600,
        timeframe=timeframe,
        token_funding=pnl_token_funding(Decimal("1000")),
        tokens=["WETH", "USDC"],
        preflight_validation=False,
        include_gas_costs=False,
    )


def _backtester(provider: Any) -> PnLBacktester:
    return PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


@pytest.mark.asyncio
async def test_coarser_native_cadence_refuses_the_explicit_request() -> None:
    provider = _CoverageProvider(observed_interval_seconds=3600)
    config = _config("15m")

    with pytest.raises(PreflightValidationError) as raised:
        await _backtester(provider).enforce_explicit_price_timeframe(config)

    assert raised.value.code == "PARTIAL_PRICE_HISTORY"
    assert raised.value.details["reason_code"] == "PRICE_TIMEFRAME_TOO_FINE"
    assert raised.value.details["cadence_mismatches"][0]["requested_timeframe"] == "15m"
    assert config.resolved_timeframe is None
    assert provider.probed == [("WETH", 900)]


@pytest.mark.asyncio
async def test_full_window_coverage_at_a_coarser_native_cadence_is_still_refused() -> None:
    provider = _CoverageProvider(observed_interval_seconds=3600, always_full=True)
    config = _config("15m")

    with pytest.raises(PreflightValidationError) as raised:
        await _backtester(provider).enforce_explicit_price_timeframe(config)

    assert raised.value.details["reason_code"] == "PRICE_TIMEFRAME_TOO_FINE"
    assert raised.value.details["cadence_mismatches"][0]["observed_interval_seconds"] == 3600
    assert config.resolved_timeframe is None


@pytest.mark.asyncio
async def test_matching_native_cadence_records_the_resolved_timeframe() -> None:
    provider = _CoverageProvider(observed_interval_seconds=900)
    config = _config("15m")

    await _backtester(provider).enforce_explicit_price_timeframe(config)

    assert config.resolved_timeframe == "15m"
    assert config.price_interval_seconds == 900


@pytest.mark.asyncio
async def test_unreported_native_cadence_leaves_the_request_unverified() -> None:
    provider = _CoverageProvider(observed_interval_seconds=None)
    config = _config("15m")

    await _backtester(provider).enforce_explicit_price_timeframe(config)

    assert config.resolved_timeframe is None


@pytest.mark.asyncio
@pytest.mark.parametrize("timeframe", [None, "auto"])
async def test_non_explicit_timeframes_are_not_probed(timeframe: str | None) -> None:
    provider = _CoverageProvider(observed_interval_seconds=3600)
    config = _config(timeframe)

    await _backtester(provider).enforce_explicit_price_timeframe(config)

    assert provider.probed == []


@pytest.mark.asyncio
async def test_provider_without_range_coverage_is_skipped() -> None:
    config = _config("15m")

    await _backtester(MockDataProvider()).enforce_explicit_price_timeframe(config)

    assert config.resolved_timeframe is None


@pytest.mark.asyncio
async def test_backtest_with_preflight_off_refuses_a_silently_coarser_price_plane() -> None:
    """Negative control for the hosted lane: the run never starts on hourly candles."""
    provider = _CoverageProvider(observed_interval_seconds=3600)

    with pytest.raises(PreflightValidationError) as raised:
        await _backtester(provider).backtest(_HoldStrategy(), _config("15m"))

    assert raised.value.details["reason_code"] == "PRICE_TIMEFRAME_TOO_FINE"


@pytest.mark.asyncio
async def test_backtest_with_preflight_off_runs_a_verified_explicit_timeframe() -> None:
    provider = _CoverageProvider(observed_interval_seconds=900)
    config = _config("15m")

    result = await _backtester(provider).backtest(_HoldStrategy(), config)

    assert result.error is None
    assert result.resolved_timeframe == "15m"
    # The caller's config is an immutable template (sweeps share it).
    assert config.resolved_timeframe is None
