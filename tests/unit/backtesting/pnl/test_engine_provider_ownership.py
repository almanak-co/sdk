"""Provider-lifetime ownership in PnLBacktester.backtest() (VIB-5621).

The engine's finally-cleanup exists so the single-run CLI path never leaks
an aiohttp session past ``asyncio.run()``. But sweep/optimize share ONE
provider across many ``backtest()`` calls; the first run to finish must not
close the shared session under the still-running ones. Ownership is
expressed via the ``close_providers_on_finish`` flag.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.models import BacktestResult, ParameterSource
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.cli.backtest.run_helpers import build_pnl_config
from tests.backtesting_funding import pnl_token_funding


def _pnl_config():
    return build_pnl_config(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        interval_seconds=3600,
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        token_funding=pnl_token_funding("10000"),
    )


def _strategy_stub():
    strategy = MagicMock()
    strategy.deployment_id = "ownership-test"
    return strategy


def _backtester(**kwargs) -> PnLBacktester:
    provider = MagicMock()
    provider.close = AsyncMock()
    return PnLBacktester(data_provider=provider, fee_models={}, slippage_models={}, **kwargs)


@pytest.mark.asyncio
async def test_backtest_closes_provider_by_default() -> None:
    """Single-run contract preserved: engine closes the provider it was given."""
    backtester = _backtester()
    sentinel = MagicMock(spec=BacktestResult)
    with patch.object(backtester, "_run_backtest", AsyncMock(return_value=sentinel)):
        result = await backtester.backtest(_strategy_stub(), _pnl_config())
    assert result is sentinel
    backtester.data_provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_backtest_leaves_caller_owned_provider_open() -> None:
    """close_providers_on_finish=False: shared provider survives run completion."""
    backtester = _backtester(close_providers_on_finish=False)
    sentinel = MagicMock(spec=BacktestResult)
    with patch.object(backtester, "_run_backtest", AsyncMock(return_value=sentinel)):
        await backtester.backtest(_strategy_stub(), _pnl_config())
    backtester.data_provider.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_backtest_leaves_caller_owned_provider_open_on_failure() -> None:
    """The flag governs the finally path too: failures must not close it either."""
    backtester = _backtester(close_providers_on_finish=False)
    with patch.object(backtester, "_run_backtest", AsyncMock(side_effect=ValueError("boom"))):
        with pytest.raises(ValueError, match="boom"):
            await backtester.backtest(_strategy_stub(), _pnl_config())
    backtester.data_provider.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_backtest_uses_run_local_config_for_provider_resolution() -> None:
    """Concurrent sweep templates cannot be pinned by a completed peer run."""
    backtester = _backtester(close_providers_on_finish=False)
    caller_config = _pnl_config()
    caller_config.timeframe = "auto"
    observed_run_config = None

    async def resolve_for_run(_strategy, run_config, *_args):
        nonlocal observed_run_config
        observed_run_config = run_config
        run_config.apply_resolved_timeframe("4h", 14_400)
        return MagicMock(spec=BacktestResult)

    with patch.object(backtester, "_run_backtest", side_effect=resolve_for_run):
        await backtester.backtest(_strategy_stub(), caller_config)

    assert observed_run_config is not caller_config
    assert observed_run_config.resolved_timeframe == "4h"
    assert observed_run_config.interval_seconds == 3600
    assert caller_config.resolved_timeframe is None
    assert caller_config.interval_seconds == 3600


def test_resolved_timeframe_parameter_source_is_provider() -> None:
    config = _pnl_config()
    config.timeframe = "auto"
    config.apply_resolved_timeframe("4h", 14_400)

    tracker = _backtester()._create_parameter_source_tracker(config)
    sources = {record.parameter_name: record.source for record in tracker.records}

    assert sources["timeframe"] is ParameterSource.EXPLICIT
    assert sources["resolved_timeframe"] is ParameterSource.PROVIDER
