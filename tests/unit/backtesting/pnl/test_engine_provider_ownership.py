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

from almanak.framework.backtesting.models import BacktestResult
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
