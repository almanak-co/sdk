"""Test that PnLBacktester properly closes data provider sessions."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.pnl.engine import PnLBacktester


@pytest.mark.asyncio
async def test_close_calls_data_provider_close():
    """PnLBacktester.close() should call data_provider.close()."""
    bt = PnLBacktester.__new__(PnLBacktester)
    bt.data_provider = MagicMock()
    bt.data_provider.close = AsyncMock()
    bt.gas_provider = None

    await bt.close()

    bt.data_provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_calls_gas_provider_close():
    """PnLBacktester.close() should call gas_provider.close() if present."""
    bt = PnLBacktester.__new__(PnLBacktester)
    bt.data_provider = MagicMock(spec=[])  # no close method
    bt.gas_provider = MagicMock()
    bt.gas_provider.close = AsyncMock()

    await bt.close()

    bt.gas_provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_handles_no_close_method():
    """PnLBacktester.close() should not crash if provider has no close()."""
    bt = PnLBacktester.__new__(PnLBacktester)
    bt.data_provider = MagicMock(spec=[])  # no close method
    bt.gas_provider = None

    # Should not raise
    await bt.close()


@pytest.mark.asyncio
async def test_close_handles_none_gas_provider():
    """PnLBacktester.close() should handle gas_provider=None."""
    bt = PnLBacktester.__new__(PnLBacktester)
    bt.data_provider = MagicMock()
    bt.data_provider.close = AsyncMock()
    bt.gas_provider = None

    await bt.close()

    bt.data_provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_called_on_run_backtest_exception():
    """close() must be called even when _run_backtest raises (e.g., data quality gate).

    Before the fix, exceptions from post-simulation code (data quality gate ValueError,
    PreflightValidationError) would exit backtest() without calling close(), leaking the
    aiohttp session. When asyncio.run() then closed the event loop, GC of the leaked
    session triggered 'RuntimeError: Event loop is closed'.
    """
    bt = PnLBacktester.__new__(PnLBacktester)
    bt.data_provider = MagicMock()
    bt.data_provider.close = AsyncMock()
    bt.gas_provider = None
    bt._current_backtest_id = None

    # Mock _run_backtest to simulate an unhandled exception (e.g., data quality gate)
    bt._run_backtest = AsyncMock(side_effect=ValueError("Data quality gate failed"))

    mock_config = MagicMock()
    mock_config.start_time = datetime(2024, 1, 1, tzinfo=UTC)
    mock_config.end_time = datetime(2024, 1, 31, tzinfo=UTC)
    mock_config.initial_capital_usd = 10000.0
    mock_strategy = MagicMock()
    mock_strategy.strategy_id = "test"

    with pytest.raises(ValueError, match="Data quality gate failed"):
        await bt.backtest(mock_strategy, mock_config)

    # The critical assertion: close() was called despite the exception
    bt.data_provider.close.assert_awaited_once()
