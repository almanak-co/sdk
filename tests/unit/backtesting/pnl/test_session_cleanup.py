"""Test that PnLBacktester properly closes data provider sessions."""

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
