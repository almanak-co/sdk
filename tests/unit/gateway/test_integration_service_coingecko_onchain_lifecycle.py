"""Unit contracts for the service-owned CoinGecko Onchain provider."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.integration_service import IntegrationServiceServicer


@pytest.mark.asyncio
async def test_provider_is_reused_across_requests_and_closed_with_service() -> None:
    """RPCs share one provider and service shutdown releases it exactly once."""
    service = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
    service._initialized = True
    service.settings = SimpleNamespace(coingecko_api_key="test-key")
    service._binance = None
    service._coingecko = None
    service._thegraph = None
    service._zerion = None
    service._portfolio_chain = None
    service._coingecko_onchain_ohlcv = None

    request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
        token="ALMANAK",
        chain="base",
        timeframe="1h",
        limit=1,
    )
    context = MagicMock()
    mock_provider = MagicMock()
    mock_provider.get_ohlcv = AsyncMock(
        return_value=[
            OHLCVCandle(
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                open=Decimal("1"),
                high=Decimal("2"),
                low=Decimal("0.5"),
                close=Decimal("1.5"),
                volume=Decimal("10"),
            )
        ]
    )
    mock_provider.close = AsyncMock()

    with patch(
        "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
        return_value=mock_provider,
    ) as provider_cls:
        first = await service.CoinGeckoOnchainGetOHLCV(request, context)
        second = await service.CoinGeckoOnchainGetOHLCV(request, context)
        await service.close()

    assert len(first.candles) == 1
    assert len(second.candles) == 1
    provider_cls.assert_called_once_with(api_key="test-key")
    assert mock_provider.get_ohlcv.await_count == 2
    mock_provider.close.assert_awaited_once()
    assert service._coingecko_onchain_ohlcv is None
