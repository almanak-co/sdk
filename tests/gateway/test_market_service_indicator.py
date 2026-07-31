"""Regression tests for MarketService indicator request contracts."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.indicators.rsi import CoinGeckoOHLCVProvider
from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.timeframes import OHLCVTimeframe
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer


@pytest.mark.asyncio
async def test_default_rsi_negotiates_coingecko_capacity() -> None:
    """Default RSI keeps its minimum history while trimming optional warm-up."""
    service = MarketServiceServicer(GatewaySettings())
    context = MagicMock()
    captured: dict[str, object] = {}

    async def _bounded_candles(self, **kwargs):  # noqa: ANN001
        captured.update(kwargs)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        return [
            OHLCVCandle(
                timestamp=start + timedelta(hours=index),
                open=Decimal(100 + index),
                high=Decimal(101 + index),
                low=Decimal(99 + index),
                close=Decimal(100 + index),
                volume=None,
            )
            for index in range(int(kwargs["limit"]))
        ]

    with patch.object(CoinGeckoOHLCVProvider, "get_ohlcv", _bounded_candles):
        response = await service.GetIndicator(
            gateway_pb2.IndicatorRequest(indicator_type="RSI", token="ETH"),
            context,
        )

    assert response.value == "100.0"
    assert response.metadata == {"period": "14", "timeframe": "1h"}
    assert captured["timeframe"] is OHLCVTimeframe.ONE_HOUR
    assert captured["limit"] == 24
    context.set_code.assert_not_called()
