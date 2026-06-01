"""Tests for the CoinGeckoGetOHLCV gRPC handler (IntegrationServiceServicer, VIB-4847)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.gateway.proto import gateway_pb2


def _make_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def _candle(hour: int) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime(2026, 1, 1, hour=hour, tzinfo=UTC),
        open=Decimal("100.0"),
        high=Decimal("101.0"),
        low=Decimal("99.0"),
        close=Decimal("100.5"),
        volume=None,  # CoinGecko OHLC is price-only.
    )


class TestCoinGeckoGetOHLCV:
    @pytest.fixture
    def service(self):
        from almanak.gateway.services.integration_service import IntegrationServiceServicer

        svc = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
        svc._initialized = True
        svc._binance = None
        svc._coingecko = MagicMock()  # provider is built from this; handler uses it
        svc._thegraph = None
        return svc

    @pytest.mark.asyncio
    async def test_empty_token_returns_invalid_argument(self, service):
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOHLCVRequest(token="", timeframe="1h")
        await service.CoinGeckoGetOHLCV(request, ctx)
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_limit_out_of_range_returns_invalid_argument(self, service):
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOHLCVRequest(token="WETH", timeframe="1h", limit=5000)
        await service.CoinGeckoGetOHLCV(request, ctx)
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert "limit must be between" in ctx.set_details.call_args[0][0]

    @pytest.mark.asyncio
    async def test_success_maps_candles_without_volume(self, service, monkeypatch):
        ctx = _make_context()

        async def _fake_get_ohlcv(self, **kwargs):  # noqa: ANN001
            return [_candle(0), _candle(1), _candle(2)]

        monkeypatch.setattr(
            "almanak.gateway.data.ohlcv.coingecko_provider.CoinGeckoOHLCVProvider.get_ohlcv",
            _fake_get_ohlcv,
        )

        request = gateway_pb2.CoinGeckoOHLCVRequest(token="WETH", timeframe="1h", limit=3)
        response = await service.CoinGeckoGetOHLCV(request, ctx)

        assert len(response.candles) == 3
        assert response.candles[0].close == "100.5"
        # Proto candle has no volume field at all — price-only, by design.
        assert not any(f.name == "volume" for f in response.candles[0].DESCRIPTOR.fields)
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_unsupported_timeframe_sets_error(self, service, monkeypatch):
        """A sub-hour timeframe makes the provider raise DataSourceUnavailable;
        the handler must surface it as a gRPC error, not crash."""
        ctx = _make_context()

        async def _raise(self, **kwargs):  # noqa: ANN001
            raise DataSourceUnavailable(source="coingecko", reason="cannot serve 5m")

        monkeypatch.setattr(
            "almanak.gateway.data.ohlcv.coingecko_provider.CoinGeckoOHLCVProvider.get_ohlcv",
            _raise,
        )

        request = gateway_pb2.CoinGeckoOHLCVRequest(token="WETH", timeframe="5m", limit=10)
        response = await service.CoinGeckoGetOHLCV(request, ctx)

        assert len(response.candles) == 0
        ctx.set_code.assert_called()  # error code set via set_error_from_upstream
