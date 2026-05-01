"""Tests for the GeckoTerminalGetOHLCV gRPC handler in IntegrationServiceServicer."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.gateway.proto import gateway_pb2


def _make_context() -> MagicMock:
    """Create a mock gRPC ServicerContext."""
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def _make_ohlcv_candle(ts_offset: int = 0) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime(2026, 1, 1, hour=ts_offset, tzinfo=UTC),
        open=Decimal("1800.0"),
        high=Decimal("1820.0"),
        low=Decimal("1790.0"),
        close=Decimal("1810.0"),
        volume=Decimal("50000.0"),
    )


class TestGeckoTerminalGetOHLCV:
    """Tests for IntegrationServiceServicer.GeckoTerminalGetOHLCV."""

    @pytest.fixture
    def service(self):
        """Create an IntegrationServiceServicer with mocked dependencies."""
        from almanak.gateway.services.integration_service import IntegrationServiceServicer

        svc = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
        svc._initialized = True
        svc._binance = None
        svc._coingecko = None
        svc._thegraph = None
        return svc

    @pytest.mark.asyncio
    async def test_empty_token_returns_invalid_argument(self, service):
        """Empty token triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.GeckoTerminalOHLCVRequest(token="", chain="base")

        await service.GeckoTerminalGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_with("token is required and cannot be empty")

    @pytest.mark.asyncio
    async def test_empty_chain_returns_invalid_argument(self, service):
        """Empty chain triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.GeckoTerminalOHLCVRequest(token="ALMANAK", chain="")

        await service.GeckoTerminalGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_with("chain is required and cannot be empty")

    @pytest.mark.asyncio
    async def test_invalid_timeframe_returns_invalid_argument(self, service):
        """Unsupported timeframe triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.GeckoTerminalOHLCVRequest(
            token="ALMANAK", chain="base", timeframe="2h",
        )

        await service.GeckoTerminalGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert "Invalid timeframe" in ctx.set_details.call_args[0][0]

    @pytest.mark.asyncio
    async def test_limit_out_of_range_returns_invalid_argument(self, service):
        """Limit outside 1-1000 triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.GeckoTerminalOHLCVRequest(
            token="ALMANAK", chain="base", timeframe="1h", limit=1001,
        )

        await service.GeckoTerminalGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert "limit must be between" in ctx.set_details.call_args[0][0]

    @pytest.mark.asyncio
    async def test_success_returns_candles(self, service):
        """Happy path: candles from provider are mapped to response proto."""
        ctx = _make_context()
        request = gateway_pb2.GeckoTerminalOHLCVRequest(
            token="ALMANAK", chain="base", timeframe="1h", limit=2,
        )

        candles = [_make_ohlcv_candle(0), _make_ohlcv_candle(1)]

        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.return_value = candles
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.geckoterminal_provider.GeckoTerminalOHLCVProvider",
            return_value=mock_provider,
        ):
            response = await service.GeckoTerminalGetOHLCV(request, ctx)

        assert len(response.candles) == 2
        assert response.candles[0].close == "1810.0"
        assert response.candles[0].volume == "50000.0"
        # Should NOT have set an error code
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_provider_error_returns_sanitized_internal(self, service):
        """Provider exceptions yield INTERNAL with sanitized message."""
        ctx = _make_context()
        request = gateway_pb2.GeckoTerminalOHLCVRequest(
            token="ALMANAK", chain="base", timeframe="1h", limit=10,
        )

        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.side_effect = RuntimeError("upstream API 500")
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.geckoterminal_provider.GeckoTerminalOHLCVProvider",
            return_value=mock_provider,
        ):
            await service.GeckoTerminalGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        # Must NOT leak raw error text — VIB-3800 sanitization replaces the
        # raw exception string with a fixed opaque message.
        details = ctx.set_details.call_args[0][0]
        assert "upstream API 500" not in details
        assert details == "Internal gateway error"

    @pytest.mark.asyncio
    async def test_value_error_returns_invalid_argument(self, service):
        """ValueError from provider yields INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.GeckoTerminalOHLCVRequest(
            token="ALMANAK", chain="base", timeframe="1h", limit=10,
        )

        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.side_effect = ValueError("unsupported timeframe")
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.geckoterminal_provider.GeckoTerminalOHLCVProvider",
            return_value=mock_provider,
        ):
            await service.GeckoTerminalGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
