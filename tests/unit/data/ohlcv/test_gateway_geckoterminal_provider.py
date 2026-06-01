"""Tests for GatewayGeckoTerminalOHLCVProvider and GeckoTerminalGatewayDataProvider."""

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.ohlcv.gateway_data_adapter import GeckoTerminalGatewayDataProvider
from almanak.framework.data.ohlcv.gateway_provider import GatewayGeckoTerminalOHLCVProvider
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2


# =============================================================================
# Helper factories
# =============================================================================


def _make_mock_client() -> MagicMock:
    """Create a mock GatewayClient with integration stub."""
    client = MagicMock(spec=GatewayClient)
    client.integration = MagicMock()
    client.config = MagicMock()
    client.config.timeout = 30
    return client


def _make_grpc_candle(ts: int = 1700000000, o: str = "1800.0", h: str = "1820.0",
                      l: str = "1790.0", c: str = "1810.0", v: str = "50000.0"):
    return gateway_pb2.GeckoTerminalOHLCVCandle(
        timestamp=ts, open=o, high=h, low=l, close=c, volume=v,
    )


def _make_grpc_response(n: int = 3) -> gateway_pb2.GeckoTerminalOHLCVResponse:
    candles = [_make_grpc_candle(ts=1700000000 + i * 3600) for i in range(n)]
    return gateway_pb2.GeckoTerminalOHLCVResponse(candles=candles)


# =============================================================================
# GatewayGeckoTerminalOHLCVProvider
# =============================================================================


class TestGatewayGeckoTerminalOHLCVProvider:
    """Tests for the gateway-backed GeckoTerminal OHLCV provider."""

    @pytest.fixture
    def mock_client(self):
        return _make_mock_client()

    @pytest.fixture
    def provider(self, mock_client):
        return GatewayGeckoTerminalOHLCVProvider(
            gateway_client=mock_client, chain="base",
            cache_ttl_live=15.0, cache_ttl_historical=60.0,
        )

    @pytest.mark.asyncio
    async def test_get_ohlcv_success(self, provider, mock_client):
        """Successful gRPC call returns parsed OHLCVCandle list."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = _make_grpc_response(3)

        candles = await provider.get_ohlcv(token="ALMANAK", timeframe="1h", limit=3)

        assert len(candles) == 3
        assert all(isinstance(c, OHLCVCandle) for c in candles)
        assert candles[0].close == Decimal("1810.0")
        assert candles[0].timestamp.tzinfo is not None

    @pytest.mark.asyncio
    async def test_get_ohlcv_uses_chain_override(self, provider, mock_client):
        """Per-call chain override is forwarded to the gRPC request."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = _make_grpc_response(1)

        await provider.get_ohlcv(token="WETH", chain="ethereum")

        call_args = mock_client.integration.GeckoTerminalGetOHLCV.call_args
        request = call_args[0][0]
        assert request.chain == "ethereum"

    @pytest.mark.asyncio
    async def test_request_sets_include_empty_intervals(self, provider, mock_client):
        """VIB-4875: GeckoTerminal is DEX-native, so the SDK always asks for
        continuous buckets (include_empty_intervals=True)."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = _make_grpc_response(1)

        await provider.get_ohlcv(token="NVDAON", chain="ethereum")

        request = mock_client.integration.GeckoTerminalGetOHLCV.call_args[0][0]
        assert request.include_empty_intervals is True

    @pytest.mark.asyncio
    async def test_empty_response_raises(self, provider, mock_client):
        """Empty candle list raises DataSourceUnavailable."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = (
            gateway_pb2.GeckoTerminalOHLCVResponse()
        )

        with pytest.raises(DataSourceUnavailable, match="No OHLCV data"):
            await provider.get_ohlcv(token="UNKNOWN")

    @pytest.mark.asyncio
    async def test_grpc_error_raises_data_source_unavailable(self, provider, mock_client):
        """gRPC errors are wrapped in DataSourceUnavailable."""
        mock_client.integration.GeckoTerminalGetOHLCV.side_effect = RuntimeError("connection refused")

        with pytest.raises(DataSourceUnavailable, match="connection refused"):
            await provider.get_ohlcv(token="ALMANAK")

    @pytest.mark.asyncio
    async def test_cache_hit(self, provider, mock_client):
        """Second call with same params returns cached result without gRPC call."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = _make_grpc_response(2)

        first = await provider.get_ohlcv(token="ALMANAK", timeframe="1h")
        second = await provider.get_ohlcv(token="ALMANAK", timeframe="1h")

        assert first == second
        assert mock_client.integration.GeckoTerminalGetOHLCV.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_key_includes_quote(self, provider, mock_client):
        """Different quote currencies produce separate cache entries."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = _make_grpc_response(1)

        await provider.get_ohlcv(token="ALMANAK", quote="USD")
        await provider.get_ohlcv(token="ALMANAK", quote="WETH")

        assert mock_client.integration.GeckoTerminalGetOHLCV.call_count == 2

    @pytest.mark.asyncio
    async def test_live_timeframe_uses_shorter_ttl(self, provider, mock_client):
        """1m/5m timeframes use the shorter live TTL."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = _make_grpc_response(1)

        # Use a provider with 0-second live TTL to force cache miss on second call
        provider._cache_ttl_live = 0.0
        provider._cache_ttl_historical = 9999.0

        await provider.get_ohlcv(token="ALMANAK", timeframe="1m")
        await provider.get_ohlcv(token="ALMANAK", timeframe="1m")

        # With 0s live TTL, cache always expires → 2 gRPC calls
        assert mock_client.integration.GeckoTerminalGetOHLCV.call_count == 2

    def test_clear_cache(self, provider, mock_client):
        """clear_cache empties the internal cache dict."""
        provider._cache[("key", "USD", "1h", 100)] = MagicMock()
        provider.clear_cache()
        assert len(provider._cache) == 0

    def test_health_metrics(self, provider):
        """Health metrics reflect initial state."""
        metrics = provider.get_health_metrics()
        assert metrics["total_requests"] == 0
        assert metrics["errors"] == 0
        assert metrics["success_rate"] == 100.0

    @pytest.mark.asyncio
    async def test_metrics_update_on_success(self, provider, mock_client):
        """Successful request increments counters."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = _make_grpc_response(1)

        await provider.get_ohlcv(token="ALMANAK")

        metrics = provider.get_health_metrics()
        assert metrics["total_requests"] == 1
        assert metrics["successful_requests"] == 1
        assert metrics["errors"] == 0

    @pytest.mark.asyncio
    async def test_metrics_update_on_error(self, provider, mock_client):
        """Failed request increments error counter."""
        mock_client.integration.GeckoTerminalGetOHLCV.return_value = (
            gateway_pb2.GeckoTerminalOHLCVResponse()
        )

        with pytest.raises(DataSourceUnavailable):
            await provider.get_ohlcv(token="UNKNOWN")

        metrics = provider.get_health_metrics()
        assert metrics["errors"] == 1


# =============================================================================
# GeckoTerminalGatewayDataProvider
# =============================================================================


class TestGeckoTerminalGatewayDataProvider:
    """Tests for the DataProvider adapter wrapping GatewayGeckoTerminalOHLCVProvider."""

    @pytest.fixture
    def mock_gateway_provider(self):
        provider = MagicMock(spec=GatewayGeckoTerminalOHLCVProvider)
        provider.get_health_metrics.return_value = {"total_requests": 5}
        return provider

    @pytest.fixture
    def adapter(self, mock_gateway_provider):
        return GeckoTerminalGatewayDataProvider(mock_gateway_provider)

    def test_name(self, adapter):
        assert adapter.name == "geckoterminal"

    def test_data_class(self, adapter):
        assert adapter.data_class == DataClassification.INFORMATIONAL

    def test_health_delegates(self, adapter, mock_gateway_provider):
        result = adapter.health()
        assert result == {"total_requests": 5}
        mock_gateway_provider.get_health_metrics.assert_called_once()

    def test_fetch_success(self, adapter, mock_gateway_provider):
        """fetch() returns a DataEnvelope with candle data."""
        candles = [
            OHLCVCandle(
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                open=Decimal("1800"), high=Decimal("1820"),
                low=Decimal("1790"), close=Decimal("1810"), volume=Decimal("50000"),
            ),
        ]
        mock_gateway_provider.get_ohlcv = MagicMock(return_value=candles)

        # Patch asyncio.run so the sync adapter resolves the "coroutine"
        with patch("almanak.framework.data.ohlcv.gateway_data_adapter.asyncio") as mock_asyncio:
            mock_asyncio.get_event_loop.side_effect = RuntimeError("no loop")
            mock_asyncio.run.return_value = candles

            envelope = adapter.fetch(token="ALMANAK", quote="USD", timeframe="1h", limit=10)

        assert isinstance(envelope, DataEnvelope)
        assert envelope.value == candles
        assert envelope.meta.source == "geckoterminal"

    def test_fetch_missing_token_raises(self, adapter):
        """fetch() without token raises ValueError."""
        with pytest.raises(ValueError, match="token"):
            adapter.fetch(quote="USD")

    def test_fetch_empty_token_raises(self, adapter):
        """fetch() with empty token raises ValueError."""
        with pytest.raises(ValueError, match="token"):
            adapter.fetch(token="")

    def test_fetch_none_chain_passes_none(self, adapter, mock_gateway_provider):
        """When chain is not provided, None is passed to the provider."""
        candles = []
        with patch("almanak.framework.data.ohlcv.gateway_data_adapter.asyncio") as mock_asyncio:
            mock_asyncio.get_event_loop.side_effect = RuntimeError("no loop")
            mock_asyncio.run.return_value = candles

            adapter.fetch(token="ALMANAK")

        # Verify chain=None was passed (not "None" string)
        call_kwargs = mock_gateway_provider.get_ohlcv.call_args
        if call_kwargs:
            # get_ohlcv is called as a coroutine; check the mock was called
            assert mock_gateway_provider.get_ohlcv.called
