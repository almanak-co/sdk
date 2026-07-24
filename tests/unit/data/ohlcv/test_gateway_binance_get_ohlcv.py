"""Tests for GatewayOHLCVProvider.get_ohlcv (Binance klines via gateway).

Mirrors the mock pattern of test_gateway_geckoterminal_provider.py: a mock
GatewayClient with an ``integration`` stub, real gateway_pb2 messages.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import (
    DataSourceRateLimited,
    DataSourceUnavailable,
    OHLCVCandle,
)
from almanak.framework.data.ohlcv.gateway_provider import GatewayOHLCVProvider
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

# =============================================================================
# Helper factories
# =============================================================================


def _make_mock_client() -> MagicMock:
    client = MagicMock(spec=GatewayClient)
    client.integration = MagicMock()
    client.config = MagicMock()
    client.config.timeout = 30
    return client


def _kline(open_time_ms: int, o="1800.0", h="1820.0", low="1790.0", c="1810.0", v="50000.0"):
    return gateway_pb2.BinanceKline(
        open_time=open_time_ms, open=o, high=h, low=low, close=c, volume=v
    )


def _klines_response(*klines) -> gateway_pb2.BinanceKlinesResponse:
    return gateway_pb2.BinanceKlinesResponse(klines=list(klines))


@pytest.fixture
def mock_client():
    return _make_mock_client()


@pytest.fixture
def provider(mock_client):
    return GatewayOHLCVProvider(gateway_client=mock_client)


# =============================================================================
# Success path
# =============================================================================


class TestGetOhlcvSuccess:
    @pytest.mark.asyncio
    async def test_parses_and_sorts_klines_ascending(self, provider, mock_client):
        # Deliberately out of order to prove the provider sorts.
        mock_client.integration.BinanceGetKlines.return_value = _klines_response(
            _kline(1_700_007_200_000, c="1812.0"),
            _kline(1_700_000_000_000, c="1810.0"),
        )

        candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=2)

        assert len(candles) == 2
        assert all(isinstance(c, OHLCVCandle) for c in candles)
        assert candles[0].timestamp < candles[1].timestamp
        assert candles[0].close == Decimal("1810.0")
        assert candles[1].close == Decimal("1812.0")
        assert candles[0].timestamp.tzinfo is not None
        assert candles[0].open == Decimal("1800.0")
        assert candles[0].volume == Decimal("50000.0")
        # Health metrics record the success.
        metrics = provider.get_health_metrics()
        assert metrics["total_requests"] == 1
        assert metrics["successful_requests"] == 1
        assert metrics["errors"] == 0

    @pytest.mark.asyncio
    async def test_request_maps_symbol_interval_and_caps_limit(self, provider, mock_client):
        mock_client.integration.BinanceGetKlines.return_value = _klines_response(
            _kline(1_700_000_000_000)
        )

        await provider.get_ohlcv("WETH", timeframe="1d", limit=5000)

        request = mock_client.integration.BinanceGetKlines.call_args[0][0]
        assert request.symbol == "ETHUSDT"
        assert request.interval == "1d"
        assert request.limit == 1000  # Binance max

    @pytest.mark.asyncio
    async def test_empty_string_fields_become_zero_and_none_volume(self, provider, mock_client):
        mock_client.integration.BinanceGetKlines.return_value = _klines_response(
            _kline(1_700_000_000_000, o="", h="", low="", c="", v="")
        )

        candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=1)

        assert candles[0].open == Decimal(0)
        assert candles[0].high == Decimal(0)
        assert candles[0].low == Decimal(0)
        assert candles[0].close == Decimal(0)
        assert candles[0].volume is None

    @pytest.mark.asyncio
    async def test_cache_hit_skips_second_rpc(self, provider, mock_client):
        mock_client.integration.BinanceGetKlines.return_value = _klines_response(
            _kline(1_700_000_000_000)
        )

        first = await provider.get_ohlcv("WETH", timeframe="1h", limit=1)
        second = await provider.get_ohlcv("WETH", timeframe="1h", limit=1)

        assert second == first
        assert mock_client.integration.BinanceGetKlines.call_count == 1
        metrics = provider.get_health_metrics()
        assert metrics["cache_hits"] == 1
        assert metrics["successful_requests"] == 2


# =============================================================================
# Failure paths
# =============================================================================


class TestGetOhlcvFailures:
    @pytest.mark.asyncio
    async def test_invalid_timeframe_raises_value_error(self, provider, mock_client):
        with pytest.raises(ValueError, match="Invalid timeframe"):
            await provider.get_ohlcv("WETH", timeframe="2h")
        mock_client.integration.BinanceGetKlines.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_token_raises_unavailable(self, provider, mock_client):
        with pytest.raises(DataSourceUnavailable, match="Unknown token"):
            await provider.get_ohlcv("NOTATOKEN", timeframe="1h")

        assert provider.get_health_metrics()["errors"] == 1
        mock_client.integration.BinanceGetKlines.assert_not_called()

    @pytest.mark.asyncio
    async def test_timeframe_without_binance_interval_raises_unavailable(self, provider, monkeypatch):
        """Defense-in-depth branch: a timeframe that passes validation but has
        no Binance interval mapping must fail loudly, not guess."""
        monkeypatch.setattr(
            "almanak.framework.data.ohlcv.gateway_provider.validate_timeframe",
            lambda _tf: None,
        )

        with pytest.raises(DataSourceUnavailable, match="Unsupported timeframe"):
            await provider.get_ohlcv("WETH", timeframe="2h")

        assert provider.get_health_metrics()["errors"] == 1

    @pytest.mark.asyncio
    async def test_empty_klines_raises_unavailable(self, provider, mock_client):
        mock_client.integration.BinanceGetKlines.return_value = _klines_response()

        with pytest.raises(DataSourceUnavailable, match="No kline data"):
            await provider.get_ohlcv("WETH", timeframe="1h")

        assert provider.get_health_metrics()["errors"] == 1

    @pytest.mark.asyncio
    async def test_generic_error_wrapped_in_unavailable(self, provider, mock_client):
        mock_client.integration.BinanceGetKlines.side_effect = RuntimeError("connection refused")

        with pytest.raises(DataSourceUnavailable, match="connection refused"):
            await provider.get_ohlcv("WETH", timeframe="1h")

        assert provider.get_health_metrics()["errors"] == 1

    @pytest.mark.asyncio
    async def test_typed_grpc_error_is_surfaced(self, provider, mock_client, monkeypatch):
        """VIB-3800: when the gateway attaches a typed error trailer, the
        provider re-raises the typed exception instead of a generic wrap."""
        typed = DataSourceRateLimited(source="binance", retry_after=17.0)
        monkeypatch.setattr(
            "almanak.framework.data.interfaces.data_source_error_from_grpc",
            lambda _e, default_source: typed,
        )
        mock_client.integration.BinanceGetKlines.side_effect = RuntimeError("rate limited")

        with pytest.raises(DataSourceRateLimited) as excinfo:
            await provider.get_ohlcv("WETH", timeframe="1h")

        assert excinfo.value is typed
        assert provider.get_health_metrics()["errors"] == 1
