"""Tests for GeckoTerminalOHLCVProvider.

Tests cover:
- OHLCVProvider protocol: get_ohlcv, supported_timeframes
- DataProvider protocol: name, data_class, fetch, health
- GeckoTerminal API response parsing
- Rate limiting with token bucket
- Chain-to-network mapping
- Timeframe mapping
- Caching behavior
- Error handling (HTTP errors, rate limits, invalid data)
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.ohlcv.geckoterminal_provider import (
    _CHAIN_TO_NETWORK,
    _TIMEFRAME_TO_GT,
    GeckoTerminalOHLCVProvider,
    _TokenBucket,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def provider() -> GeckoTerminalOHLCVProvider:
    """Create a fresh provider for each test."""
    return GeckoTerminalOHLCVProvider(cache_ttl=60, request_timeout=5.0)


def _make_ohlcv_response(
    candles: list[list[float | int]] | None = None,
) -> dict:
    """Build a mock GeckoTerminal OHLCV JSON response.

    Default produces 3 candles in descending order (API default).
    """
    if candles is None:
        # Descending timestamp order (newest first, as GeckoTerminal returns)
        candles = [
            [1700003600, 1810.5, 1820.0, 1800.0, 1815.0, 50000.0],
            [1700000000, 1800.0, 1812.0, 1795.0, 1810.0, 45000.0],
            [1699996400, 1790.0, 1805.0, 1785.0, 1800.0, 42000.0],
        ]
    return {
        "data": {
            "id": "eth_pool_123",
            "type": "ohlcv",
            "attributes": {
                "ohlcv_list": candles,
            },
        },
    }


def _make_search_response(pool_address: str = "0xabcdef1234567890") -> dict:
    """Build a mock GeckoTerminal pool search response."""
    return {
        "data": [
            {
                "id": f"eth_{pool_address}",
                "type": "pool",
                "attributes": {
                    "address": pool_address,
                    "name": "WETH / USDC",
                },
            },
        ],
    }


# ---------------------------------------------------------------------------
# DataProvider protocol tests
# ---------------------------------------------------------------------------


class TestDataProviderProtocol:
    """Test DataProvider protocol compliance."""

    def test_name(self, provider: GeckoTerminalOHLCVProvider) -> None:
        assert provider.name == "geckoterminal"

    def test_data_class(self, provider: GeckoTerminalOHLCVProvider) -> None:
        assert provider.data_class == DataClassification.INFORMATIONAL

    def test_health_initial(self, provider: GeckoTerminalOHLCVProvider) -> None:
        h = provider.health()
        assert h["status"] == "healthy"
        assert h["total_requests"] == 0
        assert h["successful_requests"] == 0
        assert h["cache_hits"] == 0
        assert h["errors"] == 0
        assert h["success_rate"] == 100.0

    def test_health_after_errors(self, provider: GeckoTerminalOHLCVProvider) -> None:
        provider._metrics.total_requests = 10
        provider._metrics.errors = 6
        provider._metrics.successful_requests = 4
        h = provider.health()
        assert h["status"] == "degraded"


# ---------------------------------------------------------------------------
# OHLCVProvider protocol tests
# ---------------------------------------------------------------------------


class TestOHLCVProviderProtocol:
    """Test OHLCVProvider protocol compliance."""

    def test_supported_timeframes(self, provider: GeckoTerminalOHLCVProvider) -> None:
        tf = provider.supported_timeframes
        assert tf == ["1m", "5m", "15m", "1h", "4h", "1d"]
        # Returns a copy, not the original list
        tf.append("999m")
        assert "999m" not in provider.supported_timeframes

    @pytest.mark.asyncio
    async def test_invalid_timeframe_raises(self, provider: GeckoTerminalOHLCVProvider) -> None:
        with pytest.raises(ValueError, match="Invalid timeframe"):
            await provider.get_ohlcv("WETH", timeframe="7m")


# ---------------------------------------------------------------------------
# get_ohlcv tests (with mocked HTTP)
# ---------------------------------------------------------------------------


class TestGetOHLCV:
    """Test get_ohlcv with mocked HTTP responses."""

    @pytest.mark.asyncio
    async def test_fetch_with_pool_address(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Fetch OHLCV with explicit pool address."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        candles = await provider.get_ohlcv(
            "WETH",
            timeframe="1h",
            limit=100,
            pool_address="0xabc123",
            chain="ethereum",
        )

        assert len(candles) == 3
        # Should be sorted ascending by timestamp
        assert candles[0].timestamp < candles[1].timestamp < candles[2].timestamp
        assert isinstance(candles[0].open, Decimal)
        assert isinstance(candles[0].close, Decimal)
        assert isinstance(candles[0].volume, Decimal)

    @pytest.mark.asyncio
    async def test_fetch_with_search(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Fetch OHLCV by searching for pool first."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value=_make_search_response())
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        ohlcv_resp = AsyncMock()
        ohlcv_resp.status = 200
        ohlcv_resp.json = AsyncMock(return_value=_make_ohlcv_response())
        ohlcv_resp.__aenter__ = AsyncMock(return_value=ohlcv_resp)
        ohlcv_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.closed = False
        # First call = search, second call = OHLCV
        mock_session.get = MagicMock(side_effect=[search_resp, ohlcv_resp])

        provider._session = mock_session

        candles = await provider.get_ohlcv(
            "WETH",
            timeframe="1h",
            limit=100,
            chain="ethereum",
        )

        assert len(candles) == 3
        assert mock_session.get.call_count == 2

    @pytest.mark.asyncio
    async def test_unsupported_chain_raises(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Unsupported chain raises DataSourceUnavailable."""
        with pytest.raises(DataSourceUnavailable, match="Unsupported chain"):
            await provider.get_ohlcv("WETH", chain="solana")

    @pytest.mark.asyncio
    async def test_http_error_raises(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Non-200 HTTP status raises DataSourceUnavailable."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="HTTP 500"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )

    @pytest.mark.asyncio
    async def test_http_429_raises_rate_limit(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """429 status raises DataSourceUnavailable with rate limit message."""
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="Rate limited"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )

    @pytest.mark.asyncio
    async def test_empty_response_raises(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Empty OHLCV list raises DataSourceUnavailable."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response(candles=[]))
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="No OHLCV data"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )


# ---------------------------------------------------------------------------
# Response parsing tests
# ---------------------------------------------------------------------------


class TestParseResponse:
    """Test OHLCV response parsing logic."""

    def test_parse_valid_response(self, provider: GeckoTerminalOHLCVProvider) -> None:
        data = _make_ohlcv_response()
        candles = provider._parse_ohlcv_response(data)
        assert len(candles) == 3
        # Sorted ascending
        assert candles[0].timestamp < candles[1].timestamp
        assert candles[1].timestamp < candles[2].timestamp

    def test_parse_candle_values(self, provider: GeckoTerminalOHLCVProvider) -> None:
        data = _make_ohlcv_response(
            candles=[
                [1700000000, 1800.5, 1812.0, 1795.3, 1810.7, 45000.0],
            ]
        )
        candles = provider._parse_ohlcv_response(data)
        assert len(candles) == 1
        c = candles[0]
        assert c.open == Decimal("1800.5")
        assert c.high == Decimal("1812.0")
        assert c.low == Decimal("1795.3")
        assert c.close == Decimal("1810.7")
        assert c.volume == Decimal("45000.0")
        assert c.timestamp == datetime.fromtimestamp(1700000000, tz=UTC)

    def test_parse_empty_response(self, provider: GeckoTerminalOHLCVProvider) -> None:
        candles = provider._parse_ohlcv_response({})
        assert candles == []

    def test_parse_malformed_entries_skipped(self, provider: GeckoTerminalOHLCVProvider) -> None:
        data = _make_ohlcv_response(
            candles=[
                [1700000000, 1800.0, 1812.0, 1795.0, 1810.0, 45000.0],  # Valid
                [1700003600, "bad"],  # Too short
                [1700007200],  # Way too short
            ]
        )
        candles = provider._parse_ohlcv_response(data)
        assert len(candles) == 1

    def test_parse_missing_attributes(self, provider: GeckoTerminalOHLCVProvider) -> None:
        candles = provider._parse_ohlcv_response({"data": {}})
        assert candles == []

    def test_parse_none_data(self, provider: GeckoTerminalOHLCVProvider) -> None:
        candles = provider._parse_ohlcv_response({"data": None})
        assert candles == []


# ---------------------------------------------------------------------------
# Caching tests
# ---------------------------------------------------------------------------


class TestCaching:
    """Test in-memory cache behavior."""

    @pytest.mark.asyncio
    async def test_cache_hit(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Second call returns cached data without HTTP request."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        # First call
        candles1 = await provider.get_ohlcv("WETH", timeframe="1h", pool_address="0xabc", chain="ethereum")
        # Second call (should be cached)
        candles2 = await provider.get_ohlcv("WETH", timeframe="1h", pool_address="0xabc", chain="ethereum")

        assert candles1 == candles2
        # Only one HTTP call was made
        assert mock_session.get.call_count == 1
        assert provider._metrics.cache_hits == 1

    def test_cache_expiry(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Cache entries expire after TTL."""
        provider._cache_ttl = 1  # 1 second TTL

        # Manually populate cache with expired data
        key = "WETH:ethereum:1h:100:auto"
        candles = [
            OHLCVCandle(
                timestamp=datetime.now(UTC),
                open=Decimal("1800"),
                high=Decimal("1810"),
                low=Decimal("1790"),
                close=Decimal("1805"),
                volume=Decimal("100"),
            )
        ]
        # Set cached_at to 10 seconds ago (well past TTL)
        provider._cache[key] = (candles, time.monotonic() - 10)

        result = provider._get_cached(key)
        assert result is None

    def test_cache_fresh(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Fresh cache entries are returned."""
        key = "WETH:ethereum:1h:100:auto"
        candles = [
            OHLCVCandle(
                timestamp=datetime.now(UTC),
                open=Decimal("1800"),
                high=Decimal("1810"),
                low=Decimal("1790"),
                close=Decimal("1805"),
                volume=Decimal("100"),
            )
        ]
        provider._cache[key] = (candles, time.monotonic())

        result = provider._get_cached(key)
        assert result is not None
        assert len(result) == 1

    def test_clear_cache(self, provider: GeckoTerminalOHLCVProvider) -> None:
        key = "test:key"
        provider._cache[key] = ([], time.monotonic())
        assert len(provider._cache) == 1
        provider.clear_cache()
        assert len(provider._cache) == 0


# ---------------------------------------------------------------------------
# Rate limiter tests
# ---------------------------------------------------------------------------


class TestTokenBucket:
    """Test _TokenBucket rate limiter."""

    def test_initial_tokens(self) -> None:
        bucket = _TokenBucket(rate=5, period=1.0)
        # Should have 5 tokens initially
        for _ in range(5):
            assert bucket.acquire() is True
        # 6th should fail
        assert bucket.acquire() is False

    def test_token_refill(self) -> None:
        bucket = _TokenBucket(rate=10, period=1.0)
        # Consume all tokens
        for _ in range(10):
            bucket.acquire()
        assert bucket.acquire() is False

        # Simulate time passing by manipulating internal state
        bucket._last_refill = time.monotonic() - 1.1  # 1.1 seconds ago
        bucket._tokens = 0.0
        # After refill, should have tokens again
        assert bucket.acquire() is True

    def test_no_exceed_max_tokens(self) -> None:
        bucket = _TokenBucket(rate=5, period=1.0)
        # Wait a long time (simulated)
        bucket._last_refill = time.monotonic() - 100.0
        bucket._tokens = 0.0
        # Should refill to max, not beyond
        assert bucket.acquire() is True
        # Should have at most rate-1 tokens left
        for _ in range(4):
            assert bucket.acquire() is True
        assert bucket.acquire() is False

    @pytest.mark.asyncio
    async def test_rate_limit_blocks_request(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """When rate limiter is exhausted, get_ohlcv raises."""
        # Exhaust the rate limiter
        provider._rate_limiter = _TokenBucket(rate=1, period=60.0)
        provider._rate_limiter.acquire()  # Use the only token

        with pytest.raises(DataSourceUnavailable, match="Rate limited"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )


# ---------------------------------------------------------------------------
# Chain and timeframe mapping tests
# ---------------------------------------------------------------------------


class TestMappings:
    """Test chain-to-network and timeframe-to-GT mappings."""

    def test_chain_to_network_coverage(self) -> None:
        """All expected chains are mapped."""
        expected = {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc", "sonic"}
        assert expected == set(_CHAIN_TO_NETWORK.keys())

    def test_chain_network_values(self) -> None:
        assert _CHAIN_TO_NETWORK["ethereum"] == "eth"
        assert _CHAIN_TO_NETWORK["arbitrum"] == "arbitrum"
        assert _CHAIN_TO_NETWORK["base"] == "base"
        assert _CHAIN_TO_NETWORK["polygon"] == "polygon_pos"
        assert _CHAIN_TO_NETWORK["avalanche"] == "avax"
        assert _CHAIN_TO_NETWORK["bsc"] == "bsc"

    def test_timeframe_mapping_coverage(self) -> None:
        """All OHLCV timeframes are mapped."""
        expected = {"1m", "5m", "15m", "1h", "4h", "1d"}
        assert expected == set(_TIMEFRAME_TO_GT.keys())

    def test_timeframe_mapping_values(self) -> None:
        assert _TIMEFRAME_TO_GT["1m"] == {"aggregate": "1", "timeframe": "minute"}
        assert _TIMEFRAME_TO_GT["1h"] == {"aggregate": "1", "timeframe": "hour"}
        assert _TIMEFRAME_TO_GT["4h"] == {"aggregate": "4", "timeframe": "hour"}
        assert _TIMEFRAME_TO_GT["1d"] == {"aggregate": "1", "timeframe": "day"}


# ---------------------------------------------------------------------------
# Pool search tests
# ---------------------------------------------------------------------------


class TestPoolSearch:
    """Test pool address resolution via search."""

    @pytest.mark.asyncio
    async def test_search_returns_pool_url(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Pool search returns correct OHLCV URL."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value=_make_search_response("0xdeadbeef"))
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        url = await provider._resolve_pool_ohlcv_url("WETH", "USDC", "eth", "hour")
        assert "0xdeadbeef" in url
        assert "/ohlcv/hour" in url

    @pytest.mark.asyncio
    async def test_search_no_pools_raises(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Empty search results raise DataSourceUnavailable."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value={"data": []})
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="No pools found"):
            await provider._resolve_pool_ohlcv_url("UNKNOWNTOKEN", "USDC", "eth", "hour")

    @pytest.mark.asyncio
    async def test_search_http_error_raises(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """Search HTTP error raises DataSourceUnavailable."""
        search_resp = AsyncMock()
        search_resp.status = 500
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="Pool search failed"):
            await provider._resolve_pool_ohlcv_url("WETH", "USDC", "eth", "hour")


# ---------------------------------------------------------------------------
# Metrics tracking tests
# ---------------------------------------------------------------------------


class TestMetrics:
    """Test health metrics tracking."""

    @pytest.mark.asyncio
    async def test_success_increments_metrics(self, provider: GeckoTerminalOHLCVProvider) -> None:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        await provider.get_ohlcv("WETH", timeframe="1h", pool_address="0xabc", chain="ethereum")

        assert provider._metrics.total_requests == 1
        assert provider._metrics.successful_requests == 1
        assert provider._metrics.errors == 0
        assert provider._metrics.total_latency_ms > 0

    @pytest.mark.asyncio
    async def test_error_increments_error_count(self, provider: GeckoTerminalOHLCVProvider) -> None:
        with pytest.raises(DataSourceUnavailable):
            await provider.get_ohlcv("WETH", timeframe="1h", chain="solana")

        assert provider._metrics.total_requests == 1
        assert provider._metrics.errors == 1


# ---------------------------------------------------------------------------
# DataProvider.fetch() sync wrapper tests
# ---------------------------------------------------------------------------


class TestFetchWrapper:
    """Test the synchronous fetch() DataProvider method."""

    def test_fetch_returns_data_envelope(self, provider: GeckoTerminalOHLCVProvider) -> None:
        """fetch() returns a DataEnvelope wrapping candle list."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        envelope = provider.fetch(
            token="WETH",
            timeframe="1h",
            limit=100,
            pool_address="0xabc",
            chain="ethereum",
        )

        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.value, list)
        assert len(envelope.value) == 3
        assert envelope.meta.source == "geckoterminal"
        assert envelope.meta.finality == "off_chain"
        assert envelope.meta.confidence == 0.9


# ---------------------------------------------------------------------------
# Limit capping test
# ---------------------------------------------------------------------------


class TestLimitCapping:
    """Test that limit is capped at 1000."""

    @pytest.mark.asyncio
    async def test_limit_capped_at_1000(self, provider: GeckoTerminalOHLCVProvider) -> None:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        await provider.get_ohlcv(
            "WETH",
            timeframe="1h",
            limit=5000,
            pool_address="0xabc",
            chain="ethereum",
        )

        # Verify the limit param sent to API was capped at 1000
        call_kwargs = mock_session.get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["limit"] == 1000


# ---------------------------------------------------------------------------
# Context manager tests
# ---------------------------------------------------------------------------


class TestContextManager:
    """Test async context manager protocol."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self) -> None:
        async with GeckoTerminalOHLCVProvider() as provider:
            assert isinstance(provider, GeckoTerminalOHLCVProvider)
        # Session should be closed after exit
        assert provider._session is None or provider._session.closed
