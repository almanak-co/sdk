"""Branch-coverage tests for ``BinanceOHLCVProvider.get_ohlcv``.

Complements ``test_binance_provider.py`` (symbol resolution) by exercising the
full ``get_ohlcv`` pipeline: timeframe validation, limit capping, cache
hit/expiry, symbol/interval resolution failures, every HTTP error branch
(rate-limit, 4xx eviction, 5xx no-eviction, empty payload), the success-path
kline parsing (including short-row skipping), and the transport-level
``aiohttp.ClientError`` / ``TimeoutError`` handlers.

All HTTP traffic is mocked at the ``_get_session`` seam — these tests never
reach the real Binance API.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.gateway.data.ohlcv import binance_provider as bp
from almanak.gateway.data.ohlcv.binance_provider import BinanceOHLCVProvider

# Two realistic Binance kline rows (open_time_ms, open, high, low, close,
# volume, close_time, quote_volume, trades, taker_base, taker_quote, ignore).
KLINE_1 = [
    1700000000000,
    "3000.10",
    "3050.55",
    "2990.00",
    "3020.25",
    "123.456",
    1700000299999,
    "372000.0",
    100,
    "60.0",
    "181000.0",
    "0",
]
KLINE_2 = [
    1700000300000,
    "3020.25",
    "3080.00",
    "3010.00",
    "3075.50",
    "98.765",
    1700000599999,
    "301000.0",
    90,
    "50.0",
    "152000.0",
    "0",
]


def _make_response(status: int = 200, json_data: object = None, text_data: str = "") -> AsyncMock:
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data)
    resp.text = AsyncMock(return_value=text_data)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _make_session(response: AsyncMock | None = None, side_effect: Exception | None = None) -> MagicMock:
    session = MagicMock()
    if side_effect is not None:
        session.get = MagicMock(side_effect=side_effect)
    else:
        session.get = MagicMock(return_value=response)
    return session


@pytest.fixture()
def provider() -> BinanceOHLCVProvider:
    return BinanceOHLCVProvider()


class TestGetOhlcvValidation:
    """Pre-flight validation branches: timeframe, symbol, interval."""

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("timeframe", ["2m", "45m", "bogus", ""])
    async def test_invalid_timeframe_raises_value_error(self, provider, timeframe):
        """validate_timeframe rejects before any metrics or HTTP work."""
        with pytest.raises(ValueError, match="Invalid timeframe"):
            await provider.get_ohlcv("WETH", timeframe=timeframe)
        assert provider.get_health_metrics()["total_requests"] == 0

    @pytest.mark.asyncio()
    async def test_unknown_token_raises_data_source_unavailable(self, provider):
        """Static miss + dynamic miss -> DataSourceUnavailable with the token name."""
        with patch.object(provider, "_resolve_symbol_dynamic", AsyncMock(return_value=None)):
            with pytest.raises(DataSourceUnavailable) as exc_info:
                await provider.get_ohlcv("ZZZUNKNOWN", timeframe="1h")

        assert exc_info.value.source == "binance_ohlcv"
        assert "ZZZUNKNOWN" in exc_info.value.reason
        assert provider.get_health_metrics()["errors"] == 1

    @pytest.mark.asyncio()
    async def test_non_alnum_token_fails_without_any_http(self, provider):
        """Non-alphanumeric tokens are rejected by the dynamic-probe URL guard
        before any session is created — no HTTP call is ever made."""
        with patch.object(provider, "_get_session", side_effect=AssertionError("no HTTP expected")):
            with pytest.raises(DataSourceUnavailable, match="Unknown token"):
                await provider.get_ohlcv("ZZ_9", timeframe="1h")

    @pytest.mark.asyncio()
    async def test_timeframe_missing_from_interval_map(self, provider, monkeypatch):
        """A timeframe that passes validate_timeframe but has no Binance interval
        mapping raises DataSourceUnavailable (defensive branch: every entry of
        VALID_TIMEFRAMES is currently present in BINANCE_INTERVAL_MAP)."""
        monkeypatch.delitem(bp.BINANCE_INTERVAL_MAP, "1h")

        with pytest.raises(DataSourceUnavailable, match="Unsupported timeframe: 1h"):
            await provider.get_ohlcv("WETH", timeframe="1h")
        assert provider.get_health_metrics()["errors"] == 1


class TestGetOhlcvCacheAndLimit:
    """Cache-hit, cache-expiry, and limit-capping branches."""

    @pytest.mark.asyncio()
    async def test_cache_hit_skips_http_and_counts_metrics(self, provider):
        session = _make_session(_make_response(json_data=[KLINE_1, KLINE_2]))

        with patch.object(provider, "_get_session", return_value=session):
            first = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)
            second = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)

        assert second == first
        assert session.get.call_count == 1
        metrics = provider.get_health_metrics()
        assert metrics["total_requests"] == 2
        assert metrics["successful_requests"] == 2
        assert metrics["cache_hits"] == 1

    @pytest.mark.asyncio()
    async def test_expired_cache_entry_refetches(self, provider):
        stale = [OHLCVCandle(timestamp=datetime(2020, 1, 1, tzinfo=UTC), open=1, high=1, low=1, close=1, volume=1)]
        provider._update_cache("WETH", "1h", 100, stale)
        # Backdate the entry beyond the TTL so _get_cached treats it as expired.
        key = provider._get_cache_key("WETH", "1h", 100)
        entry = provider._cache[key]
        entry.cached_at = datetime(2020, 1, 1, tzinfo=UTC)

        session = _make_session(_make_response(json_data=[KLINE_1]))
        with patch.object(provider, "_get_session", return_value=session):
            result = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)

        assert session.get.call_count == 1
        assert len(result) == 1
        assert result[0].close == Decimal("3020.25")
        assert provider.get_health_metrics()["cache_hits"] == 0

    @pytest.mark.asyncio()
    async def test_limit_capped_at_binance_maximum(self, provider):
        session = _make_session(_make_response(json_data=[KLINE_1]))

        with patch.object(provider, "_get_session", return_value=session):
            await provider.get_ohlcv("WETH", timeframe="1h", limit=5000)

        params = session.get.call_args.kwargs["params"]
        assert params["limit"] == 1000


class TestGetOhlcvHttpErrors:
    """HTTP status-code branches inside the request block."""

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("status", [429, 418])
    async def test_rate_limited_raises_with_retry_after(self, provider, status):
        session = _make_session(_make_response(status=status))

        with patch.object(provider, "_get_session", return_value=session):
            with pytest.raises(DataSourceUnavailable) as exc_info:
                await provider.get_ohlcv("WETH", timeframe="1h")

        assert exc_info.value.retry_after == 60.0
        assert f"HTTP {status}" in exc_info.value.reason
        assert provider.get_health_metrics()["errors"] == 1

    @pytest.mark.asyncio()
    async def test_definitive_4xx_evicts_stale_dynamic_mapping(self, provider):
        provider._dynamic_symbol_cache["FOO"] = "FOOUSDT"
        session = _make_session(_make_response(status=400, text_data="Invalid symbol"))

        with patch.object(provider, "_get_session", return_value=session):
            with pytest.raises(DataSourceUnavailable, match="HTTP 400: Invalid symbol"):
                await provider.get_ohlcv("FOO", timeframe="1h")

        assert "FOO" not in provider._dynamic_symbol_cache

    @pytest.mark.asyncio()
    async def test_4xx_without_dynamic_mapping_just_raises(self, provider):
        session = _make_session(_make_response(status=404, text_data="not found"))

        with patch.object(provider, "_get_session", return_value=session):
            with pytest.raises(DataSourceUnavailable, match="HTTP 404"):
                await provider.get_ohlcv("WETH", timeframe="1h")

        assert provider.get_health_metrics()["errors"] == 1

    @pytest.mark.asyncio()
    async def test_5xx_does_not_evict_dynamic_mapping(self, provider):
        """Server errors are transient: the dynamic mapping must survive."""
        provider._dynamic_symbol_cache["FOO"] = "FOOUSDT"
        session = _make_session(_make_response(status=503, text_data="unavailable"))

        with patch.object(provider, "_get_session", return_value=session):
            with pytest.raises(DataSourceUnavailable, match="HTTP 503"):
                await provider.get_ohlcv("FOO", timeframe="1h")

        assert provider._dynamic_symbol_cache["FOO"] == "FOOUSDT"

    @pytest.mark.asyncio()
    async def test_empty_payload_raises(self, provider):
        session = _make_session(_make_response(json_data=[]))

        with patch.object(provider, "_get_session", return_value=session):
            with pytest.raises(DataSourceUnavailable, match="No OHLCV data returned for WETH"):
                await provider.get_ohlcv("WETH", timeframe="1h")

        assert provider.get_health_metrics()["errors"] == 1


class TestGetOhlcvSuccess:
    """Happy-path parsing, caching, and metrics."""

    @pytest.mark.asyncio()
    async def test_parses_klines_and_skips_short_rows(self, provider, monkeypatch):
        short_row = [1700000600000, "3075.50", "3090.00"]  # len < 6 -> skipped
        session = _make_session(_make_response(json_data=[KLINE_1, KLINE_2, short_row]))

        # Deterministic clock scoped to the provider module: every call
        # advances 50ms, so the recorded latency is strictly positive even
        # though the mocked request itself is instantaneous.
        import itertools
        from types import SimpleNamespace

        ticks = itertools.count(start=1)
        monkeypatch.setattr(
            bp, "time", SimpleNamespace(time=lambda: next(ticks) * 0.05)
        )

        with patch.object(provider, "_get_session", return_value=session):
            result = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)

        assert len(result) == 2
        first = result[0]
        assert isinstance(first, OHLCVCandle)
        assert first.timestamp == datetime.fromtimestamp(1700000000, tz=UTC)
        assert first.open == Decimal("3000.10")
        assert first.high == Decimal("3050.55")
        assert first.low == Decimal("2990.00")
        assert first.close == Decimal("3020.25")
        assert first.volume == Decimal("123.456")
        assert result[1].close == Decimal("3075.50")

        url = session.get.call_args.args[0]
        params = session.get.call_args.kwargs["params"]
        assert url == f"{BinanceOHLCVProvider.API_BASE}/klines"
        assert params == {"symbol": "ETHUSDT", "interval": "1h", "limit": 100}

        metrics = provider.get_health_metrics()
        assert metrics["total_requests"] == 1
        assert metrics["successful_requests"] == 1
        assert metrics["errors"] == 0
        assert metrics["average_latency_ms"] > 0

        # The parsed result is cached for subsequent calls.
        assert provider._get_cached("WETH", "1h", 100) == result

    @pytest.mark.asyncio()
    async def test_token_uppercased_and_quote_ignored(self, provider):
        session = _make_session(_make_response(json_data=[KLINE_1]))

        with patch.object(provider, "_get_session", return_value=session):
            result = await provider.get_ohlcv("weth", quote="EUR", timeframe="5m")

        assert len(result) == 1
        params = session.get.call_args.kwargs["params"]
        assert params["symbol"] == "ETHUSDT"
        assert params["interval"] == "5m"

    @pytest.mark.asyncio()
    async def test_success_via_dynamically_resolved_symbol(self, provider):
        """Static-map miss falls through to dynamic resolution (cached here)."""
        provider._dynamic_symbol_cache["FOO"] = "FOOUSDT"
        session = _make_session(_make_response(json_data=[KLINE_1]))

        with patch.object(provider, "_get_session", return_value=session):
            result = await provider.get_ohlcv("foo", timeframe="1h")

        assert len(result) == 1
        assert session.get.call_args.kwargs["params"]["symbol"] == "FOOUSDT"


class TestGetOhlcvTransportErrors:
    """aiohttp.ClientError and TimeoutError handler branches."""

    @pytest.mark.asyncio()
    async def test_client_error_wrapped_as_data_source_unavailable(self, provider):
        session = _make_session(side_effect=aiohttp.ClientError("connection reset by peer"))

        with patch.object(provider, "_get_session", return_value=session):
            with pytest.raises(DataSourceUnavailable) as exc_info:
                await provider.get_ohlcv("WETH", timeframe="1h")

        assert exc_info.value.source == "binance_ohlcv"
        assert "connection reset by peer" in exc_info.value.reason
        assert isinstance(exc_info.value.__cause__, aiohttp.ClientError)
        assert provider.get_health_metrics()["errors"] == 1

    @pytest.mark.asyncio()
    async def test_timeout_wrapped_with_configured_timeout_in_reason(self):
        provider = BinanceOHLCVProvider(request_timeout=5.0)
        resp = _make_response()
        resp.__aenter__ = AsyncMock(side_effect=TimeoutError())
        session = _make_session(resp)

        with patch.object(provider, "_get_session", return_value=session):
            with pytest.raises(DataSourceUnavailable, match="Timeout after 5.0s"):
                await provider.get_ohlcv("WETH", timeframe="1h")

        assert provider.get_health_metrics()["errors"] == 1
