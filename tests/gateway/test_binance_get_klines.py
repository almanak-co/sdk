"""Tests for BinanceIntegration.get_klines request building and parsing.

Mirrors the OKX harness style (test_okx_fetch.py): the aiohttp session seam
is mocked via ``_get_session`` and serves canned responses through the base
``_fetch``. Covered: interval validation, symbol normalization, limit
capping, startTime/endTime param building and their cache bypass, the
falsy-zero timestamp gotcha, kline row transformation, the 60s klines
cache, and HTTP error propagation.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from almanak.gateway.integrations.base import IntegrationError
from almanak.gateway.integrations.binance import BinanceIntegration


class FakeResponse:
    """Minimal stand-in for aiohttp.ClientResponse."""

    def __init__(
        self,
        status: int = 200,
        body: object = None,
        headers: dict[str, str] | None = None,
        text_body: str = "",
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self._body = body
        self._text = text_body

    async def json(self):
        return self._body

    async def text(self):
        return self._text


def _session_with(responses: list[FakeResponse], calls: list[dict] | None = None):
    """Build a fake aiohttp session serving canned responses in order."""

    @asynccontextmanager
    async def fake_request(method, url, params=None, json=None, headers=None):
        if calls is not None:
            calls.append(
                {"method": method, "url": url, "params": params, "json": json, "headers": headers}
            )
        yield responses.pop(0)

    session = AsyncMock()
    session.request = fake_request
    return session


def _patched(binance, session):
    return patch.object(binance, "_get_session", AsyncMock(return_value=session))


# Raw Binance kline row: 12 fields, only the first 9 are surfaced.
RAW_KLINE = [
    1700000000000,
    "2000.1",
    "2100.5",
    "1950.0",
    "2050.2",
    "123.45",
    1700003599999,
    "253000.0",
    42,
    "60.0",
    "123456.0",
    "0",
]

EXPECTED_KLINE = {
    "open_time": 1700000000000,
    "open": "2000.1",
    "high": "2100.5",
    "low": "1950.0",
    "close": "2050.2",
    "volume": "123.45",
    "close_time": 1700003599999,
    "quote_volume": "253000.0",
    "trades": 42,
}


@pytest.fixture
def binance():
    return BinanceIntegration()


class TestGetKlinesValidation:
    @pytest.mark.asyncio
    async def test_invalid_interval_raises_before_any_request(self, binance):
        calls: list[dict] = []
        session = _session_with([], calls)
        with _patched(binance, session):
            with pytest.raises(ValueError, match="Invalid interval: 7h"):
                await binance.get_klines("BTCUSDT", interval="7h")
        assert calls == []

    @pytest.mark.asyncio
    async def test_limit_capped_at_1000(self, binance):
        calls: list[dict] = []
        session = _session_with([FakeResponse(body=[])], calls)
        with _patched(binance, session):
            await binance.get_klines("BTCUSDT", limit=5000)
        (call,) = calls
        assert call["params"]["limit"] == 1000


class TestGetKlinesRequestBuilding:
    @pytest.mark.asyncio
    async def test_happy_path_params_and_transformation(self, binance):
        calls: list[dict] = []
        session = _session_with([FakeResponse(body=[RAW_KLINE])], calls)
        with _patched(binance, session):
            klines = await binance.get_klines("ethusdt", interval="4h", limit=7)

        assert klines == [EXPECTED_KLINE]
        (call,) = calls
        assert call["method"] == "GET"
        assert call["url"].endswith("/api/v3/klines")
        # Symbol is upper-cased; no time params unless requested.
        assert call["params"] == {"symbol": "ETHUSDT", "interval": "4h", "limit": 7}
        assert call["json"] is None

    @pytest.mark.asyncio
    async def test_start_time_only_adds_start_param(self, binance):
        calls: list[dict] = []
        session = _session_with([FakeResponse(body=[])], calls)
        with _patched(binance, session):
            await binance.get_klines("BTCUSDT", start_time=1700000000000)
        (call,) = calls
        assert call["params"]["startTime"] == 1700000000000
        assert "endTime" not in call["params"]

    @pytest.mark.asyncio
    async def test_end_time_only_adds_end_param(self, binance):
        calls: list[dict] = []
        session = _session_with([FakeResponse(body=[])], calls)
        with _patched(binance, session):
            await binance.get_klines("BTCUSDT", end_time=1700009999999)
        (call,) = calls
        assert call["params"]["endTime"] == 1700009999999
        assert "startTime" not in call["params"]


class TestGetKlinesCaching:
    @pytest.mark.asyncio
    async def test_second_call_served_from_cache(self, binance):
        calls: list[dict] = []
        session = _session_with([FakeResponse(body=[RAW_KLINE])], calls)
        with _patched(binance, session):
            first = await binance.get_klines("BTCUSDT")
            second = await binance.get_klines("BTCUSDT")

        assert first == second == [EXPECTED_KLINE]
        assert len(calls) == 1  # second call never hit the session
        assert binance._metrics.cache_hits == 1

    @pytest.mark.asyncio
    async def test_time_ranged_queries_bypass_cache(self, binance):
        responses = [FakeResponse(body=[RAW_KLINE]), FakeResponse(body=[RAW_KLINE])]
        calls: list[dict] = []
        session = _session_with(responses, calls)
        with _patched(binance, session):
            await binance.get_klines("BTCUSDT", start_time=1, end_time=2)
            await binance.get_klines("BTCUSDT", start_time=1, end_time=2)

        # Specific time ranges are never cached: both calls hit the API.
        assert len(calls) == 2
        assert binance._metrics.cache_hits == 0

    @pytest.mark.asyncio
    async def test_zero_timestamps_dropped_from_params_but_still_bypass_cache(self, binance):
        # Asymmetry in the implementation: param building uses truthiness
        # (0 is dropped from the query), while cache gating uses `is None`
        # (0 still counts as a ranged query, so nothing is cached).
        calls: list[dict] = []
        responses = [FakeResponse(body=[RAW_KLINE]), FakeResponse(body=[RAW_KLINE])]
        session = _session_with(responses, calls)
        with _patched(binance, session):
            await binance.get_klines("BTCUSDT", start_time=0, end_time=0)
            await binance.get_klines("BTCUSDT", start_time=0, end_time=0)

        assert len(calls) == 2
        for call in calls:
            assert "startTime" not in call["params"]
            assert "endTime" not in call["params"]
        assert binance._metrics.cache_hits == 0


class TestGetKlinesErrors:
    @pytest.mark.asyncio
    async def test_http_error_propagates_integration_error(self, binance):
        session = _session_with([FakeResponse(status=500, text_body="internal boom")])
        with _patched(binance, session):
            with pytest.raises(IntegrationError, match="HTTP 500: internal boom") as exc_info:
                await binance.get_klines("BTCUSDT")
        assert exc_info.value.code == "HTTP_500"
        # Failed fetches must not populate the klines cache.
        assert binance._get_cached("klines:BTCUSDT:1h:100") is None
