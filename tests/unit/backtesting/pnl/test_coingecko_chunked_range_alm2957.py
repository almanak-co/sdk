"""Chunked market_chart/range fetching (ALM-2957).

CoinGecko decides granularity from the REQUESTED range length: hourly up to
90 days, silently DAILY above. A single 6-month request therefore came back
daily, the engine spread ~182 points across hourly ticks, and hourly
indicators saturated (RSI pinned ~0/100 — band-gated strategies froze).
These tests pin the fix: long ranges are fetched in ≤80-day chunks so every
point stays hourly, seams are deduped, and a failed chunk fails the fetch.

The fake _make_request reproduces CoinGecko's auto-granularity contract, so
reverting the chunking makes the resolution test fail with daily spacing.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.coingecko import (
    _CG_HOURLY_SAFE_RANGE_SECONDS,
    CoinGeckoDataProvider,
)

START = datetime(2026, 1, 17, tzinfo=UTC)
END_182D = datetime(2026, 7, 18, tzinfo=UTC)
END_30D = datetime(2026, 2, 16, tzinfo=UTC)

_DAY = 86400


def _autogranularity_response(params: dict) -> dict:
    """CoinGecko's documented behavior: hourly ≤90d, daily above."""
    start = int(params["from"])
    end = int(params["to"])
    step = 3600 if (end - start) <= 90 * _DAY else _DAY
    return {
        "prices": [
            [ts * 1000, 3000.0 + (ts % 97)]  # deterministic, non-flat
            for ts in range(start, end + 1, step)
        ]
    }


def _provider_with_fake_cg() -> tuple[CoinGeckoDataProvider, AsyncMock]:
    provider = CoinGeckoDataProvider()
    fake = AsyncMock(side_effect=lambda path, params: _autogranularity_response(params))
    return provider, fake


class TestChunkedRange:
    @pytest.mark.asyncio
    async def test_long_range_stays_hourly(self):
        provider, fake = _provider_with_fake_cg()
        with (
            patch.object(provider, "_make_request", fake),
            patch.object(provider, "_resolve_token_id", AsyncMock(return_value="weth")),
        ):
            candles = await provider.get_ohlcv("WETH", START, END_182D, 3600)

        # Every fetched chunk stayed under the daily-granularity cliff...
        assert fake.await_count >= 3
        for call in fake.await_args_list:
            params = call.args[1]
            assert int(params["to"]) - int(params["from"]) <= _CG_HOURLY_SAFE_RANGE_SECONDS
        # ...so the stitched series is HOURLY across all 182 days: the exact
        # property whose absence froze RSI-gated strategies (ALM-2957).
        spacings = {
            int((b.timestamp - a.timestamp).total_seconds())
            for a, b in zip(candles, candles[1:], strict=False)
        }
        assert spacings == {3600}
        assert len(candles) == (int(END_182D.timestamp()) - int(START.timestamp())) // 3600 + 1

    @pytest.mark.asyncio
    async def test_seam_candles_are_deduped_and_sorted(self):
        provider, fake = _provider_with_fake_cg()
        with (
            patch.object(provider, "_make_request", fake),
            patch.object(provider, "_resolve_token_id", AsyncMock(return_value="weth")),
        ):
            candles = await provider.get_ohlcv("WETH", START, END_182D, 3600)

        timestamps = [c.timestamp for c in candles]
        assert timestamps == sorted(timestamps)
        assert len(timestamps) == len(set(timestamps))  # chunk edges appear once

    @pytest.mark.asyncio
    async def test_short_range_is_a_single_request(self):
        provider, fake = _provider_with_fake_cg()
        with (
            patch.object(provider, "_make_request", fake),
            patch.object(provider, "_resolve_token_id", AsyncMock(return_value="weth")),
        ):
            candles = await provider.get_ohlcv("WETH", START, END_30D, 3600)

        assert fake.await_count == 1
        assert len(candles) > 0

    @pytest.mark.asyncio
    async def test_failed_chunk_fails_the_whole_fetch(self):
        # A silently missing span would re-create the flat plane the fix
        # removes — fail loud instead, matching the single-request contract.
        provider = CoinGeckoDataProvider()
        calls = {"n": 0}

        async def flaky(path, params):
            calls["n"] += 1
            if calls["n"] == 2:
                raise ValueError("CoinGecko API error 502: bad gateway")
            return _autogranularity_response(params)

        with (
            patch.object(provider, "_make_request", AsyncMock(side_effect=flaky)),
            patch.object(provider, "_resolve_token_id", AsyncMock(return_value="weth")),
        ):
            with pytest.raises(ValueError, match="502"):
                await provider.get_ohlcv("WETH", START, END_182D, 3600)


class TestMeasuredGranularity:
    def test_daily_candles_measured_as_daily(self):
        from decimal import Decimal

        from almanak.framework.backtesting.pnl.data_provider import OHLCV

        def _candles(step_s: int, n: int) -> list:
            return [
                OHLCV(
                    timestamp=datetime.fromtimestamp(1750000000 + i * step_s, tz=UTC),
                    open=Decimal("1"), high=Decimal("1"), low=Decimal("1"), close=Decimal("1"),
                    volume=None,
                )
                for i in range(n)
            ]

        measure = CoinGeckoDataProvider._measure_granularity
        assert measure({"WETH": _candles(86400, 10)}) == 86400
        assert measure({"WETH": _candles(3600, 10)}) == 3600
        # Coarsest token wins: one daily-served token degrades the plane.
        assert measure({"WETH": _candles(3600, 10), "ARB": _candles(86400, 10)}) == 86400
        assert measure({"WETH": _candles(3600, 1)}) is None  # single candle: unmeasurable
        assert measure({}) is None


class TestChunkingIsIntervalAware:
    @pytest.mark.asyncio
    async def test_daily_tick_backtest_keeps_the_single_request(self):
        # A daily-or-coarser tick grid is honestly served by CG's daily
        # points — chunking would spend calls to fetch resolution nothing
        # reads (the measured-granularity backstop still guards the result).
        provider, fake = _provider_with_fake_cg()
        with (
            patch.object(provider, "_make_request", fake),
            patch.object(provider, "_resolve_token_id", AsyncMock(return_value="weth")),
        ):
            candles = await provider.get_ohlcv("WETH", START, END_182D, 86400)

        assert fake.await_count == 1
        assert len(candles) > 0
