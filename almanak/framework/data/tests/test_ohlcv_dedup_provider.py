"""Tests for DedupingOHLCVProvider (VIB-3783 stop-gap).

Verifies the per-strategy OHLCV request-coalescing wrapper:
- Tail-slice equivalence: large fetch followed by smaller fetch hits cache.
- Cache miss when cached series is too small.
- Don't shrink cache on smaller successful fetch.
- Per-key separation (different token / quote / timeframe must not collide).
- Inner provider exceptions pass through unchanged.
- clear() drops all cached entries.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.ohlcv.dedup_provider import DedupingOHLCVProvider


def _make_candles(n: int) -> list[OHLCVCandle]:
    """Build a deterministic series of n candles, oldest-first."""
    base = datetime(2026, 4, 30, tzinfo=UTC)
    return [
        OHLCVCandle(
            timestamp=base + timedelta(hours=i),
            open=Decimal(f"{1000 + i}"),
            high=Decimal(f"{1010 + i}"),
            low=Decimal(f"{990 + i}"),
            close=Decimal(f"{1005 + i}"),
            volume=Decimal(f"{100 + i}"),
        )
        for i in range(n)
    ]


class _StaticInnerProvider:
    """Inner OHLCV provider whose ``get_ohlcv`` returns the latest ``limit`` candles
    from a pre-built 85-candle series. Mirrors the real-provider contract.
    """

    supported_timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]

    def __init__(self, full_series: list[OHLCVCandle]) -> None:
        self._series = full_series
        self.calls: list[dict] = []

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        self.calls.append({"token": token, "quote": quote, "timeframe": timeframe, "limit": limit})
        return self._series[-limit:]


@pytest.mark.asyncio
async def test_tail_slice_equivalence_after_larger_fetch() -> None:
    """Larger fetch primes cache; smaller follow-up fetch is served via tail-slice."""
    full_series = _make_candles(85)
    inner = _StaticInnerProvider(full_series)
    provider = DedupingOHLCVProvider(inner)

    # First request: limit=85 hits upstream and primes the cache.
    first = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    assert first == full_series
    assert len(inner.calls) == 1

    # Second request: limit=34 is fully covered by the cached 85.
    second = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=34)

    # Returned series is the tail of the cached 85 -- bit-for-bit identical
    # to what the upstream would have returned for limit=34.
    assert second == full_series[-34:]
    assert len(second) == 34
    # Upstream was NOT called a second time.
    assert len(inner.calls) == 1


@pytest.mark.asyncio
async def test_cache_miss_when_cached_series_smaller_than_request() -> None:
    """A cached series of 10 candles cannot satisfy a limit=85 follow-up."""
    full_series = _make_candles(85)
    inner = _StaticInnerProvider(full_series)
    provider = DedupingOHLCVProvider(inner)

    first = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=10)
    assert len(first) == 10
    assert len(inner.calls) == 1

    second = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    assert len(second) == 85
    # Cache miss -- upstream invoked a second time.
    assert len(inner.calls) == 2
    assert inner.calls[-1]["limit"] == 85


@pytest.mark.asyncio
async def test_cache_does_not_shrink_on_smaller_followup() -> None:
    """Once cached at 85, a successful 10-candle hit must not replace the entry."""
    full_series = _make_candles(85)
    inner = _StaticInnerProvider(full_series)
    provider = DedupingOHLCVProvider(inner)

    # Prime with 85.
    await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    assert len(inner.calls) == 1

    # Smaller hit (10) -- served from cache.
    smaller = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=10)
    assert len(smaller) == 10
    assert len(inner.calls) == 1  # still no upstream

    # Mid-size hit (50) -- must still be served from the cached 85.
    mid = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=50)
    assert len(mid) == 50
    assert mid == full_series[-50:]
    assert len(inner.calls) == 1  # cache survived


@pytest.mark.asyncio
async def test_keys_do_not_collide_across_token_quote_timeframe() -> None:
    """(token, quote, timeframe) is the cache key -- no cross-key collisions."""
    full_series = _make_candles(85)
    inner = _StaticInnerProvider(full_series)
    provider = DedupingOHLCVProvider(inner)

    await provider.get_ohlcv(token="WETH", quote="USD", timeframe="1h", limit=85)
    await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    await provider.get_ohlcv(token="WETH", quote="EUR", timeframe="1h", limit=85)
    await provider.get_ohlcv(token="WETH", quote="USD", timeframe="4h", limit=85)

    # Each distinct key triggered a fresh upstream call.
    assert len(inner.calls) == 4

    # Re-querying a previously-seen key with limit <= 85 hits cache.
    cached_hit = await provider.get_ohlcv(token="WETH", quote="USD", timeframe="1h", limit=20)
    assert len(cached_hit) == 20
    assert len(inner.calls) == 4  # no new upstream


@pytest.mark.asyncio
async def test_inner_provider_exception_passes_through() -> None:
    """If the inner provider raises, the wrapper raises -- nothing is swallowed.

    Asserts the exception instance payload (``source`` + ``reason``) is preserved
    too, not just the exception class -- catches a hypothetical regression where
    the wrapper might re-raise a fresh exception with a degraded message.
    """
    inner = AsyncMock()
    inner.supported_timeframes = ["1h"]
    inner.get_ohlcv.side_effect = DataSourceUnavailable(source="upstream", reason="rate-limited")
    provider = DedupingOHLCVProvider(inner)

    with pytest.raises(DataSourceUnavailable) as exc_info:
        await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)

    assert exc_info.value.source == "upstream"
    assert exc_info.value.reason == "rate-limited"


@pytest.mark.asyncio
async def test_clear_drops_all_cached_entries() -> None:
    """clear() empties the cache so the next request re-fetches upstream."""
    full_series = _make_candles(85)
    inner = _StaticInnerProvider(full_series)
    provider = DedupingOHLCVProvider(inner)

    await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    await provider.get_ohlcv(token="WETH", quote="USD", timeframe="1h", limit=85)
    assert len(inner.calls) == 2

    # Cache hit (no upstream).
    await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=34)
    assert len(inner.calls) == 2

    provider.clear()

    # After clear, upstream is hit again.
    await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=34)
    assert len(inner.calls) == 3


@pytest.mark.asyncio
async def test_supported_timeframes_passes_through() -> None:
    """Wrapper exposes the inner provider's supported_timeframes property."""
    inner = _StaticInnerProvider(_make_candles(10))
    provider = DedupingOHLCVProvider(inner)
    assert provider.supported_timeframes == inner.supported_timeframes


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_limit", [0, -1, -100])
async def test_non_positive_limit_rejected(bad_limit: int) -> None:
    """limit=0 would return the full cached series via cached[-0:]; negative
    limits yield arbitrary windows. Both are wrong -- the wrapper raises.
    """
    full_series = _make_candles(85)
    inner = _StaticInnerProvider(full_series)
    provider = DedupingOHLCVProvider(inner)

    # Prime the cache so we exercise the cache-hit path too -- both paths
    # must reject non-positive limits.
    await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    primed_calls = len(inner.calls)

    with pytest.raises(ValueError, match="limit must be a positive integer"):
        await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=bad_limit)

    # Upstream is NOT invoked on the rejected call.
    assert len(inner.calls) == primed_calls


@pytest.mark.asyncio
async def test_returned_list_is_safe_to_mutate() -> None:
    """Caller mutations of the returned list must not corrupt the cache.

    Both the cache-miss and cache-hit paths return tail-slice copies, so a
    caller that does ``result.clear()`` or ``result.pop()`` cannot poison the
    next get_ohlcv() call.
    """
    full_series = _make_candles(85)
    inner = _StaticInnerProvider(full_series)
    provider = DedupingOHLCVProvider(inner)

    # Cache-miss path -- caller mutates returned list.
    miss_result = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    miss_result.clear()
    miss_result.append("garbage")  # type: ignore[arg-type]

    # Cache-hit path on next call must yield the original 85 candles intact.
    hit_result = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=85)
    assert hit_result == full_series
    assert len(inner.calls) == 1  # still only one upstream call

    # Caller mutates the cache-hit result too -- cache should remain intact.
    hit_result.clear()
    hit_again = await provider.get_ohlcv(token="cbBTC", quote="USD", timeframe="1h", limit=34)
    assert hit_again == full_series[-34:]
    assert len(inner.calls) == 1
