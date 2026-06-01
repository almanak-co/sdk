"""POOL-6 (VIB-4754) finality re-promotion — umbrella UAT card §D3.F9.

A bar at ``t = now - 23h`` is provisional; the same bar at ``t = now - 25h`` is
finalized (24h cutoff). The PUBLIC cache key MUST be stable across this
promotion — only the TTL band flips, in place, with no miss-then-insert window
for a concurrent reader.

Re-promotion lives on ``HistoryCache`` (the ``repromoter`` hook), so the core
mechanism is tested there deterministically with an injected clock — no
freezegun dependency. ``test_repromote_public_entry_*`` then pins the servicer's
domain-specific finality decision, and ``test_provisional_response_*`` proves the
handler stamps ``finalized_only=False`` for a recent trailing bar.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, patch

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services._history_cache import (
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    HistoryCache,
)
from almanak.gateway.services.pool_history_service import PoolHistoryServiceServicer

HOUR = 3600
DAY = 86400
_CUTOFF = 24 * HOUR  # the_graph / geckoterminal finality cutoff (matches settings default)
_ARB_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"


class _Clock:
    """Settable monotonic-ish clock standing in for wall time."""

    def __init__(self, t: float) -> None:
        self.t = float(t)

    def __call__(self) -> float:
        return self.t


def _response(newest_ts: int) -> gateway_pb2.PoolHistoryResponse:
    return gateway_pb2.PoolHistoryResponse(
        snapshots=[gateway_pb2.PoolSnapshot(timestamp=newest_ts, volume_24h="50")],
        source="the_graph",
        finalized_only=False,
        success=True,
    )


def _make_repromoter(clock: _Clock, calls: list[int]):
    """Repromoter mirroring ``PoolHistoryServiceServicer._repromote_public_entry``
    but reading ``clock`` (the same clock the cache uses for TTL) as wall-now."""

    def _repromote(value: gateway_pb2.PoolHistoryResponse) -> str | None:
        calls.append(1)
        newest = max((int(s.timestamp) for s in value.snapshots), default=0)
        if (int(clock()) - newest) > _CUTOFF:
            value.finalized_only = True
            return FINALITY_FINALIZED
        return None

    return _repromote


def _cache(clock: _Clock, repromoter) -> HistoryCache:
    return HistoryCache(
        max_entries=16,
        max_bytes=10**9,
        size_estimator=lambda v: v.ByteSize(),
        provisional_ttl_seconds=60.0,
        finalized_ttl_seconds=float(DAY),
        clock=clock,
        repromoter=repromoter,
        name="test_public",
    )


@pytest.mark.acceptance_pack
def test_finality_re_promotion_stable_cache_key() -> None:
    # Newest bar timestamp T; "now" starts at T+23h (provisional).
    newest = 1_700_000_000
    clock = _Clock(newest + 23 * HOUR)
    calls: list[int] = []
    cache = _cache(clock, _make_repromoter(clock, calls))
    key = ("arbitrum", _ARB_POOL, "uniswap_v3", newest - 7 * DAY, newest + HOUR, 1, FINALITY_FINALIZED)

    value = _response(newest)
    cache.put(key, value, FINALITY_PROVISIONAL)
    assert value.finalized_only is False

    # Within the provisional TTL: a normal hit, still provisional.
    got = cache.get(key)
    assert got is value
    assert got.finalized_only is False

    # Advance to T+25h: the provisional TTL (60s) has long expired AND the bar
    # has aged past the 24h cutoff. The GET re-promotes in place.
    clock.t = newest + 25 * HOUR
    repromoted = cache.get(key)
    assert repromoted is value  # SAME object — no re-shape
    assert repromoted.finalized_only is True  # flipped to finalized
    assert calls == [1]  # repromoter consulted exactly once

    # The key is unchanged and now carries the long finalized TTL: a subsequent
    # GET well past the provisional window is still a hit (not re-fetched).
    clock.t = newest + 25 * HOUR + 5 * 60  # +5min, far beyond the 60s provisional TTL
    still_there = cache.get(key)
    assert still_there is value
    assert cache.stats()["cache_misses"] == 0


def test_finality_no_repromote_while_within_cutoff() -> None:
    # Provisional TTL expires but the bar is STILL within the cutoff: the
    # repromoter declines, so the entry is evicted and the GET is a miss
    # (forcing a fresh fetch that would catch any revision).
    newest = 1_700_000_000
    clock = _Clock(newest + 10 * HOUR)
    calls: list[int] = []
    cache = _cache(clock, _make_repromoter(clock, calls))
    key = ("arbitrum", _ARB_POOL, "uniswap_v3", newest - DAY, newest + HOUR, 1, FINALITY_FINALIZED)
    cache.put(key, _response(newest), FINALITY_PROVISIONAL)

    clock.t = newest + 10 * HOUR + 120  # +2min: provisional TTL expired, still 10h<24h old
    assert cache.get(key) is None  # miss — not re-promoted
    assert calls == [1]
    assert cache.stats()["cache_misses"] == 1


def test_finality_re_promotion_concurrent_readers_no_miss() -> None:
    # Two threads hit the SAME expired-provisional key at the flip instant. The
    # cache lock serializes re-promotion: both readers get the value (never a
    # miss-then-insert), and the repromoter runs exactly once.
    newest = 1_700_000_000
    clock = _Clock(newest + 25 * HOUR)  # already aged past cutoff
    calls: list[int] = []
    cache = _cache(clock, _make_repromoter(clock, calls))
    key = ("arbitrum", _ARB_POOL, "uniswap_v3", newest - 7 * DAY, newest + HOUR, 1, FINALITY_FINALIZED)
    value = _response(newest)
    # Seed as provisional with an already-expired TTL by putting then advancing.
    put_clock = _Clock(newest)  # put "in the past" so the 60s TTL is expired at 25h
    cache._clock = put_clock  # type: ignore[attr-defined]
    cache.put(key, value, FINALITY_PROVISIONAL)
    cache._clock = clock  # type: ignore[attr-defined]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: cache.get(key), range(2)))

    assert all(r is value for r in results)  # both readers got the entry
    assert value.finalized_only is True
    assert calls == [1]  # serialized: re-promoted once, second reader hit the finalized entry
    assert cache.stats()["cache_misses"] == 0


def test_repromote_public_entry_flips_when_aged() -> None:
    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True))
    aged = _response(int(time.time()) - 100 * DAY)  # far past any cutoff
    band = servicer._repromote_public_entry(aged)
    assert band == FINALITY_FINALIZED
    assert aged.finalized_only is True


def test_repromote_public_entry_declines_when_recent() -> None:
    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True))
    recent = _response(int(time.time()) - HOUR)  # within the 24h cutoff
    band = servicer._repromote_public_entry(recent)
    assert band is None
    assert recent.finalized_only is False


def test_provisional_response_when_trailing_bar_recent() -> None:
    # A fresh window whose newest bar is within the cutoff is stamped
    # finalized_only=False (the trailing bar is provisional).
    end = (int(time.time()) // HOUR) * HOUR
    start = end - 7 * DAY
    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True))

    async def _query(*, url: str, query: str, variables: dict) -> dict:
        s = int(variables["start"])
        e = int(variables["end"])
        skip = int(variables.get("skip", 0))
        first = int(variables.get("first", 1000))
        rows = [{"periodStartUnix": t, "tvlUSD": "1000", "volumeUSD": "50", "feesUSD": "5"} for t in range(s, e, HOUR)]
        return {"poolHourDatas": rows[skip : skip + first]}

    req = gateway_pb2.PoolHistoryRequest(
        pool_address=_ARB_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=start,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )

    class _Ctx:
        code = None

        def set_code(self, code: grpc.StatusCode) -> None:
            self.code = code

        def set_details(self, details: str) -> None:
            pass

    with patch.object(servicer._dispatcher._graphql, "query", new=AsyncMock(side_effect=_query)):
        resp = asyncio.run(servicer.GetPoolHistory(req, _Ctx()))

    assert resp.success is True
    assert resp.finalized_only is False  # trailing bar (end - 1h) is within 24h
    assert resp.truncation_reason == gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED
