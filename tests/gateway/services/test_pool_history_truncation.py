"""POOL-6 (VIB-4754) truncation-reason semantics — umbrella UAT card §D3.F7 (HARD GATE).

The bool-typed ``truncated`` field was rejected (Codex Round 1 C1). The
``TruncationReason`` enum MUST carry the right value in each scenario, and
``next_start_ts`` is a forward cursor (``> 0`` for re-chunkable reasons; ``0``
sentinel for ``PROVIDER_RETENTION`` and ``UNSPECIFIED``):

* ``test_truncation_reason_cap_exceeded`` — 200d-1h above the 90d soft cap.
* ``test_truncation_reason_provider_page_cap`` — 90d-1h with TheGraph's row
  ceiling forced to 100.
* ``test_truncation_reason_provider_retention`` — 400d-1d under the 730d cap;
  TheGraph fails, DefiLlama serves only ~365d; GeckoTerminal is NOT consulted to
  fill the gap. Inverse: a 300d cap makes the SAME request CAP_EXCEEDED, proving
  the two paths are mechanically distinct.
* ``test_next_start_ts_zero_sentinel_rejects_reissue`` — a (broken) caller that
  re-issues from a ``next_start_ts == 0`` sentinel (``start_ts == 0``) is
  rejected by the validator, so the sentinel can't drive an infinite loop.

Upstream provider seams are patched with synthetic rows (no live API), matching
the POOL-5 harness in ``test_pool_history_service.py``.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.pool_history._graphql import SubgraphConnectionError
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_history_service import PoolHistoryServiceServicer

HOUR = 3600
DAY = 86400
_TR = gateway_pb2.TruncationReason
_ARB_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"  # USDC/WETH Arbitrum


class _Ctx:
    """Captures grpc status code + details set on a ServicerContext."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _servicer(**overrides: object) -> PoolHistoryServiceServicer:
    return PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True, **overrides))


def _request(
    *,
    pool_address: str = _ARB_POOL,
    chain: str = "arbitrum",
    protocol: str = "uniswap_v3",
    start_ts: int,
    end_ts: int,
    resolution: int,
) -> gateway_pb2.PoolHistoryRequest:
    return gateway_pb2.PoolHistoryRequest(
        pool_address=pool_address,
        chain=chain,
        protocol=protocol,
        start_ts=start_ts,
        end_ts=end_ts,
        resolution=resolution,
    )


def _synth_thegraph_mock(data_key: str, time_field: str, step: int) -> AsyncMock:
    """Window-aware ``GatewayGraphQLClient.query`` mock.

    Generates one row per ``step`` across the queried ``[start, end)`` window
    and paginates by ``skip``/``first`` — so re-chunking from ``next_start_ts``
    naturally yields the next (newer) slice.
    """

    async def _side_effect(*, url: str, query: str, variables: dict) -> dict:
        start = int(variables["start"])
        end = int(variables["end"])
        skip = int(variables.get("skip", 0))
        first = int(variables.get("first", 1000))
        rows = [{time_field: t, "tvlUSD": "1000", "volumeUSD": "50", "feesUSD": "5"} for t in range(start, end, step)]
        return {data_key: rows[skip : skip + first]}

    return AsyncMock(side_effect=_side_effect)


@pytest.mark.acceptance_pack
def test_truncation_reason_cap_exceeded() -> None:
    # 200d-1h request, default 90d soft cap.
    end = (int(time.time()) // HOUR) * HOUR
    start = end - 200 * DAY
    servicer = _servicer()
    mock = _synth_thegraph_mock("poolHourDatas", "periodStartUnix", HOUR)
    with patch.object(servicer._dispatcher._graphql, "query", new=mock):
        ctx = _Ctx()
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                ctx,
            )
        )

    assert ctx.code is None  # gRPC OK — soft cap truncates, it does NOT raise.
    assert resp.success is True
    assert resp.source == "the_graph"
    assert resp.truncation_reason == _TR.CAP_EXCEEDED
    assert resp.next_start_ts == start + 90 * DAY  # clamped window's exclusive end
    assert resp.next_start_ts > 0
    assert len(resp.snapshots) <= 90 * 24  # 2160 bars
    assert resp.snapshots[0].timestamp == start

    # Re-issuing with start_ts = next_start_ts returns the NEXT (newer) slice.
    with patch.object(servicer._dispatcher._graphql, "query", new=mock):
        ctx2 = _Ctx()
        resp2 = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=resp.next_start_ts, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                ctx2,
            )
        )
    assert resp2.success is True
    assert resp2.truncation_reason == _TR.CAP_EXCEEDED  # 110d still > 90d cap
    assert resp2.snapshots[0].timestamp == resp.next_start_ts
    assert resp2.snapshots[0].timestamp > resp.snapshots[-1].timestamp


def test_truncation_reason_provider_page_cap() -> None:
    # 90d-1h request (exactly at the cap, NOT clamped), TheGraph row ceiling
    # forced to 100 so the page-cap path is exercised on a sub-cap window.
    end = (int(time.time()) // HOUR) * HOUR
    start = end - 90 * DAY
    servicer = _servicer(pool_history_page_cap_rows_the_graph=100)
    mock = _synth_thegraph_mock("poolHourDatas", "periodStartUnix", HOUR)
    with patch.object(servicer._dispatcher._graphql, "query", new=mock):
        ctx = _Ctx()
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                ctx,
            )
        )

    assert ctx.code is None
    assert resp.success is True
    assert resp.source == "the_graph"
    assert resp.truncation_reason == _TR.PROVIDER_PAGE_CAP
    assert resp.next_start_ts > 0
    assert len(resp.snapshots) == 100  # oldest ceiling-many rows
    assert resp.snapshots[0].timestamp == start
    # Forward cursor continues just past the last served bar.
    assert resp.next_start_ts == resp.snapshots[-1].timestamp + HOUR


def _defillama_catalog() -> list[dict]:
    return [
        {
            "pool": f"arbitrum-{_ARB_POOL}",
            "chain": "Arbitrum",
            "project": "uniswap-v3",
            "symbol": "USDC-WETH",
        }
    ]


def _defillama_daily_chart(*, oldest_ts: int, end_ts: int) -> list[dict]:
    return [{"timestamp": t, "tvlUsd": 1_000_000.0, "volumeUsd1d": 50_000.0} for t in range(oldest_ts, end_ts, DAY)]


def test_truncation_reason_provider_retention() -> None:
    # 400d-1d request, well under the 730d 1d cap (so CAP_EXCEEDED cannot
    # intercept). TheGraph fails -> DefiLlama serves only the most recent ~365d.
    end = (int(time.time()) // DAY) * DAY
    start = end - 400 * DAY
    servicer = _servicer()

    tg_down = AsyncMock(side_effect=SubgraphConnectionError("the_graph down"))
    dl_pools = AsyncMock(return_value=_defillama_catalog())
    dl_chart = AsyncMock(return_value=_defillama_daily_chart(oldest_ts=end - 365 * DAY, end_ts=end))
    gt_ohlcv = AsyncMock()  # MUST NOT be called — no cross-provider gap-filling.

    with (
        patch.object(servicer._dispatcher._graphql, "query", new=tg_down),
        patch.object(servicer._dispatcher._defillama, "_query_pools", new=dl_pools),
        patch.object(servicer._dispatcher._defillama, "_query_chart", new=dl_chart),
        patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_ohlcv),
    ):
        ctx = _Ctx()
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1D),
                ctx,
            )
        )

    assert ctx.code is None
    assert resp.success is True
    assert resp.source == "defillama"  # the provider that DID serve, not the_graph
    assert resp.truncation_reason == _TR.PROVIDER_RETENTION
    assert resp.next_start_ts == 0  # do-not-re-chunk sentinel
    assert gt_ohlcv.call_count == 0  # GeckoTerminal NOT consulted to fill the gap
    # The served slice starts at the provider's retention horizon, not the
    # requested start (~365d, not 400d).
    assert resp.snapshots[0].timestamp >= end - 366 * DAY
    assert resp.snapshots[0].timestamp - start > DAY


def test_truncation_reason_provider_retention_inverse_is_cap_exceeded() -> None:
    # The SAME 400d-1d request under a 300d cap is CAP_EXCEEDED, not retention —
    # proving the two truncation paths are mechanically distinguishable.
    end = (int(time.time()) // DAY) * DAY
    start = end - 400 * DAY
    servicer = _servicer(pool_history_max_days_1d=300)

    tg_down = AsyncMock(side_effect=SubgraphConnectionError("the_graph down"))
    dl_pools = AsyncMock(return_value=_defillama_catalog())
    dl_chart = AsyncMock(return_value=_defillama_daily_chart(oldest_ts=end - 365 * DAY, end_ts=end))

    with (
        patch.object(servicer._dispatcher._graphql, "query", new=tg_down),
        patch.object(servicer._dispatcher._defillama, "_query_pools", new=dl_pools),
        patch.object(servicer._dispatcher._defillama, "_query_chart", new=dl_chart),
    ):
        ctx = _Ctx()
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1D),
                ctx,
            )
        )

    assert resp.success is True
    assert resp.source == "defillama"
    assert resp.truncation_reason == _TR.CAP_EXCEEDED
    assert resp.next_start_ts == start + 300 * DAY


def test_next_start_ts_zero_sentinel_rejects_reissue() -> None:
    # A broken caller that re-issues from a next_start_ts == 0 sentinel (i.e.
    # start_ts == 0) is rejected by the validator — the sentinel cannot drive an
    # accidental infinite loop.
    end = (int(time.time()) // HOUR) * HOUR
    servicer = _servicer()
    ctx = _Ctx()
    resp = asyncio.run(
        servicer.GetPoolHistory(
            _request(start_ts=0, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
            ctx,
        )
    )
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert resp.success is False


def test_cap_exceeded_and_exact_window_do_not_share_cache_entry() -> None:
    # Regression (Codex P1): an over-cap request [T0, T0+200d) clamps to the
    # same served slice [T0, T0+90d) as an exact-cap request [T0, T0+90d), but
    # the two carry DIFFERENT truncation envelopes (CAP_EXCEEDED+next vs
    # UNSPECIFIED+0). Keying the public cache by the clamped window would let the
    # exact-window entry mask the over-cap one and silently strand the caller at
    # 90d. They must stay distinct regardless of warm order.
    end_over = (int(time.time()) // HOUR) * HOUR
    start = end_over - 200 * DAY
    end_exact = start + 90 * DAY  # exact-cap window, NOT clamped
    servicer = _servicer()
    mock = _synth_thegraph_mock("poolHourDatas", "periodStartUnix", HOUR)

    with patch.object(servicer._dispatcher._graphql, "query", new=mock):
        # Warm the cache with the EXACT-window request FIRST (the dangerous order).
        exact = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end_exact, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                _Ctx(),
            )
        )
        over = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end_over, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                _Ctx(),
            )
        )

    assert exact.truncation_reason == _TR.TRUNCATION_REASON_UNSPECIFIED
    assert exact.next_start_ts == 0
    # The over-cap request MUST still report CAP_EXCEEDED with a forward cursor —
    # NOT inherit the exact-window's UNSPECIFIED envelope from a shared entry.
    assert over.truncation_reason == _TR.CAP_EXCEEDED
    assert over.next_start_ts == start + 90 * DAY


def test_truncation_unspecified_when_window_fully_served() -> None:
    # A within-cap window the provider fully serves carries UNSPECIFIED + 0.
    end = (int(time.time()) // HOUR) * HOUR
    start = end - 7 * DAY
    servicer = _servicer()
    mock = _synth_thegraph_mock("poolHourDatas", "periodStartUnix", HOUR)
    with patch.object(servicer._dispatcher._graphql, "query", new=mock):
        ctx = _Ctx()
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.truncation_reason == _TR.TRUNCATION_REASON_UNSPECIFIED
    assert resp.next_start_ts == 0
    assert len(resp.snapshots) == 7 * 24
