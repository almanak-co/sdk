"""POOL-8 (VIB-4756) telemetry counters + structured logs — UAT card §D2.M2.b.

Covers the five sub-steps:

* ``test_truncated_counter_scalar_and_per_reason`` — D2.M2.b.1: scalar
  ``truncated`` AND ``truncated_by_reason[NAME]`` move per response;
  full-dict equality after each event catches bucket cross-contamination;
  the sum identity catches scalar-vs-dict divergence.
* ``test_errors_by_grpc_code_populates`` — D2.M2.b.2: per-gRPC-status-code
  dict keyed by ``StatusCode.NAME``; kill-switch ``UNAVAILABLE`` and
  exhausted-providers ``UNAVAILABLE`` share one key.
* ``test_bucket_throttle_waits_ms_per_provider`` — D2.M2.b.3: theoretical
  ms-until-next-token (``round(period * 1000 / rate)``) accumulates on
  EVERY bucket refusal (TheGraph ``_ProviderError`` primary AND
  GeckoTerminal ``_NotAttempted`` fallback); monotonic — a successful
  call NEVER resets the accumulator.
* ``test_structured_logs_at_boundary`` — D2.M2.b.4 part 1: exactly 2 INFO
  records per happy path (entry + exit), 1 INFO + 1 WARNING per error
  path. Bound fields populated via ``extra=``; message templates stable.
* ``test_api_key_never_in_logs`` — D2.M2.b.4 part 2: a high-entropy API-key
  sentinel set via ``ALMANAK_GATEWAY_THEGRAPH_API_KEY`` MUST NEVER appear
  in any ``almanak.gateway``-tree log record at DEBUG+, in ``record.message``
  OR in ``repr(record.__dict__)``, and MUST NEVER appear in ``health()``.
* ``test_health_schema_unchanged_under_pool8`` — D2.M2.b.5: shape stability
  + value-type assertions (catches ``dict→int`` collapse + ``bool→int``
  trap).

Upstream provider seams are patched with synthetic responses (no live API),
matching the POOL-5 / POOL-6 harness patterns.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any
from unittest.mock import AsyncMock, patch

import grpc
import pytest

import almanak.gateway.data.pool_history.dispatcher as _dispatcher_mod
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.pool_history._graphql import SubgraphConnectionError
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_history_service import (
    _PER_RPC_COUNTER_NAMES,
    PoolHistoryServiceServicer,
)

HOUR = 3600
DAY = 86400
_TR = gateway_pb2.TruncationReason
_ARB_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"  # USDC/WETH Arbitrum
_BASE_AERO_POOL = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"  # Aerodrome Base


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
    return PoolHistoryServiceServicer(
        GatewaySettings(pool_history_enabled=True, coingecko_api_key="test-key", **overrides)
    )


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


def _synth_thegraph_mock(time_field: str, step: int, data_key: str = "poolHourDatas") -> AsyncMock:
    """Window-aware ``GatewayGraphQLClient.query`` mock.

    Generates one synthetic row per ``step`` across the queried window
    and paginates by ``skip``/``first``. Each row carries deterministic
    string-decimal TVL/volume/fees so the dispatcher always assembles a
    non-empty payload.
    """

    async def _side_effect(*, url: str, query: str, variables: dict) -> dict:
        start = int(variables["start"])
        end = int(variables["end"])
        skip = int(variables.get("skip", 0))
        first = int(variables.get("first", 1000))
        rows = [{time_field: t, "tvlUSD": "1000", "volumeUSD": "50", "feesUSD": "5"} for t in range(start, end, step)]
        return {data_key: rows[skip : skip + first]}

    return AsyncMock(side_effect=_side_effect)


def _aligned_recent_hour() -> int:
    """Return a recent hour-aligned timestamp (deterministic per test run)."""
    return (int(time.time()) // HOUR) * HOUR


# =============================================================================
# D2.M2.b.1 — scalar `truncated` + `truncated_by_reason[NAME]` per response
# =============================================================================


def test_truncated_counter_scalar_and_per_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    """D2.M2.b.1: per-event full-dict equality (catches bucket cross-contamination).

    Uses ONE servicer with a running tally so the test pins both per-event
    deltas AND the multi-event terminal state. The page cap is toggled
    between steps because the classifier orders ``PROVIDER_PAGE_CAP`` ahead
    of ``CAP_EXCEEDED`` when both apply — step 1 needs the default 100k cap
    (so the 90d soft-cap clamp is the only truncation), step 2 needs a low
    cap (so a within-soft-cap request page-cap-clamps).
    """
    servicer = _servicer()
    tg_mock = _synth_thegraph_mock("periodStartUnix", HOUR)
    end = _aligned_recent_hour()

    # Baseline: full-dict equality on construction.
    assert servicer.health()["per_rpc"]["truncated"] == 0
    assert servicer.health()["per_rpc"]["truncated_by_reason"] == {
        "TRUNCATION_REASON_UNSPECIFIED": 0,
        "CAP_EXCEEDED": 0,
        "PROVIDER_PAGE_CAP": 0,
        "PROVIDER_RETENTION": 0,
    }

    # 1) CAP_EXCEEDED — 200d 1h, default 90d soft cap. Page cap stays at
    # 100k so the soft-cap clamp wins.
    cap_start = end - 200 * DAY
    with patch.object(servicer._dispatcher._graphql, "query", new=tg_mock):
        resp_cap = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    start_ts=cap_start,
                    end_ts=end,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                _Ctx(),
            )
        )
    assert resp_cap.success is True
    assert resp_cap.truncation_reason == _TR.CAP_EXCEEDED
    assert servicer.health()["per_rpc"]["truncated"] == 1
    assert servicer.health()["per_rpc"]["truncated_by_reason"] == {
        "TRUNCATION_REASON_UNSPECIFIED": 0,
        "CAP_EXCEEDED": 1,
        "PROVIDER_PAGE_CAP": 0,
        "PROVIDER_RETENTION": 0,
    }

    # 2) PROVIDER_PAGE_CAP — 30d 1h window: 720 rows from the synth mock,
    # within the 90d soft cap, but the per-provider page cap forces a clamp
    # at 100 rows. Use a distinct pool address so the cache doesn't merge
    # with step (1).
    servicer._page_cap_rows["the_graph"] = 100
    pc_start = end - 30 * DAY
    with patch.object(servicer._dispatcher._graphql, "query", new=tg_mock):
        resp_pc = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address="0x" + "1" * 40,
                    start_ts=pc_start,
                    end_ts=end,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                _Ctx(),
            )
        )
    assert resp_pc.success is True
    assert resp_pc.truncation_reason == _TR.PROVIDER_PAGE_CAP
    assert servicer.health()["per_rpc"]["truncated"] == 2
    assert servicer.health()["per_rpc"]["truncated_by_reason"] == {
        "TRUNCATION_REASON_UNSPECIFIED": 0,
        "CAP_EXCEEDED": 1,
        "PROVIDER_PAGE_CAP": 1,
        "PROVIDER_RETENTION": 0,
    }

    # 3) PROVIDER_RETENTION — 400d 1d under the 730d 1d cap; force TheGraph
    # down so DefiLlama serves only the most recent ~365d. GeckoTerminal
    # MUST NOT be consulted to fill the gap. Distinct pool address so the
    # cache stays unique from step 1 / step 2. Mirror the canonical DefiLlama
    # fixture shape used by `test_pool_history_truncation.py` (chain segment
    # in the catalog pool field; per-row 1000 + 50 + 5 string-decimal fields).
    # Refill the TheGraph bucket so its bucket-empty path doesn't intercept
    # the SubgraphConnectionError side_effect (steps 1+2 may have depleted it).
    servicer._dispatcher._thegraph_bucket._tokens = float(_dispatcher_mod._THEGRAPH_RATE_PER_S)
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph down"))
    end_day = (end // DAY) * DAY
    ret_start = end_day - 400 * DAY
    short_slice_start = end_day - 365 * DAY
    ret_pool = "0x" + "a" * 40
    chart_rows = [
        {"timestamp": t, "tvlUsd": 1_000_000.0, "volumeUsd1d": 50_000.0}
        for t in range(short_slice_start, end_day, DAY)
    ]
    pools_rows = [
        {"pool": f"arbitrum-{ret_pool}", "chain": "Arbitrum", "project": "uniswap-v3", "symbol": "USDC-WETH"}
    ]
    gt_mock = AsyncMock(return_value=[])
    with (
        patch.object(servicer._dispatcher._graphql, "query", new=tg_down),
        patch.object(servicer._dispatcher._defillama, "_query_pools", new=AsyncMock(return_value=pools_rows)),
        patch.object(servicer._dispatcher._defillama, "_query_chart", new=AsyncMock(return_value=chart_rows)),
        patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock),
    ):
        resp_ret = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=ret_pool,
                    chain="arbitrum",
                    start_ts=ret_start,
                    end_ts=end_day,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                _Ctx(),
            )
        )
    assert resp_ret.success is True, f"expected success; got error: {resp_ret.error}"
    assert resp_ret.source == "defillama"
    assert resp_ret.truncation_reason == _TR.PROVIDER_RETENTION
    assert resp_ret.next_start_ts == 0  # do-not-rechunk sentinel
    assert gt_mock.call_count == 0  # GeckoTerminal NEVER consulted to "fill the gap"
    assert servicer.health()["per_rpc"]["truncated"] == 3
    assert servicer.health()["per_rpc"]["truncated_by_reason"] == {
        "TRUNCATION_REASON_UNSPECIFIED": 0,
        "CAP_EXCEEDED": 1,
        "PROVIDER_PAGE_CAP": 1,
        "PROVIDER_RETENTION": 1,
    }

    # 4) Non-truncated success (45d 1d, well within cap, no page cap trip).
    # Reset the page cap so this request doesn't trip PROVIDER_PAGE_CAP.
    servicer._page_cap_rows["the_graph"] = 100_000
    short_start = end_day - 45 * DAY
    tg_daily = _synth_thegraph_mock("date", DAY, data_key="poolDayDatas")
    with patch.object(servicer._dispatcher._graphql, "query", new=tg_daily):
        resp_ok = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address="0x" + "2" * 40,
                    start_ts=short_start,
                    end_ts=end_day,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                _Ctx(),
            )
        )
    assert resp_ok.success is True
    assert resp_ok.truncation_reason == _TR.TRUNCATION_REASON_UNSPECIFIED
    # Full-dict equality unchanged after the non-truncation event.
    assert servicer.health()["per_rpc"]["truncated"] == 3
    assert servicer.health()["per_rpc"]["truncated_by_reason"] == {
        "TRUNCATION_REASON_UNSPECIFIED": 0,
        "CAP_EXCEEDED": 1,
        "PROVIDER_PAGE_CAP": 1,
        "PROVIDER_RETENTION": 1,
    }

    # 5) Failure response — exhausted providers. Distinct pool/window so it
    # doesn't cache-hit a prior success.
    err_mock = AsyncMock(side_effect=SubgraphConnectionError("down"))
    err_pools = AsyncMock(return_value=[])  # DefiLlama returns no match
    err_chart = AsyncMock(return_value=[])
    err_gt = AsyncMock(return_value=None)  # GeckoTerminal returns "not found"
    with (
        patch.object(servicer._dispatcher._graphql, "query", new=err_mock),
        patch.object(servicer._dispatcher._defillama, "_query_pools", new=err_pools),
        patch.object(servicer._dispatcher._defillama, "_query_chart", new=err_chart),
        patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=err_gt),
    ):
        resp_err = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address="0x" + "3" * 40,
                    chain="ethereum",
                    start_ts=end_day - 45 * DAY,
                    end_ts=end_day,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                _Ctx(),
            )
        )
    assert resp_err.success is False
    # Failures DO NOT touch truncation counters.
    assert servicer.health()["per_rpc"]["truncated"] == 3
    assert servicer.health()["per_rpc"]["truncated_by_reason"] == {
        "TRUNCATION_REASON_UNSPECIFIED": 0,
        "CAP_EXCEEDED": 1,
        "PROVIDER_PAGE_CAP": 1,
        "PROVIDER_RETENTION": 1,
    }

    # Sum identity (independent layer that catches scalar-vs-dict divergence).
    by_reason = servicer.health()["per_rpc"]["truncated_by_reason"]
    assert servicer.health()["per_rpc"]["truncated"] == sum(by_reason.values())


def test_truncated_counter_anti_collapse_classification_gated(monkeypatch: pytest.MonkeyPatch) -> None:
    """D2.M2.b.1 anti-collapse: ``truncated`` is gated on the CLASSIFIED
    reason, not on the request shape.

    Monkey-patches the truncation classifier to return UNSPECIFIED on a
    200d request that would normally classify as CAP_EXCEEDED. ``truncated``
    MUST stay at 0.
    """
    from almanak.gateway.services._history_common import TruncationOutcome

    servicer = _servicer()
    tg_mock = _synth_thegraph_mock("periodStartUnix", HOUR)
    end = _aligned_recent_hour()
    start = end - 200 * DAY

    def _unspecified(*args: Any, **kwargs: Any) -> TruncationOutcome:
        # Force every request through this monkey-patched classifier; return
        # UNSPECIFIED + the entire kept list (no truncation envelope).
        snapshots = kwargs.get("snapshots") if "snapshots" in kwargs else args[0]
        return TruncationOutcome(
            kept=snapshots,
            reason=_TR.TRUNCATION_REASON_UNSPECIFIED,
            next_start_ts=0,
        )

    import almanak.gateway.services.pool_history_service as svc_mod

    with (
        patch.object(servicer._dispatcher._graphql, "query", new=tg_mock),
        patch.object(svc_mod, "classify_truncation", _unspecified),
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(start_ts=start, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                _Ctx(),
            )
        )
    assert resp.success is True
    assert resp.truncation_reason == _TR.TRUNCATION_REASON_UNSPECIFIED
    # The counter MUST stay at 0 — the patched classifier doesn't classify
    # this as a truncation event, so `truncated` MUST NOT bump even though
    # the request range exceeded the cap.
    assert servicer.health()["per_rpc"]["truncated"] == 0


# =============================================================================
# D2.M2.b.2 — `errors_by_grpc_code` per gRPC status code
# =============================================================================


def test_errors_by_grpc_code_populates() -> None:
    """D2.M2.b.2: each non-OK return bumps `errors_by_grpc_code[NAME]` by 1.

    Three exit paths share one running tally on the same servicer:
      (a) kill-switch off  -> UNAVAILABLE
      (b) validator reject -> INVALID_ARGUMENT
      (c) all providers    -> UNAVAILABLE (same key as (a))
    """
    # (a) Kill-switch off. Construct a separate disabled servicer because
    # the kill-switch is read once at __init__.
    disabled = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=False))
    assert disabled.health()["per_rpc"]["errors_by_grpc_code"] == {}  # empty baseline
    end = _aligned_recent_hour()
    req = _request(
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    ctx = _Ctx()
    resp = asyncio.run(disabled.GetPoolHistory(req, ctx))
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert resp.success is False
    assert disabled.health()["per_rpc"]["errors_by_grpc_code"] == {"UNAVAILABLE": 1}

    # Switch to an enabled servicer for the rest. Each step accumulates on
    # one running tally.
    servicer = _servicer()
    assert servicer.health()["per_rpc"]["errors_by_grpc_code"] == {}

    # (a) Kill-switch UNAVAILABLE on the enabled servicer requires disabling
    # mid-flight; the kill-switch is read at __init__, so we synthesize the
    # case by toggling the private flag. This is the SAME entry path the
    # `disabled` servicer above takes.
    servicer._enabled = False
    asyncio.run(servicer.GetPoolHistory(req, _Ctx()))
    servicer._enabled = True
    assert servicer.health()["per_rpc"]["errors_by_grpc_code"] == {"UNAVAILABLE": 1}

    # (b) Validator INVALID_ARGUMENT — empty pool address.
    invalid = gateway_pb2.PoolHistoryRequest(
        pool_address="",
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    ctx_b = _Ctx()
    asyncio.run(servicer.GetPoolHistory(invalid, ctx_b))
    assert ctx_b.code == grpc.StatusCode.INVALID_ARGUMENT
    assert servicer.health()["per_rpc"]["errors_by_grpc_code"] == {
        "UNAVAILABLE": 1,
        "INVALID_ARGUMENT": 1,
    }

    # (c) All providers fail -> UNAVAILABLE (same key as kill-switch).
    err_tg = AsyncMock(side_effect=SubgraphConnectionError("down"))
    err_dl_pools = AsyncMock(return_value=[])
    err_dl_chart = AsyncMock(return_value=[])
    err_gt = AsyncMock(return_value=None)  # not found
    with (
        patch.object(servicer._dispatcher._graphql, "query", new=err_tg),
        patch.object(servicer._dispatcher._defillama, "_query_pools", new=err_dl_pools),
        patch.object(servicer._dispatcher._defillama, "_query_chart", new=err_dl_chart),
        patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=err_gt),
    ):
        ctx_c = _Ctx()
        asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address="0x" + "9" * 40,
                    start_ts=end - 7 * HOUR,
                    end_ts=end,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx_c,
            )
        )
    assert ctx_c.code == grpc.StatusCode.UNAVAILABLE
    assert servicer.health()["per_rpc"]["errors_by_grpc_code"] == {
        "UNAVAILABLE": 2,
        "INVALID_ARGUMENT": 1,
    }

    # Anti-bypass: a success path does NOT touch errors_by_grpc_code.
    ok_mock = _synth_thegraph_mock("periodStartUnix", HOUR)
    with patch.object(servicer._dispatcher._graphql, "query", new=ok_mock):
        resp_ok = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address="0x" + "a" * 40,
                    start_ts=end - 7 * HOUR,
                    end_ts=end,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                _Ctx(),
            )
        )
    assert resp_ok.success is True
    assert servicer.health()["per_rpc"]["errors_by_grpc_code"] == {
        "UNAVAILABLE": 2,
        "INVALID_ARGUMENT": 1,
    }


# =============================================================================
# D2.M2.b.3 — per-provider `bucket_throttle_waits_ms`
# =============================================================================


def test_bucket_throttle_waits_ms_per_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """D2.M2.b.3: theoretical wait-to-next-token bumps on every bucket refusal.

    Two scenarios cover the two refusal paths:
      (a) TheGraph primary bucket empty -> `_ProviderError` -> +errors and
          +bucket_throttle_waits_ms.
      (b) GeckoTerminal fallback bucket empty -> `_NotAttempted` silent skip
          -> +bucket_throttle_waits_ms WITHOUT +errors.
    Then anti-reset: a successful call MUST NOT reset the accumulator.
    """
    # Clamp the TheGraph rate to 1 req/s so `round(1.0 * 1000 / 1) == 1000`
    # ms per refusal — matches the UAT card's pinned magnitude.
    monkeypatch.setattr(_dispatcher_mod, "_THEGRAPH_RATE_PER_S", 1)
    servicer = _servicer()
    tg_mock = _synth_thegraph_mock("periodStartUnix", HOUR)
    end = _aligned_recent_hour()

    # Baseline.
    assert servicer.health()["per_provider"] == {}

    req = _request(
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )

    # First request: bucket has 1 token -> acquires -> serves.
    with patch.object(servicer._dispatcher._graphql, "query", new=tg_mock):
        resp1 = asyncio.run(servicer.GetPoolHistory(req, _Ctx()))
    assert resp1.success is True
    tg_counters = servicer.health()["per_provider"]["the_graph"]
    assert tg_counters["requests"] == 1
    assert tg_counters["errors"] == 0
    assert tg_counters.get("bucket_throttle_waits_ms", 0) == 0

    # Second request immediately after: bucket empty -> refusal -> 1000 ms.
    # Use a distinct pool to avoid cache hit.
    req2 = _request(
        pool_address="0x" + "1" * 40,
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    gt_mock = AsyncMock(return_value=[])  # GeckoTerminal serves empty
    with (
        patch.object(servicer._dispatcher._graphql, "query", new=tg_mock),
        patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock),
    ):
        asyncio.run(servicer.GetPoolHistory(req2, _Ctx()))
    tg_counters = servicer.health()["per_provider"]["the_graph"]
    assert tg_counters["bucket_throttle_waits_ms"] == 1000  # formula-exact
    assert tg_counters["errors"] == 1  # _ProviderError on bucket-empty primary
    assert tg_counters["requests"] == 1  # only the first call acquired
    assert servicer.health()["per_rpc"]["provider_fallback"] >= 1

    # Anti-reset: wait > 1s for the bucket to refill, then a SUCCESSFUL
    # request. The accumulator MUST NOT reset.
    time.sleep(1.1)
    req3 = _request(
        pool_address="0x" + "2" * 40,
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    with patch.object(servicer._dispatcher._graphql, "query", new=tg_mock):
        resp3 = asyncio.run(servicer.GetPoolHistory(req3, _Ctx()))
    assert resp3.success is True
    tg_counters = servicer.health()["per_provider"]["the_graph"]
    assert tg_counters["bucket_throttle_waits_ms"] == 1000  # still 1000, not reset

    # Drain again -> accumulator goes monotonic to 2000.
    req4 = _request(
        pool_address="0x" + "3" * 40,
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    with (
        patch.object(servicer._dispatcher._graphql, "query", new=tg_mock),
        patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=AsyncMock(return_value=[])),
    ):
        asyncio.run(servicer.GetPoolHistory(req4, _Ctx()))
    tg_counters = servicer.health()["per_provider"]["the_graph"]
    assert tg_counters["bucket_throttle_waits_ms"] == 2000

    # GeckoTerminal `_NotAttempted` path. Default rate is 30/60s ->
    # `round(60.0 * 1000 / 30) == 2000` ms per refusal. Drain its bucket
    # then issue a request on (base, aerodrome) where TheGraph is
    # `_NotAttempted` (no subgraph capability registered) and GeckoTerminal
    # is the only viable provider.
    gt_servicer = _servicer()
    # Drain GeckoTerminal bucket: 30 tokens at start; acquire 30 times.
    for _ in range(30):
        gt_servicer._dispatcher._geckoterminal_bucket.acquire()
    # Issue ONE request — the GeckoTerminal bucket is empty -> _NotAttempted.
    req_aero = _request(
        pool_address=_BASE_AERO_POOL,
        chain="base",
        protocol="aerodrome",
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    asyncio.run(gt_servicer.GetPoolHistory(req_aero, _Ctx()))
    gt_counters = gt_servicer.health()["per_provider"]["geckoterminal"]
    assert gt_counters["bucket_throttle_waits_ms"] == 2000  # 30/60s formula
    assert gt_counters["errors"] == 0  # _NotAttempted is NOT an error (D2.M2 contract)

    # Anti-overcount: a successful provider call (bucket has a token) MUST
    # NOT bump bucket_throttle_waits_ms beyond its prior value.
    fresh = _servicer()
    fresh_tg_mock = _synth_thegraph_mock("periodStartUnix", HOUR)
    with patch.object(fresh._dispatcher._graphql, "query", new=fresh_tg_mock):
        asyncio.run(fresh.GetPoolHistory(req, _Ctx()))
    fresh_tg = fresh.health()["per_provider"]["the_graph"]
    assert fresh_tg.get("bucket_throttle_waits_ms", 0) == 0

    # Anti-cross-bump: a defensive-fallthrough exception MUST NOT bump
    # bucket_throttle_waits_ms (only `errors` + `provider_fallback`).
    boom_servicer = _servicer()
    boom_tg = AsyncMock(side_effect=RuntimeError("synthetic-not-bucket"))
    with (
        patch.object(boom_servicer._dispatcher._graphql, "query", new=boom_tg),
        patch.object(boom_servicer._dispatcher._geckoterminal, "_query_ohlcv", new=AsyncMock(return_value=[])),
    ):
        asyncio.run(boom_servicer.GetPoolHistory(req, _Ctx()))
    # TheGraph counter exists (errors) but bucket_throttle_waits_ms stays 0.
    boom_tg_counters = boom_servicer.health()["per_provider"].get("the_graph", {})
    assert boom_tg_counters.get("bucket_throttle_waits_ms", 0) == 0
    assert boom_tg_counters.get("errors", 0) >= 1


# =============================================================================
# D2.M2.b.4 — structured logs at boundaries + API-key redaction
# =============================================================================


_LOGGER_NAME = "almanak.gateway.services.pool_history_service"


def test_structured_logs_at_boundary(caplog: pytest.LogCaptureFixture) -> None:
    """D2.M2.b.4 part 1: exactly 2 INFO per success, 1 INFO + 1 WARNING per error."""
    servicer = _servicer()
    tg_mock = _synth_thegraph_mock("periodStartUnix", HOUR)
    end = _aligned_recent_hour()
    req = _request(
        start_ts=end - 7 * HOUR,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )

    # Happy-path: exactly 2 INFO records (entry + success exit).
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        with patch.object(servicer._dispatcher._graphql, "query", new=tg_mock):
            resp = asyncio.run(servicer.GetPoolHistory(req, _Ctx()))
    assert resp.success is True

    svc_records = [r for r in caplog.records if r.name == _LOGGER_NAME]
    info_records = [r for r in svc_records if r.levelno == logging.INFO]
    assert len(info_records) == 2, f"expected 2 INFO records, got {len(info_records)}: {[r.getMessage() for r in info_records]}"  # noqa: E501

    entry, success = info_records
    assert entry.getMessage() == "pool_history.request"
    assert entry.chain == "arbitrum"
    assert entry.protocol == "uniswap_v3"
    assert entry.resolution == gateway_pb2.Resolution.RESOLUTION_1H
    assert entry.pool_address == _ARB_POOL
    assert entry.start_ts == end - 7 * HOUR
    assert entry.end_ts == end

    assert success.getMessage() == "pool_history.response"
    assert success.pool_address == _ARB_POOL  # per-request log correlation; CodeRabbit 2026-05-28
    assert success.source == "the_graph"
    assert success.snapshots_count == len(resp.snapshots)
    assert success.truncation_reason == "TRUNCATION_REASON_UNSPECIFIED"
    assert success.finality_band in ("finalized", "provisional")
    assert isinstance(success.latency_ms, int) and success.latency_ms >= 0
    assert success.grpc_code == "OK"
    assert success.error == ""

    # Error path: 1 INFO entry + 1 WARNING exit.
    caplog.clear()
    err_servicer = _servicer()
    err_tg = AsyncMock(side_effect=SubgraphConnectionError("down"))
    err_pools = AsyncMock(return_value=[])
    err_chart = AsyncMock(return_value=[])
    err_gt = AsyncMock(return_value=None)
    with caplog.at_level(logging.INFO, logger=_LOGGER_NAME):
        with (
            patch.object(err_servicer._dispatcher._graphql, "query", new=err_tg),
            patch.object(err_servicer._dispatcher._defillama, "_query_pools", new=err_pools),
            patch.object(err_servicer._dispatcher._defillama, "_query_chart", new=err_chart),
            patch.object(err_servicer._dispatcher._geckoterminal, "_query_ohlcv", new=err_gt),
        ):
            asyncio.run(err_servicer.GetPoolHistory(req, _Ctx()))
    svc_records = [r for r in caplog.records if r.name == _LOGGER_NAME]
    info_records = [r for r in svc_records if r.levelno == logging.INFO]
    warn_records = [r for r in svc_records if r.levelno == logging.WARNING]
    assert len(info_records) == 1
    assert len(warn_records) == 1
    assert info_records[0].getMessage() == "pool_history.request"
    assert warn_records[0].getMessage() == "pool_history.error"
    assert warn_records[0].pool_address == _ARB_POOL  # per-request log correlation
    assert warn_records[0].grpc_code == "UNAVAILABLE"
    assert warn_records[0].source == ""
    assert warn_records[0].error  # non-empty
    assert isinstance(warn_records[0].latency_ms, int) and warn_records[0].latency_ms >= 0


_API_KEY_CANARY = "kr_test_REDACTION_CANARY_d8f3a7e2"


def test_api_key_never_in_logs(caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    """D2.M2.b.4 part 2: API-key sentinel never appears in any almanak.gateway log record."""
    monkeypatch.setenv("ALMANAK_GATEWAY_THEGRAPH_API_KEY", _API_KEY_CANARY)
    settings = GatewaySettings(pool_history_enabled=True)
    assert settings.thegraph_api_key == _API_KEY_CANARY  # confirm wiring
    servicer = PoolHistoryServiceServicer(settings)

    end = _aligned_recent_hour()
    tg_mock = _synth_thegraph_mock("periodStartUnix", HOUR)
    err_tg = AsyncMock(side_effect=SubgraphConnectionError("down"))

    # Three requests: (1) happy, (2) TheGraph-error fallback, (3) validator reject.
    with caplog.at_level(logging.DEBUG, logger="almanak.gateway"):
        # (1) happy
        with patch.object(servicer._dispatcher._graphql, "query", new=tg_mock):
            asyncio.run(
                servicer.GetPoolHistory(
                    _request(start_ts=end - 7 * HOUR, end_ts=end, resolution=gateway_pb2.Resolution.RESOLUTION_1H),
                    _Ctx(),
                )
            )
        # (2) TheGraph error -> fallback to GeckoTerminal (which returns empty -> ultimately UNAVAILABLE)
        with (
            patch.object(servicer._dispatcher._graphql, "query", new=err_tg),
            patch.object(servicer._dispatcher._defillama, "_query_pools", new=AsyncMock(return_value=[])),
            patch.object(servicer._dispatcher._defillama, "_query_chart", new=AsyncMock(return_value=[])),
            patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=AsyncMock(return_value=None)),
        ):
            asyncio.run(
                servicer.GetPoolHistory(
                    _request(
                        pool_address="0x" + "5" * 40,
                        start_ts=end - 7 * HOUR,
                        end_ts=end,
                        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                    ),
                    _Ctx(),
                )
            )
        # (3) validator reject
        asyncio.run(
            servicer.GetPoolHistory(
                gateway_pb2.PoolHistoryRequest(
                    pool_address="",
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=end - 7 * HOUR,
                    end_ts=end,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                _Ctx(),
            )
        )

    leaks: list[str] = []
    for record in caplog.records:
        if _API_KEY_CANARY in record.getMessage():
            leaks.append(f"message-leak in {record.name}@{record.levelname}: {record.getMessage()}")
        if _API_KEY_CANARY in repr(record.__dict__):
            leaks.append(f"extra-dict-leak in {record.name}@{record.levelname}: {record.__dict__}")
    assert leaks == [], "API key leaked into log records:\n  " + "\n  ".join(leaks)

    # health() payload — the other observability surface — must not contain the key.
    assert _API_KEY_CANARY not in repr(servicer.health())


# =============================================================================
# D2.M2.b.5 — health() schema stability + value-type assertions
# =============================================================================


def test_safe_truncation_name_falls_back_on_unknown_enum_int() -> None:
    """`_safe_truncation_name` routes an unknown proto enum int to `UNKNOWN_<int>`.

    Protects against ``ValueError`` from ``proto.TruncationReason.Name(...)``
    on proto-version skew (a newer client / provider introducing a reason
    the local descriptor doesn't know about). The fallback keeps the
    structured-log emission AND the ``truncated_by_reason`` counter
    increment alive — silently dropping either would mask a legitimate
    truncation event from ops (CodeRabbit 2026-05-28).
    """
    # The locked enum has 4 values (0..3); pick an int far outside that range.
    invalid_reason: int = 9999
    assert PoolHistoryServiceServicer._safe_truncation_name(invalid_reason) == "UNKNOWN_9999"

    # The four known values map to their canonical names.
    assert PoolHistoryServiceServicer._safe_truncation_name(_TR.TRUNCATION_REASON_UNSPECIFIED) == "TRUNCATION_REASON_UNSPECIFIED"  # noqa: E501
    assert PoolHistoryServiceServicer._safe_truncation_name(_TR.CAP_EXCEEDED) == "CAP_EXCEEDED"
    assert PoolHistoryServiceServicer._safe_truncation_name(_TR.PROVIDER_PAGE_CAP) == "PROVIDER_PAGE_CAP"
    assert PoolHistoryServiceServicer._safe_truncation_name(_TR.PROVIDER_RETENTION) == "PROVIDER_RETENTION"

    # The fallback path also wires through ``_bump_truncated`` — verify the
    # counter increments under an UNKNOWN bucket key when an unknown int
    # is classified (synthesized via monkey-patching the classifier).
    servicer = _servicer()
    servicer._bump_truncated(invalid_reason)
    assert servicer.health()["per_rpc"]["truncated"] == 1
    assert servicer.health()["per_rpc"]["truncated_by_reason"].get("UNKNOWN_9999") == 1


def test_health_schema_unchanged_under_pool8() -> None:
    """D2.M2.b.5: schema + value types are stable; `truncated` is the only new key."""
    servicer = _servicer()
    snapshot = servicer.health()

    # Top-level keyset.
    assert set(snapshot.keys()) == {"per_rpc", "per_provider", "budget"}

    per_rpc = snapshot["per_rpc"]
    expected_per_rpc_keys = set(_PER_RPC_COUNTER_NAMES) | {
        "truncated_by_reason",
        "errors_by_grpc_code",
        "raw_cache_entries_by_provider",
    }
    assert set(per_rpc.keys()) == expected_per_rpc_keys

    # Value-type assertions (catch dict->int collapse + bool->int trap).
    assert isinstance(per_rpc["truncated_by_reason"], dict)
    assert isinstance(per_rpc["errors_by_grpc_code"], dict)
    assert isinstance(per_rpc["raw_cache_entries_by_provider"], dict)
    assert isinstance(per_rpc["truncated"], int) and not isinstance(per_rpc["truncated"], bool)
    assert per_rpc["truncated"] == 0
    for name in (
        "requests_total",
        "cache_hits",
        "cache_misses",
        "provider_fallback",
        "inflight_dedup_hits",
        "cache_evictions_by_entries",
        "cache_evictions_by_bytes",
        "cache_bytes_resident",
    ):
        assert isinstance(per_rpc[name], int) and not isinstance(per_rpc[name], bool), name

    # POOL-8 migration: `truncated` is in _PER_RPC_COUNTER_NAMES (was absent pre-POOL-8).
    assert "truncated" in _PER_RPC_COUNTER_NAMES
