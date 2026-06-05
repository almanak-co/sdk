"""Audit-fix regression tests for POOL-5 / VIB-4753 (PR #2460 multi-auditor review).

Each test pins a fix for a specific audit finding so it cannot silently regress:

- **B#1** GeckoTerminal OHLCV backward pagination — a window longer than one
  page is fetched across multiple ``before_timestamp`` calls, never silently
  truncated to the most-recent 1000 bars.
- **B#2** TheGraph 4h down-sampling SUMS the per-hour flows (volume/fees) across
  the 4 constituent hours (they are flows, not levels) and carries TVL as the
  close level — not ¼ of the true total.
- **I#3** ``PoolHistoryServiceServicer.close()`` releases the dispatcher's
  aiohttp / GraphQL sessions (wired into gateway shutdown).
- **I#4** the TheGraph monthly-budget breaker is enforced BETWEEN paginated
  pages — a nearly-exhausted cap cannot be overspent mid-request.
- **I#6** ``dispatch()`` rejects an unvalidated ``pool_address`` before any
  provider egress (defense-in-depth).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, patch

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.pool_history._base import (
    _MonthlyBudgetTracker,
    _ProviderError,
    _TokenBucket,
)
from almanak.gateway.data.pool_history.defillama import DefiLlamaPoolHistoryProvider
from almanak.gateway.data.pool_history.dispatcher import PoolHistoryDispatcher
from almanak.gateway.data.pool_history.geckoterminal import GeckoTerminalPoolHistoryProvider
from almanak.gateway.data.pool_history.thegraph import (
    TheGraphPoolHistoryProvider,
    _aggregate_4h,
    _sum_flow,
)
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_history_service import PoolHistoryServiceServicer

# ===========================================================================
# B#2 — TheGraph 4h down-sampling sums per-hour flows (not 1/4)
# ===========================================================================


def test_thegraph_4h_sums_hourly_flows_and_closes_tvl():
    """A 4h bar's volume/fees == SUM of its 4 constituent hours; TVL == close."""
    rows = [
        {"periodStartUnix": 0, "tvlUSD": "100", "volumeUSD": "10", "feesUSD": "1"},
        {"periodStartUnix": 3600, "tvlUSD": "110", "volumeUSD": "20", "feesUSD": "2"},
        {"periodStartUnix": 7200, "tvlUSD": "120", "volumeUSD": "30", "feesUSD": "3"},
        {"periodStartUnix": 10800, "tvlUSD": "130", "volumeUSD": "40", "feesUSD": "4"},
    ]
    parsed = [(int(r["periodStartUnix"]), r) for r in rows]
    snaps = _aggregate_4h(parsed)

    assert len(snaps) == 1
    bar = snaps[0]
    assert bar.timestamp == 0  # 4h-aligned bucket start
    assert Decimal(bar.volume_24h) == Decimal("100")  # 10+20+30+40 — NOT 10 (the old 1/4 bug)
    assert Decimal(bar.fee_revenue_24h) == Decimal("10")  # 1+2+3+4
    assert Decimal(bar.tvl) == Decimal("130")  # close (last) level, NOT summed


def test_thegraph_4h_spans_two_buckets():
    """8 hourly rows aggregate into exactly 2 four-hour bars, each summed."""
    rows = [{"periodStartUnix": h * 3600, "tvlUSD": "1", "volumeUSD": "1", "feesUSD": "0"} for h in range(8)]
    parsed = [(int(r["periodStartUnix"]), r) for r in rows]
    snaps = _aggregate_4h(parsed)
    assert [s.timestamp for s in snaps] == [0, 14400]
    assert all(Decimal(s.volume_24h) == Decimal("4") for s in snaps)  # 4 hours x 1


def test_thegraph_4h_flow_empty_neq_zero():
    """Flow is "" only if EVERY constituent hour was unmeasured; measured-zero counts."""
    assert _sum_flow([{"volumeUSD": ""}, {"volumeUSD": ""}], "volumeUSD") == ""  # all unmeasured -> unmeasured
    assert _sum_flow([{"volumeUSD": ""}, {"volumeUSD": "5"}, {"volumeUSD": "0"}], "volumeUSD") == "5"  # 5 + measured-0


# ===========================================================================
# B#1 — GeckoTerminal paginates backward (no silent 1000-bar truncation)
# ===========================================================================


class _FakeResp:
    def __init__(self, status: int, payload: Any) -> None:
        self.status = status
        self._payload = payload

    async def json(self) -> Any:
        return self._payload

    async def text(self) -> str:
        return str(self._payload)

    async def __aenter__(self) -> _FakeResp:
        return self

    async def __aexit__(self, *_a: Any) -> bool:
        return False


class _FakeSession:
    """Returns pre-canned OHLCV pages in order; records the params per call."""

    def __init__(self, pages: list[Any]) -> None:
        self._pages = pages
        self.calls: list[dict] = []
        self.request_kwargs: list[dict[str, Any]] = []

    def get(self, url: str, params: dict | None = None, **kwargs: Any) -> _FakeResp:
        self.calls.append(params or {})
        self.request_kwargs.append({"url": url, "params": params or {}, **kwargs})
        idx = len(self.calls) - 1
        return self._pages[idx]


def _ohlcv_payload(rows: list[list[Any]]) -> dict:
    return {"data": {"attributes": {"ohlcv_list": rows}}}


def test_geckoterminal_paginates_backward_over_window():
    """A window longer than one page triggers a second before_timestamp call and
    returns the FULL series — never just the most-recent page (audit blocker #1)."""
    # newest-first pages; patch the limit small so 2 pages are needed.
    page1 = [[9 * 3600, 0, 0, 0, 0, "9"], [8 * 3600, 0, 0, 0, 0, "8"], [7 * 3600, 0, 0, 0, 0, "7"]]
    page2 = [[6 * 3600, 0, 0, 0, 0, "6"], [5 * 3600, 0, 0, 0, 0, "5"]]  # partial -> stop
    session = _FakeSession([_FakeResp(200, _ohlcv_payload(page1)), _FakeResp(200, _ohlcv_payload(page2))])

    provider = GeckoTerminalPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        api_key="test-key",
    )

    with patch("almanak.gateway.data.pool_history.geckoterminal._OHLCV_LIMIT", 3):
        result = asyncio.run(
            provider.fetch(
                chain="base",
                pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
                protocol="aerodrome",
                start_ts=0,
                end_ts=10 * 3600,
                resolution=gateway_pb2.Resolution.RESOLUTION_1H,
            )
        )

    assert isinstance(result, list)
    assert len(result) == 5  # both pages, not just the 3 most-recent
    assert [s.timestamp for s in result] == [5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600]
    assert len(session.calls) == 2  # paginated
    # 2nd call pages backward from the 1st page's oldest bar (7*3600).
    assert session.calls[1].get("before_timestamp") == str(7 * 3600)


def test_geckoterminal_404_first_page_is_not_found():
    """404 on the first page => pool not found (None), not a partial success."""
    session = _FakeSession([_FakeResp(404, {})])
    provider = GeckoTerminalPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        api_key="test-key",
    )
    result = asyncio.run(
        provider.fetch(
            chain="base",
            pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
            protocol="aerodrome",
            start_ts=0,
            end_ts=10 * 3600,
            resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        )
    )
    assert result is None


def test_geckoterminal_missing_api_key_fails_before_egress():
    """CoinGecko Onchain pool-history fallback requires a gateway-owned key."""
    session = _FakeSession([_FakeResp(200, _ohlcv_payload([[3600, 0, 0, 0, 0, "1"]]))])
    provider = GeckoTerminalPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        api_key="",
    )

    try:
        asyncio.run(
            provider.fetch(
                chain="base",
                pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
                protocol="aerodrome",
                start_ts=0,
                end_ts=10 * 3600,
                resolution=gateway_pb2.Resolution.RESOLUTION_1H,
            )
        )
        raise AssertionError("expected _ProviderError")
    except _ProviderError as exc:
        assert "requires a valid COINGECKO_API_KEY" in str(exc)

    assert session.calls == []


def test_geckoterminal_401_with_key_mentions_key_validation():
    """Invalid or expired keys get the same operator-facing 401 guidance."""
    session = _FakeSession([_FakeResp(401, {"error": "bad key"})])
    provider = GeckoTerminalPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        api_key="bad-key",
    )

    try:
        asyncio.run(
            provider.fetch(
                chain="base",
                pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
                protocol="aerodrome",
                start_ts=0,
                end_ts=10 * 3600,
                resolution=gateway_pb2.Resolution.RESOLUTION_1H,
            )
        )
        raise AssertionError("expected _ProviderError")
    except _ProviderError as exc:
        text = str(exc)
        assert "requires a valid COINGECKO_API_KEY" in text
        assert "HTTP 401" in text

    assert session.request_kwargs[0]["headers"]["x-cg-pro-api-key"] == "bad-key"


# ===========================================================================
# I#4 — TheGraph monthly budget enforced BETWEEN paginated pages
# ===========================================================================


def test_thegraph_budget_enforced_between_pages():
    """With budget_max=1 and a multi-page response, the breaker raises mid-
    pagination (after page 1) instead of overspending the cap (audit Important #4)."""
    full_page = [{"periodStartUnix": h * 3600, "tvlUSD": "1", "volumeUSD": "1", "feesUSD": "1"} for h in range(2)]
    client = AsyncMock()
    client.query = AsyncMock(return_value={"poolHourDatas": full_page})

    provider = TheGraphPoolHistoryProvider(
        client=client,
        url_resolver=lambda _p, _c: "https://example/subgraph",
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        budget=_MonthlyBudgetTracker(budget_max=1),
    )

    with patch("almanak.gateway.data.pool_history.thegraph._PAGE_SIZE", 2):
        try:
            asyncio.run(
                provider.fetch(
                    chain="arbitrum",
                    pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
                    protocol="uniswap_v3",
                    start_ts=0,
                    end_ts=100 * 3600,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                )
            )
            raised = None
        except _ProviderError as exc:
            raised = exc

    assert raised is not None
    assert "budget exhausted mid-pagination" in str(raised)
    assert client.query.await_count == 1  # only page 0 issued; page 1 blocked by the breaker


# ===========================================================================
# I#6 — dispatch() rejects an unvalidated pool_address before any provider
# ===========================================================================


def _dispatcher() -> PoolHistoryDispatcher:
    return PoolHistoryDispatcher(
        thegraph_api_key=None,
        thegraph_monthly_budget_max=100000,
        is_supported_fn=lambda _c, _p: True,
    )


def test_dispatch_rejects_invalid_address_before_providers():
    """A malformed EVM address never reaches a provider egress URL."""
    disp = _dispatcher()
    disp._thegraph.fetch = AsyncMock()  # type: ignore[method-assign]
    disp._geckoterminal.fetch = AsyncMock()  # type: ignore[method-assign]

    outcome = asyncio.run(
        disp.dispatch(
            chain="arbitrum",
            pool_address="0xdead",  # invalid EVM syntax
            protocol="uniswap_v3",
            start_ts=0,
            end_ts=3600,
            resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        )
    )

    assert outcome.success is False
    assert "invalid pool_address" in outcome.error
    disp._thegraph.fetch.assert_not_called()
    disp._geckoterminal.fetch.assert_not_called()
    asyncio.run(disp.close())


# ===========================================================================
# I#3 — servicer.close() releases the dispatcher's sessions (idempotent)
# ===========================================================================


def test_servicer_close_releases_session_and_is_idempotent():
    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True))

    async def _run() -> None:
        session = await servicer._dispatcher._get_http_session()
        assert not session.closed
        await servicer.close()
        assert session.closed
        # idempotent: a second close (and with no live session) must not raise.
        await servicer.close()

    asyncio.run(_run())


# ===========================================================================
# Gemini review — provider robustness against malformed / null upstream bodies
# ===========================================================================
# Two classes of upstream garbage must NOT escape the 3-state provider taxonomy:
#   1. A 200 with a body that fails JSON decode (json.JSONDecodeError, a
#      ValueError) -> must map to _ProviderError, not crash the handler.
#   2. A present-but-null "data" field ({"data": null}) -> must coerce to a
#      safe empty result, not raise TypeError/AttributeError downstream.


class _RaisingResp(_FakeResp):
    """A response whose ``json()`` raises (e.g. JSONDecodeError on a 200)."""

    def __init__(self, status: int, exc: BaseException) -> None:
        super().__init__(status, None)
        self._exc = exc

    async def json(self) -> Any:
        raise self._exc


def test_geckoterminal_null_data_does_not_crash():
    """A ``{"data": null}`` body coerces to no-rows (None), not AttributeError."""
    session = _FakeSession([_FakeResp(200, {"data": None})])
    provider = GeckoTerminalPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        api_key="test-key",
    )
    result = asyncio.run(
        provider.fetch(
            chain="base",
            pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
            protocol="aerodrome",
            start_ts=0,
            end_ts=10 * 3600,
            resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        )
    )
    assert result is None


def test_geckoterminal_json_decode_error_maps_to_provider_error():
    """A JSONDecodeError (ValueError) on a 200 maps to _ProviderError, not a crash."""
    import json

    session = _FakeSession([_RaisingResp(200, json.JSONDecodeError("boom", "", 0))])
    provider = GeckoTerminalPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        api_key="test-key",
    )
    try:
        asyncio.run(
            provider.fetch(
                chain="base",
                pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
                protocol="aerodrome",
                start_ts=0,
                end_ts=10 * 3600,
                resolution=gateway_pb2.Resolution.RESOLUTION_1H,
            )
        )
        raise AssertionError("expected _ProviderError")
    except _ProviderError as exc:
        assert "coingecko_onchain" in str(exc)


def test_defillama_json_decode_error_maps_to_provider_error():
    """A malformed catalog body (JSONDecodeError) maps to _ProviderError."""
    import json

    session = _FakeSession([_RaisingResp(200, json.JSONDecodeError("boom", "", 0))])
    provider = DefiLlamaPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        slug_resolver=lambda _protocol: None,
        rate_limiter=_TokenBucket(rate=100, period=1.0),
    )
    try:
        asyncio.run(
            provider.fetch(
                chain="ethereum",
                pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
                protocol="uniswap_v3",
                start_ts=0,
                end_ts=86400,
                resolution=gateway_pb2.Resolution.RESOLUTION_1D,
            )
        )
        raise AssertionError("expected _ProviderError")
    except _ProviderError as exc:
        assert "defillama" in str(exc)


def test_defillama_null_catalog_data_coerces_to_empty():
    """A ``{"data": null}`` catalog coerces to [] -> no match -> None (no TypeError)."""
    session = _FakeSession([_FakeResp(200, {"data": None})])
    provider = DefiLlamaPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        slug_resolver=lambda _protocol: None,
        rate_limiter=_TokenBucket(rate=100, period=1.0),
    )
    result = asyncio.run(
        provider.fetch(
            chain="ethereum",
            pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
            protocol="uniswap_v3",
            start_ts=0,
            end_ts=86400,
            resolution=gateway_pb2.Resolution.RESOLUTION_1D,
        )
    )
    assert result is None
