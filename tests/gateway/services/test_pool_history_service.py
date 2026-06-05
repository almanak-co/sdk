"""Gateway-side PoolHistoryService provider + dispatcher tests (POOL-5 / VIB-4753).

Covers the POOL-5 rows of the umbrella UAT card
``docs/internal/uat-cards/VIB-4728.md``:

* D1.S1  — ``test_get_pool_history_arbitrum_univ3_thegraph``
* D1.S3  — ``test_recorded_fixture_per_provider_chain``
* D2.M1  — ``test_chain_matrix_arbitrum_ethereum_base``
* D2.M2  — ``test_provider_fallback_*`` + ``test_defillama_skipped_for_1h``
* D2.M3  — ``test_resolution_matrix`` + ``test_defillama_skipped_for_4h`` +
           ``test_thegraph_multipage_aggregation``
* D3.F2  — ``test_all_providers_unavailable``
* D3.F6  — ``test_pool_not_found_never_returns_empty_envelope``
* D3.F11 — ``test_budget_trip_falls_back`` (trip behaviour) +
           ``test_thegraph_budget_counter_health_export``

Tests patch the upstream provider seams (``GatewayGraphQLClient.query`` for
TheGraph; ``_query_pools`` / ``_query_chart`` for DefiLlama; ``_query_ohlcv``
for GeckoTerminal) with recorded JSON fixtures under
``tests/gateway/services/fixtures/pool_history/`` — no live external API is
reached. Patching at those seams is the "equivalent in-test HTTP mocking" the
UAT card permits in lieu of ``aioresponses`` (not a project dependency).

The kill-switch is flipped ``true`` inside every test (prod default stays
false until POOL-9).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.pool_history._graphql import SubgraphConnectionError
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_history_service import PoolHistoryServiceServicer

_FIXTURES = Path(__file__).parent / "fixtures" / "pool_history"

# Canonical pool addresses (lowercased EVM).
_ARB_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"  # USDC/WETH Arbitrum
_ETH_POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC/WETH Ethereum
_BASE_POOL = "0x4c36388be6f416a29c8d8eee81c771ce6be14b18"  # USDC/WETH Base (univ3)
_BASE_AERO_POOL = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"  # Aerodrome Base

HOUR = 3600
DAY = 86400


def _fx(name: str) -> Any:
    with (_FIXTURES / name).open("r") as f:
        return json.load(f)


def _enabled_servicer() -> PoolHistoryServiceServicer:
    return PoolHistoryServiceServicer(
        GatewaySettings(pool_history_enabled=True, coingecko_api_key="test-key")
    )


class _Ctx:
    """Captures grpc status code + details set on a ServicerContext."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _request(
    *,
    pool_address: str,
    chain: str,
    protocol: str,
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


# -----------------------------------------------------------------------------
# Mock seams — each returns an AsyncMock so call_count can be asserted.
# -----------------------------------------------------------------------------


def _thegraph_query_mock(data_key: str, rows: list[dict], page_size: int = 1000) -> AsyncMock:
    """Mock ``GatewayGraphQLClient.query`` paginating ``rows`` by ``page_size``.

    Returns ``{data_key: <page slice>}`` keyed by the ``skip`` variable so the
    provider's pagination loop assembles the full series across page boundaries.
    """

    async def _side_effect(*, url: str, query: str, variables: dict) -> dict:
        skip = int(variables.get("skip", 0))
        first = int(variables.get("first", page_size))
        page = rows[skip : skip + first]
        return {data_key: page}

    return AsyncMock(side_effect=_side_effect)


def _patch_thegraph(servicer: PoolHistoryServiceServicer, mock: AsyncMock):
    return patch.object(servicer._dispatcher._graphql, "query", new=mock)


def _patch_defillama(
    servicer: PoolHistoryServiceServicer,
    *,
    pools: list[dict] | Exception,
    chart: list[dict] | Exception,
) -> list:
    pools_mock = AsyncMock(side_effect=pools) if isinstance(pools, Exception) else AsyncMock(return_value=pools)
    chart_mock = AsyncMock(side_effect=chart) if isinstance(chart, Exception) else AsyncMock(return_value=chart)
    return [
        patch.object(servicer._dispatcher._defillama, "_query_pools", new=pools_mock),
        patch.object(servicer._dispatcher._defillama, "_query_chart", new=chart_mock),
    ]


def _patch_geckoterminal(servicer: PoolHistoryServiceServicer, ohlcv: list | None | Exception) -> AsyncMock:
    mock = AsyncMock(side_effect=ohlcv) if isinstance(ohlcv, Exception) else AsyncMock(return_value=ohlcv)
    return mock


# =============================================================================
# D1.S1 — Arbitrum UniV3 happy path through TheGraph
# =============================================================================


def test_get_pool_history_arbitrum_univ3_thegraph():
    """D1.S1: string-decimal snapshots for an Arbitrum UniV3 pool via TheGraph.

    168 rows (7d @ 1h), aligned to the hour, source='the_graph', no truncation.
    """
    fx = _fx("the_graph_arbitrum_univ3_7d_1h.json")
    rows = fx["poolHourDatas"]
    start = fx["meta"]["start_ts"]
    end = start + 168 * HOUR

    servicer = _enabled_servicer()
    ctx = _Ctx()
    req = _request(
        pool_address=_ARB_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=start,
        end_ts=end,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    with _patch_thegraph(servicer, _thegraph_query_mock("poolHourDatas", rows)):
        response = asyncio.run(servicer.GetPoolHistory(req, ctx))

    assert ctx.code is None  # gRPC OK
    assert response.success is True
    assert len(response.snapshots) == 168
    assert all(s.timestamp % HOUR == 0 for s in response.snapshots)
    assert response.snapshots[0].timestamp == start
    assert response.snapshots[0].tvl == "1210000.0"
    assert response.source == "the_graph"
    assert response.truncation_reason == gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED
    assert response.next_start_ts == 0
    assert response.finalized_only is True
    # Empty != Zero: reserves are unmeasured on the V3 hour schema -> "" and
    # listed in unmeasured_fields, NEVER "0".
    row0 = response.snapshots[0]
    assert row0.token0_reserve == ""
    assert row0.token1_reserve == ""
    assert "token0_reserve" in row0.unmeasured_fields
    assert "token1_reserve" in row0.unmeasured_fields
    assert "tvl" not in row0.unmeasured_fields  # measured


# =============================================================================
# D1.S3 — recorded fixture per (provider × chain) representative cell
# =============================================================================


@pytest.mark.acceptance_pack
def test_recorded_fixture_per_provider_chain():
    """D1.S3: each PROVIDER exercised on at least one chain at its eligible
    resolution, with the OTHER providers' seams proven uncalled."""
    # Cell 1: the_graph, arbitrum, uniswap_v3, 1h, 7d.
    fx = _fx("the_graph_arbitrum_univ3_7d_1h.json")
    start = fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_mock = _thegraph_query_mock("poolHourDatas", fx["poolHourDatas"])
    gt_mock = _patch_geckoterminal(servicer, [])
    with _patch_thegraph(servicer, tg_mock), patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 168 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "the_graph"
    assert len(resp.snapshots) == 168
    assert resp.snapshots[0].tvl == "1210000.0"
    assert gt_mock.call_count == 0  # TheGraph won; GeckoTerminal never hit.

    # Cell 2: defillama, ethereum, uniswap_v3, 1d, 30d. Force TheGraph down so
    # DefiLlama (1d-eligible) serves.
    dl = _fx("defillama_ethereum_univ3_30d_1d.json")
    start_1d = dl["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph down"))
    gt_mock = _patch_geckoterminal(servicer, [])
    patches = _patch_defillama(servicer, pools=dl["catalog"]["data"], chart=dl["chart"]["data"])
    with _patch_thegraph(servicer, tg_down), patches[0], patches[1], patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ETH_POOL,
                    chain="ethereum",
                    protocol="uniswap_v3",
                    start_ts=start_1d,
                    end_ts=start_1d + 30 * DAY,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "defillama"
    assert len(resp.snapshots) == 30
    assert resp.snapshots[0].tvl == "215000000.0"
    assert gt_mock.call_count == 0  # DefiLlama served; GeckoTerminal never hit.

    # Cell 3: geckoterminal, base, aerodrome, 1h, 7d. Aerodrome has no
    # subgraph URL (the_graph -> _NOT_ATTEMPTED) and DefiLlama is 1h-ineligible
    # -> GeckoTerminal serves.
    gt_fx = _fx("geckoterminal_base_aerodrome_7d_1h.json")
    start_1h = gt_fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_mock = AsyncMock()  # must NOT be called (no aerodrome subgraph)
    dl_pools = AsyncMock()  # must NOT be called (DefiLlama skipped @1h)
    gt_mock = _patch_geckoterminal(servicer, gt_fx["ohlcv_list"])
    with _patch_thegraph(servicer, tg_mock), patch.object(
        servicer._dispatcher._defillama, "_query_pools", new=dl_pools
    ), patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_BASE_AERO_POOL,
                    chain="base",
                    protocol="aerodrome",
                    start_ts=start_1h,
                    end_ts=start_1h + 168 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "geckoterminal"
    assert len(resp.snapshots) == 168
    assert resp.snapshots[0].volume_24h == "2000.0"
    assert tg_mock.call_count == 0  # no aerodrome subgraph -> never queried
    assert dl_pools.call_count == 0  # DefiLlama skipped at 1h


# =============================================================================
# D2.M1 — chain matrix Arbitrum + Ethereum + Base (+ Base aerodrome)
# =============================================================================


@pytest.mark.parametrize(
    "chain, protocol, pool, fixture, expected_tvl",
    [
        ("arbitrum", "uniswap_v3", _ARB_POOL, "the_graph_arbitrum_univ3_7d_1h.json", "1210000.0"),
        ("ethereum", "uniswap_v3", _ETH_POOL, "the_graph_ethereum_univ3_7d_1h.json", "215000000.0"),
        ("base", "uniswap_v3", _BASE_POOL, "the_graph_base_univ3_7d_1h.json", "5000000.0"),
    ],
)
@pytest.mark.acceptance_pack
def test_chain_matrix_arbitrum_ethereum_base(chain, protocol, pool, fixture, expected_tvl):
    """D2.M1: each chain resolves its own subgraph URL and serves correct data."""
    fx = _fx(fixture)
    start = fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    captured_urls: list[str] = []

    async def _capture(*, url: str, query: str, variables: dict) -> dict:
        captured_urls.append(url)
        skip = int(variables.get("skip", 0))
        return {"poolHourDatas": fx["poolHourDatas"][skip : skip + 1000]}

    with _patch_thegraph(servicer, AsyncMock(side_effect=_capture)):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=pool,
                    chain=chain,
                    protocol=protocol,
                    start_ts=start,
                    end_ts=start + 168 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "the_graph"
    assert resp.snapshots[0].tvl == expected_tvl
    # The outgoing TheGraph query went to this chain's registered subgraph URL.
    from almanak.gateway.data.pool_history.dispatcher import _resolve_subgraph_url

    expected_url = _resolve_subgraph_url(protocol, chain)
    assert expected_url is not None
    assert captured_urls and all(u == expected_url for u in captured_urls)


def test_chain_matrix_base_aerodrome_falls_through_to_geckoterminal():
    """D2.M1: (base, aerodrome) has no registered subgraph URL, so the
    TheGraph leg is NOT_ATTEMPTED and GeckoTerminal serves the cell."""
    gt_fx = _fx("geckoterminal_base_aerodrome_7d_1h.json")
    start = gt_fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_mock = AsyncMock()
    gt_mock = _patch_geckoterminal(servicer, gt_fx["ohlcv_list"])
    with _patch_thegraph(servicer, tg_mock), patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_BASE_AERO_POOL,
                    chain="base",
                    protocol="aerodrome",
                    start_ts=start,
                    end_ts=start + 168 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "geckoterminal"
    assert tg_mock.call_count == 0  # no aerodrome subgraph URL registered


# =============================================================================
# D2.M2 — provider fallback matrix
# =============================================================================


def test_provider_fallback_thegraph_to_geckoterminal_1h():
    """TheGraph raises @1h -> GeckoTerminal serves; DefiLlama call count 0."""
    gt_fx = _fx("geckoterminal_arbitrum_univ3_7d_1h.json")
    start = gt_fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph down"))
    dl_pools = AsyncMock()
    gt_mock = _patch_geckoterminal(servicer, gt_fx["ohlcv_list"])
    with _patch_thegraph(servicer, tg_down), patch.object(
        servicer._dispatcher._defillama, "_query_pools", new=dl_pools
    ), patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 168 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "geckoterminal"
    assert dl_pools.call_count == 0  # DefiLlama skipped at 1h
    # provider_fallback incremented for the TheGraph failure.
    assert servicer.health()["per_rpc"]["provider_fallback"] >= 1
    assert servicer.health()["per_provider"]["the_graph"]["errors"] == 1


def test_provider_fallback_thegraph_to_defillama_1d():
    """TheGraph raises @1d -> DefiLlama serves; GeckoTerminal call count 0."""
    dl = _fx("defillama_arbitrum_univ3_30d_1d.json")
    start = dl["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph down"))
    gt_mock = _patch_geckoterminal(servicer, [])
    patches = _patch_defillama(servicer, pools=dl["catalog"]["data"], chart=dl["chart"]["data"])
    with _patch_thegraph(servicer, tg_down), patches[0], patches[1], patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 30 * DAY,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "defillama"
    assert gt_mock.call_count == 0  # DefiLlama served; GeckoTerminal never hit.


@pytest.mark.acceptance_pack
def test_provider_fallback_full_chain_1d_to_geckoterminal():
    """TheGraph raises + DefiLlama raises @1d -> GeckoTerminal serves.

    All three tried in order; provider_fallback increments to 2 (the_graph
    fail + defillama fail)."""
    gt_fx = _fx("geckoterminal_arbitrum_univ3_90d_1d.json")
    start = gt_fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph down"))
    gt_mock = _patch_geckoterminal(servicer, gt_fx["ohlcv_list"])
    patches = _patch_defillama(
        servicer,
        pools=aiohttp.ClientError("defillama down"),
        chart=aiohttp.ClientError("defillama down"),
    )
    with _patch_thegraph(servicer, tg_down), patches[0], patches[1], patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 90 * DAY,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "geckoterminal"
    health = servicer.health()
    assert health["per_rpc"]["provider_fallback"] == 2  # the_graph + defillama
    assert health["per_provider"]["the_graph"]["errors"] == 1
    assert health["per_provider"]["defillama"]["errors"] == 1
    assert gt_mock.call_count == 1


def test_defillama_skipped_for_1h():
    """TheGraph raises @1h: DefiLlama HTTP seam is NEVER hit (daily-only)."""
    gt_fx = _fx("geckoterminal_arbitrum_univ3_7d_1h.json")
    start = gt_fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph down"))
    dl_pools = AsyncMock()
    gt_mock = _patch_geckoterminal(servicer, gt_fx["ohlcv_list"])
    with _patch_thegraph(servicer, tg_down), patch.object(
        servicer._dispatcher._defillama, "_query_pools", new=dl_pools
    ), patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 168 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert dl_pools.call_count == 0


# =============================================================================
# D2.M3 — resolution matrix 1h / 4h / 1d + 4h-skip + multi-page
# =============================================================================


@pytest.mark.parametrize(
    "resolution, fixture, data_key, n_rows, grid",
    [
        (gateway_pb2.Resolution.RESOLUTION_1H, "the_graph_arbitrum_univ3_7d_1h.json", "poolHourDatas", 168, HOUR),
        (gateway_pb2.Resolution.RESOLUTION_4H, "the_graph_arbitrum_univ3_30d_4h_hourly.json", "poolHourDatas", 180, 14400),
        (gateway_pb2.Resolution.RESOLUTION_1D, "the_graph_arbitrum_univ3_90d_1d.json", "poolDayDatas", 90, DAY),
    ],
)
def test_resolution_matrix(resolution, fixture, data_key, n_rows, grid):
    """D2.M3: each resolution serves the right row count, aligned to the grid.

    1h: 168 rows @3600; 4h: 180 rows @14400 (down-sampled from hourly); 1d:
    90 rows @86400.
    """
    fx = _fx(fixture)
    rows = fx[data_key]
    start = fx["meta"]["start_ts"]
    # Window spans the whole fixture (hourly fixture for 4h covers 720h = 30d).
    span = len(rows) * (HOUR if data_key == "poolHourDatas" else DAY)
    servicer = _enabled_servicer()
    ctx = _Ctx()
    with _patch_thegraph(servicer, _thegraph_query_mock(data_key, rows)):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + span,
                    resolution=resolution,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "the_graph"
    assert len(resp.snapshots) == n_rows
    assert all(s.timestamp % grid == 0 for s in resp.snapshots)


def test_defillama_skipped_for_4h():
    """D2.M3 (Codex Round-3 #1): TheGraph raises @4h -> DefiLlama seam NEVER
    hit; chain falls through to GeckoTerminal. Prevents silently relabeling
    DefiLlama daily data as 4h history."""
    gt_fx = _fx("geckoterminal_arbitrum_univ3_7d_1h.json")  # hour timeframe, aggregate=4
    start = gt_fx["meta"]["start_ts"]
    # The OHLCV fixture is hourly; for 4h only rows on the 4h grid survive.
    servicer = _enabled_servicer()
    ctx = _Ctx()
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph down"))
    dl_pools = AsyncMock()
    gt_mock = _patch_geckoterminal(servicer, gt_fx["ohlcv_list"])
    with _patch_thegraph(servicer, tg_down), patch.object(
        servicer._dispatcher._defillama, "_query_pools", new=dl_pools
    ), patch.object(servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 168 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_4H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "geckoterminal"
    assert dl_pools.call_count == 0  # DefiLlama skipped at 4h
    assert all(s.timestamp % 14400 == 0 for s in resp.snapshots)


def test_thegraph_multipage_aggregation():
    """D2.M3 (Codex Round-3 #2): a 90d 1h request (2160 rows) is served by
    TheGraph in three paginated chunks (1000 + 1000 + 160), assembled
    continuous across page boundaries. NOT a truncation case."""
    fx = _fx("the_graph_arbitrum_univ3_90d_1h.json")
    rows = fx["poolHourDatas"]
    start = fx["meta"]["start_ts"]
    assert len(rows) == 2160
    servicer = _enabled_servicer()
    ctx = _Ctx()

    captured_skips: list[int] = []

    async def _paged(*, url: str, query: str, variables: dict) -> dict:
        skip = int(variables.get("skip", 0))
        captured_skips.append(skip)
        return {"poolHourDatas": rows[skip : skip + 1000]}

    with _patch_thegraph(servicer, AsyncMock(side_effect=_paged)):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 2160 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert len(resp.snapshots) == 2160
    assert captured_skips == [0, 1000, 2000]  # exactly 3 pages
    # Continuous, no gap at page boundaries.
    assert resp.snapshots[0].timestamp + HOUR == resp.snapshots[1].timestamp
    assert resp.snapshots[999].timestamp + HOUR == resp.snapshots[1000].timestamp  # page 0->1 seam
    assert resp.snapshots[1999].timestamp + HOUR == resp.snapshots[2000].timestamp  # page 1->2 seam
    assert resp.truncation_reason == gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED
    assert resp.next_start_ts == 0


# =============================================================================
# D3.F2 — all upstream providers fail -> success=False + UNAVAILABLE
# =============================================================================


def test_all_providers_unavailable():
    """D3.F2: all eligible providers raise ClientError -> success=False,
    UNAVAILABLE, error mentions provider names, 'All providers failed' logged."""
    servicer = _enabled_servicer()
    ctx = _Ctx()
    start = 1_699_920_000  # day-aligned
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph 503"))
    gt_down = _patch_geckoterminal(servicer, aiohttp.ClientError("gt 503"))
    patches = _patch_defillama(
        servicer,
        pools=aiohttp.ClientError("llama 503"),
        chart=aiohttp.ClientError("llama 503"),
    )
    with _patch_thegraph(servicer, tg_down), patches[0], patches[1], patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_down
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 30 * DAY,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                ctx,
            )
        )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    # 1d chain attempts all three eligible providers.
    assert "the_graph" in resp.error
    assert "defillama" in resp.error
    assert "geckoterminal" in resp.error
    # Failure-envelope shape.
    assert resp.source == ""
    assert resp.next_start_ts == 0
    assert resp.finalized_only is False
    assert len(resp.snapshots) == 0


def test_keyless_coingecko_onchain_fallback_fails_before_egress():
    """A keyless gateway must fail fast before CoinGecko Onchain egress."""
    servicer = PoolHistoryServiceServicer(
        GatewaySettings(pool_history_enabled=True, coingecko_api_key="")
    )
    ctx = _Ctx()
    start = 1_699_920_000
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("thegraph 503"))
    gt_ohlcv = AsyncMock()

    with _patch_thegraph(servicer, tg_down), patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_ohlcv
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 7 * HOUR,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1H,
                ),
                ctx,
            )
        )

    assert resp.success is False
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert "requires a valid COINGECKO_API_KEY" in resp.error
    assert gt_ohlcv.call_count == 0


def test_failed_then_success_reattempts_providers():
    """Decision #4 (no negative caching): a failed cold call followed by an
    identical succeeding call MUST re-hit providers (failure is not cached)."""
    fx = _fx("the_graph_arbitrum_univ3_7d_1h.json")
    rows = fx["poolHourDatas"]
    start = fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    req = _request(
        pool_address=_ARB_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=start,
        end_ts=start + 168 * HOUR,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )

    # First call: TheGraph down, GeckoTerminal down -> failure.
    ctx1 = _Ctx()
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("down"))
    gt_down = _patch_geckoterminal(servicer, aiohttp.ClientError("down"))
    with _patch_thegraph(servicer, tg_down), patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_down
    ):
        resp1 = asyncio.run(servicer.GetPoolHistory(req, ctx1))
    assert resp1.success is False
    assert ctx1.code == grpc.StatusCode.UNAVAILABLE

    # Second identical call: TheGraph now serves -> success. Proves the
    # failure was NOT cached (the fetcher re-ran and hit providers again).
    ctx2 = _Ctx()
    tg_ok = _thegraph_query_mock("poolHourDatas", rows)
    with _patch_thegraph(servicer, tg_ok):
        resp2 = asyncio.run(servicer.GetPoolHistory(req, ctx2))
    assert resp2.success is True
    assert resp2.source == "the_graph"
    assert tg_ok.call_count >= 1  # providers were re-attempted, not served from a negative cache


# =============================================================================
# D3.F6 — pool not found never returns an empty success envelope
# =============================================================================


@pytest.mark.acceptance_pack
def test_pool_not_found_never_returns_empty_envelope():
    """D3.F6 (HARD GATE): all providers return 'not found' -> success=False,
    UNAVAILABLE, NEVER success=True with snapshots=[]. Failure-envelope shape
    is internally consistent; error mentions 'not found'."""
    servicer = _enabled_servicer()
    ctx = _Ctx()
    start = 1_699_920_000
    # TheGraph returns {pool: null} -> empty rows (not-found). DefiLlama returns
    # no matching pool (empty catalog). GeckoTerminal returns 404 (None).
    tg_empty = AsyncMock(return_value={"poolDayDatas": []})
    gt_404 = _patch_geckoterminal(servicer, None)  # 404
    patches = _patch_defillama(servicer, pools=[], chart=[])
    with _patch_thegraph(servicer, tg_empty), patches[0], patches[1], patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_404
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 30 * DAY,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                ctx,
            )
        )
    assert resp.success is False
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert "not found" in resp.error
    # Internally-consistent failure envelope (Codex Round-2 #4).
    assert resp.truncation_reason == gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED
    assert resp.next_start_ts == 0
    assert resp.finalized_only is False
    assert resp.source in ("", "none")
    assert len(resp.snapshots) == 0
    assert resp.error  # non-empty


def test_defillama_substring_match_does_not_false_positive():
    """Inherited #5 / decision #9: DefiLlama matches on EQUALITY of the
    address segment, NOT substring. A catalog entry whose pool id contains the
    requested address as a substring of a LONGER hex MUST NOT match."""
    servicer = _enabled_servicer()
    ctx = _Ctx()
    start = 1_699_920_000
    # Only the decoy (longer hex containing _ARB_POOL) is in the catalog.
    decoy_catalog = [
        {"pool": f"arbitrum-{_ARB_POOL}deadbeef", "chain": "Arbitrum", "project": "uniswap-v3", "symbol": "DECOY"},
    ]
    tg_down = AsyncMock(side_effect=SubgraphConnectionError("down"))
    gt_404 = _patch_geckoterminal(servicer, None)
    patches = _patch_defillama(servicer, pools=decoy_catalog, chart=[])
    with _patch_thegraph(servicer, tg_down), patches[0], patches[1], patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_404
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 30 * DAY,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                ctx,
            )
        )
    # The decoy must NOT match -> not found -> failure.
    assert resp.success is False
    assert "defillama" in resp.error and "not found" in resp.error


# =============================================================================
# D3.F11 — TheGraph monthly-budget circuit breaker (trip behaviour)
# =============================================================================


def test_budget_trip_falls_back():
    """D3.F11 trip: with the monthly budget tripped, a 1d request skips
    TheGraph entirely (0 calls) and serves from DefiLlama."""
    dl = _fx("defillama_arbitrum_univ3_30d_1d.json")
    start = dl["meta"]["start_ts"]
    # Force the breaker tripped: budget_max=1, pre-record one query.
    servicer = PoolHistoryServiceServicer(
        GatewaySettings(pool_history_enabled=True, pool_history_thegraph_monthly_budget_max=1)
    )
    servicer._dispatcher._budget.record_query()
    assert servicer._dispatcher._budget.is_tripped()

    ctx = _Ctx()
    tg_mock = AsyncMock()  # must NOT be called
    gt_mock = _patch_geckoterminal(servicer, [])
    patches = _patch_defillama(servicer, pools=dl["catalog"]["data"], chart=dl["chart"]["data"])
    with _patch_thegraph(servicer, tg_mock), patches[0], patches[1], patch.object(
        servicer._dispatcher._geckoterminal, "_query_ohlcv", new=gt_mock
    ):
        resp = asyncio.run(
            servicer.GetPoolHistory(
                _request(
                    pool_address=_ARB_POOL,
                    chain="arbitrum",
                    protocol="uniswap_v3",
                    start_ts=start,
                    end_ts=start + 30 * DAY,
                    resolution=gateway_pb2.Resolution.RESOLUTION_1D,
                ),
                ctx,
            )
        )
    assert resp.success is True
    assert resp.source == "defillama"
    assert tg_mock.call_count == 0  # TheGraph skipped — breaker tripped.
    assert gt_mock.call_count == 0  # DefiLlama served before GeckoTerminal.


def test_thegraph_budget_counter_health_export():
    """D3.F11 export: after N successful TheGraph calls, health() exposes
    the_graph_monthly_queries == N and the configured budget_max."""
    fx = _fx("the_graph_arbitrum_univ3_7d_1h.json")
    rows = fx["poolHourDatas"]
    start = fx["meta"]["start_ts"]
    servicer = PoolHistoryServiceServicer(
        GatewaySettings(pool_history_enabled=True, pool_history_thegraph_monthly_budget_max=12345)
    )
    req = _request(
        pool_address=_ARB_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=start,
        end_ts=start + 168 * HOUR,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    # One successful TheGraph call == one page == one budget query (168 rows < page_size).
    with _patch_thegraph(servicer, _thegraph_query_mock("poolHourDatas", rows)):
        asyncio.run(servicer.GetPoolHistory(req, _Ctx()))
    budget = servicer.health()["budget"]
    assert budget["the_graph_monthly_queries"] == 1
    assert budget["the_graph_monthly_budget_max"] == 12345


# =============================================================================
# Cache: identical request within TTL skips upstream (D2.M4-adjacent;
# proves the dispatch path writes the public cache through get_or_fetch).
# =============================================================================


def test_cache_hit_skips_upstream_on_second_call():
    """A second identical request returns from the public cache with NO new
    TheGraph call (the dispatch path writes the cache via get_or_fetch)."""
    fx = _fx("the_graph_arbitrum_univ3_7d_1h.json")
    rows = fx["poolHourDatas"]
    start = fx["meta"]["start_ts"]
    servicer = _enabled_servicer()
    req = _request(
        pool_address=_ARB_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=start,
        end_ts=start + 168 * HOUR,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    tg_mock = _thegraph_query_mock("poolHourDatas", rows)
    with _patch_thegraph(servicer, tg_mock):
        r1 = asyncio.run(servicer.GetPoolHistory(req, _Ctx()))
        calls_after_first = tg_mock.call_count
        r2 = asyncio.run(servicer.GetPoolHistory(req, _Ctx()))
    assert r1.success is True and r2.success is True
    assert len(r1.snapshots) == len(r2.snapshots) == 168
    assert tg_mock.call_count == calls_after_first  # no new upstream call
    assert servicer.health()["per_rpc"]["cache_hits"] >= 1
