"""DEX volume dispatcher integration tests (VIB-4870 / W7-followup).

Exercises the full ``GetDexVolumeHistory`` path with a mocked TheGraph
subgraph, verifying that:

1. The dispatcher routes ``dex=<name>`` to the right gateway connector's
   ``GatewayDexVolumeCapability``.
2. Each DEX-family schema decodes correctly via the shared
   ``_dex_volume_subgraph`` helper:
   * V3-family (``poolDayDatas`` / ``pool`` / ``volumeUSD``) — uniswap_v3 and
     aerodrome (its configured subgraph is the Slipstream/CL deployment).
   * Messari (``liquidityPoolDailySnapshots`` / ``day`` days-since-epoch /
     ``dailyVolumeUSD``) — curve.
   * Balancer V2 (``poolSnapshots`` / unix ``timestamp`` / ``swapVolume``).
3. "No silent zeros": empty / errored subgraph responses surface as
   ``success=False`` (NEVER a zero-fill row).
4. Validator rejects unknown dex / unsupported chain with
   ``INVALID_ARGUMENT``.

The per-call decoder is locked here (the heavyweight full-backtest replay
harness called out in the W7 plan §6 stays out of a per-connector commit).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer


class _MockContext:
    """Captures ``(code, details)`` set by the servicer."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _make_servicer_with_subgraph(
    response_body: dict[str, Any],
    *,
    status: int = 200,
) -> tuple[RateHistoryServiceServicer, list[dict[str, Any]]]:
    """Build the servicer wired to a mocked TheGraph subgraph POST.

    Returns ``(servicer, captured_payloads)`` so tests can assert the
    GraphQL query / variables the connector built. ``thegraph_api_key``
    is set so the helper does not short-circuit on missing config.
    """
    captured: list[dict[str, Any]] = []

    mock_response = AsyncMock()
    mock_response.status = status
    mock_response.json = AsyncMock(return_value=response_body)
    mock_response.text = AsyncMock(return_value="error-body")
    mock_response.headers = {}

    def _post(url: str, *, json: dict[str, Any], headers: dict[str, str]) -> Any:
        captured.append({"url": url, "json": json, "headers": headers})
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_response)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    mock_session = MagicMock()
    mock_session.post = _post

    settings = GatewaySettings()
    settings.thegraph_api_key = "test-key"  # type: ignore[misc]
    servicer = RateHistoryServiceServicer(settings)
    servicer._get_http_session = AsyncMock(return_value=mock_session)  # type: ignore[method-assign]
    return servicer, captured


def _run_volume(
    servicer: RateHistoryServiceServicer,
    *,
    dex: str,
    chain: str,
    pool_address: str,
    start_ts: int = 1_700_000_000,
    end_ts: int = 1_700_604_800,
    interval_secs: int = 86400,
) -> tuple[Any, _MockContext]:
    request = gateway_pb2.GetDexVolumeHistoryRequest(
        dex=dex,
        chain=chain,
        pool_address=pool_address,
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=interval_secs,
    )
    ctx = _MockContext()
    response = asyncio.run(servicer.GetDexVolumeHistory(request, ctx))  # type: ignore[arg-type]
    return response, ctx


# =============================================================================
# Happy path: V3-family (poolDayDatas / pool / volumeUSD)
# =============================================================================


def test_uniswap_v3_pool_day_datas_decode() -> None:
    body = {
        "data": {
            "poolDayDatas": [
                {"date": 1_700_000_000, "volumeUSD": "1234567.89"},
                {"date": 1_700_086_400, "volumeUSD": "2222222.22"},
            ]
        }
    }
    servicer, captured = _make_servicer_with_subgraph(body)
    response, ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC123")

    assert ctx.code is None
    assert response.success is True
    assert response.dex == "uniswap_v3"
    assert response.source == "the_graph"
    assert len(response.points) == 2
    assert response.points[0].timestamp == 1_700_000_000
    assert Decimal(response.points[0].volume_usd) == Decimal("1234567.89")
    assert Decimal(response.points[1].volume_usd) == Decimal("2222222.22")

    # The connector queried poolDayDatas filtered by ``pool`` and
    # lower-cased the pool address.
    query = captured[0]["json"]["query"]
    assert "poolDayDatas" in query
    assert "pool: $poolAddress" in query
    assert captured[0]["json"]["variables"]["poolAddress"] == "0xabc123"
    assert captured[0]["headers"]["Authorization"] == "Bearer test-key"


# =============================================================================
# Happy path: Aerodrome Slipstream — Uniswap-V3-style (poolDayDatas / pool / volumeUSD)
# =============================================================================


def test_aerodrome_pool_day_datas_decode() -> None:
    # Aerodrome's configured subgraph is the Slipstream (CL) deployment, which
    # uses the Uniswap-V3 poolDayDatas schema — NOT Solidly pairDayDatas (that
    # entity does not exist on it and failed for every pool).
    body = {
        "data": {
            "poolDayDatas": [
                {"date": 1_700_000_000, "volumeUSD": "9999.5"},
            ]
        }
    }
    servicer, captured = _make_servicer_with_subgraph(body)
    response, _ctx = _run_volume(servicer, dex="aerodrome", chain="base", pool_address="0xDEAD")

    assert response.success is True
    assert len(response.points) == 1
    assert response.points[0].timestamp == 1_700_000_000
    assert Decimal(response.points[0].volume_usd) == Decimal("9999.5")

    query = captured[0]["json"]["query"]
    assert "poolDayDatas" in query
    assert "pool: $poolAddress" in query


# =============================================================================
# Happy path: Messari day-unit (curve)
# =============================================================================


def test_curve_messari_day_unit_converts_to_unix_seconds() -> None:
    # Messari ``day`` = days since epoch. day 19676 → 19676 * 86400.
    body = {
        "data": {
            "liquidityPoolDailySnapshots": [
                {"day": 19676, "dailyVolumeUSD": "500000"},
            ]
        }
    }
    servicer, captured = _make_servicer_with_subgraph(body)
    response, _ctx = _run_volume(servicer, dex="curve", chain="ethereum", pool_address="0xPOOL")

    assert response.success is True
    assert len(response.points) == 1
    # day → unix seconds.
    assert response.points[0].timestamp == 19676 * 86400
    assert Decimal(response.points[0].volume_usd) == Decimal("500000")

    # The request window (unix seconds) was converted to day numbers in
    # the filter variables.
    variables = captured[0]["json"]["variables"]
    assert variables["startTime"] == 1_700_000_000 // 86400
    assert variables["endTime"] == 1_700_604_800 // 86400
    assert "day_gte" in captured[0]["json"]["query"]


# =============================================================================
# Happy path: Balancer V2 (poolSnapshots / timestamp / swapVolume)
# =============================================================================


def test_balancer_v2_pool_snapshots_decode() -> None:
    body = {
        "data": {
            "poolSnapshots": [
                {"timestamp": 1_700_000_000, "swapVolume": "42.0"},
            ]
        }
    }
    servicer, captured = _make_servicer_with_subgraph(body)
    response, _ctx = _run_volume(servicer, dex="balancer_v2", chain="ethereum", pool_address="0xBAL")

    assert response.success is True
    assert len(response.points) == 1
    assert response.points[0].timestamp == 1_700_000_000
    assert Decimal(response.points[0].volume_usd) == Decimal("42.0")

    query = captured[0]["json"]["query"]
    assert "poolSnapshots" in query
    assert "timestamp_gte" in query


# =============================================================================
# VIB-5090: Balancer V2 bare-address → full-pool-ID auto-resolution
# =============================================================================
#
# Balancer V2 ``poolSnapshots`` are keyed by the FULL 32-byte pool ID
# (address + pool-type/index suffix), not the 42-char pool address. The
# spec opts into ``resolve_bare_address_pool_id``: a bare address is
# resolved via a ``pools(where: {address})`` lookup (cached on the
# servicer), full IDs pass through unchanged, and no-match / ambiguity
# stay loud ``success=False`` envelopes naming the address.

# BAL/WETH 80/20 on Ethereum: bare address vs full pool ID.
_BAL_ADDRESS = "0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56"
_BAL_POOL_ID = "0x5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014"

# One recorded poolSnapshots fixture served to BOTH the bare-address and
# the direct-pool-ID paths, so the parity test compares identical rows.
_BAL_SNAPSHOT_ROWS = [
    {"timestamp": 1_700_000_000, "swapVolume": "42.0"},
    {"timestamp": 1_700_086_400, "swapVolume": "77.5"},
]


def _make_servicer_with_routed_subgraph(
    pools_body: dict[str, Any],
) -> tuple[RateHistoryServiceServicer, list[dict[str, Any]]]:
    """Mock transport that routes per-request: pool-ID lookup vs snapshots.

    The pool-ID lookup (query contains ``ResolvePoolId``) is answered
    with ``pools_body``; every other request gets the shared
    ``_BAL_SNAPSHOT_ROWS`` fixture. Returns ``(servicer, captured)``.
    """
    captured: list[dict[str, Any]] = []

    def _post(url: str, *, json: dict[str, Any], headers: dict[str, str]) -> Any:
        captured.append({"url": url, "json": json, "headers": headers})
        query = json["query"]
        if "pools(" in query and "address: $address" in query:
            body = pools_body
        else:
            body = {"data": {"poolSnapshots": list(_BAL_SNAPSHOT_ROWS)}}
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=body)
        mock_response.text = AsyncMock(return_value="error-body")
        mock_response.headers = {}
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_response)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    mock_session = MagicMock()
    mock_session.post = _post

    settings = GatewaySettings()
    settings.thegraph_api_key = "test-key"  # type: ignore[misc]
    servicer = RateHistoryServiceServicer(settings)
    servicer._get_http_session = AsyncMock(return_value=mock_session)  # type: ignore[method-assign]
    return servicer, captured


def _lookup_requests(captured: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [c for c in captured if "ResolvePoolId" in c["json"]["query"]]


def test_balancer_bare_address_resolves_pool_id_then_fetches() -> None:
    """A bare 42-char address triggers a pools(where: {address}) lookup first."""
    pools_body = {"data": {"pools": [{"id": _BAL_POOL_ID}]}}
    servicer, captured = _make_servicer_with_routed_subgraph(pools_body)
    response, ctx = _run_volume(servicer, dex="balancer_v2", chain="ethereum", pool_address=_BAL_ADDRESS)

    assert ctx.code is None
    assert response.success is True
    assert len(captured) == 2

    # First request: the pool-ID lookup, filtered by the lowercased address.
    lookup = captured[0]["json"]
    assert "ResolvePoolId" in lookup["query"]
    assert "pools(" in lookup["query"]
    assert "address: $address" in lookup["query"]
    assert lookup["variables"] == {"address": _BAL_ADDRESS.lower()}
    assert captured[0]["headers"]["Authorization"] == "Bearer test-key"

    # Second request: the snapshot query now carries the FULL pool ID.
    snapshot = captured[1]["json"]
    assert "poolSnapshots" in snapshot["query"]
    assert snapshot["variables"]["poolAddress"] == _BAL_POOL_ID


def test_balancer_bare_address_parity_with_direct_pool_id() -> None:
    """Bare address and direct full-pool-ID queries decode identical rows.

    Both paths are served the same recorded ``_BAL_SNAPSHOT_ROWS``
    fixture; the resolved-address response must be point-for-point
    identical to the direct-pool-ID response.
    """
    pools_body = {"data": {"pools": [{"id": _BAL_POOL_ID}]}}

    servicer_a, captured_a = _make_servicer_with_routed_subgraph(pools_body)
    via_address, _ = _run_volume(servicer_a, dex="balancer_v2", chain="ethereum", pool_address=_BAL_ADDRESS)

    servicer_b, captured_b = _make_servicer_with_routed_subgraph(pools_body)
    via_pool_id, _ = _run_volume(servicer_b, dex="balancer_v2", chain="ethereum", pool_address=_BAL_POOL_ID)

    assert via_address.success is True
    assert via_pool_id.success is True

    # The direct path never issued a lookup; the bare-address path did.
    assert len(_lookup_requests(captured_a)) == 1
    assert len(_lookup_requests(captured_b)) == 0
    # Both snapshot queries carried the same full pool ID.
    assert captured_a[-1]["json"]["variables"]["poolAddress"] == _BAL_POOL_ID
    assert captured_b[-1]["json"]["variables"]["poolAddress"] == _BAL_POOL_ID

    # Point-for-point parity.
    assert len(via_address.points) == len(via_pool_id.points) == len(_BAL_SNAPSHOT_ROWS)
    for point_a, point_b in zip(via_address.points, via_pool_id.points, strict=True):
        assert point_a.timestamp == point_b.timestamp
        assert Decimal(point_a.volume_usd) == Decimal(point_b.volume_usd)


def test_balancer_pool_id_cache_prevents_second_lookup() -> None:
    """The address → pool-ID mapping is cached: one lookup per process."""
    pools_body = {"data": {"pools": [{"id": _BAL_POOL_ID}]}}
    servicer, captured = _make_servicer_with_routed_subgraph(pools_body)

    first, _ = _run_volume(servicer, dex="balancer_v2", chain="ethereum", pool_address=_BAL_ADDRESS)
    second, _ = _run_volume(servicer, dex="balancer_v2", chain="ethereum", pool_address=_BAL_ADDRESS)

    assert first.success is True
    assert second.success is True
    # 3 requests total: lookup + snapshots, then snapshots only.
    assert len(captured) == 3
    assert len(_lookup_requests(captured)) == 1
    assert captured[2]["json"]["variables"]["poolAddress"] == _BAL_POOL_ID


def test_balancer_bare_address_no_match_fails_loudly() -> None:
    """An address matching no pool stays a loud failure naming the address."""
    servicer, captured = _make_servicer_with_routed_subgraph({"data": {"pools": []}})
    response, _ctx = _run_volume(servicer, dex="balancer_v2", chain="ethereum", pool_address=_BAL_ADDRESS)

    assert response.success is False
    assert len(response.points) == 0
    assert "no balancer_v2 pool found for address" in response.error
    assert _BAL_ADDRESS in response.error
    # The failed lookup is NOT cached and no snapshot query was issued.
    assert len(captured) == 1
    assert servicer._dex_pool_id_cache == {}


def test_balancer_ambiguous_address_fails_loudly() -> None:
    """Two pools sharing one address (impossible for Balancer V2, guarded anyway)."""
    pools_body = {"data": {"pools": [{"id": _BAL_POOL_ID}, {"id": _BAL_POOL_ID[:-1] + "5"}]}}
    servicer, captured = _make_servicer_with_routed_subgraph(pools_body)
    response, _ctx = _run_volume(servicer, dex="balancer_v2", chain="ethereum", pool_address=_BAL_ADDRESS)

    assert response.success is False
    assert "ambiguous pool address" in response.error
    assert _BAL_ADDRESS in response.error
    assert len(captured) == 1
    assert servicer._dex_pool_id_cache == {}


def test_balancer_full_pool_id_passes_through_without_lookup() -> None:
    """A full 66-char pool ID skips resolution entirely."""
    servicer, captured = _make_servicer_with_routed_subgraph({"data": {"pools": []}})
    response, _ctx = _run_volume(servicer, dex="balancer_v2", chain="ethereum", pool_address=_BAL_POOL_ID)

    assert response.success is True
    assert len(captured) == 1
    assert len(_lookup_requests(captured)) == 0
    assert captured[0]["json"]["variables"]["poolAddress"] == _BAL_POOL_ID


# =============================================================================
# No silent zeros: empty subgraph response → success=False
# =============================================================================


def test_empty_subgraph_yields_success_false_no_points() -> None:
    servicer, _captured = _make_servicer_with_subgraph({"data": {"poolDayDatas": []}})
    response, _ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC")

    assert response.success is False
    assert response.source == "uniswap_v3"
    assert len(response.points) == 0
    assert "no poolDayDatas" in response.error


def test_graphql_errors_yield_success_false() -> None:
    body = {"errors": [{"message": "bad query"}]}
    servicer, _captured = _make_servicer_with_subgraph(body)
    response, _ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC")

    assert response.success is False
    assert "GraphQL errors" in response.error


def test_http_error_yields_success_false() -> None:
    servicer, _captured = _make_servicer_with_subgraph({}, status=502)
    response, _ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC")

    assert response.success is False
    assert "HTTP 502" in response.error


def test_missing_api_key_yields_success_false() -> None:
    servicer, _captured = _make_servicer_with_subgraph({"data": {"poolDayDatas": []}})
    servicer.settings.thegraph_api_key = None  # type: ignore[misc]
    response, _ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC")

    assert response.success is False
    assert "thegraph_api_key" in response.error


def test_missing_time_field_is_malformed_row_not_epoch() -> None:
    """A row missing its time field must raise — never emit a ``timestamp=0`` point."""
    body = {"data": {"poolDayDatas": [{"volumeUSD": "100"}]}}  # no ``date``
    servicer, _captured = _make_servicer_with_subgraph(body)
    response, _ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC")

    assert response.success is False
    assert len(response.points) == 0
    assert "missing 'date'" in response.error


def test_non_object_json_yields_success_false() -> None:
    """An array / scalar JSON body must surface cleanly, not AttributeError."""
    servicer, _captured = _make_servicer_with_subgraph(["unexpected", "array"])  # type: ignore[arg-type]
    response, _ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC")

    assert response.success is False
    assert "non-object JSON" in response.error


def test_non_daily_interval_rejected() -> None:
    """Only daily (86400s) volume is served; an hourly request is rejected."""
    servicer, _captured = _make_servicer_with_subgraph({"data": {"poolDayDatas": []}})
    response, _ctx = _run_volume(
        servicer,
        dex="uniswap_v3",
        chain="ethereum",
        pool_address="0xABC",
        interval_secs=3600,
    )

    assert response.success is False
    assert "interval_secs" in response.error


def test_window_exceeding_page_cap_rejected() -> None:
    """A window wider than the single 1000-row page fails fast (no silent truncation)."""
    servicer, _captured = _make_servicer_with_subgraph({"data": {"poolDayDatas": []}})
    start = 1_600_000_000
    end = start + 2000 * 86400  # 2000 daily points > 1000-row page cap
    response, _ctx = _run_volume(
        servicer,
        dex="uniswap_v3",
        chain="ethereum",
        pool_address="0xABC",
        start_ts=start,
        end_ts=end,
    )

    assert response.success is False
    assert "exceeds single-page limit" in response.error


# =============================================================================
# Validators
# =============================================================================


def test_unknown_dex_yields_invalid_argument() -> None:
    servicer, _captured = _make_servicer_with_subgraph({"data": {}})
    response, ctx = _run_volume(servicer, dex="does_not_exist", chain="ethereum", pool_address="0xABC")

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False


def test_unsupported_chain_yields_invalid_argument() -> None:
    servicer, _captured = _make_servicer_with_subgraph({"data": {}})
    # Aerodrome volume is Base-only.
    response, ctx = _run_volume(servicer, dex="aerodrome", chain="ethereum", pool_address="0xABC")

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False
    assert "does not support volume on chain 'ethereum'" in ctx.details


class TestMalformedEnvelopeHandling:
    """CodeRabbit review on PR #2768: malformed GraphQL envelopes must surface
    as the loud success=False RateHistoryUnavailable envelope, never as an
    AttributeError that degrades to INTERNAL."""

    @pytest.mark.parametrize(
        "body",
        [
            {"errors": "rate limited"},
            {"errors": ["plain string error"]},
            {"data": "not-a-dict"},
        ],
        ids=["errors-not-list", "error-entry-not-dict", "data-not-dict"],
    )
    def test_malformed_envelope_fails_loudly(self, body: dict) -> None:
        servicer, _captured = _make_servicer_with_subgraph(body)

        response, ctx = _run_volume(servicer, dex="uniswap_v3", chain="ethereum", pool_address="0xABC123")

        assert ctx.code is None
        assert response.success is False
        assert "subgraph" in response.error.lower()
