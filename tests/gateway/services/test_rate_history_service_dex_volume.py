"""DEX volume dispatcher integration tests (VIB-4870 / W7-followup).

Exercises the full ``GetDexVolumeHistory`` path with a mocked TheGraph
subgraph, verifying that:

1. The dispatcher routes ``dex=<name>`` to the right gateway connector's
   ``GatewayDexVolumeCapability``.
2. Each DEX-family schema decodes correctly via the shared
   ``_dex_volume_subgraph`` helper:
   * V3-family (``poolDayDatas`` / ``pool`` / ``volumeUSD``) — uniswap_v3.
   * Solidly (``pairDayDatas`` / ``pairAddress`` / ``dailyVolumeUSD``) —
     aerodrome.
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
# Happy path: Solidly (pairDayDatas / pairAddress / dailyVolumeUSD)
# =============================================================================


def test_aerodrome_pair_day_datas_decode() -> None:
    body = {
        "data": {
            "pairDayDatas": [
                {"date": 1_700_000_000, "dailyVolumeUSD": "9999.5"},
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
    assert "pairDayDatas" in query
    assert "pairAddress: $poolAddress" in query


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
