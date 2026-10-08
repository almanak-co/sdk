"""Aster Pro funding rates on the gateway funding lanes (live + settled history).

Responses mirror the live ``fapi.asterdex.com`` shapes captured 2026-10-07.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import urlsplit

import aiohttp
import pytest

from almanak.connectors.aster_perps.gateway import funding_rates
from almanak.connectors.aster_perps.gateway.provider import AsterPerpsGatewayConnector
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.funding_rate_service import FundingRateServiceServicer
from almanak.gateway.services.rate_history_service import (
    RateHistoryInvalidRequest,
    RateHistoryRateLimited,
    RateHistoryUnavailable,
)

BASE = "https://fapi.asterdex.com"
NEXT_FUNDING_MS = 1791417600000
TIME_MS = 1791392268000


def _premium(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "symbol": "ETHUSDT",
        "markPrice": "2566.09842248",
        "indexPrice": "2567.37046512",
        "estimatedSettlePrice": "2571.39946305",
        "lastFundingRate": "0.00005196",
        "interestRate": "0.00010000",
        "nextFundingTime": NEXT_FUNDING_MS,
        "time": TIME_MS,
    }
    payload.update(overrides)
    return payload


def _info(symbol: str = "ETHUSDT", hours: Any = 8) -> list[dict[str, Any]]:
    return [{"symbol": symbol, "interestRate": "0.0001", "time": TIME_MS, "fundingIntervalHours": hours}]


class _Response:
    def __init__(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def text(self) -> str:
        import json

        return self._body if isinstance(self._body, str) else json.dumps(self._body, separators=(",", ":"))

    async def json(self, content_type: str | None = None) -> Any:
        if isinstance(self._body, str):
            raise ValueError("not json")
        return self._body

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _Session:
    """Routes ``session.get`` by endpoint path; records every request."""

    def __init__(self, routes: dict[str, Callable[[dict[str, str]], _Response]]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, params: dict[str, str] | None = None) -> _Response:
        path = urlsplit(url).path
        self.calls.append((url, dict(params or {})))
        return self.routes[path](dict(params or {}))


def _live_session(premium: _Response | None = None, info: _Response | None = None) -> _Session:
    return _Session(
        {
            funding_rates.PREMIUM_INDEX_PATH: lambda _p: premium or _Response(200, _premium()),
            funding_rates.FUNDING_INFO_PATH: lambda _p: info or _Response(200, _info()),
        }
    )


def _servicer(session: _Session, base_url: str | None = None) -> SimpleNamespace:
    settings = SimpleNamespace(network="mainnet", aster_perps_base_url=base_url)
    return SimpleNamespace(settings=settings, _get_http_session=AsyncMock(return_value=session))


def _fetch_live(session: _Session, market: str = "ETH-USD", **kwargs: Any) -> Any:
    return asyncio.run(AsterPerpsGatewayConnector().fetch_funding_rate(_servicer(session), market, "bsc", **kwargs))


def test_live_rate_is_the_interval_rate_divided_by_the_symbols_interval() -> None:
    data = _fetch_live(_live_session())

    assert data.venue == "aster_perps"
    assert data.market == "ETH-USD"
    assert data.rate_hourly == Decimal("0.00005196") / Decimal(8)
    assert data.short_rate_hourly == data.rate_hourly
    assert data.long_rate_hourly == -data.rate_hourly
    assert data.mark_price == Decimal("2566.09842248")
    assert data.index_price == Decimal("2567.37046512")
    assert data.next_funding_time == datetime.fromtimestamp(NEXT_FUNDING_MS / 1000, tz=UTC)
    assert data.observed_at == datetime.fromtimestamp(TIME_MS / 1000, tz=UTC)
    assert data.is_live_data is True
    assert data.open_interest_long is None and data.open_interest_short is None


def test_a_four_hour_symbol_is_scaled_by_four_not_eight() -> None:
    data = _fetch_live(_live_session(info=_Response(200, _info(hours=4))))

    assert data.rate_hourly == Decimal("0.00005196") / Decimal(4)


def test_negative_funding_pays_the_short() -> None:
    data = _fetch_live(_live_session(premium=_Response(200, _premium(lastFundingRate="-0.0008"))))

    assert data.rate_hourly == Decimal("-0.0001")
    assert data.short_rate_hourly == Decimal("-0.0001")
    assert data.long_rate_hourly == Decimal("0.0001")


def test_requests_use_the_aster_symbol_and_the_configured_base_url() -> None:
    session = _live_session()
    servicer = _servicer(session, base_url="https://aster.example/")

    asyncio.run(AsterPerpsGatewayConnector().fetch_funding_rate(servicer, "ETH-USD", "bsc"))

    assert {url for url, _ in session.calls} == {
        f"https://aster.example{funding_rates.PREMIUM_INDEX_PATH}",
        f"https://aster.example{funding_rates.FUNDING_INFO_PATH}",
    }
    assert all(params == {"symbol": "ETHUSDT"} for _, params in session.calls)


def test_default_base_url_when_unconfigured() -> None:
    assert funding_rates.base_url_from_settings(SimpleNamespace()) == BASE


@pytest.mark.parametrize(
    "premium",
    [
        _Response(200, _premium(lastFundingRate="")),
        _Response(200, _premium(lastFundingRate=None)),
        _Response(200, _premium(lastFundingRate="NaN")),
        _Response(200, _premium(lastFundingRate="abc")),
        _Response(200, _premium(markPrice="0")),
        _Response(200, _premium(nextFundingTime="soon")),
        _Response(200, _premium(symbol="BTCUSDT")),
        _Response(200, "<html>maintenance</html>"),
        _Response(503, {"code": -1001, "msg": "down"}),
    ],
    ids=["empty", "missing", "nan", "garbage", "zero-mark", "bad-next-time", "wrong-symbol", "non-json", "http-503"],
)
def test_a_malformed_or_failed_premium_read_raises(premium: _Response) -> None:
    with pytest.raises(funding_rates.AsterFundingUnavailable):
        _fetch_live(_live_session(premium=premium))


@pytest.mark.parametrize("hours", [0, -8, None, "8", True])
def test_an_invalid_interval_raises(hours: Any) -> None:
    with pytest.raises(funding_rates.AsterFundingUnavailable):
        _fetch_live(_live_session(info=_Response(200, _info(hours=hours))))


def test_a_transport_failure_raises() -> None:
    session = _live_session()
    session.routes[funding_rates.PREMIUM_INDEX_PATH] = MagicMock(side_effect=aiohttp.ClientConnectionError("reset"))

    with pytest.raises(funding_rates.AsterFundingUnavailable, match="transport failure"):
        _fetch_live(session)


def test_an_unlisted_symbol_is_an_unknown_market() -> None:
    invalid = _Response(400, {"code": -1121, "msg": "Invalid symbol."})

    with pytest.raises(funding_rates.AsterUnknownMarket):
        _fetch_live(_live_session(premium=invalid, info=invalid))


def test_a_market_outside_the_served_set_is_refused_before_any_request() -> None:
    session = _live_session()

    with pytest.raises(funding_rates.AsterUnknownMarket):
        _fetch_live(session, market="DOGE-USD")
    with pytest.raises(funding_rates.AsterUnknownMarket):
        _fetch_live(session, market_address="0x" + "1" * 40)
    assert session.calls == []


def _grpc_context() -> MagicMock:
    return MagicMock()


def _servicer_with_session(session: _Session) -> FundingRateServiceServicer:
    servicer = FundingRateServiceServicer(SimpleNamespace(network="mainnet"))  # type: ignore[arg-type]
    servicer._get_http_session = AsyncMock(return_value=session)  # type: ignore[method-assign]
    return servicer


def test_the_gateway_servicer_dispatches_aster_perps() -> None:
    servicer = _servicer_with_session(_live_session())
    assert "aster_perps" in servicer._funding_rate_providers

    response = asyncio.run(
        servicer.GetFundingRate(
            gateway_pb2.FundingRateRequest(venue="aster_perps", market="ETH/USD", chain="bsc"), _grpc_context()
        )
    )

    assert response.success is True
    assert Decimal(response.rate_hourly) == Decimal("0.00005196") / Decimal(8)
    assert Decimal(response.rate_8h) == Decimal("0.00005196")
    assert Decimal(response.short_rate_hourly) == Decimal(response.rate_hourly)
    assert response.open_interest_long == ""
    assert response.is_live_data is True


def test_a_failed_read_reaches_the_wire_as_unsuccessful_with_no_rate() -> None:
    servicer = _servicer_with_session(_live_session(premium=_Response(503, "down")))
    context = _grpc_context()

    response = asyncio.run(
        servicer.GetFundingRate(gateway_pb2.FundingRateRequest(venue="aster_perps", market="ETH-USD"), context)
    )

    assert response.success is False
    assert response.rate_hourly == ""
    assert response.error


class _InProcessFundingStub:
    """The strategy's gRPC stub, answered by the real gateway servicer in-process."""

    def __init__(self, servicer: FundingRateServiceServicer) -> None:
        self._servicer = servicer
        self.requests: list[Any] = []

    def GetFundingRate(self, request: Any, timeout: float | None = None) -> Any:  # noqa: N802
        self.requests.append(request)
        return asyncio.run(self._servicer.GetFundingRate(request, _grpc_context()))


def _snapshot_over(session: _Session) -> tuple[Any, _InProcessFundingStub]:
    from almanak.framework.data.funding import GatewayFundingRateProvider
    from almanak.framework.market import MarketSnapshotBuilder

    stub = _InProcessFundingStub(_servicer_with_session(session))
    client = MagicMock()
    client.funding_rate = stub
    client.config = SimpleNamespace(timeout=5.0)
    strategy = SimpleNamespace(
        chain="bsc",
        wallet_address="0x" + "1" * 40,
        funding_rate_provider=GatewayFundingRateProvider(client, chain="bsc", cache_ttl_seconds=0),
    )
    return MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, runtime_surface="unit_test"), stub


def test_market_snapshot_reads_the_aster_rate_through_the_gateway() -> None:
    snapshot, stub = _snapshot_over(_live_session())

    rate = snapshot.funding_rate("aster_perps", "ETH/USD")

    assert rate.venue == "aster_perps"
    assert rate.market == "ETH-USD"
    assert rate.rate_hourly == Decimal("0.00005196") / Decimal(8)
    assert rate.rate_8h == Decimal("0.00005196")
    assert rate.short_rate_hourly == rate.rate_hourly
    assert rate.mark_price == Decimal("2566.09842248")
    assert rate.is_live_data is True
    assert stub.requests[0].venue == "aster_perps"
    assert stub.requests[0].chain == "bsc"


def test_market_snapshot_surfaces_an_unavailable_aster_rate_as_an_error() -> None:
    from almanak.framework.data.funding import FundingRateUnavailableError

    snapshot, _ = _snapshot_over(_live_session(premium=_Response(200, _premium(lastFundingRate=""))))

    with pytest.raises(FundingRateUnavailableError):
        snapshot.funding_rate("aster_perps", "ETH/USD")


H = 3_600_000
T0 = 1_791_302_400_000  # an 8h-aligned settlement


def _row(ms: int, rate: str) -> dict[str, Any]:
    return {"symbol": "ETHUSDT", "fundingTime": ms, "fundingRate": rate}


def test_history_scales_each_settlement_by_the_spacing_since_the_previous_one() -> None:
    rows = [_row(T0, "0.0008"), _row(T0 + 8 * H + 1, "0.0004"), _row(T0 + 12 * H, "0.0004")]

    points = funding_rates.history_points(
        rows, current_interval_hours=8, start_ts=T0 // 1000 + 1, end_ts=(T0 + 12 * H) // 1000
    )

    assert [p.timestamp for p in points] == [(T0 + 8 * H + 1) // 1000, (T0 + 12 * H) // 1000]
    assert [p.rate_hourly for p in points] == [Decimal("0.00005"), Decimal("0.0001")]
    assert points[0].short_rate_hourly == Decimal("0.00005")
    assert points[0].long_rate_hourly == Decimal("-0.00005")
    assert points[0].rate_annualized == Decimal("0.00005") * 8760


def test_history_falls_back_to_the_current_interval_across_a_missing_settlement() -> None:
    rows = [_row(T0, "0.0008"), _row(T0 + 16 * H, "0.0008")]

    points = funding_rates.history_points(rows, current_interval_hours=8, start_ts=0, end_ts=T0 // 1000 + 86400)

    assert [p.rate_hourly for p in points] == [Decimal("0.0001"), Decimal("0.0001")]


@pytest.mark.parametrize("interval", [4, 1])
def test_a_missing_settlement_whose_gap_is_itself_a_published_interval_is_not_taken_for_one(interval: int) -> None:
    # 4h -> a gap spans 8h, 1h -> 2h: both are published intervals.
    times = [T0, T0 + interval * H, T0 + 3 * interval * H, T0 + 4 * interval * H]
    rows = [_row(ms, str(Decimal("0.0001") * interval)) for ms in times]

    points = funding_rates.history_points(rows, current_interval_hours=interval, start_ts=0, end_ts=2**40)

    assert [p.rate_hourly for p in points] == [Decimal("0.0001")] * 4


def test_a_gap_before_the_newest_settlement_scales_it_by_the_current_interval() -> None:
    rows = [_row(T0, "0.0004"), _row(T0 + 4 * H, "0.0004"), _row(T0 + 12 * H, "0.0004")]

    points = funding_rates.history_points(rows, current_interval_hours=4, start_ts=0, end_ts=2**40)

    assert [p.rate_hourly for p in points] == [Decimal("0.0001")] * 3


def test_a_schedule_change_keeps_the_old_interval_up_to_the_switch() -> None:
    times = [T0, T0 + 8 * H, T0 + 16 * H, T0 + 20 * H, T0 + 24 * H]
    rates = ["0.0008", "0.0008", "0.0008", "0.0004", "0.0004"]

    points = funding_rates.history_points(
        [_row(ms, r) for ms, r in zip(times, rates, strict=True)], current_interval_hours=4, start_ts=0, end_ts=2**40
    )

    assert [p.rate_hourly for p in points] == [Decimal("0.0002")] + [Decimal("0.0001")] * 4


def test_a_malformed_history_row_raises_instead_of_leaving_a_gap() -> None:
    with pytest.raises(funding_rates.AsterFundingUnavailable):
        funding_rates.history_points(
            [_row(T0, "0.0008"), _row(T0 + 8 * H, "")], current_interval_hours=8, start_ts=0, end_ts=2**40
        )


def _history_session(pages: list[list[dict[str, Any]]], info: _Response | None = None) -> _Session:
    remaining = list(pages)
    return _Session(
        {
            funding_rates.FUNDING_INFO_PATH: lambda _p: info or _Response(200, _info()),
            funding_rates.FUNDING_HISTORY_PATH: lambda _p: _Response(200, remaining.pop(0)),
        }
    )


def _fetch_history(session: _Session, start_ts: int, end_ts: int, market: str = "ETH-USD") -> Any:
    return asyncio.run(
        AsterPerpsGatewayConnector().fetch_funding_history(
            _servicer(session), market=market, market_address="", chain="bsc", start_ts=start_ts, end_ts=end_ts
        )
    )


def test_history_reads_back_one_interval_and_follows_pages() -> None:
    first = [_row(T0 + i * 8 * H, "0.0008") for i in range(funding_rates.HISTORY_PAGE_LIMIT)]
    last_ms = first[-1]["fundingTime"]
    session = _history_session([first, [_row(last_ms + 8 * H, "0.0016")]])
    start_ts = (T0 + 8 * H) // 1000

    points = _fetch_history(session, start_ts, (last_ms + 8 * H) // 1000)

    history_calls = [params for url, params in session.calls if url.endswith(funding_rates.FUNDING_HISTORY_PATH)]
    assert history_calls[0]["startTime"] == str(start_ts * 1000 - 8 * H)
    assert history_calls[1]["startTime"] == str(last_ms + 1)
    assert len(points) == funding_rates.HISTORY_PAGE_LIMIT
    assert points[-1].rate_hourly == Decimal("0.0002")


def test_an_empty_history_window_is_unavailable_not_empty() -> None:
    with pytest.raises(RateHistoryUnavailable):
        _fetch_history(_history_session([[]]), 1, 2)


def test_history_rate_limit_is_retryable() -> None:
    session = _Session(
        {
            funding_rates.FUNDING_INFO_PATH: lambda _p: _Response(429, "slow down", {"Retry-After": "3"}),
        }
    )

    with pytest.raises(RateHistoryRateLimited) as excinfo:
        _fetch_history(session, 1, 2)
    assert excinfo.value.retry_after == 3.0


def test_history_unknown_market_is_an_invalid_request() -> None:
    invalid = _Response(400, {"code": -1121, "msg": "Invalid symbol."})

    with pytest.raises(RateHistoryInvalidRequest):
        _fetch_history(_history_session([], info=invalid), 1, 2)
    with pytest.raises(RateHistoryInvalidRequest):
        asyncio.run(
            AsterPerpsGatewayConnector().fetch_funding_history(
                _servicer(_history_session([])),
                market="ETH-USD",
                market_address="0x" + "1" * 40,
                chain="bsc",
                start_ts=1,
                end_ts=2,
            )
        )


def test_history_refuses_a_market_the_live_lane_does_not_serve() -> None:
    session = _history_session([])
    with pytest.raises(RateHistoryInvalidRequest):
        _fetch_history(session, 1, 2, market="DOGE-USD")
    assert session.calls == []


class _CountingLimiter:
    def __init__(self) -> None:
        self.acquired = 0

    async def acquire(self) -> float:
        self.acquired += 1
        return 0.0


def test_every_upstream_request_takes_its_own_rate_limit_token() -> None:
    first = [_row(T0 + i * 8 * H, "0.0008") for i in range(funding_rates.HISTORY_PAGE_LIMIT)]
    session = _history_session([first, [_row(first[-1]["fundingTime"] + 8 * H, "0.0016")]])
    connector = AsterPerpsGatewayConnector()
    limiter = _CountingLimiter()
    connector._request_limiter = limiter  # type: ignore[assignment]

    asyncio.run(
        connector.fetch_funding_history(
            _servicer(session), market="ETH-USD", market_address="", chain="bsc", start_ts=T0 // 1000, end_ts=2**40
        )
    )

    assert limiter.acquired == len(session.calls) == 3


def test_funding_markets_are_one_list_on_both_sides() -> None:
    from almanak.connectors.aster_perps.connector import CONNECTOR

    assert CONNECTOR.funding_history is not None
    assert frozenset(CONNECTOR.funding_history.markets) == AsterPerpsGatewayConnector().funding_supported_markets()
    assert all(funding_rates.resolve_symbol(m).endswith("USDT") for m in CONNECTOR.funding_history.markets)
