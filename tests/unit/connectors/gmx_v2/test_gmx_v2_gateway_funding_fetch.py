"""GMX-native funding is dynamic, address-first, side-aware, and fail-closed."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.connectors.gmx_v2.gateway.provider import GmxV2GatewayConnector
from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

_XMR_MARKET = "0x7c54D547FAD72f8AFbf6E5b04403A0168b654C6f"
_LONG_FACTOR = "-25903000627149213888888"
_SHORT_FACTOR = "40512692907534675501066"


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def json(self) -> dict[str, Any]:
        return self._payload

    async def text(self) -> str:
        return str(self._payload)


class _Session:
    def __init__(self, *, lag_hours: int = 0) -> None:
        self.requests: list[dict[str, Any]] = []
        self.lag_hours = lag_hours

    def post(self, url: str, *, json: dict[str, Any], headers: dict[str, str]) -> _Response:
        self.requests.append({"url": url, "json": json, "headers": headers})
        variables = json["variables"]
        timestamp = variables.get("start", variables.get("end", 0)) - self.lag_hours * 3600
        rows = [
            {
                "marketAddress": variables["marketAddress"],
                "snapshotTimestamp": timestamp,
                "fundingFactorPerSecondLong": _LONG_FACTOR,
                "fundingFactorPerSecondShort": _SHORT_FACTOR,
            }
        ]
        return _Response({"data": {"fundingRateSnapshots": rows}})


class _Registry:
    def __init__(self, *, record: Any = None, error: Exception | None = None) -> None:
        self.record = record
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def resolve(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.record


class _Servicer:
    def __init__(self, session: _Session) -> None:
        self.settings = SimpleNamespace(network="mainnet")
        self._session = session

    async def _get_http_session(self) -> _Session:
        return self._session


def _connector(registry: _Registry) -> GmxV2GatewayConnector:
    connector = GmxV2GatewayConnector()
    connector._market_registry = registry
    return connector


def _xmr_record() -> Any:
    return SimpleNamespace(market_token=_XMR_MARKET, index_symbol="XMR")


@pytest.mark.asyncio
async def test_xmr_current_funding_is_native_side_specific_and_address_first() -> None:
    session = _Session()
    registry = _Registry(record=_xmr_record())
    connector = _connector(registry)

    data = await connector.fetch_funding_rate(
        _Servicer(session),
        "XMR/USD",
        "arbitrum",
        _XMR_MARKET.lower(),
    )

    long_rate = Decimal(_LONG_FACTOR) * Decimal(3600) / (Decimal(10) ** 30)
    short_rate = Decimal(_SHORT_FACTOR) * Decimal(3600) / (Decimal(10) ** 30)
    assert data.market == "XMR-USD"
    assert data.market_address == _XMR_MARKET
    assert data.long_rate_hourly == long_rate
    assert data.short_rate_hourly == short_rate
    assert data.rate_hourly == -long_rate  # legacy positive-longs-pay projection
    assert data.open_interest_long is None
    assert data.open_interest_short is None
    assert data.mark_price is None
    assert data.index_price is None
    assert data.is_live_data is False
    assert data.observed_at == datetime.fromtimestamp(
        session.requests[0]["json"]["variables"]["end"],
        tz=UTC,
    )
    assert data.next_funding_time is None
    assert registry.calls[0]["market"] == _XMR_MARKET.lower()
    assert session.requests[0]["json"]["variables"]["marketAddress"] == _XMR_MARKET


@pytest.mark.asyncio
async def test_current_funding_uses_latest_completed_snapshot_when_indexer_lags() -> None:
    session = _Session(lag_hours=2)
    connector = _connector(_Registry(record=_xmr_record()))

    data = await connector.fetch_funding_rate(_Servicer(session), "XMR-USD", "arbitrum", _XMR_MARKET)

    requested_end = session.requests[0]["json"]["variables"]["end"]
    assert int(data.observed_at.timestamp()) == requested_end - 7200
    assert data.is_live_data is False
    assert data.next_funding_time is None


@pytest.mark.asyncio
async def test_history_uses_exact_verified_address_and_gmx_source() -> None:
    session = _Session()
    registry = _Registry(record=_xmr_record())
    connector = _connector(registry)

    points = await connector.fetch_funding_history(
        _Servicer(session),
        market="XMR-USD",
        market_address=_XMR_MARKET,
        chain="arbitrum",
        start_ts=1_750_449_600,
        end_ts=1_750_449_600,
    )

    assert len(points) == 1
    assert points[0].long_rate_hourly < 0 < points[0].short_rate_hourly
    assert connector.funding_supported_markets() is None
    source = connector.funding_history_source("arbitrum")
    assert source.key == "gmx_synthetics_subsquid"
    assert source.scope == "arbitrum"


@pytest.mark.asyncio
async def test_unknown_market_fails_instead_of_returning_a_default() -> None:
    connector = _connector(_Registry(record=None))

    with pytest.raises(RateHistoryUnavailable, match="does not exist or is not listed"):
        await connector.fetch_funding_rate(_Servicer(_Session()), "XMR-USD", "arbitrum", _XMR_MARKET)


@pytest.mark.asyncio
async def test_ambiguous_or_unverifiable_market_fails_instead_of_guessing() -> None:
    connector = _connector(_Registry(error=ValueError("ambiguous collateral variants")))

    with pytest.raises(RateHistoryUnavailable, match="could not be verified.*ambiguous"):
        await connector.fetch_funding_history(
            _Servicer(_Session()),
            market="ETH-USD",
            market_address="",
            chain="arbitrum",
            start_ts=1_750_449_600,
            end_ts=1_750_449_600,
        )


def test_gmx_connector_has_no_funding_default_or_static_market_table() -> None:
    import almanak.connectors.gmx_v2.gateway.provider as provider

    connector = GmxV2GatewayConnector()
    assert not hasattr(connector, "default_funding_rate")
    assert not hasattr(provider, "_GMX_V2_DEFAULT_RATES")
    assert not hasattr(provider, "_UNKNOWN_MARKET_DEFAULT")
    assert not hasattr(provider, "_GMX_HISTORICAL_MARKETS")
