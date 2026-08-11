"""Dynamic GMX-native backtest price history regressions (ALM-3149)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest

from almanak.connectors._base.gateway_capabilities import (
    PerpMarketCatalogueUnavailable,
    PerpMarketVerificationError,
    PerpPriceCandle,
    PerpPriceCandlePage,
)
from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.perp_price_history_registry import PerpPriceHistoryRegistry
from almanak.connectors.gmx_v2.backtest_prices import (
    GMXOracleDataProvider,
    GMXPriceHistoryCoverageError,
    _GMXOracleMarketSource,
)
from almanak.connectors.gmx_v2.connector import CONNECTOR
from almanak.connectors.gmx_v2.gateway.market_registry import GmxV2MarketRegistry
from almanak.connectors.gmx_v2.gateway.provider import GmxV2GatewayConnector
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import OHLCV, HistoricalDataConfig, normalize_token_key
from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
from almanak.framework.backtesting.pnl.providers.coingecko import OHLCVCache
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer

MARKET_TOKEN = "0x" + "1" * 40
INDEX_TOKEN = "0x" + "2" * 40
OTHER_INDEX_TOKEN = "0x" + "3" * 40
LONG_TOKEN = "0x" + "4" * 40
SHORT_TOKEN = "0x" + "5" * 40


def _candle(timestamp: datetime, price: str = "12.5") -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=int(timestamp.timestamp()),
        open=price,
        high=price,
        low=price,
        close=price,
    )


def _page(
    timeframe: str,
    *timestamps: datetime,
    market_token: str = MARKET_TOKEN,
    index_token: str = INDEX_TOKEN,
    index_symbol: str = "NEWMARKET",
) -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        error="",
        market="NEWMARKET/USD",
        market_token=market_token,
        index_token=index_token,
        index_symbol=index_symbol,
        timeframe=timeframe,
        candles=[_candle(timestamp) for timestamp in timestamps],
    )


@pytest.mark.asyncio
async def test_gateway_candle_page_uses_finite_client_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    rpc = Mock(return_value=SimpleNamespace(success=True))
    client = SimpleNamespace(
        config=SimpleNamespace(timeout=12.5),
        rate_history=SimpleNamespace(GetPerpPriceCandles=rpc),
    )
    monkeypatch.setattr(
        _GMXOracleMarketSource,
        "_gateway",
        staticmethod(lambda: (client, gateway_pb2)),
    )
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")

    await source._fetch_page(timeframe="4h", before_ts=1_800_000_000)

    rpc.assert_called_once()
    assert rpc.call_args.kwargs == {"timeout": 12.5}


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> object:
        return self.payload


@pytest.mark.asyncio
async def test_gateway_resolves_new_market_then_uses_verified_dynamic_symbol() -> None:
    """No market/symbol allowlist is consulted before GMX candle egress."""
    record = SimpleNamespace(
        label="NEWMARKET/USD",
        market_token=MARKET_TOKEN,
        index_token=INDEX_TOKEN,
        index_symbol="NEWMARKET",
    )
    connector = GmxV2GatewayConnector()
    connector._market_registry.resolve = AsyncMock(return_value=record)
    response = _Response(
        {
            "period": "4h",
            "candles": [[1_700_000_000, "12", "13", "11", "12.5"]],
        }
    )
    session = SimpleNamespace(get=Mock(return_value=response))
    servicer = SimpleNamespace(
        settings=SimpleNamespace(network="mainnet"),
        _get_http_session=AsyncMock(return_value=session),
    )

    with patch("almanak.gateway.services.pt_rpc_adapter.build_gateway_eth_call", return_value=AsyncMock()):
        page = await connector.fetch_price_candles(
            servicer,
            market="NEWMARKET/USD",
            chain="arbitrum",
            timeframe="4h",
            before_ts=1_800_000_000,
            limit=100,
        )

    assert page.index_symbol == "NEWMARKET"
    assert page.index_token == INDEX_TOKEN
    connector._market_registry.resolve.assert_awaited_once()
    assert connector._market_registry.resolve.await_args.kwargs["allow_delisted_address"] is False
    assert session.get.call_args.kwargs["params"] == {
        "tokenSymbol": "NEWMARKET",
        "period": "4h",
        "before": "1800000000",
        "limit": "100",
    }


@pytest.mark.asyncio
async def test_removed_market_is_not_eligible_for_historical_open_path() -> None:
    registry = GmxV2MarketRegistry()
    markets = {
        "markets": [
            {
                "name": "NEWMARKET/USD [LONG-SHORT]",
                "marketToken": MARKET_TOKEN,
                "indexToken": INDEX_TOKEN,
                "longToken": LONG_TOKEN,
                "shortToken": SHORT_TOKEN,
                "isListed": False,
            }
        ]
    }
    tokens = {
        "tokens": [
            {"symbol": "NEWMARKET", "address": INDEX_TOKEN, "decimals": 8},
            {"symbol": "LONG", "address": LONG_TOKEN, "decimals": 18},
            {"symbol": "SHORT", "address": SHORT_TOKEN, "decimals": 6},
        ]
    }
    with patch.object(registry, "_get_json", AsyncMock(side_effect=[markets, tokens])):
        resolved = await registry.resolve(
            chain="arbitrum",
            market=MARKET_TOKEN,
            eth_call=AsyncMock(),
            allow_delisted_address=False,
        )
    assert resolved is None


@pytest.mark.asyncio
async def test_rate_history_service_dispatches_typed_price_page() -> None:
    provider = SimpleNamespace(
        price_history_chains=lambda: frozenset({"arbitrum"}),
        price_history_timeframes=lambda: ("1h", "4h"),
        fetch_price_candles=AsyncMock(
            return_value=PerpPriceCandlePage(
                market="NEWMARKET/USD",
                market_token=MARKET_TOKEN,
                index_token=INDEX_TOKEN,
                index_symbol="NEWMARKET",
                timeframe="4h",
                candles=(
                    PerpPriceCandle(
                        timestamp=1_700_000_000,
                        open=Decimal("12"),
                        high=Decimal("13"),
                        low=Decimal("11"),
                        close=Decimal("12.5"),
                    ),
                ),
            )
        ),
    )
    service = RateHistoryServiceServicer.__new__(RateHistoryServiceServicer)
    service._perp_price_providers = {"gmx_v2": provider}
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    response = await service.GetPerpPriceCandles(
        gateway_pb2.GetPerpPriceCandlesRequest(
            venue="gmx_v2",
            chain="arbitrum",
            market="NEWMARKET/USD",
            timeframe="4h",
            before_ts=1_800_000_000,
            limit=100,
        ),
        context,
    )

    assert response.success is True
    assert response.index_symbol == "NEWMARKET"
    assert response.candles[0].close == "12.5"
    provider.fetch_price_candles.assert_awaited_once()
    context.set_code.assert_not_called()


@pytest.mark.asyncio
async def test_rate_history_service_rejects_unsupported_exact_timeframe() -> None:
    provider = SimpleNamespace(
        price_history_chains=lambda: frozenset({"arbitrum"}),
        price_history_timeframes=lambda: ("1h", "4h"),
    )
    service = RateHistoryServiceServicer.__new__(RateHistoryServiceServicer)
    service._perp_price_providers = {"gmx_v2": provider}
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    response = await service.GetPerpPriceCandles(
        gateway_pb2.GetPerpPriceCandlesRequest(
            venue="gmx_v2",
            chain="arbitrum",
            market="NEWMARKET/USD",
            timeframe="2h",
            before_ts=1_800_000_000,
            limit=100,
        ),
        context,
    )

    assert response.success is False
    assert "does not support timeframe" in response.error
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.parametrize(
    ("request_overrides", "provider", "message"),
    [
        ({"market": ""}, None, "venue, chain, market, and timeframe are required"),
        ({"venue": "unknown"}, None, "unknown perp price-history venue"),
        (
            {"chain": "base"},
            SimpleNamespace(
                price_history_chains=lambda: frozenset({"arbitrum"}),
                price_history_timeframes=lambda: ("1h",),
            ),
            "has no perp price history",
        ),
        (
            {"before_ts": 0},
            SimpleNamespace(
                price_history_chains=lambda: frozenset({"arbitrum"}),
                price_history_timeframes=lambda: ("1h",),
            ),
            "before_ts must be positive",
        ),
        (
            {"limit": 0},
            SimpleNamespace(
                price_history_chains=lambda: frozenset({"arbitrum"}),
                price_history_timeframes=lambda: ("1h",),
            ),
            "limit must be between 1 and 10000",
        ),
    ],
)
@pytest.mark.asyncio
async def test_rate_history_service_validates_every_request_boundary(
    request_overrides: dict[str, object],
    provider: object | None,
    message: str,
) -> None:
    values: dict[str, object] = {
        "venue": "gmx_v2",
        "chain": "arbitrum",
        "market": "NEWMARKET/USD",
        "timeframe": "1h",
        "before_ts": 1_800_000_000,
        "limit": 100,
    }
    values.update(request_overrides)
    service = RateHistoryServiceServicer.__new__(RateHistoryServiceServicer)
    service._perp_price_providers = {} if provider is None else {"gmx_v2": provider}
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    response = await service.GetPerpPriceCandles(gateway_pb2.GetPerpPriceCandlesRequest(**values), context)

    assert response.success is False
    assert message in response.error
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.parametrize(
    ("error", "status", "visible_error"),
    [
        (PerpMarketCatalogueUnavailable("catalogue down"), grpc.StatusCode.UNAVAILABLE, "catalogue down"),
        (PerpMarketVerificationError("identity mismatch"), grpc.StatusCode.INVALID_ARGUMENT, "identity mismatch"),
        (RuntimeError("sensitive upstream detail"), grpc.StatusCode.INTERNAL, "internal server error"),
    ],
)
@pytest.mark.asyncio
async def test_rate_history_service_maps_provider_failures_without_leaking(
    error: Exception,
    status: grpc.StatusCode,
    visible_error: str,
) -> None:
    provider = SimpleNamespace(
        price_history_chains=lambda: frozenset({"arbitrum"}),
        price_history_timeframes=lambda: ("1h",),
        fetch_price_candles=AsyncMock(side_effect=error),
    )
    service = RateHistoryServiceServicer.__new__(RateHistoryServiceServicer)
    service._perp_price_providers = {"gmx_v2": provider}
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    response = await service.GetPerpPriceCandles(
        gateway_pb2.GetPerpPriceCandlesRequest(
            venue="gmx_v2",
            chain="arbitrum",
            market="NEWMARKET/USD",
            timeframe="1h",
            before_ts=1_800_000_000,
            limit=100,
        ),
        context,
    )

    assert response.success is False
    assert response.error == visible_error
    assert "sensitive upstream detail" not in response.error
    context.set_code.assert_called_once_with(status)


@pytest.mark.asyncio
async def test_rate_history_service_sanitizes_upstream_json_decode_failure() -> None:
    raw_fragment = "sensitive upstream payload"
    provider = SimpleNamespace(
        price_history_chains=lambda: frozenset({"arbitrum"}),
        price_history_timeframes=lambda: ("1h",),
        fetch_price_candles=AsyncMock(side_effect=json.JSONDecodeError("invalid upstream JSON", raw_fragment, 0)),
    )
    service = RateHistoryServiceServicer.__new__(RateHistoryServiceServicer)
    service._perp_price_providers = {"gmx_v2": provider}
    context = SimpleNamespace(set_code=Mock(), set_details=Mock())

    response = await service.GetPerpPriceCandles(
        gateway_pb2.GetPerpPriceCandlesRequest(
            venue="gmx_v2",
            chain="arbitrum",
            market="NEWMARKET/USD",
            timeframe="1h",
            before_ts=1_800_000_000,
            limit=100,
        ),
        context,
    )

    assert response.success is False
    assert response.error == "internal server error"
    assert raw_fragment not in response.error
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    context.set_details.assert_called_once_with("internal server error")


@pytest.mark.asyncio
async def test_auto_selects_finest_cadence_with_actual_complete_coverage() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=200)
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")
    calls: list[str] = []
    counts: dict[str, int] = {}

    async def fetch_page(*, timeframe: str, before_ts: int, limit: int) -> SimpleNamespace:
        del before_ts, limit
        calls.append(timeframe)
        counts[timeframe] = counts.get(timeframe, 0) + 1
        if timeframe == "4h":
            native_opens = [
                start - timedelta(hours=4) + timedelta(hours=4 * index)
                for index in range(int((end - start).total_seconds() // 14_400) + 1)
            ]
            return _page("4h", *native_opens)
        if counts[timeframe] == 1:
            return _page(timeframe, start + timedelta(days=1), end - timedelta(hours=1))
        return _page(timeframe)

    source._fetch_page = fetch_page  # type: ignore[method-assign]

    resolved = await source.prepare(requested="auto", start=start, end=end)

    assert resolved == "4h"
    assert calls == ["1m", "1m", "5m", "5m", "15m", "15m", "1h", "1h", "4h"]
    assert source.provenance["timeframe"] == "4h"


@pytest.mark.asyncio
async def test_auto_continues_after_data_level_cadence_failure() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")
    candle = OHLCV(start, Decimal("12"), Decimal("12"), Decimal("12"), Decimal("12"))
    source._fetch_complete = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            DataSourceUnavailable(source="gmx_oracle", reason="malformed 1m response"),
            [candle],
        ]
    )

    resolved = await source.prepare(requested="auto", start=start, end=start + timedelta(minutes=5))

    assert resolved == "5m"
    assert [call.kwargs["timeframe"] for call in source._fetch_complete.await_args_list] == ["1m", "5m"]


@pytest.mark.asyncio
async def test_auto_aborts_on_transport_failure_without_probing_other_cadences() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")
    source._fetch_complete = AsyncMock(  # type: ignore[method-assign]
        side_effect=DataSourceUnavailable(source="gateway", reason="RPC unavailable", transport=True)
    )

    with pytest.raises(DataSourceUnavailable, match="RPC unavailable"):
        await source.prepare(requested="auto", start=start, end=start + timedelta(minutes=5))

    source._fetch_complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_explicit_timeframe_fails_without_silent_downgrade() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=200)
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")
    calls: list[str] = []

    async def fetch_page(*, timeframe: str, before_ts: int, limit: int) -> SimpleNamespace:
        del before_ts, limit
        calls.append(timeframe)
        if len(calls) == 1:
            return _page(timeframe, start + timedelta(days=1), end - timedelta(hours=1))
        return _page(timeframe)

    source._fetch_page = fetch_page  # type: ignore[method-assign]

    with pytest.raises(GMXPriceHistoryCoverageError, match="does not provide every native candle"):
        await source.prepare(requested="1h", start=start, end=end)

    assert calls == ["1h", "1h"]


@pytest.mark.asyncio
async def test_market_identity_drift_is_rejected_while_paging() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=2)
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")
    pages = iter(
        [
            _page("1h", start + timedelta(days=1), end - timedelta(hours=1)),
            _page("1h", start, market_token=MARKET_TOKEN, index_token=OTHER_INDEX_TOKEN),
        ]
    )

    async def fetch_page(*, timeframe: str, before_ts: int, limit: int) -> SimpleNamespace:
        del timeframe, before_ts, limit
        return next(pages)

    source._fetch_page = fetch_page  # type: ignore[method-assign]

    with pytest.raises(DataSourceUnavailable, match="identity changed"):
        await source.prepare(requested="1h", start=start, end=end)


@pytest.mark.asyncio
async def test_fetch_complete_bounds_page_to_requested_window() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")
    limits: list[int] = []

    async def fetch_page(*, timeframe: str, before_ts: int, limit: int) -> SimpleNamespace:
        del before_ts
        limits.append(limit)
        native_opens = [start - timedelta(hours=4) + timedelta(hours=4 * index) for index in range(8)]
        return _page(timeframe, *native_opens)

    source._fetch_page = fetch_page  # type: ignore[method-assign]

    series = await source._fetch_complete(timeframe="4h", start=start, end=end)

    assert limits == [8]
    assert [candle.timestamp for candle in series] == [start + timedelta(hours=4 * index) for index in range(7)]


@pytest.mark.asyncio
async def test_verified_index_never_falls_back_to_coingecko() -> None:
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    fallback = SimpleNamespace(
        get_price=AsyncMock(return_value=Decimal("1")),
        get_ohlcv=AsyncMock(return_value=[]),
    )
    provider = GMXOracleDataProvider(
        fallback=fallback,
        chain="arbitrum",
        market="NEWMARKET/USD",
    )
    provider._source.index_symbol = "NEWMARKET"
    provider._source.index_token = INDEX_TOKEN.lower()
    provider._source.timeframe = "1h"
    provider._source._coverage = (timestamp, timestamp + timedelta(hours=1))
    provider._source._series = [
        OHLCV(
            timestamp=timestamp,
            open=Decimal("12"),
            high=Decimal("12"),
            low=Decimal("12"),
            close=Decimal("12"),
            volume=None,
        )
    ]
    provider._source._series_timestamps = [timestamp]

    assert await provider.get_price("NEWMARKET", timestamp) == Decimal("12")
    assert await provider.get_price("USDC", timestamp) == Decimal("1")
    fallback.get_price.assert_awaited_once_with("USDC", timestamp)
    assert provider.required_price_tokens == (normalize_token_key("arbitrum", INDEX_TOKEN),)


@pytest.mark.asyncio
async def test_provider_prepares_requested_and_legacy_cadences() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = GMXOracleDataProvider(fallback=SimpleNamespace(), chain="arbitrum", market="NEWMARKET/USD")
    provider._source.prepare = AsyncMock(return_value="4h")
    auto_config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=200),
        timeframe="auto",
    )

    assert await provider.prepare_backtest(auto_config) == "4h"
    assert auto_config.resolved_timeframe == "4h"
    assert auto_config.interval_seconds == 3600
    provider._source.prepare.assert_awaited_once_with(
        requested="auto",
        start=auto_config.start_time,
        end=auto_config.end_time,
    )

    legacy_config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        interval_seconds=3600,
    )
    assert provider._requested_timeframe(legacy_config) == "1h"
    with pytest.raises(ValueError, match="cannot serve explicit 17s ticks exactly"):
        provider._canonical_timeframe_for_interval(17)


def test_market_source_series_enforces_prepared_contract() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")

    with pytest.raises(RuntimeError, match="not prepared"):
        source.series(start=start, end=start + timedelta(hours=1), interval_seconds=3600)

    candle = OHLCV(
        timestamp=start,
        open=Decimal("12"),
        high=Decimal("12"),
        low=Decimal("12"),
        close=Decimal("12"),
        volume=None,
    )
    source.timeframe = "1h"
    source._coverage = (start, start + timedelta(hours=2))
    source._series = [candle]

    with pytest.raises(ValueError, match="does not match resolved GMX timeframe"):
        source.series(start=start, end=start + timedelta(hours=1), interval_seconds=14_400)
    with pytest.raises(ValueError, match="outside the prepared backtest window"):
        source.series(start=start - timedelta(hours=1), end=start, interval_seconds=3600)
    assert source.series(start=start, end=start + timedelta(hours=1), interval_seconds=3600) == [candle]


@pytest.mark.asyncio
async def test_provider_iteration_uses_only_latest_observable_candle() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = GMXOracleDataProvider(fallback=SimpleNamespace(), chain="arbitrum", market="NEWMARKET/USD")
    provider._source.index_symbol = "NEWMARKET"
    provider._source.index_token = INDEX_TOKEN.lower()
    candle = OHLCV(
        timestamp=start + timedelta(hours=1),
        open=Decimal("12"),
        high=Decimal("12"),
        low=Decimal("12"),
        close=Decimal("12"),
        volume=None,
    )
    index_key = normalize_token_key("arbitrum", INDEX_TOKEN)
    provider._prefetch = AsyncMock(return_value=SimpleNamespace(data={index_key: [candle]}))  # type: ignore[method-assign]
    config = HistoricalDataConfig(
        start_time=start,
        end_time=start + timedelta(hours=2),
        interval_seconds=3600,
        tokens=["NEWMARKET"],
        chains=["arbitrum"],
        include_ohlcv=True,
    )

    states = [state async for _timestamp, state in provider.iterate(config)]

    assert states[0].prices == {}
    assert states[1].prices == {index_key: Decimal("12")}
    assert states[2].prices == {index_key: Decimal("12")}
    assert states[1].ohlcv == {index_key: candle}
    assert states[1].get_price("NEWMARKET") == Decimal("12")
    assert states[1].get_price(index_key) == Decimal("12")
    snapshot = create_market_snapshot_from_state(states[1], chain="arbitrum")
    assert snapshot.price("NEWMARKET") == Decimal("12")


@pytest.mark.asyncio
async def test_fallback_assets_delegate_to_complete_prefetch_policy() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    usdc = OHLCV(start, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"))
    fallback_cache = OHLCVCache(data={"USDC": [usdc]}, fetched_at=start, default_chain="arbitrum")
    fallback = SimpleNamespace(
        provider_name="coingecko",
        prefetch_ohlcv_data=AsyncMock(return_value=fallback_cache),
    )
    provider = GMXOracleDataProvider(fallback=fallback, chain="arbitrum", market="NEWMARKET/USD")
    provider._source.index_symbol = "NEWMARKET"
    provider._source.index_token = INDEX_TOKEN.lower()
    provider._source.timeframe = "4h"
    provider._source._coverage = (start, start + timedelta(hours=4))
    provider._source._series = [OHLCV(start, Decimal("12"), Decimal("12"), Decimal("12"), Decimal("12"))]
    config = HistoricalDataConfig(
        start_time=start,
        end_time=start + timedelta(hours=4),
        interval_seconds=3600,
        tokens=["NEWMARKET", "USDC"],
        chains=["arbitrum"],
    )

    cache = await provider._prefetch(config)

    index_key = normalize_token_key("arbitrum", INDEX_TOKEN)
    assert set(cache.data) == {index_key, "USDC"}
    delegated = fallback.prefetch_ohlcv_data.await_args.args[0]
    assert delegated.tokens == ["USDC"]
    assert delegated.interval_seconds == 3600
    assert provider.measured_granularity_seconds == 14_400


@pytest.mark.asyncio
async def test_generic_fallback_isolates_token_errors_and_seeds_from_prior_day() -> None:
    start = datetime(2025, 1, 2, tzinfo=UTC)
    first = OHLCV(start + timedelta(hours=1), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"))
    prior = OHLCV(start - timedelta(hours=12), Decimal("0.9"), Decimal("0.9"), Decimal("0.9"), Decimal("0.9"))
    fallback = SimpleNamespace(
        provider_name="custom",
        get_ohlcv=AsyncMock(side_effect=[ValueError("bad token"), [first], [prior]]),
    )
    provider = GMXOracleDataProvider(fallback=fallback, chain="arbitrum", market="NEWMARKET/USD")
    config = HistoricalDataConfig(
        start_time=start,
        end_time=start + timedelta(hours=2),
        interval_seconds=3600,
        tokens=["BAD", "USDC"],
        chains=["arbitrum"],
    )

    cache = await provider._prefetch(config)

    assert cache.data["BAD"] == []
    assert cache.data["USDC"] == [prior, first]
    prior_call = fallback.get_ohlcv.await_args_list[2]
    assert prior_call.args[1] == start - timedelta(days=1)
    assert prior_call.args[2] == start


def test_manifest_registry_is_dynamic_and_contains_no_market_allowlist() -> None:
    declaration = CONNECTOR.perp_price_history
    assert declaration is not None
    assert not hasattr(declaration, "markets")
    assert PerpPriceHistoryRegistry.canonical("gmx") == "gmx_v2"
    assert PerpPriceHistoryRegistry.venue_for("gmx_v2") == "gmx_v2"


def test_connector_clear_invalidates_and_rebuilds_price_history_registry() -> None:
    PerpPriceHistoryRegistry.reset_cache()
    assert PerpPriceHistoryRegistry.canonical("gmx") == "gmx_v2"
    assert PerpPriceHistoryRegistry.backtest_provider("gmx_v2") is GMXOracleDataProvider
    original_venue_map = PerpPriceHistoryRegistry._venue_map
    assert original_venue_map is not None

    CONNECTOR_REGISTRY.clear()

    assert PerpPriceHistoryRegistry._venue_map is None
    assert PerpPriceHistoryRegistry._provider_class_cache == {}
    assert PerpPriceHistoryRegistry.canonical("gmx") == "gmx_v2"
    assert PerpPriceHistoryRegistry._venue_map is not original_venue_map
    assert PerpPriceHistoryRegistry.backtest_provider("gmx_v2") is GMXOracleDataProvider


def test_manifest_registry_loads_caches_and_validates_provider_classes() -> None:
    PerpPriceHistoryRegistry.reset_cache()
    try:
        provider_factory = PerpPriceHistoryRegistry.backtest_provider("gmx")
        assert provider_factory is GMXOracleDataProvider
        assert PerpPriceHistoryRegistry.backtest_provider("gmx_v2") is provider_factory
        assert PerpPriceHistoryRegistry.backtest_provider("unknown") is None

        PerpPriceHistoryRegistry._venue_map = {"bad": "bad"}
        PerpPriceHistoryRegistry._alias_map = {}
        PerpPriceHistoryRegistry._chains_map = {"bad": ()}
        PerpPriceHistoryRegistry._provider_ref_map = {"bad": SimpleNamespace(load=Mock(return_value=object()))}
        PerpPriceHistoryRegistry._provider_class_cache.clear()
        with pytest.raises(TypeError, match="must resolve to a class"):
            PerpPriceHistoryRegistry.backtest_provider("bad")

        PerpPriceHistoryRegistry._provider_ref_map = {}
        assert PerpPriceHistoryRegistry.backtest_provider("bad") is None
    finally:
        PerpPriceHistoryRegistry.reset_cache()


def test_config_records_requested_and_resolved_native_timeframe() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=200),
        timeframe="auto",
    )
    unresolved_hash = config.calculate_config_hash()

    config.apply_resolved_timeframe("4h", 14_400)

    assert config.interval_seconds == 3600
    assert config.resolved_timeframe == "4h"
    assert config.calculate_config_hash() != unresolved_hash
    restored = PnLBacktestConfig.from_dict(config.to_dict())
    assert restored.timeframe == "auto"
    assert restored.resolved_timeframe == "4h"
    assert restored.interval_seconds == 3600


def test_explicit_price_timeframe_does_not_change_simulation_interval() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    config = PnLBacktestConfig(
        start_time=start,
        end_time=start + timedelta(days=1),
        interval_seconds=3600,
        timeframe="4h",
    )
    assert config.interval_seconds == 3600


def test_gmx_candle_close_is_not_visible_at_its_open_time() -> None:
    opened_at = datetime(2025, 1, 1, tzinfo=UTC)
    candle = _GMXOracleMarketSource._decode_candle(_candle(opened_at), "4h")
    assert candle.timestamp == opened_at + timedelta(hours=4)


# ---------------------------------------------------------------------------
# ALM-3148 — fully dynamic index-plane disambiguation on the candle path
# ---------------------------------------------------------------------------


async def _candle_resolution_for(market: str, chain: str) -> dict[str, object]:
    """Return the dynamic-registry request made by the candle path."""
    record = SimpleNamespace(
        label=market,
        market_token=MARKET_TOKEN,
        index_token=INDEX_TOKEN,
        index_symbol="ETH",
    )
    connector = GmxV2GatewayConnector()
    connector._market_registry.resolve = AsyncMock(return_value=record)
    session = SimpleNamespace(
        get=Mock(return_value=_Response({"period": "5m", "candles": [[1_700_000_000, "12", "13", "11", "12.5"]]}))
    )
    servicer = SimpleNamespace(
        settings=SimpleNamespace(network="mainnet"),
        _get_http_session=AsyncMock(return_value=session),
    )
    with patch("almanak.gateway.services.pt_rpc_adapter.build_gateway_eth_call", return_value=AsyncMock()):
        await connector.fetch_price_candles(
            servicer, market=market, chain=chain, timeframe="5m", before_ts=1_800_000_000, limit=3
        )
    return connector._market_registry.resolve.await_args.kwargs


@pytest.mark.asyncio
async def test_candle_path_always_uses_dynamic_index_resolution() -> None:
    """Labels and addresses reach the venue registry unchanged; no SDK market table participates."""
    for market in ("ETH/USD", "ETH-USD", "HYPE/USD", MARKET_TOKEN):
        request = await _candle_resolution_for(market, "arbitrum")
        assert request["market"] == market
        assert request["chain"] == "arbitrum"
        assert request["allow_delisted_address"] is False
        assert request["allow_index_equivalent"] is True


# ---------------------------------------------------------------------------
# Native-labeled index markets vs the run chain's native sentinel (ALM-3067):
# GMX labels the WETH-indexed market "ETH", while the engine registers the
# gas symbol at the native sentinel. One symbol must never claim two
# identities — the venue plane publishes its alias under the index token's
# registry symbol instead, and native reads price through the 1:1 wrap plane.
# ---------------------------------------------------------------------------

WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
WETH_KEY = normalize_token_key("arbitrum", WETH_ARBITRUM)
NATIVE_SENTINEL_KEY = normalize_token_key("arbitrum", "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")


def _eth_index_provider(start: datetime, index_token: str = WETH_ARBITRUM) -> GMXOracleDataProvider:
    provider = GMXOracleDataProvider(fallback=SimpleNamespace(), chain="arbitrum", market="ETH/USD")
    provider._source.index_symbol = "ETH"
    provider._source.index_token = index_token
    candle = OHLCV(
        timestamp=start + timedelta(hours=1),
        open=Decimal("2500"),
        high=Decimal("2500"),
        low=Decimal("2500"),
        close=Decimal("2500"),
        volume=None,
    )
    index_key = normalize_token_key("arbitrum", index_token)
    provider._prefetch = AsyncMock(  # type: ignore[method-assign]
        return_value=SimpleNamespace(data={index_key: [candle]})
    )
    return provider


async def _second_tick_state(provider: GMXOracleDataProvider, start: datetime):
    config = HistoricalDataConfig(
        start_time=start,
        end_time=start + timedelta(hours=2),
        interval_seconds=3600,
        tokens=["ETH"],
        chains=["arbitrum"],
        include_ohlcv=True,
    )
    states = [state async for _timestamp, state in provider.iterate(config)]
    return states[1]


@pytest.mark.asyncio
async def test_eth_index_market_survives_native_sentinel_registration_at_tick_one() -> None:
    """The exact tick-1 abort: static ETH->sentinel vs venue ETH->WETH alias."""
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = _eth_index_provider(start)
    state = await _second_tick_state(provider, start)

    # The venue label is remapped to the index token's registry symbol; the
    # bare gas symbol stays free for the engine's native registration.
    assert state.symbol_aliases == {"WETH": WETH_KEY}
    assert provider._source.owns("ETH")
    assert provider._cache_key("ETH") == WETH_KEY

    # The engine's per-tick registration of the run's static map (which
    # carries ETH at the native sentinel, ALM-3067) must not be ambiguous.
    token_addresses = {"ETH": NATIVE_SENTINEL_KEY, "WETH": WETH_KEY}
    state.register_symbol_aliases(token_addresses)

    # Native-symbol reads price through the registry's 1:1 wrap plane onto
    # the venue series — on the state and on the strategy-facing snapshot.
    assert state.get_price("ETH") == Decimal("2500")
    assert state.get_price(NATIVE_SENTINEL_KEY) == Decimal("2500")
    snapshot = create_market_snapshot_from_state(state, chain="arbitrum", token_addresses=token_addresses)
    assert snapshot.price("ETH") == Decimal("2500")
    assert snapshot.price("WETH") == Decimal("2500")


@pytest.mark.asyncio
async def test_native_collision_without_registry_identity_publishes_no_alias() -> None:
    """An ETH-labeled index the registry cannot name yields no symbol alias."""
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = _eth_index_provider(start, index_token=OTHER_INDEX_TOKEN)
    state = await _second_tick_state(provider, start)

    assert state.symbol_aliases == {}
    other_key = normalize_token_key("arbitrum", OTHER_INDEX_TOKEN)
    assert state.get_price(other_key) == Decimal("2500")
    state.register_symbol_aliases({"ETH": NATIVE_SENTINEL_KEY})
    with pytest.raises(KeyError):
        state.get_price("ETH")


@pytest.mark.asyncio
async def test_genuine_identity_conflict_still_fails_closed() -> None:
    """The ambiguity guards stay intact for real two-identity claims."""
    start = datetime(2025, 1, 1, tzinfo=UTC)
    provider = _eth_index_provider(start)
    state = await _second_tick_state(provider, start)

    conflicting = {"WETH": normalize_token_key("arbitrum", OTHER_INDEX_TOKEN)}
    with pytest.raises(ValueError, match="Ambiguous market-data identity for WETH"):
        state.register_symbol_aliases(conflicting)
    with pytest.raises(ValueError, match="Ambiguous snapshot identity for WETH"):
        create_market_snapshot_from_state(state, chain="arbitrum", token_addresses=conflicting)


def test_non_native_index_symbol_keeps_the_venue_alias() -> None:
    source = _GMXOracleMarketSource(chain="arbitrum", market="NEWMARKET/USD", venue="gmx_v2")
    source.index_symbol = "NEWMARKET"
    source.index_token = INDEX_TOKEN.lower()
    assert source.alias_symbol() == "NEWMARKET"


@pytest.mark.asyncio
async def test_tracked_alias_spelling_routes_to_venue_series_not_fallback() -> None:
    """Every spelling of the index identity is provider-owned (review P1).

    A WETH-labeled address-first strategy tracks "WETH"; unowned, it would go
    to the fallback, whose token map normalizes it onto the SAME
    (chain, index_token) cache key — and the fallback merge would silently
    replace the venue candles with another provider's data.
    """
    start = datetime(2025, 1, 1, tzinfo=UTC)
    venue_candle = OHLCV(start, Decimal("2500"), Decimal("2500"), Decimal("2500"), Decimal("2500"))
    usdc_candle = OHLCV(start, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"))
    fallback_cache = OHLCVCache(data={"USDC": [usdc_candle]}, fetched_at=start, default_chain="arbitrum")
    fallback = SimpleNamespace(
        provider_name="coingecko",
        prefetch_ohlcv_data=AsyncMock(return_value=fallback_cache),
    )
    provider = GMXOracleDataProvider(fallback=fallback, chain="arbitrum", market="ETH/USD")
    provider._source.index_symbol = "ETH"
    provider._source.index_token = WETH_ARBITRUM
    provider._source.timeframe = "4h"
    provider._source._coverage = (start, start + timedelta(hours=4))
    provider._source._series = [venue_candle]

    for spelling in ("WETH", "ETH", WETH_ARBITRUM, f"arbitrum:{WETH_ARBITRUM}", WETH_KEY):
        assert provider._source.owns(spelling), spelling

    config = HistoricalDataConfig(
        start_time=start,
        end_time=start + timedelta(hours=4),
        interval_seconds=3600,
        tokens=["WETH", "USDC"],
        chains=["arbitrum"],
    )
    cache = await provider._prefetch(config)

    assert cache.data[WETH_KEY] == [venue_candle]
    delegated = fallback.prefetch_ohlcv_data.await_args.args[0]
    assert delegated.tokens == ["USDC"]
    assert provider._price_sources[f"arbitrum:{WETH_ARBITRUM}"] == "gmx_oracle_candles"


@pytest.mark.asyncio
async def test_fallback_series_never_replaces_the_owned_venue_series() -> None:
    """A custom-registered symbol for the index token cannot clobber the venue key."""
    start = datetime(2025, 1, 1, tzinfo=UTC)
    venue_candle = OHLCV(start, Decimal("2500"), Decimal("2500"), Decimal("2500"), Decimal("2500"))
    imposter = OHLCV(start, Decimal("9"), Decimal("9"), Decimal("9"), Decimal("9"))
    fallback_cache = OHLCVCache(data={WETH_KEY: [imposter]}, fetched_at=start, default_chain="arbitrum")
    fallback = SimpleNamespace(
        provider_name="coingecko",
        prefetch_ohlcv_data=AsyncMock(return_value=fallback_cache),
    )
    provider = GMXOracleDataProvider(fallback=fallback, chain="arbitrum", market="ETH/USD")
    provider._source.index_symbol = "ETH"
    provider._source.index_token = WETH_ARBITRUM
    provider._source.timeframe = "4h"
    provider._source._coverage = (start, start + timedelta(hours=4))
    provider._source._series = [venue_candle]

    config = HistoricalDataConfig(
        start_time=start,
        end_time=start + timedelta(hours=4),
        interval_seconds=3600,
        tokens=["ETH", "MYETH"],
        chains=["arbitrum"],
    )
    cache = await provider._prefetch(config)

    assert cache.data[WETH_KEY] == [venue_candle]
    assert provider._price_sources[f"arbitrum:{WETH_ARBITRUM}"] == "gmx_oracle_candles"


# ---------------------------------------------------------------------------
# Backtest population of the perps-read catalog (address-form fill pricing)
# ---------------------------------------------------------------------------


def _verified_market_response(market_token: str = MARKET_TOKEN) -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        error="",
        market=SimpleNamespace(
            label="NEWMARKET/USD",
            market_token=market_token,
            index_token=INDEX_TOKEN,
            index_symbol="NEWMARKET",
            index_token_decimals=8,
            long_token=LONG_TOKEN,
            long_token_symbol="LONG",
            short_token=SHORT_TOKEN,
            short_token_symbol="SHORT",
            verified=True,
        ),
    )


def _prepared_provider(monkeypatch: pytest.MonkeyPatch, client: SimpleNamespace) -> GMXOracleDataProvider:
    """Provider whose candle-lane prepare succeeds with an accepted identity."""
    monkeypatch.setattr(_GMXOracleMarketSource, "_gateway", staticmethod(lambda: (client, gateway_pb2)))
    provider = GMXOracleDataProvider(fallback=SimpleNamespace(), chain="arbitrum", market=MARKET_TOKEN)

    async def _prepare(**_kwargs: object) -> str:
        provider._source.resolved_market = "NEWMARKET/USD"
        provider._source.market_token = MARKET_TOKEN.lower()
        provider._source.index_token = INDEX_TOKEN.lower()
        provider._source.index_symbol = "NEWMARKET"
        return "1h"

    provider._source.prepare = _prepare  # type: ignore[method-assign]
    return provider


def _hourly_config() -> PnLBacktestConfig:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    return PnLBacktestConfig(start_time=start, end_time=start + timedelta(days=1), interval_seconds=3600)


@pytest.mark.asyncio
async def test_prepare_backtest_remembers_verified_market_for_fill_pricing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backtest run must own the market identity the live compiler would.

    The live path populates the perps-read catalog at compile time; a backtest
    never compiles, so address-form PERP_OPEN fill pricing rejected every open
    (gmx_v2_directional_perp: 2189 rejected fills, 0 trades) while the candle
    lane held the verified identity. prepare_backtest now resolves through the
    same venue-verified GetPerpMarket surface and remembers it.
    """
    from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry
    from almanak.connectors.gmx_v2 import market_catalog

    rpc = Mock(return_value=_verified_market_response())
    client = SimpleNamespace(
        config=SimpleNamespace(timeout=5.0),
        market=SimpleNamespace(GetPerpMarket=rpc),
    )
    provider = _prepared_provider(monkeypatch, client)

    assert await provider.prepare_backtest(_hourly_config()) == "1h"

    record = market_catalog.by_address("arbitrum", MARKET_TOKEN)
    assert record is not None
    assert record.index_symbol == "NEWMARKET"
    meta = PerpsReadRegistry.market_metadata("gmx_v2", MARKET_TOKEN, "arbitrum")
    assert meta is not None
    assert meta.index_token_symbol == "NEWMARKET"
    assert meta.index_token_decimals == 8


@pytest.mark.asyncio
async def test_metadata_unavailable_keeps_fill_lane_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    # A gateway without GetPerpMarket (or a venue miss) must not fail the run
    # and must not invent an identity: the catalog stays empty and the fill
    # lane keeps its named per-intent rejection.
    from almanak.connectors.gmx_v2 import market_catalog

    client = SimpleNamespace(config=SimpleNamespace(timeout=5.0), market=None)
    provider = _prepared_provider(monkeypatch, client)

    assert await provider.prepare_backtest(_hourly_config()) == "1h"

    assert market_catalog.by_address("arbitrum", MARKET_TOKEN) is None


@pytest.mark.asyncio
async def test_metadata_identity_mismatch_is_not_remembered(monkeypatch: pytest.MonkeyPatch) -> None:
    # Defense in depth: metadata whose market token disagrees with the candle
    # provenance this run already verified is refused, never remembered.
    from almanak.connectors.gmx_v2 import market_catalog

    rpc = Mock(return_value=_verified_market_response(market_token="0x" + "9" * 40))
    client = SimpleNamespace(
        config=SimpleNamespace(timeout=5.0),
        market=SimpleNamespace(GetPerpMarket=rpc),
    )
    provider = _prepared_provider(monkeypatch, client)

    await provider.prepare_backtest(_hourly_config())

    assert market_catalog.by_address("arbitrum", MARKET_TOKEN) is None
    assert market_catalog.by_address("arbitrum", "0x" + "9" * 40) is None


@pytest.mark.asyncio
async def test_remember_is_a_noop_without_accepted_candle_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No candle identity accepted -> nothing to cross-check -> the gateway is
    # never touched (also keeps prepare-mocking tests hermetic).
    monkeypatch.setattr(
        _GMXOracleMarketSource,
        "_gateway",
        staticmethod(Mock(side_effect=AssertionError("gateway must not be touched"))),
    )
    provider = GMXOracleDataProvider(fallback=SimpleNamespace(), chain="arbitrum", market=MARKET_TOKEN)
    provider._source.prepare = AsyncMock(return_value="1h")

    assert await provider.prepare_backtest(_hourly_config()) == "1h"
