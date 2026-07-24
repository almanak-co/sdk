"""Unit tests for IntegrationServiceServicer gRPC handlers.

Covers the three highest-CRAP handlers with all HTTP/integration clients
mocked (no network):

- ``BinanceGetKlines``
- ``CoinGeckoGetMarkets``
- ``CoinGeckoGetMarketChartRange``

Pattern mirrors ``tests/gateway/test_coingecko_ohlcv_handler.py``: the
servicer is constructed via ``__new__`` with ``_initialized = True`` so
``_ensure_initialized`` short-circuits, and the integration clients are
plain mocks.
"""

from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.integrations.base import IntegrationRateLimitError
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.integration_service import IntegrationServiceServicer


def _make_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    ctx.set_trailing_metadata = MagicMock()
    return ctx


@pytest.fixture
def service() -> IntegrationServiceServicer:
    svc = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
    svc._initialized = True  # _ensure_initialized becomes a no-op
    svc._binance = MagicMock()
    svc._coingecko = MagicMock()
    svc._thegraph = None
    svc._zerion = None
    svc._portfolio_chain = None
    return svc


# =============================================================================
# BinanceGetKlines
# =============================================================================


class TestBinanceGetKlines:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_symbol", ["", "btc/usdt", "BTC USDT"])
    async def test_invalid_symbol_returns_invalid_argument(self, service, bad_symbol):
        ctx = _make_context()
        request = gateway_pb2.BinanceKlinesRequest(symbol=bad_symbol)

        response = await service.BinanceGetKlines(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert len(response.klines) == 0
        service._binance.get_klines.assert_not_called()

    @pytest.mark.asyncio
    async def test_defaults_applied_and_symbol_normalized(self, service):
        ctx = _make_context()
        service._binance.get_klines = AsyncMock(return_value=[])
        request = gateway_pb2.BinanceKlinesRequest(symbol="btcusdt")

        await service.BinanceGetKlines(request, ctx)

        service._binance.get_klines.assert_awaited_once_with(
            symbol="BTCUSDT",
            interval="1h",
            limit=100,
            start_time=None,
            end_time=None,
        )
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_explicit_params_propagated(self, service):
        ctx = _make_context()
        service._binance.get_klines = AsyncMock(return_value=[])
        request = gateway_pb2.BinanceKlinesRequest(
            symbol="ETHUSDC",
            interval="4h",
            limit=7,
            start_time=1_700_000_000_000,
            end_time=1_700_000_360_000,
        )

        await service.BinanceGetKlines(request, ctx)

        service._binance.get_klines.assert_awaited_once_with(
            symbol="ETHUSDC",
            interval="4h",
            limit=7,
            start_time=1_700_000_000_000,
            end_time=1_700_000_360_000,
        )

    @pytest.mark.asyncio
    async def test_success_maps_klines(self, service):
        ctx = _make_context()
        service._binance.get_klines = AsyncMock(
            return_value=[
                {
                    "open_time": 1_700_000_000_000,
                    "open": "100.1",
                    "high": "105.5",
                    "low": "99.9",
                    "close": "104.2",
                    "volume": "1234.5",
                    "close_time": 1_700_000_359_999,
                    "quote_volume": "128000.7",
                    "trades": 42,
                },
                {},  # missing keys fall back to defaults
            ]
        )
        request = gateway_pb2.BinanceKlinesRequest(symbol="BTCUSDT")

        response = await service.BinanceGetKlines(request, ctx)

        assert len(response.klines) == 2
        first = response.klines[0]
        assert first.open_time == 1_700_000_000_000
        assert first.open == "100.1"
        assert first.high == "105.5"
        assert first.low == "99.9"
        assert first.close == "104.2"
        assert first.volume == "1234.5"
        assert first.close_time == 1_700_000_359_999
        assert first.quote_volume == "128000.7"
        assert first.trades == 42
        second = response.klines[1]
        assert second.open_time == 0
        assert second.open == ""
        assert second.trades == 0
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_klines_returns_empty_response(self, service):
        ctx = _make_context()
        service._binance.get_klines = AsyncMock(return_value=[])
        request = gateway_pb2.BinanceKlinesRequest(symbol="BTCUSDT")

        response = await service.BinanceGetKlines(request, ctx)

        assert len(response.klines) == 0
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_value_error_maps_to_invalid_argument(self, service):
        ctx = _make_context()
        service._binance.get_klines = AsyncMock(side_effect=ValueError("bad interval"))
        request = gateway_pb2.BinanceKlinesRequest(symbol="BTCUSDT", interval="99x")

        response = await service.BinanceGetKlines(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_once_with("bad interval")
        assert len(response.klines) == 0

    @pytest.mark.asyncio
    async def test_rate_limit_error_maps_to_resource_exhausted(self, service):
        ctx = _make_context()
        service._binance.get_klines = AsyncMock(
            side_effect=IntegrationRateLimitError(integration="binance", retry_after=2.5)
        )
        request = gateway_pb2.BinanceKlinesRequest(symbol="BTCUSDT")

        response = await service.BinanceGetKlines(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.RESOURCE_EXHAUSTED)
        ctx.set_trailing_metadata.assert_called_once()  # typed RetryInfo/ErrorInfo trailer
        assert len(response.klines) == 0

    @pytest.mark.asyncio
    async def test_generic_error_maps_to_internal_with_opaque_message(self, service):
        ctx = _make_context()
        service._binance.get_klines = AsyncMock(side_effect=RuntimeError("secret stack details"))
        request = gateway_pb2.BinanceKlinesRequest(symbol="BTCUSDT")

        response = await service.BinanceGetKlines(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        # Non-upstream errors must NOT leak str(exc) across the trust boundary.
        ctx.set_details.assert_called_once_with("Internal gateway error")
        assert len(response.klines) == 0


# =============================================================================
# CoinGeckoGetMarkets
# =============================================================================


class TestCoinGeckoGetMarkets:
    @pytest.mark.asyncio
    async def test_defaults_propagated(self, service):
        ctx = _make_context()
        service._coingecko.get_markets = AsyncMock(return_value=[])
        request = gateway_pb2.CoinGeckoGetMarketsRequest()

        await service.CoinGeckoGetMarkets(request, ctx)

        service._coingecko.get_markets.assert_awaited_once_with(
            vs_currency="usd",
            ids=None,
            order="market_cap_desc",
            per_page=100,
            page=1,
        )
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_explicit_params_propagated(self, service):
        ctx = _make_context()
        service._coingecko.get_markets = AsyncMock(return_value=[])
        request = gateway_pb2.CoinGeckoGetMarketsRequest(
            vs_currency="eur",
            ids=["ethereum", "bitcoin"],
            order="volume_desc",
            per_page=5,
            page=2,
        )

        await service.CoinGeckoGetMarkets(request, ctx)

        service._coingecko.get_markets.assert_awaited_once_with(
            vs_currency="eur",
            ids=["ethereum", "bitcoin"],
            order="volume_desc",
            per_page=5,
            page=2,
        )

    @pytest.mark.asyncio
    async def test_success_maps_markets(self, service):
        ctx = _make_context()
        service._coingecko.get_markets = AsyncMock(
            return_value=[
                {
                    "id": "ethereum",
                    "symbol": "eth",
                    "name": "Ethereum",
                    "current_price": "3500.12",
                    "market_cap": "420000000000",
                    "market_cap_rank": 2,
                    "total_volume": "18000000000",
                    "high_24h": "3550.0",
                    "low_24h": "3400.0",
                    "price_change_24h": "50.5",
                    "price_change_percentage_24h": "1.46",
                },
                # market_cap_rank=None exercises the `or 0` fallback;
                # remaining keys missing exercise the .get() defaults.
                {"id": "newcoin", "market_cap_rank": None},
            ]
        )
        request = gateway_pb2.CoinGeckoGetMarketsRequest()

        response = await service.CoinGeckoGetMarkets(request, ctx)

        assert len(response.markets) == 2
        eth = response.markets[0]
        assert eth.id == "ethereum"
        assert eth.symbol == "eth"
        assert eth.name == "Ethereum"
        assert eth.current_price == "3500.12"
        assert eth.market_cap == "420000000000"
        assert eth.market_cap_rank == 2
        assert eth.total_volume == "18000000000"
        assert eth.high_24h == "3550.0"
        assert eth.low_24h == "3400.0"
        assert eth.price_change_24h == "50.5"
        assert eth.price_change_percentage_24h == "1.46"
        assert eth.last_updated == 0  # handler never parses the ISO timestamp
        sparse = response.markets[1]
        assert sparse.id == "newcoin"
        assert sparse.symbol == ""
        assert sparse.market_cap_rank == 0
        assert sparse.current_price == "0"
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_markets_returns_empty_response(self, service):
        ctx = _make_context()
        service._coingecko.get_markets = AsyncMock(return_value=[])
        request = gateway_pb2.CoinGeckoGetMarketsRequest(vs_currency="usd")

        response = await service.CoinGeckoGetMarkets(request, ctx)

        assert len(response.markets) == 0
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_client_error_maps_to_internal(self, service):
        ctx = _make_context()
        service._coingecko.get_markets = AsyncMock(side_effect=RuntimeError("upstream down"))
        request = gateway_pb2.CoinGeckoGetMarketsRequest()

        response = await service.CoinGeckoGetMarkets(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        ctx.set_details.assert_called_once_with("upstream down")
        assert len(response.markets) == 0


# =============================================================================
# CoinGeckoGetMarketChartRange
# =============================================================================


class TestCoinGeckoGetMarketChartRange:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_token", ["", "bad token!", "UPPER_CASE@"])
    async def test_invalid_token_id_returns_invalid_argument(self, service, bad_token):
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoMarketChartRangeRequest(token_id=bad_token, from_timestamp=0, to_timestamp=100)

        response = await service.CoinGeckoGetMarketChartRange(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert response.success is False
        assert response.error != ""
        service._coingecko.get_market_chart_range.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("from_ts", "to_ts", "expected_detail"),
        [
            (-1, 100, "from_timestamp must be non-negative"),
            (0, -1, "to_timestamp must be non-negative"),
            (200, 100, "from_timestamp must be less than or equal to to_timestamp"),
        ],
    )
    async def test_invalid_timestamp_range(self, service, from_ts, to_ts, expected_detail):
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoMarketChartRangeRequest(
            token_id="ethereum", from_timestamp=from_ts, to_timestamp=to_ts
        )

        response = await service.CoinGeckoGetMarketChartRange(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_once_with(expected_detail)
        assert response.success is False
        assert response.error == expected_detail
        service._coingecko.get_market_chart_range.assert_not_called()

    @pytest.mark.asyncio
    async def test_success_maps_all_series_and_normalizes_token_id(self, service):
        ctx = _make_context()
        service._coingecko.get_market_chart_range = AsyncMock(
            return_value={
                "prices": [[1_700_000_000_000, 3500.5], [1_700_000_060_000, 3501.25]],
                "market_caps": [[1_700_000_000_000, 420_000_000_000.0]],
                "total_volumes": [[1_700_000_000_000, 18_000_000_000.0]],
            }
        )
        # Uppercase token_id is normalized to lowercase before hitting the client.
        request = gateway_pb2.CoinGeckoMarketChartRangeRequest(
            token_id="ETHEREUM", from_timestamp=1_700_000_000, to_timestamp=1_700_003_600
        )

        response = await service.CoinGeckoGetMarketChartRange(request, ctx)

        service._coingecko.get_market_chart_range.assert_awaited_once_with(
            token_id="ethereum",
            from_timestamp=1_700_000_000,
            to_timestamp=1_700_003_600,
            vs_currency="usd",
        )
        assert response.success is True
        assert response.error == ""
        assert len(response.prices) == 2
        assert response.prices[0].timestamp == 1_700_000_000_000
        assert response.prices[0].value == "3500.5"
        assert response.prices[1].value == "3501.25"
        assert len(response.market_caps) == 1
        assert response.market_caps[0].value == "420000000000.0"
        assert len(response.total_volumes) == 1
        assert response.total_volumes[0].timestamp == 1_700_000_000_000
        assert response.total_volumes[0].value == "18000000000.0"
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_explicit_vs_currency_propagated(self, service):
        ctx = _make_context()
        service._coingecko.get_market_chart_range = AsyncMock(return_value={})
        request = gateway_pb2.CoinGeckoMarketChartRangeRequest(
            token_id="bitcoin",
            from_timestamp=100,
            to_timestamp=100,  # from == to is allowed
            vs_currency="eur",
        )

        response = await service.CoinGeckoGetMarketChartRange(request, ctx)

        service._coingecko.get_market_chart_range.assert_awaited_once_with(
            token_id="bitcoin",
            from_timestamp=100,
            to_timestamp=100,
            vs_currency="eur",
        )
        assert response.success is True
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_data_is_success_with_empty_series(self, service):
        ctx = _make_context()
        service._coingecko.get_market_chart_range = AsyncMock(return_value={})
        request = gateway_pb2.CoinGeckoMarketChartRangeRequest(token_id="ethereum", from_timestamp=0, to_timestamp=1)

        response = await service.CoinGeckoGetMarketChartRange(request, ctx)

        assert response.success is True
        assert len(response.prices) == 0
        assert len(response.market_caps) == 0
        assert len(response.total_volumes) == 0

    @pytest.mark.asyncio
    async def test_client_error_maps_to_internal(self, service):
        ctx = _make_context()
        service._coingecko.get_market_chart_range = AsyncMock(side_effect=RuntimeError("api exploded"))
        request = gateway_pb2.CoinGeckoMarketChartRangeRequest(token_id="ethereum", from_timestamp=0, to_timestamp=100)

        response = await service.CoinGeckoGetMarketChartRange(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        ctx.set_details.assert_called_once_with("api exploded")
        assert response.success is False
        assert response.error == "api exploded"


# =============================================================================
# BinanceGetOrderBook
# =============================================================================


class TestBinanceGetOrderBook:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_symbol", ["", "btc/usdt", "BTC USDT"])
    async def test_invalid_symbol_returns_invalid_argument(self, service, bad_symbol):
        ctx = _make_context()
        request = gateway_pb2.BinanceOrderBookRequest(symbol=bad_symbol)

        response = await service.BinanceGetOrderBook(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert len(response.bids) == 0
        assert len(response.asks) == 0
        service._binance.get_order_book.assert_not_called()

    @pytest.mark.asyncio
    async def test_defaults_applied_and_symbol_normalized(self, service):
        ctx = _make_context()
        service._binance.get_order_book = AsyncMock(return_value={})
        request = gateway_pb2.BinanceOrderBookRequest(symbol="ethusdt")

        response = await service.BinanceGetOrderBook(request, ctx)

        service._binance.get_order_book.assert_awaited_once_with(symbol="ETHUSDT", limit=100)
        ctx.set_code.assert_not_called()
        # Missing bids/asks/last_update_id keys degrade to empty response.
        assert response.last_update_id == 0
        assert len(response.bids) == 0
        assert len(response.asks) == 0

    @pytest.mark.asyncio
    async def test_success_maps_order_book(self, service):
        ctx = _make_context()
        service._binance.get_order_book = AsyncMock(
            return_value={
                "last_update_id": 987654,
                "bids": [
                    {"price": "3000.10", "quantity": "1.5"},
                    {"price": "2999.90", "quantity": "0.25"},
                ],
                "asks": [{"price": "3000.20", "quantity": "2.0"}],
            }
        )
        request = gateway_pb2.BinanceOrderBookRequest(symbol="ETHUSDT", limit=5)

        response = await service.BinanceGetOrderBook(request, ctx)

        service._binance.get_order_book.assert_awaited_once_with(symbol="ETHUSDT", limit=5)
        assert response.last_update_id == 987654
        assert [(b.price, b.quantity) for b in response.bids] == [
            ("3000.10", "1.5"),
            ("2999.90", "0.25"),
        ]
        assert [(a.price, a.quantity) for a in response.asks] == [("3000.20", "2.0")]
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_entry_fields_default_to_empty_strings(self, service):
        ctx = _make_context()
        service._binance.get_order_book = AsyncMock(
            return_value={"bids": [{}], "asks": [{"price": "1.0"}]}
        )
        request = gateway_pb2.BinanceOrderBookRequest(symbol="ETHUSDT")

        response = await service.BinanceGetOrderBook(request, ctx)

        assert (response.bids[0].price, response.bids[0].quantity) == ("", "")
        assert (response.asks[0].price, response.asks[0].quantity) == ("1.0", "")

    @pytest.mark.asyncio
    async def test_upstream_error_maps_to_internal(self, service):
        ctx = _make_context()
        service._binance.get_order_book = AsyncMock(side_effect=RuntimeError("binance down"))
        request = gateway_pb2.BinanceOrderBookRequest(symbol="ETHUSDT")

        response = await service.BinanceGetOrderBook(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        ctx.set_details.assert_called_once_with("binance down")
        assert len(response.bids) == 0


# =============================================================================
# CoinGeckoGetPrices
# =============================================================================


class TestCoinGeckoGetPrices:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_token", ["has spaces", "UPPER_CASE!", "a" * 100])
    async def test_invalid_token_id_returns_invalid_argument(self, service, bad_token):
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoGetPricesRequest(token_ids=["ethereum", bad_token])

        response = await service.CoinGeckoGetPrices(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert len(response.tokens) == 0
        service._coingecko.get_prices.assert_not_called()

    @pytest.mark.asyncio
    async def test_defaults_and_token_normalization(self, service):
        ctx = _make_context()
        service._coingecko.get_prices = AsyncMock(return_value={})
        request = gateway_pb2.CoinGeckoGetPricesRequest(token_ids=["Ethereum", "BITCOIN"])

        await service.CoinGeckoGetPrices(request, ctx)

        service._coingecko.get_prices.assert_awaited_once_with(
            token_ids=["ethereum", "bitcoin"],
            vs_currencies=["usd"],
        )
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_explicit_vs_currencies_propagated(self, service):
        ctx = _make_context()
        service._coingecko.get_prices = AsyncMock(return_value={})
        request = gateway_pb2.CoinGeckoGetPricesRequest(
            token_ids=["ethereum"],
            vs_currencies=["eur", "btc"],
        )

        await service.CoinGeckoGetPrices(request, ctx)

        service._coingecko.get_prices.assert_awaited_once_with(
            token_ids=["ethereum"],
            vs_currencies=["eur", "btc"],
        )

    @pytest.mark.asyncio
    async def test_success_maps_token_prices_and_timestamp(self, service):
        ctx = _make_context()
        # CoinGeckoIntegration.get_prices returns string prices (proto map is
        # map<string, string>).
        service._coingecko.get_prices = AsyncMock(
            return_value={
                "ethereum": {"usd": "3000.5", "eur": "2800.25"},
                "bitcoin": {"usd": "60000.0"},
            }
        )
        request = gateway_pb2.CoinGeckoGetPricesRequest(token_ids=["ethereum", "bitcoin"])

        response = await service.CoinGeckoGetPrices(request, ctx)

        by_id = {t.token_id: dict(t.prices) for t in response.tokens}
        assert by_id == {
            "ethereum": {"usd": "3000.5", "eur": "2800.25"},
            "bitcoin": {"usd": "60000.0"},
        }
        assert response.timestamp > 0
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_token_ids_passes_empty_list_upstream(self, service):
        ctx = _make_context()
        service._coingecko.get_prices = AsyncMock(return_value={})
        request = gateway_pb2.CoinGeckoGetPricesRequest()

        response = await service.CoinGeckoGetPrices(request, ctx)

        service._coingecko.get_prices.assert_awaited_once_with(token_ids=[], vs_currencies=["usd"])
        assert len(response.tokens) == 0
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_upstream_error_maps_to_internal(self, service):
        ctx = _make_context()
        service._coingecko.get_prices = AsyncMock(side_effect=RuntimeError("coingecko down"))
        request = gateway_pb2.CoinGeckoGetPricesRequest(token_ids=["ethereum"])

        response = await service.CoinGeckoGetPrices(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        ctx.set_details.assert_called_once_with("coingecko down")
        assert len(response.tokens) == 0


# =============================================================================
# TheGraphQuery
# =============================================================================


class TestTheGraphQueryHandler:
    QUERY = "{ pools(first: 1) { id } }"

    @pytest.fixture
    def graph_service(self, service):
        service._thegraph = MagicMock()
        return service

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_query", ["", "{ __schema { types { name } } }"])
    async def test_invalid_query_returns_invalid_argument_with_errors_json(self, graph_service, bad_query):
        import json

        ctx = _make_context()
        request = gateway_pb2.TheGraphQueryRequest(subgraph_id="uniswap-v3-ethereum", query=bad_query)

        response = await graph_service.TheGraphQuery(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert response.success is False
        errors = json.loads(response.errors)
        assert len(errors) == 1 and "message" in errors[0]
        graph_service._thegraph.query.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_variables_json_returns_invalid_argument(self, graph_service):
        ctx = _make_context()
        request = gateway_pb2.TheGraphQueryRequest(
            subgraph_id="uniswap-v3-ethereum",
            query=self.QUERY,
            variables="{not json",
        )

        response = await graph_service.TheGraphQuery(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_once_with("Invalid variables JSON")
        assert response.success is False
        graph_service._thegraph.query.assert_not_called()

    @pytest.mark.asyncio
    async def test_variables_parsed_and_passed_upstream(self, graph_service):
        ctx = _make_context()
        graph_service._thegraph.query = AsyncMock(return_value={"data": {}, "success": True})
        request = gateway_pb2.TheGraphQueryRequest(
            subgraph_id="uniswap-v3-ethereum",
            query=self.QUERY,
            variables='{"first": 5}',
        )

        await graph_service.TheGraphQuery(request, ctx)

        graph_service._thegraph.query.assert_awaited_once_with(
            subgraph_id="uniswap-v3-ethereum",
            query=self.QUERY,
            variables={"first": 5},
        )

    @pytest.mark.asyncio
    async def test_success_serializes_data_and_omits_empty_errors(self, graph_service):
        import json

        ctx = _make_context()
        graph_service._thegraph.query = AsyncMock(
            return_value={"data": {"pools": [{"id": "0xpool"}]}, "success": True}
        )
        request = gateway_pb2.TheGraphQueryRequest(subgraph_id="uniswap-v3-ethereum", query=self.QUERY)

        response = await graph_service.TheGraphQuery(request, ctx)

        assert response.success is True
        assert json.loads(response.data) == {"pools": [{"id": "0xpool"}]}
        assert response.errors == ""
        # No variables supplied -> None passed upstream.
        graph_service._thegraph.query.assert_awaited_once_with(
            subgraph_id="uniswap-v3-ethereum",
            query=self.QUERY,
            variables=None,
        )
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_graphql_errors_serialized_and_empty_data_omitted(self, graph_service):
        import json

        ctx = _make_context()
        graph_service._thegraph.query = AsyncMock(
            return_value={"data": None, "errors": [{"message": "boom"}], "success": False}
        )
        request = gateway_pb2.TheGraphQueryRequest(subgraph_id="uniswap-v3-ethereum", query=self.QUERY)

        response = await graph_service.TheGraphQuery(request, ctx)

        assert response.success is False
        assert response.data == ""
        assert json.loads(response.errors) == [{"message": "boom"}]

    @pytest.mark.asyncio
    async def test_upstream_error_maps_to_internal_with_errors_json(self, graph_service):
        import json

        ctx = _make_context()
        graph_service._thegraph.query = AsyncMock(side_effect=RuntimeError("thegraph down"))
        request = gateway_pb2.TheGraphQueryRequest(subgraph_id="uniswap-v3-ethereum", query=self.QUERY)

        response = await graph_service.TheGraphQuery(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        ctx.set_details.assert_called_once_with("thegraph down")
        assert response.success is False
        assert json.loads(response.errors) == [{"message": "thegraph down"}]
