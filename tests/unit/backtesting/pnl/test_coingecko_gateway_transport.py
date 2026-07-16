"""Gateway transport for the backtest CoinGecko lanes (ALM-2952).

Covers the endpoint->RPC mapping and reshaping, sticky-death fallback
semantics, provider auto-detection from ALMANAK_GATEWAY_HOST, the benchmark
hook, and the crisis-guard pro-tier signal.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider
from almanak.framework.backtesting.pnl.providers.coingecko_gateway import (
    GatewayCoinGeckoTransport,
    gateway_coingecko_configured,
    shared_gateway_transport,
)
from almanak.gateway.proto import gateway_pb2


def _point(timestamp: int, value: str) -> gateway_pb2.CoinGeckoMarketChartDataPoint:
    return gateway_pb2.CoinGeckoMarketChartDataPoint(timestamp=timestamp, value=value)


def _transport_with_stub(stub: MagicMock) -> GatewayCoinGeckoTransport:
    transport = GatewayCoinGeckoTransport()

    async def _fake_ensure():
        return SimpleNamespace(integration=stub), gateway_pb2

    transport._ensure = _fake_ensure  # type: ignore[method-assign]
    return transport


class TestEndpointMapping:
    @pytest.mark.asyncio
    async def test_market_chart_range_maps_and_reshapes(self):
        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.return_value = gateway_pb2.CoinGeckoMarketChartRangeResponse(
            prices=[_point(1_700_000_000_000, "1592.2587069058052")],
            total_volumes=[_point(1_700_000_000_000, "12345.6")],
            success=True,
        )
        transport = _transport_with_stub(stub)

        data = await transport.request(
            "/coins/coinbase-wrapped-btc/market_chart/range",
            {"vs_currency": "usd", "from": "1699990000", "to": "1700000000"},
        )

        request = stub.CoinGeckoGetMarketChartRange.call_args.args[0]
        assert request.token_id == "coinbase-wrapped-btc"
        assert request.from_timestamp == 1699990000
        assert request.to_timestamp == 1700000000
        assert request.vs_currency == "usd"
        assert data == {
            "prices": [[1_700_000_000_000, "1592.2587069058052"]],
            "market_caps": [],
            "total_volumes": [[1_700_000_000_000, "12345.6"]],
        }
        # The consumer contract: values parse via Decimal(str(x)).
        assert Decimal(str(data["prices"][0][1])) == Decimal("1592.2587069058052")

    @pytest.mark.asyncio
    async def test_history_maps_and_reshapes(self):
        stub = MagicMock()
        stub.CoinGeckoGetHistoricalPrice.return_value = gateway_pb2.CoinGeckoHistoricalPriceResponse(
            price_usd="67123.45", success=True
        )
        transport = _transport_with_stub(stub)

        data = await transport.request("/coins/bitcoin/history", {"date": "30-12-2022", "localization": "false"})

        request = stub.CoinGeckoGetHistoricalPrice.call_args.args[0]
        assert request.token_id == "bitcoin"
        assert request.date == "30-12-2022"
        assert data == {"market_data": {"current_price": {"usd": "67123.45"}}}

    @pytest.mark.asyncio
    async def test_history_no_data_mirrors_rest_shape(self):
        stub = MagicMock()
        stub.CoinGeckoGetHistoricalPrice.return_value = gateway_pb2.CoinGeckoHistoricalPriceResponse(
            price_usd="0", success=True
        )
        transport = _transport_with_stub(stub)

        data = await transport.request("/coins/bitcoin/history", {"date": "30-12-2022"})

        assert data == {}

    @pytest.mark.asyncio
    async def test_unmapped_endpoint_returns_none_without_dialing(self):
        transport = GatewayCoinGeckoTransport()

        async def _explode():
            raise AssertionError("unmapped endpoint must not dial the gateway")

        transport._ensure = _explode  # type: ignore[method-assign]

        assert await transport.request("/coins/base/contract/0xcbb7", {}) is None
        assert await transport.request("/coins/list", {}) is None

    @pytest.mark.asyncio
    async def test_application_failure_raises_without_fallback(self):
        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.return_value = gateway_pb2.CoinGeckoMarketChartRangeResponse(
            success=False, error="upstream 500"
        )
        transport = _transport_with_stub(stub)

        with pytest.raises(ValueError, match="upstream 500"):
            await transport.request("/coins/bitcoin/market_chart/range", {"from": "1", "to": "2"})
        assert transport._dead is False


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code, details="boom"):
        super().__init__(details)
        self._code, self._details = code, details

    def code(self):
        return self._code

    def details(self):
        return self._details


class TestRpcErrorClassification:
    @pytest.mark.asyncio
    async def test_application_status_raises_without_killing_transport(self):
        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.side_effect = _FakeRpcError(grpc.StatusCode.INTERNAL, "upstream CG 500")
        transport = _transport_with_stub(stub)

        with pytest.raises(ValueError, match="upstream CG 500"):
            await transport.request("/coins/bitcoin/market_chart/range", {"from": "1", "to": "2"})
        assert transport._dead is False

    @pytest.mark.asyncio
    async def test_unavailable_status_marks_dead(self):
        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.side_effect = _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "sidecar gone")
        transport = _transport_with_stub(stub)

        assert await transport.request("/coins/bitcoin/market_chart/range", {"from": "1", "to": "2"}) is None
        assert transport._dead is True

    @pytest.mark.asyncio
    async def test_rpc_called_with_deadline(self):
        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.return_value = gateway_pb2.CoinGeckoMarketChartRangeResponse(
            prices=[_point(1, "1")], success=True
        )
        transport = _transport_with_stub(stub)
        transport._timeout = 12.5

        await transport.request("/coins/bitcoin/market_chart/range", {"from": "1", "to": "2"})

        assert stub.CoinGeckoGetMarketChartRange.call_args.kwargs["timeout"] == 12.5


class TestStickyDeath:
    @pytest.mark.asyncio
    async def test_rpc_transport_failure_marks_dead_and_skips_future_dials(self, caplog):
        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.side_effect = ConnectionError("sidecar gone")
        transport = _transport_with_stub(stub)

        with caplog.at_level("WARNING"):
            first = await transport.request("/coins/bitcoin/market_chart/range", {"from": "1", "to": "2"})
            second = await transport.request("/coins/bitcoin/market_chart/range", {"from": "1", "to": "2"})

        assert first is None and second is None
        assert transport._dead is True
        assert stub.CoinGeckoGetMarketChartRange.call_count == 1
        assert sum("falling back to direct HTTP" in r.message for r in caplog.records) == 1

    @pytest.mark.asyncio
    async def test_connect_failure_marks_dead(self, monkeypatch):
        transport = GatewayCoinGeckoTransport()
        client = SimpleNamespace(is_connected=False, connect=MagicMock(side_effect=RuntimeError("refused")))
        monkeypatch.setattr("almanak.framework.gateway_client.get_gateway_client", lambda: client)

        assert await transport.request("/coins/bitcoin/history", {"date": "01-01-2024"}) is None
        assert transport._dead is True
        client.connect.assert_called_once()

        # Sticky across subsequent requests: no re-dial.
        assert await transport.request("/coins/bitcoin/history", {"date": "01-01-2024"}) is None
        client.connect.assert_called_once()


class TestProviderIntegration:
    @pytest.mark.asyncio
    async def test_make_request_prefers_gateway(self):
        provider = CoinGeckoDataProvider(use_gateway=True)
        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.return_value = gateway_pb2.CoinGeckoMarketChartRangeResponse(
            prices=[_point(1_700_000_000_000, "42.5")], success=True
        )
        provider._gateway_transport = _transport_with_stub(stub)

        data = await provider._make_request(
            "/coins/arbitrum/market_chart/range", {"vs_currency": "usd", "from": "1", "to": "2"}
        )

        assert data["prices"] == [[1_700_000_000_000, "42.5"]]
        assert provider._session is None  # no HTTP session was ever created

    @pytest.mark.asyncio
    async def test_dead_transport_falls_back_to_http(self):
        provider = CoinGeckoDataProvider(use_gateway=True)
        assert provider._gateway_transport is not None
        provider._gateway_transport._dead = True

        sentinel = RuntimeError("http path reached")

        async def _http_sentinel():
            raise sentinel

        provider._get_session = _http_sentinel  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="http path reached"):
            await provider._make_request("/coins/arbitrum/market_chart/range", {"from": "1", "to": "2"})

    def test_auto_detect_from_env(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_HOST", "127.0.0.1")
        assert gateway_coingecko_configured() is True
        assert CoinGeckoDataProvider()._gateway_transport is not None

        monkeypatch.delenv("ALMANAK_GATEWAY_HOST")
        assert gateway_coingecko_configured() is False
        assert CoinGeckoDataProvider()._gateway_transport is None
        assert CoinGeckoDataProvider(use_gateway=False)._gateway_transport is None

    def test_shared_transport_singleton(self, monkeypatch):
        import almanak.framework.backtesting.pnl.providers.coingecko_gateway as module

        monkeypatch.setattr(module, "_shared_transport", None)
        monkeypatch.delenv("ALMANAK_GATEWAY_HOST", raising=False)
        assert shared_gateway_transport() is None

        monkeypatch.setenv("ALMANAK_GATEWAY_HOST", "127.0.0.1")
        first = shared_gateway_transport()
        assert first is not None
        assert shared_gateway_transport() is first


class TestBenchmarkHook:
    @pytest.mark.asyncio
    async def test_benchmark_series_served_via_gateway(self, monkeypatch):
        from datetime import UTC, datetime

        import almanak.framework.backtesting.pnl.providers.benchmark as benchmark

        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.return_value = gateway_pb2.CoinGeckoMarketChartRangeResponse(
            prices=[
                _point(1_700_000_000_000, "2000.0"),
                _point(1_700_003_600_000, "2100.0"),
            ],
            success=True,
        )
        monkeypatch.setattr(benchmark, "shared_gateway_transport", lambda: _transport_with_stub(stub))

        points = await benchmark._get_single_token_prices(
            "ETH",
            datetime(2023, 11, 14, tzinfo=UTC),
            datetime(2023, 11, 15, tzinfo=UTC),
            3600,
        )

        assert [p.price for p in points] == [Decimal("2000.0"), Decimal("2100.0")]
        stub.CoinGeckoGetMarketChartRange.assert_called_once()

    @pytest.mark.asyncio
    async def test_benchmark_gateway_error_returns_empty(self, monkeypatch):
        from datetime import UTC, datetime

        import almanak.framework.backtesting.pnl.providers.benchmark as benchmark

        stub = MagicMock()
        stub.CoinGeckoGetMarketChartRange.return_value = gateway_pb2.CoinGeckoMarketChartRangeResponse(
            success=False, error="upstream 500"
        )
        monkeypatch.setattr(benchmark, "shared_gateway_transport", lambda: _transport_with_stub(stub))

        points = await benchmark._get_single_token_prices(
            "ETH",
            datetime(2023, 11, 14, tzinfo=UTC),
            datetime(2023, 11, 15, tzinfo=UTC),
            3600,
        )

        assert points == []


class TestCrisisGuard:
    def test_gateway_counts_as_pro_tier(self, monkeypatch):
        from datetime import UTC, datetime, timedelta

        from almanak.framework.backtesting.scenarios import crisis_runner

        scenario = MagicMock()
        scenario.name = "covid-crash"
        scenario.warmup_start_date = datetime.now(UTC) - timedelta(days=800)
        scenario.warmup_days = 30
        backtester = MagicMock()

        monkeypatch.setattr(crisis_runner, "_is_coingecko_provider", lambda _: True)
        monkeypatch.setattr(
            crisis_runner,
            "backtest_config_from_env",
            lambda: SimpleNamespace(coingecko_api_key=None),
        )

        monkeypatch.delenv("ALMANAK_GATEWAY_HOST", raising=False)
        with pytest.raises(crisis_runner.CrisisScenarioDateRangeError):
            crisis_runner._validate_scenario_date_range(scenario, backtester)

        monkeypatch.setenv("ALMANAK_GATEWAY_HOST", "127.0.0.1")
        crisis_runner._validate_scenario_date_range(scenario, backtester)
