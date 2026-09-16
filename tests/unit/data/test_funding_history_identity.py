"""Funding history preserves chain identity and explicit fallback semantics."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.cache.versioned_cache import VersionedDataCache
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.rates.history import RateHistoryReader
from almanak.framework.market import MarketSnapshotBuilder
from almanak.framework.market.errors import FundingRateHistoryUnavailableError
from almanak.gateway.proto import gateway_pb2


def _snapshot(reader, chain="arbitrum"):
    snapshot = MarketSnapshotBuilder.seeded(chain=chain)
    snapshot._rate_history_reader = reader
    return snapshot


def test_history_gateway_and_cache_are_chain_scoped(tmp_path):
    client = MagicMock()
    client.rate_history.GetFundingRateHistory.return_value = gateway_pb2.FundingRateHistoryResponse(success=True)
    reader = RateHistoryReader(cache=VersionedDataCache(cache_dir=tmp_path, data_type="rate_history"))
    address = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
    with (
        patch(
            "almanak.framework.data.rates.history._rate_history_get_connected_gateway_client",
            return_value=(client, gateway_pb2),
        ),
        patch("almanak.framework.data.rates.history.datetime", wraps=datetime) as clock,
    ):
        clock.now.return_value = datetime(2026, 9, 1, tzinfo=UTC)
        for chain in ("arbitrum", "avalanche", "arbitrum"):
            _snapshot(reader, chain).funding_rate_history("gmx_v2", address, hours=1)
    calls = client.rate_history.GetFundingRateHistory.call_args_list
    assert len(calls) == 2
    assert [call.args[0].chain for call in calls] == ["arbitrum", "avalanche"]
    assert all(call.args[0].market == address.lower() for call in calls)
    assert all(call.args[0].market_address == address.lower() for call in calls)


def test_explicit_optional_failure_returns_default_without_clearing_required_failures(caplog):
    reader = MagicMock()
    reader.get_funding_rate_history.side_effect = DataSourceUnavailable(source="gateway", reason="RPC timeout")
    snapshot = _snapshot(reader)
    assert snapshot.funding_rate_history("gmx_v2", "ETH/USD", default=None) is None
    assert not snapshot.has_critical_data_failures()
    assert "using explicit default" in caplog.text
    with pytest.raises(FundingRateHistoryUnavailableError):
        snapshot.funding_rate_history("gmx_v2", "ETH/USD")
    assert snapshot.has_critical_data_failures()
    snapshot.funding_rate_history("gmx_v2", "ETH/USD", default=None)
    assert snapshot.has_critical_data_failures()


def test_optional_history_does_not_clear_an_unrelated_failure():
    reader = MagicMock()
    reader.get_funding_rate_history.side_effect = DataSourceUnavailable(source="gateway", reason="RPC timeout")
    snapshot = _snapshot(reader)
    snapshot._record_critical_data_failure("price", "WETH", "unavailable")
    snapshot.funding_rate_history("gmx_v2", "ETH/USD", default=None)
    assert snapshot.has_critical_data_failures()
    assert "WETH" in snapshot.summarize_critical_data_failures()


def test_optional_history_does_not_swallow_unexpected_errors():
    reader = MagicMock()
    reader.get_funding_rate_history.side_effect = ValueError("invalid request")
    snapshot = _snapshot(reader)
    with pytest.raises(FundingRateHistoryUnavailableError):
        snapshot.funding_rate_history("gmx_v2", "ETH/USD", default=None)
    assert snapshot.has_critical_data_failures()


def test_snapshot_passes_explicit_chain_override():
    reader = MagicMock()
    snapshot = _snapshot(reader)
    snapshot.funding_rate_history("gmx_v2", "ETH/USD", chain="avalanche")
    assert reader.get_funding_rate_history.call_args.kwargs["chain"] == "avalanche"


@pytest.mark.parametrize("optional", [True, False])
def test_runner_hold_respects_explicit_history_requirement(optional):
    from types import SimpleNamespace

    from almanak.framework.runner.strategy_runner import StrategyRunner

    reader = MagicMock()
    reader.get_funding_rate_history.side_effect = DataSourceUnavailable(source="gateway", reason="RPC timeout")
    snapshot = _snapshot(reader)
    if optional:
        snapshot.funding_rate_history("gmx_v2", "ETH/USD", default=None)
    else:
        with pytest.raises(FundingRateHistoryUnavailableError):
            snapshot.funding_rate_history("gmx_v2", "ETH/USD")
    runner = MagicMock()
    state = SimpleNamespace(market=snapshot, deployment_id="test", start_time=datetime.now(UTC))
    result = StrategyRunner._validate_no_action_market_data(runner, state, None)
    if optional:
        assert result is None
        runner._create_error_result.assert_not_called()
    else:
        assert runner._create_error_result.call_args.args[1].value == "DATA_ERROR"


@pytest.mark.parametrize(
    "code_name,optional", [("INVALID_ARGUMENT", False), ("INTERNAL", False), ("UNAVAILABLE", True)]
)
def test_real_reader_preserves_rpc_error_category(tmp_path, code_name, optional):
    import grpc

    class RpcFailure(grpc.RpcError):
        def code(self):
            return getattr(grpc.StatusCode, code_name)

    client = MagicMock()
    client.rate_history.GetFundingRateHistory.side_effect = RpcFailure()
    reader = RateHistoryReader(cache=VersionedDataCache(cache_dir=tmp_path, data_type="rate_history"))
    snapshot = _snapshot(reader)
    with patch(
        "almanak.framework.data.rates.history._rate_history_get_connected_gateway_client",
        return_value=(client, gateway_pb2),
    ):
        if optional:
            assert snapshot.funding_rate_history("gmx_v2", "ETH-USD", default=None) is None
            assert not snapshot.has_critical_data_failures()
        else:
            with pytest.raises(FundingRateHistoryUnavailableError):
                snapshot.funding_rate_history("gmx_v2", "ETH-USD", default=None)
            assert snapshot.has_critical_data_failures()


def test_backtest_funding_unavailability_supports_explicit_default():
    from almanak.framework.backtesting.pnl.engine import BacktestRateHistoryReader
    from almanak.framework.data.funding.models import FundingRateUnavailableError
    from tests.unit.backtesting.pnl.test_rate_history_serve import TICK, _FakeSource

    class MissingSource(_FakeSource):
        async def funding_rate_at(self, venue, market, timestamp):
            raise FundingRateUnavailableError(venue, market, "No measured funding point")

    reader = BacktestRateHistoryReader(MissingSource(), "arbitrum")
    reader.bind(TICK)
    snapshot = _snapshot(reader)
    assert snapshot.funding_rate_history("gmx_v2", "ETH-USD", hours=1, default=None) is None
    assert not snapshot.has_critical_data_failures()


@pytest.mark.parametrize("market", ["HYPE/USD", "0xBcb8FE13d02b023e8f94f6881Cc0192fd918A5C0"])
def test_reader_through_gateway_and_real_gmx_identity_verification(tmp_path, market):
    import asyncio
    from decimal import Decimal
    from unittest.mock import AsyncMock

    from almanak.connectors.gmx_v2.gateway.market_registry import GmxV2MarketRegistry
    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.rate_history_service import FundingRatePoint, RateHistoryServiceServicer
    from tests.gateway.services.test_funding_history_controls import _Context
    from tests.unit.connectors.gmx_v2.test_dynamic_market_registry_vib6561 import (
        HYPE_MARKET,
        MARKETS,
        TOKENS,
        _reader_result,
    )

    service = RateHistoryServiceServicer(GatewaySettings())
    provider = service._funding_providers["gmx_v2"]
    client = MagicMock()
    context = _Context()
    client.rate_history.GetFundingRateHistory.side_effect = lambda request: asyncio.run(
        service.GetFundingRateHistory(request, context)
    )
    reader = RateHistoryReader(cache=VersionedDataCache(cache_dir=tmp_path, data_type="rate_history"))
    verified_rpc = AsyncMock(return_value=_reader_result())
    fetch = AsyncMock(return_value=[FundingRatePoint(timestamp=1700000000, rate_hourly=Decimal("0.0001"))])
    with (
        patch(
            "almanak.framework.data.rates.history._rate_history_get_connected_gateway_client",
            return_value=(client, gateway_pb2),
        ),
        patch.object(provider, "_market_registry", GmxV2MarketRegistry()),
        patch.object(provider._market_registry, "_get_json", AsyncMock(side_effect=[MARKETS, TOKENS])),
        patch("almanak.gateway.services.pt_rpc_adapter.build_gateway_eth_call", return_value=verified_rpc),
        patch.object(service, "_get_http_session", AsyncMock()),
        patch("almanak.connectors.gmx_v2.gateway.funding_history.fetch_gmx_funding_history", fetch),
    ):
        result = _snapshot(reader).funding_rate_history("gmx_v2", market, hours=1)
    assert context.code is None
    assert result.value[0].rate == Decimal("0.0001")
    verified_rpc.assert_awaited()
    assert fetch.call_args.kwargs["market_address"] == HYPE_MARKET.lower()
    assert fetch.call_args.kwargs["chain"] == "arbitrum"


@pytest.mark.parametrize(
    "code_name,exception_type,transport",
    [
        ("UNAVAILABLE", "DataSourceUnavailable", True),
        ("NOT_FOUND", "DataSourceUnavailable", False),
        ("DEADLINE_EXCEEDED", "DataSourceTimeout", None),
        ("RESOURCE_EXHAUSTED", "DataSourceRateLimited", None),
    ],
)
def test_funding_rpc_preserves_typed_status(code_name, exception_type, transport):
    import grpc

    from almanak.framework.data import interfaces
    from almanak.framework.data.rates.history import _call_get_funding_rate_history

    class RpcFailure(grpc.RpcError):
        def code(self):
            return getattr(grpc.StatusCode, code_name)

    client = MagicMock()
    client.rate_history.GetFundingRateHistory.side_effect = RpcFailure()
    with pytest.raises(getattr(interfaces, exception_type)) as failure:
        _call_get_funding_rate_history(
            client, gateway_pb2, venue="gmx_v2", market="ETH-USD", chain="arbitrum", start_ts=1, end_ts=2
        )
    if transport is not None:
        assert failure.value.transport is transport


@pytest.mark.parametrize("chain,market", [("ethereum", "ETH-USD"), ("arbitrum", "0xnot-an-address")])
def test_invalid_gmx_identity_stays_critical_through_gateway(tmp_path, chain, market):
    import asyncio

    import grpc

    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer
    from tests.gateway.services.test_funding_history_controls import _Context

    service = RateHistoryServiceServicer(GatewaySettings())
    context = _Context()

    class RpcFailure(grpc.RpcError):
        def code(self):
            return context.code

    def rpc(request):
        response = asyncio.run(service.GetFundingRateHistory(request, context))
        assert context.code == grpc.StatusCode.INVALID_ARGUMENT
        if context.code is not None:
            raise RpcFailure()
        return response

    client = MagicMock()
    client.rate_history.GetFundingRateHistory.side_effect = rpc
    reader = RateHistoryReader(cache=VersionedDataCache(cache_dir=tmp_path, data_type="rate_history"))
    snapshot = _snapshot(reader, chain)
    with patch(
        "almanak.framework.data.rates.history._rate_history_get_connected_gateway_client",
        return_value=(client, gateway_pb2),
    ):
        with pytest.raises(FundingRateHistoryUnavailableError):
            snapshot.funding_rate_history("gmx_v2", market, hours=1, default=None)
    assert snapshot.has_critical_data_failures()


@pytest.mark.asyncio
async def test_backtest_timeout_and_orphan_support_optional_default():
    import asyncio

    from almanak.framework.backtesting.pnl.engine import BacktestRateHistoryReader
    from tests.unit.backtesting.pnl.test_rate_history_serve import TICK, _FakeSource

    source = _FakeSource()
    reader = BacktestRateHistoryReader(source, "arbitrum")
    reader.bind(TICK)
    future = MagicMock()
    future.result.side_effect = TimeoutError()
    future.done.return_value = False
    reader._bridge_executor = MagicMock()

    def submit(fn, coro):
        coro.close()
        return future

    reader._bridge_executor.submit.side_effect = submit
    snapshot = _snapshot(reader)
    assert snapshot.funding_rate_history("gmx_v2", "ETH-USD", hours=1, default=None) is None
    assert snapshot.funding_rate_history("gmx_v2", "ETH-USD", hours=1, default=None) is None
    assert not snapshot.has_critical_data_failures()
    reader._bridge_executor.submit.assert_called_once()
    assert source.asked == []
    future.done.return_value = True
    # The orphan guard releases the lane after the worker has finished.
    with patch.object(reader, "_bridge_executor", None):
        # Run outside this event loop to exercise the normal synchronous bridge.
        result = await asyncio.to_thread(
            reader.get_funding_rate_history, venue="gmx_v2", market_symbol="ETH-USD", hours=1
        )
    assert len(result.value) == 1


def test_invalid_hours_stay_critical_with_fallback_only_backtest_source():
    from almanak.framework.backtesting.pnl.engine import BacktestRateHistoryReader
    from tests.unit.backtesting.pnl.test_rate_history_serve import TICK, _FakeSource

    reader = BacktestRateHistoryReader(_FakeSource(history_capable=False), "arbitrum")
    reader.bind(TICK)
    snapshot = _snapshot(reader)
    with pytest.raises(FundingRateHistoryUnavailableError, match="hours >= 1"):
        snapshot.funding_rate_history("gmx_v2", "ETH-USD", hours=0, default=None)
    assert snapshot.has_critical_data_failures()
