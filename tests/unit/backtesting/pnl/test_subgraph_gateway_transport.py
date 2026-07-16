"""Gateway transport for the TheGraph subgraph lane (ALM-2952).

Covers the TheGraphQuery RPC mapping, the shared error-classification path,
sticky-death fallback, and auto-detection on SubgraphClient and its
LiquidityDepthProvider consumer.
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.backtesting.pnl.providers.gateway_transport import (
    GatewaySubgraphTransport,
    gateway_backtest_configured,
)
from almanak.framework.backtesting.pnl.providers.liquidity_depth import LiquidityDepthProvider
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphClient,
    SubgraphQueryError,
)
from almanak.gateway.proto import gateway_pb2

_QUERY = "query GetPoolFee($poolAddress: ID!) { pool(id: $poolAddress) { feeTier } }"


def _transport_with_stub(stub: MagicMock) -> GatewaySubgraphTransport:
    transport = GatewaySubgraphTransport()

    async def _fake_ensure():
        return SimpleNamespace(integration=stub), gateway_pb2

    transport._ensure = _fake_ensure  # type: ignore[method-assign]
    return transport


def _client_with_stub(stub: MagicMock) -> SubgraphClient:
    client = SubgraphClient(use_gateway=True)
    client._gateway_transport = _transport_with_stub(stub)
    return client


class TestTheGraphQueryMapping:
    @pytest.mark.asyncio
    async def test_query_served_via_gateway(self):
        stub = MagicMock()
        stub.TheGraphQuery.return_value = gateway_pb2.TheGraphQueryResponse(
            data=json.dumps({"pool": {"feeTier": "500"}}), success=True
        )
        client = _client_with_stub(stub)

        data = await client.query("SUBGRAPH_ID", _QUERY, variables={"poolAddress": "0xabc"})

        request = stub.TheGraphQuery.call_args.args[0]
        assert request.subgraph_id == "SUBGRAPH_ID"
        assert request.query == _QUERY
        assert json.loads(request.variables) == {"poolAddress": "0xabc"}
        assert data == {"pool": {"feeTier": "500"}}

    @pytest.mark.asyncio
    async def test_no_variables_sends_empty_string(self):
        stub = MagicMock()
        stub.TheGraphQuery.return_value = gateway_pb2.TheGraphQueryResponse(
            data=json.dumps({"pools": []}), success=True
        )
        client = _client_with_stub(stub)

        data = await client.query("SUBGRAPH_ID", "{ pools { id } }")

        assert stub.TheGraphQuery.call_args.args[0].variables == ""
        assert data == {"pools": []}

    @pytest.mark.asyncio
    async def test_graphql_errors_classified_by_shared_path(self):
        stub = MagicMock()
        stub.TheGraphQuery.return_value = gateway_pb2.TheGraphQueryResponse(
            errors=json.dumps([{"message": "Type `Pool` has no field `feeTier`"}]),
            success=False,
        )
        client = _client_with_stub(stub)

        with pytest.raises(SubgraphQueryError, match="has no field"):
            await client.query("SUBGRAPH_ID", _QUERY)

    @pytest.mark.asyncio
    async def test_success_false_without_errors_still_raises(self):
        stub = MagicMock()
        stub.TheGraphQuery.return_value = gateway_pb2.TheGraphQueryResponse(success=False)
        transport = _transport_with_stub(stub)

        payload = await transport.query_payload("SUBGRAPH_ID", _QUERY)

        assert payload is not None and payload["errors"]
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
    async def test_application_status_becomes_graphql_error_not_fallback(self):
        stub = MagicMock()
        stub.TheGraphQuery.side_effect = _FakeRpcError(
            grpc.StatusCode.INTERNAL, "Subgraph 'xyz' is not in allowlist"
        )
        client = _client_with_stub(stub)

        with pytest.raises(SubgraphQueryError, match="not in allowlist"):
            await client.query("SUBGRAPH_ID", _QUERY)
        assert client._gateway_transport is not None
        assert client._gateway_transport._dead is False


class TestStickyDeathFallback:
    @pytest.mark.asyncio
    async def test_rpc_failure_marks_dead_and_falls_back_to_http(self):
        stub = MagicMock()
        stub.TheGraphQuery.side_effect = ConnectionError("sidecar gone")
        client = _client_with_stub(stub)

        sentinel = RuntimeError("http path reached")

        async def _http_sentinel():
            raise sentinel

        client._get_session = _http_sentinel  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="http path reached"):
            await client._execute_query("SUBGRAPH_ID", _QUERY)

        assert client._gateway_transport is not None
        assert client._gateway_transport._dead is True
        assert stub.TheGraphQuery.call_count == 1

        with pytest.raises(RuntimeError, match="http path reached"):
            await client._execute_query("SUBGRAPH_ID", _QUERY)
        assert stub.TheGraphQuery.call_count == 1


class TestAutoDetection:
    def test_client_auto_detects_from_env(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_HOST", "127.0.0.1")
        assert gateway_backtest_configured() is True
        assert SubgraphClient()._gateway_transport is not None

        monkeypatch.delenv("ALMANAK_GATEWAY_HOST")
        assert SubgraphClient()._gateway_transport is None
        assert SubgraphClient(use_gateway=False)._gateway_transport is None

    def test_liquidity_provider_picks_up_gateway_client(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_GATEWAY_HOST", "127.0.0.1")
        provider = LiquidityDepthProvider()
        assert provider._client._gateway_transport is not None
