"""Tests for GatewayWeb3Provider and AsyncGatewayWeb3Provider.

Phase 0 of VIB-2986 — these providers are the compliant Web3 client that all
EVM connectors will migrate to. Before Phase 1/2 depends on them, prove:
    1. Sync provider handles happy-path and error responses correctly.
    2. Async provider (via run_in_executor) behaves the same.
    3. Both chains POA/non-POA route through the same code path unchanged.
"""

import asyncio
import json
from unittest.mock import MagicMock

import pytest
from web3 import AsyncWeb3, Web3

from almanak.framework.gateway_client import GatewayClient
from almanak.framework.web3.gateway_provider import (
    AsyncGatewayWeb3Provider,
    GatewayWeb3Provider,
    get_gateway_web3,
)
from almanak.gateway.proto import gateway_pb2


def _make_mock_client(response: gateway_pb2.RpcResponse) -> MagicMock:
    """Build a mocked GatewayClient whose rpc.Call returns `response`."""
    client = MagicMock(spec=GatewayClient)
    client.rpc = MagicMock()
    client.rpc.Call = MagicMock(return_value=response)
    return client


class TestGatewayWeb3Provider:
    """Sync provider tests."""

    def test_make_request_success_returns_result(self):
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps("0x1234"), id="1")
        client = _make_mock_client(response)

        provider = GatewayWeb3Provider(client, chain="arbitrum")
        result = provider.make_request("eth_blockNumber", [])  # type: ignore[arg-type]

        assert result["result"] == "0x1234"
        assert "error" not in result

    def test_make_request_forwards_chain_method_params(self):
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps(None), id="1")
        client = _make_mock_client(response)

        provider = GatewayWeb3Provider(client, chain="Arbitrum")  # verify lowercasing
        provider.make_request("eth_call", [{"to": "0xabc", "data": "0xdd"}, "latest"])  # type: ignore[arg-type]

        call_args = client.rpc.Call.call_args
        rpc_request = call_args.args[0]
        assert isinstance(rpc_request, gateway_pb2.RpcRequest)
        assert rpc_request.chain == "arbitrum"
        assert rpc_request.method == "eth_call"
        assert json.loads(rpc_request.params) == [{"to": "0xabc", "data": "0xdd"}, "latest"]

    def test_make_request_error_response_returned_to_web3(self):
        error_payload = json.dumps({"code": -32000, "message": "execution reverted"})
        response = gateway_pb2.RpcResponse(success=False, error=error_payload, id="1")
        client = _make_mock_client(response)

        provider = GatewayWeb3Provider(client, chain="arbitrum")
        result = provider.make_request("eth_call", [])  # type: ignore[arg-type]

        assert result["error"]["code"] == -32000
        assert result["error"]["message"] == "execution reverted"

    def test_make_request_gateway_exception_returns_internal_error(self):
        client = MagicMock(spec=GatewayClient)
        client.rpc = MagicMock()
        client.rpc.Call = MagicMock(side_effect=RuntimeError("gRPC unavailable"))

        provider = GatewayWeb3Provider(client, chain="arbitrum")
        result = provider.make_request("eth_blockNumber", [])  # type: ignore[arg-type]

        assert result["error"]["code"] == -32603
        assert "gRPC unavailable" in result["error"]["message"]

    def test_request_id_monotonic(self):
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps("0x1"), id="ignored")
        client = _make_mock_client(response)

        provider = GatewayWeb3Provider(client, chain="arbitrum")
        r1 = provider.make_request("eth_blockNumber", [])  # type: ignore[arg-type]
        r2 = provider.make_request("eth_blockNumber", [])  # type: ignore[arg-type]

        assert int(r2["id"]) == int(r1["id"]) + 1

    def test_web3_wrapping_works(self):
        """Sanity: plugging the provider into Web3 yields a usable client."""
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps("0x1"), id="1")
        client = _make_mock_client(response)

        w3 = Web3(GatewayWeb3Provider(client, chain="arbitrum"))
        # web3.py calls make_request under the hood — this exercises the plumbing.
        block_number = w3.eth.block_number
        assert block_number == 1

    def test_get_gateway_web3_convenience(self):
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps("0x1"), id="1")
        client = _make_mock_client(response)

        w3 = get_gateway_web3(client, "arbitrum")
        assert isinstance(w3, Web3)
        assert w3.eth.block_number == 1


class TestAsyncGatewayWeb3Provider:
    """Async provider tests."""

    def test_make_request_success_returns_result(self):
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps("0x2"), id="1")
        client = _make_mock_client(response)

        provider = AsyncGatewayWeb3Provider(client, chain="ethereum")
        result = asyncio.run(provider.make_request("eth_blockNumber", []))  # type: ignore[arg-type]

        assert result["result"] == "0x2"
        assert "error" not in result

    def test_make_request_error_response_returned_to_async_web3(self):
        error_payload = json.dumps({"code": -32000, "message": "nope"})
        response = gateway_pb2.RpcResponse(success=False, error=error_payload, id="1")
        client = _make_mock_client(response)

        provider = AsyncGatewayWeb3Provider(client, chain="ethereum")
        result = asyncio.run(provider.make_request("eth_call", []))  # type: ignore[arg-type]

        assert result["error"]["code"] == -32000

    def test_is_async_flag_set(self):
        """AsyncWeb3 uses is_async to dispatch; this must stay True."""
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps("0x1"), id="1")
        client = _make_mock_client(response)

        provider = AsyncGatewayWeb3Provider(client, chain="ethereum")
        assert provider.is_async is True

    def test_async_web3_wrapping_works(self):
        """Sanity: plugging the async provider into AsyncWeb3 yields a usable client."""
        response = gateway_pb2.RpcResponse(success=True, result=json.dumps("0x5"), id="1")
        client = _make_mock_client(response)

        async def _go():
            w3 = AsyncWeb3(AsyncGatewayWeb3Provider(client, chain="ethereum"))
            return await w3.eth.block_number

        block_number = asyncio.run(_go())
        assert block_number == 5

    def test_make_request_gateway_exception_returns_internal_error(self):
        client = MagicMock(spec=GatewayClient)
        client.rpc = MagicMock()
        client.rpc.Call = MagicMock(side_effect=RuntimeError("gRPC unavailable"))

        provider = AsyncGatewayWeb3Provider(client, chain="ethereum")
        result = asyncio.run(provider.make_request("eth_blockNumber", []))  # type: ignore[arg-type]

        assert result["error"]["code"] == -32603
        assert "gRPC unavailable" in result["error"]["message"]


class TestSolanaPassthroughSmoke:
    """Smoke test proving the sync provider works with Solana chain + method.

    The gateway RpcService validates method against a chain-aware allowlist
    (EVM vs Solana). This test ensures we don't accidentally gate Solana
    on the EVM allowlist at the provider level.
    """

    def test_solana_chain_routes_unchanged(self):
        response = gateway_pb2.RpcResponse(
            success=True,
            result=json.dumps({"value": 1_000_000}),
            id="1",
        )
        client = _make_mock_client(response)

        provider = GatewayWeb3Provider(client, chain="solana")
        result = provider.make_request("getBalance", ["7VHUFJHWu2CuExkJcJrzhQPJ2oygMTuL2p5rTa9YPt3E"])  # type: ignore[arg-type]

        assert result["result"] == {"value": 1_000_000}

        rpc_request = client.rpc.Call.call_args.args[0]
        assert rpc_request.chain == "solana"
        assert rpc_request.method == "getBalance"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
