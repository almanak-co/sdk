"""Unit tests for GatewayWeb3Provider and AsyncGatewayWeb3Provider.

Focus: grpc.RpcError propagates (not converted to JSON-RPC error), while
non-gRPC exceptions are returned as JSON-RPC error payloads.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.framework.web3.gateway_provider import (
    AsyncGatewayWeb3Provider,
    GatewayWeb3Provider,
)


def _make_sync_provider() -> GatewayWeb3Provider:
    client = MagicMock()
    provider = GatewayWeb3Provider.__new__(GatewayWeb3Provider)
    provider._gateway_client = client
    provider._chain = "arbitrum"
    provider._request_timeout = 10.0
    provider._request_counter = 0
    return provider


def _make_async_provider() -> AsyncGatewayWeb3Provider:
    client = MagicMock()
    provider = AsyncGatewayWeb3Provider.__new__(AsyncGatewayWeb3Provider)
    provider._gateway_client = client
    provider._chain = "arbitrum"
    provider._request_timeout = 10.0
    provider._request_counter = 0
    return provider


class _FakeRpcError(grpc.RpcError):
    pass


# ---------------------------------------------------------------------------
# GatewayWeb3Provider (sync)
# ---------------------------------------------------------------------------


def test_sync_make_request_propagates_grpc_rpc_error() -> None:
    provider = _make_sync_provider()
    provider._gateway_client.rpc.Call.side_effect = _FakeRpcError("auth failed")

    with pytest.raises(_FakeRpcError):
        provider.make_request("eth_blockNumber", [])


def test_sync_make_request_converts_non_grpc_exception_to_error_payload() -> None:
    provider = _make_sync_provider()
    provider._gateway_client.rpc.Call.side_effect = ValueError("boom")

    response = provider.make_request("eth_blockNumber", [])

    assert "error" in response
    assert response["error"]["code"] == -32603
    assert "boom" in response["error"]["message"]


def test_sync_make_request_returns_result_on_success() -> None:
    provider = _make_sync_provider()
    provider._gateway_client.rpc.Call.return_value = SimpleNamespace(
        success=True, result='{"number": "0x1"}', error=""
    )

    response = provider.make_request("eth_blockNumber", [])

    assert "result" in response
    assert response["result"] == {"number": "0x1"}


# ---------------------------------------------------------------------------
# AsyncGatewayWeb3Provider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_make_request_propagates_grpc_rpc_error() -> None:
    provider = _make_async_provider()

    # asyncio is imported locally inside make_request, so patch via the asyncio module
    with patch("asyncio.to_thread", new=AsyncMock(side_effect=_FakeRpcError("auth failed"))):
        with pytest.raises(_FakeRpcError):
            await provider.make_request("eth_blockNumber", [])


@pytest.mark.asyncio
async def test_async_make_request_converts_non_grpc_exception_to_error_payload() -> None:
    provider = _make_async_provider()

    with patch("asyncio.to_thread", new=AsyncMock(side_effect=RuntimeError("network failure"))):
        response = await provider.make_request("eth_blockNumber", [])

    assert "error" in response
    assert response["error"]["code"] == -32603
    assert "network failure" in response["error"]["message"]


@pytest.mark.asyncio
async def test_async_make_request_returns_result_on_success() -> None:
    provider = _make_async_provider()

    with patch(
        "asyncio.to_thread",
        new=AsyncMock(
            return_value=SimpleNamespace(success=True, result='{"number": "0x2"}', error="")
        ),
    ):
        response = await provider.make_request("eth_blockNumber", [])

    assert "result" in response
    assert response["result"] == {"number": "0x2"}
