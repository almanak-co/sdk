"""Unit tests for GatewayClient.eth_call() method."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import grpc

from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig


def _make_client() -> GatewayClient:
    """Create a GatewayClient with a mocked RPC stub."""
    config = GatewayClientConfig(host="localhost", port=50051, timeout=10.0)
    client = GatewayClient(config)
    client._rpc_stub = MagicMock()
    return client


class TestEthCallSuccess:
    """Tests for successful eth_call invocations."""

    def test_returns_parsed_json_result(self):
        """eth_call should return the JSON-parsed result on success."""
        client = _make_client()
        expected_hex = "0x000000000000000000000000abcdef1234567890abcdef1234567890abcdef12"

        response = MagicMock()
        response.success = True
        response.result = json.dumps(expected_hex)
        client._rpc_stub.Call.return_value = response

        result = client.eth_call(chain="base", to="0xfactory", data="0xcalldata")

        assert result == expected_hex
        client._rpc_stub.Call.assert_called_once()
        call_args = client._rpc_stub.Call.call_args
        assert call_args[1]["timeout"] == 10.0


class TestEthCallNotConnected:
    """Tests for eth_call when client is not connected."""

    def test_returns_none_when_stub_is_none(self):
        """eth_call should return None when _rpc_stub is None."""
        config = GatewayClientConfig(host="localhost", port=50051, timeout=10.0)
        client = GatewayClient(config)
        client._rpc_stub = None

        result = client.eth_call(chain="base", to="0xfactory", data="0xcalldata")

        assert result is None


class TestEthCallRpcFailure:
    """Tests for eth_call when the RPC returns an error."""

    def test_returns_none_on_rpc_failure(self):
        """eth_call should return None when response.success is False."""
        client = _make_client()

        response = MagicMock()
        response.success = False
        response.error = "execution reverted"
        client._rpc_stub.Call.return_value = response

        result = client.eth_call(chain="base", to="0xfactory", data="0xcalldata")

        assert result is None


class TestEthCallEmptyResult:
    """Tests for eth_call when RPC returns success but empty result."""

    def test_returns_none_on_empty_result(self):
        """eth_call should return None when response.result is empty."""
        client = _make_client()

        response = MagicMock()
        response.success = True
        response.result = ""
        client._rpc_stub.Call.return_value = response

        result = client.eth_call(chain="base", to="0xfactory", data="0xcalldata")

        assert result is None


class TestEthCallGrpcError:
    """Tests for eth_call when a gRPC error occurs."""

    def test_returns_none_on_grpc_error(self):
        """eth_call should return None when gRPC raises RpcError."""
        client = _make_client()
        client._rpc_stub.Call.side_effect = grpc.RpcError()

        result = client.eth_call(chain="base", to="0xfactory", data="0xcalldata")

        assert result is None


class TestBlockNumber:
    """VIB-3350: GatewayClient.block_number(chain) via the RpcService proxy."""

    def test_decodes_hex_head(self):
        """A 0x-prefixed hex quantity is decoded to an int block number."""
        client = _make_client()
        response = MagicMock()
        response.success = True
        response.result = json.dumps(hex(21_000_000))  # "0x1406f40"
        client._rpc_stub.Call.return_value = response

        assert client.block_number("base") == 21_000_000
        req = client._rpc_stub.Call.call_args[0][0]
        assert req.method == "eth_blockNumber"
        assert req.chain == "base"
        # default: no explicit timeout -> falls back to the client's configured timeout
        assert client._rpc_stub.Call.call_args[1]["timeout"] == 10.0

    def test_explicit_timeout_bounds_the_rpc_call(self):
        """VIB-3350 (CodeRabbit): the confirmation-wait poll passes its remaining
        budget so one stalled eth_blockNumber cannot outlive the caller deadline."""
        client = _make_client()
        response = MagicMock()
        response.success = True
        response.result = json.dumps(hex(21_000_000))
        client._rpc_stub.Call.return_value = response

        assert client.block_number("base", timeout=0.25) == 21_000_000
        assert client._rpc_stub.Call.call_args[1]["timeout"] == 0.25

    def test_accepts_plain_int_result(self):
        """A non-string numeric result is coerced to int (defensive)."""
        client = _make_client()
        response = MagicMock()
        response.success = True
        response.result = json.dumps(123)
        client._rpc_stub.Call.return_value = response

        assert client.block_number("base") == 123

    def test_returns_none_when_not_connected(self):
        config = GatewayClientConfig(host="localhost", port=50051, timeout=10.0)
        client = GatewayClient(config)
        client._rpc_stub = None
        assert client.block_number("base") is None

    def test_returns_none_on_rpc_failure(self):
        client = _make_client()
        response = MagicMock()
        response.success = False
        response.error = "boom"
        client._rpc_stub.Call.return_value = response
        assert client.block_number("base") is None

    def test_returns_none_on_empty_result(self):
        client = _make_client()
        response = MagicMock()
        response.success = True
        response.result = ""
        client._rpc_stub.Call.return_value = response
        assert client.block_number("base") is None

    def test_returns_none_on_grpc_error(self):
        client = _make_client()
        client._rpc_stub.Call.side_effect = grpc.RpcError()
        assert client.block_number("base") is None

    def test_returns_none_on_malformed_hex(self):
        """A non-numeric string result fails the int() decode -> None, no crash."""
        client = _make_client()
        response = MagicMock()
        response.success = True
        response.result = json.dumps("not-a-number")
        client._rpc_stub.Call.return_value = response
        assert client.block_number("base") is None
