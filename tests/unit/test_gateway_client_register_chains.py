"""Tests for GatewayClient.register_chains() wallet-registry discovery.

Covers the RegisterChains RPC contract: per-chain wallet discovery, the
registered_chains / registered_with_wallet_registry bookkeeping, the
UNIMPLEMENTED legacy fallback, and loud propagation of every other RPC error.
"""

from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig


class _FakeRpcError(grpc.RpcError):
    """RpcError carrying a status code, like a real UnaryUnary failure."""

    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code
        super().__init__()

    def code(self) -> grpc.StatusCode:
        return self._code


def _make_client() -> GatewayClient:
    """Create a GatewayClient with a mock gateway-health stub (no connection)."""
    config = GatewayClientConfig(host="localhost", port=50051, timeout=1.0)
    client = GatewayClient(config)
    client._gateway_health_stub = MagicMock()
    return client


def _response(initialized_chains, chain_wallets):
    resp = MagicMock()
    resp.initialized_chains = initialized_chains
    resp.chain_wallets = chain_wallets
    return resp


class TestRegisterChainsNotConnected:
    def test_raises_runtime_error_when_stub_missing(self):
        client = GatewayClient(GatewayClientConfig(host="localhost", port=50051, timeout=1.0))

        with pytest.raises(RuntimeError, match="not connected"):
            client.register_chains(["arbitrum"])


class TestRegisterChainsSuccess:
    def test_returns_wallets_and_records_registry_state(self):
        client = _make_client()
        wallets = {
            "arbitrum": "0x" + "a" * 40,
            "base": "0x" + "b" * 40,
        }
        client._gateway_health_stub.RegisterChains.return_value = _response(
            ["arbitrum", "base"], wallets
        )

        result = client.register_chains(["arbitrum", "base"])

        assert result == wallets
        assert client.registered_chains == ["arbitrum", "base"]
        assert client.registered_with_wallet_registry is True
        # The RPC receives the requested chains and the configured timeout.
        call = client._gateway_health_stub.RegisterChains.call_args
        assert list(call.args[0].chains) == ["arbitrum", "base"]
        assert call.kwargs["timeout"] == 1.0

    def test_no_wallet_registry_returns_empty_dict(self):
        client = _make_client()
        client._gateway_health_stub.RegisterChains.return_value = _response(["arbitrum"], {})

        result = client.register_chains(["arbitrum"])

        assert result == {}
        assert client.registered_chains == ["arbitrum"]
        assert client.registered_with_wallet_registry is False

    def test_registered_chains_come_from_response_not_request(self):
        """The gateway may initialize a subset; trust its answer."""
        client = _make_client()
        client._gateway_health_stub.RegisterChains.return_value = _response(["arbitrum"], {})

        client.register_chains(["arbitrum", "base"])

        assert client.registered_chains == ["arbitrum"]


class TestRegisterChainsRpcErrors:
    def test_unimplemented_falls_back_to_legacy_flow(self):
        client = _make_client()
        client._gateway_health_stub.RegisterChains.side_effect = _FakeRpcError(
            grpc.StatusCode.UNIMPLEMENTED
        )

        result = client.register_chains(["arbitrum", "base"])

        # Legacy gateway: no wallet registry, requested chains recorded as-is.
        assert result == {}
        assert client.registered_chains == ["arbitrum", "base"]
        assert client.registered_with_wallet_registry is False

    @pytest.mark.parametrize(
        "code",
        [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL, grpc.StatusCode.UNAUTHENTICATED],
    )
    def test_other_rpc_errors_propagate(self, code):
        client = _make_client()
        client._gateway_health_stub.RegisterChains.side_effect = _FakeRpcError(code)

        with pytest.raises(grpc.RpcError):
            client.register_chains(["arbitrum"])

        # Failed registration must not record chains as registered.
        assert client.registered_chains is None
        assert client.registered_with_wallet_registry is False
