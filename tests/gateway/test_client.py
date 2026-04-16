"""Tests for gateway client."""

from unittest.mock import MagicMock, patch

import grpc
import pytest
from grpc_health.v1 import health_pb2

from almanak.framework.gateway_client import (
    GatewayClient,
    GatewayClientConfig,
    get_gateway_client,
    reset_gateway_client,
)


class TestGatewayClientConfig:
    """Tests for GatewayClientConfig."""

    def test_default_config(self):
        """Config has sensible defaults."""
        config = GatewayClientConfig()

        assert config.host == "localhost"
        assert config.port == 50051
        assert config.timeout == 30.0

    def test_config_from_env(self, monkeypatch):
        """Config loads from legacy GATEWAY_* environment variables."""
        # Must unset ALMANAK_GATEWAY_* to test legacy fallback behavior
        monkeypatch.delenv("ALMANAK_GATEWAY_HOST", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_PORT", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_TIMEOUT", raising=False)
        monkeypatch.setenv("GATEWAY_HOST", "gateway.local")
        monkeypatch.setenv("GATEWAY_PORT", "50052")
        monkeypatch.setenv("GATEWAY_TIMEOUT", "60.0")

        config = GatewayClientConfig.from_env()

        assert config.host == "gateway.local"
        assert config.port == 50052
        assert config.timeout == 60.0

    def test_config_prefers_almanak_host(self, monkeypatch):
        """Config prefers ALMANAK_GATEWAY_HOST over GATEWAY_HOST."""
        # Clean all related env vars to ensure deterministic test
        monkeypatch.delenv("ALMANAK_GATEWAY_HOST", raising=False)
        monkeypatch.delenv("GATEWAY_HOST", raising=False)
        monkeypatch.setenv("ALMANAK_GATEWAY_HOST", "almanak.local")
        monkeypatch.setenv("GATEWAY_HOST", "gateway.local")

        config = GatewayClientConfig.from_env()

        assert config.host == "almanak.local"

    def test_config_prefers_almanak_port(self, monkeypatch):
        """Config prefers ALMANAK_GATEWAY_PORT over GATEWAY_PORT."""
        # Clean all related env vars to ensure deterministic test
        monkeypatch.delenv("ALMANAK_GATEWAY_PORT", raising=False)
        monkeypatch.delenv("GATEWAY_PORT", raising=False)
        monkeypatch.setenv("ALMANAK_GATEWAY_PORT", "60051")
        monkeypatch.setenv("GATEWAY_PORT", "50052")

        config = GatewayClientConfig.from_env()

        assert config.port == 60051

    def test_config_prefers_almanak_timeout(self, monkeypatch):
        """Config prefers ALMANAK_GATEWAY_TIMEOUT over GATEWAY_TIMEOUT."""
        # Clean all related env vars to ensure deterministic test
        monkeypatch.delenv("ALMANAK_GATEWAY_TIMEOUT", raising=False)
        monkeypatch.delenv("GATEWAY_TIMEOUT", raising=False)
        monkeypatch.setenv("ALMANAK_GATEWAY_TIMEOUT", "120.0")
        monkeypatch.setenv("GATEWAY_TIMEOUT", "60.0")

        config = GatewayClientConfig.from_env()

        assert config.timeout == 120.0

    def test_config_auth_token_default_none(self):
        """Config auth_token defaults to None."""
        config = GatewayClientConfig()
        assert config.auth_token is None

    def test_config_auth_token_from_almanak_env(self, monkeypatch):
        """Config loads auth_token from ALMANAK_GATEWAY_AUTH_TOKEN (preferred)."""
        monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "my-almanak-token")

        config = GatewayClientConfig.from_env()

        assert config.auth_token == "my-almanak-token"

    def test_config_auth_token_from_legacy_env(self, monkeypatch):
        """Config loads auth_token from GATEWAY_AUTH_TOKEN (legacy fallback)."""
        # Must unset ALMANAK_GATEWAY_AUTH_TOKEN to test fallback behavior
        monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "my-secret-token")

        config = GatewayClientConfig.from_env()

        assert config.auth_token == "my-secret-token"

    def test_config_auth_token_prefers_almanak_over_legacy(self, monkeypatch):
        """Config prefers ALMANAK_GATEWAY_AUTH_TOKEN over GATEWAY_AUTH_TOKEN."""
        # Clean all related env vars to ensure deterministic test
        monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
        monkeypatch.delenv("GATEWAY_AUTH_TOKEN", raising=False)
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "almanak-token")
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "legacy-token")

        config = GatewayClientConfig.from_env()

        assert config.auth_token == "almanak-token"

    def test_config_auth_token_custom(self):
        """Config accepts custom auth_token."""
        config = GatewayClientConfig(auth_token="custom-token")

        assert config.auth_token == "custom-token"

    def test_config_custom_values(self):
        """Config accepts custom values."""
        config = GatewayClientConfig(
            host="custom-host",
            port=9999,
            timeout=5.0,
        )

        assert config.host == "custom-host"
        assert config.port == 9999
        assert config.timeout == 5.0


class TestGatewayClient:
    """Tests for GatewayClient."""

    def test_client_target(self):
        """Client target is correctly formatted."""
        config = GatewayClientConfig(host="example.com", port=12345)
        client = GatewayClient(config)

        assert client.target == "example.com:12345"

    def test_client_not_connected_by_default(self):
        """Client is not connected after initialization."""
        client = GatewayClient()

        assert not client.is_connected
        assert client._channel is None
        assert client._health_stub is None

    def test_client_connect(self):
        """Client establishes connection."""
        client = GatewayClient(GatewayClientConfig(host="localhost", port=50052))

        with patch("grpc.insecure_channel") as mock_channel:
            mock_channel.return_value = MagicMock()
            client.connect()

            mock_channel.assert_called_once_with("localhost:50052")
            assert client.is_connected
            assert client._channel is not None

    def test_client_connect_with_auth_token(self):
        """Client wraps channel with auth interceptor when token is configured."""
        config = GatewayClientConfig(host="localhost", port=50052, auth_token="test-token")
        client = GatewayClient(config)

        with (
            patch("grpc.insecure_channel") as mock_channel,
            patch("grpc.intercept_channel") as mock_intercept,
        ):
            mock_base_channel = MagicMock()
            mock_channel.return_value = mock_base_channel
            mock_intercept.return_value = MagicMock()

            client.connect()

            # Base channel should be created
            mock_channel.assert_called_once_with("localhost:50052")
            # Channel should be wrapped with interceptor
            mock_intercept.assert_called_once()
            # First arg should be the base channel
            assert mock_intercept.call_args[0][0] is mock_base_channel
            assert client.is_connected

    def test_client_connect_without_auth_token(self):
        """Client wraps channel with cycle_id interceptor even without auth."""
        config = GatewayClientConfig(host="localhost", port=50052, auth_token=None)
        client = GatewayClient(config)

        with (
            patch("grpc.insecure_channel") as mock_channel,
            patch("grpc.intercept_channel") as mock_intercept,
        ):
            mock_base_channel = MagicMock()
            mock_channel.return_value = mock_base_channel
            mock_intercept.return_value = mock_base_channel

            client.connect()

            # Base channel should be created
            mock_channel.assert_called_once_with("localhost:50052")
            # intercept_channel is called (cycle_id interceptor always active)
            mock_intercept.assert_called_once()
            # Only cycle_id interceptor, no auth interceptor
            args = mock_intercept.call_args
            interceptors = args[0][1:]  # Skip base_channel arg
            assert len(interceptors) == 1

    def test_client_disconnect(self):
        """Client closes connection."""
        client = GatewayClient()

        with patch("grpc.insecure_channel") as mock_channel:
            mock_chan = MagicMock()
            mock_channel.return_value = mock_chan

            client.connect()
            assert client.is_connected

            client.disconnect()
            mock_chan.close.assert_called_once()
            assert not client.is_connected
            assert client._channel is None

    def test_client_context_manager(self):
        """Client works as context manager."""
        with patch("grpc.insecure_channel") as mock_channel:
            mock_chan = MagicMock()
            mock_channel.return_value = mock_chan

            with GatewayClient() as client:
                assert client.is_connected

            mock_chan.close.assert_called_once()

    def test_health_check_when_disconnected(self):
        """Health check returns False when not connected."""
        client = GatewayClient()

        assert client.health_check() is False

    def test_health_check_success(self):
        """Health check returns True when gateway is serving."""
        client = GatewayClient()

        with patch("grpc.insecure_channel"):
            client.connect()

            # Mock the health stub
            mock_response = MagicMock()
            mock_response.status = health_pb2.HealthCheckResponse.SERVING
            client._health_stub = MagicMock()
            client._health_stub.Check.return_value = mock_response

            assert client.health_check() is True
            client._health_stub.Check.assert_called_once()

    def test_health_check_failure(self):
        """Health check returns False when gateway is not serving."""
        client = GatewayClient()

        with patch("grpc.insecure_channel"):
            client.connect()

            mock_response = MagicMock()
            mock_response.status = health_pb2.HealthCheckResponse.NOT_SERVING
            client._health_stub = MagicMock()
            client._health_stub.Check.return_value = mock_response

            assert client.health_check() is False

    def test_health_check_rpc_error(self):
        """Health check returns False on RPC error."""
        client = GatewayClient()

        with patch("grpc.insecure_channel"):
            client.connect()

            client._health_stub = MagicMock()
            client._health_stub.Check.side_effect = grpc.RpcError()

            assert client.health_check() is False


class TestGatewayClientSingleton:
    """Tests for gateway client singleton functions."""

    def teardown_method(self):
        """Reset singleton after each test."""
        reset_gateway_client()

    def test_get_gateway_client_returns_same_instance(self):
        """get_gateway_client returns the same instance."""
        client1 = get_gateway_client()
        client2 = get_gateway_client()

        assert client1 is client2

    def test_reset_gateway_client(self):
        """reset_gateway_client clears the singleton."""
        client1 = get_gateway_client()
        reset_gateway_client()
        client2 = get_gateway_client()

        assert client1 is not client2
