"""Tests for GatewayClient.wait_for_ready() silent retry behavior."""

from unittest.mock import MagicMock, patch

import grpc
from grpc_health.v1 import health_pb2

from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig


def _make_client() -> GatewayClient:
    """Create a GatewayClient with a mock health stub (no real connection)."""
    config = GatewayClientConfig(host="localhost", port=50051, timeout=1.0)
    client = GatewayClient(config)
    client._health_stub = MagicMock()
    return client


def _serving_response():
    resp = MagicMock()
    resp.status = health_pb2.HealthCheckResponse.SERVING
    return resp


def _not_serving_response():
    resp = MagicMock()
    resp.status = health_pb2.HealthCheckResponse.NOT_SERVING
    return resp


class TestWaitForReadySuccessBeforeTimeout:
    def test_returns_true_on_first_attempt(self):
        client = _make_client()
        client._health_stub.Check.return_value = _serving_response()

        assert client.wait_for_ready(timeout=5.0, interval=0.01) is True
        assert client._health_stub.Check.call_count == 1

    def test_returns_true_after_retries(self):
        client = _make_client()
        client._health_stub.Check.side_effect = [
            grpc.RpcError(),
            _not_serving_response(),
            _serving_response(),
        ]

        with patch("almanak.framework.gateway_client.logger") as mock_logger:
            assert client.wait_for_ready(timeout=5.0, interval=0.01) is True
        assert client._health_stub.Check.call_count == 3
        # Silent retry contract: no warnings or errors during intermediate failures
        mock_logger.warning.assert_not_called()
        mock_logger.error.assert_not_called()


class TestWaitForReadyTimeout:
    def test_returns_false_and_logs_error_on_rpc_failure(self):
        client = _make_client()
        client._health_stub.Check.side_effect = grpc.RpcError()

        with patch("almanak.framework.gateway_client.logger") as mock_logger:
            result = client.wait_for_ready(timeout=0.05, interval=0.01)

        assert result is False
        mock_logger.error.assert_called_once()
        mock_logger.warning.assert_not_called()
        # Per-attempt timeout is capped to min(config.timeout, remaining)
        assert client._health_stub.Check.call_args_list[0].kwargs["timeout"] <= 0.05
        log_msg = mock_logger.error.call_args[0][0]
        assert "Gateway not ready" in log_msg

    def test_returns_false_and_logs_error_on_not_serving(self):
        client = _make_client()
        client._health_stub.Check.return_value = _not_serving_response()

        with patch("almanak.framework.gateway_client.logger") as mock_logger:
            result = client.wait_for_ready(timeout=0.05, interval=0.01)

        assert result is False
        mock_logger.error.assert_called_once()
        mock_logger.warning.assert_not_called()
        # Per-attempt timeout is capped to min(config.timeout, remaining)
        assert client._health_stub.Check.call_args_list[0].kwargs["timeout"] <= 0.05
        log_msg = mock_logger.error.call_args[0][0]
        assert "status=" in log_msg


class TestWaitForReadyDisconnected:
    def test_returns_false_when_not_connected(self):
        config = GatewayClientConfig(host="localhost", port=50051, timeout=1.0)
        client = GatewayClient(config)
        # _health_stub is None (never connected)

        with patch("almanak.framework.gateway_client.logger") as mock_logger:
            result = client.wait_for_ready(timeout=0.05, interval=0.01)

        assert result is False
        mock_logger.error.assert_called_once()
        mock_logger.warning.assert_not_called()
        log_msg = mock_logger.error.call_args[0][0]
        assert "Gateway not ready" in log_msg
        assert "not connected" in log_msg
