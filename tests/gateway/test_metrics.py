"""Tests for gateway metrics module."""

import socket
import time

import pytest
from prometheus_client import REGISTRY

from almanak.gateway.metrics import (
    ACTIVE_REQUESTS,
    ERROR_COUNT,
    GATEWAY_REGISTRY,
    INTEGRATION_LATENCY,
    INTEGRATION_REQUESTS,
    REQUEST_COUNT,
    REQUEST_LATENCY,
    RPC_LATENCY,
    RPC_REQUESTS,
    MetricsServer,
    _parse_method_name,
    record_integration_latency,
    record_integration_request,
    record_rpc_latency,
    record_rpc_request,
)


def get_free_port() -> int:
    """Obtain a dynamically allocated free port from the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class TestMetricsHelpers:
    """Tests for metrics helper functions."""

    def test_parse_method_name_full_path(self):
        """Parse full gRPC method name."""
        service, method = _parse_method_name("/almanak.gateway.MarketService/GetPrice")
        assert service == "MarketService"
        assert method == "GetPrice"

    def test_parse_method_name_simple(self):
        """Parse simple method name."""
        service, method = _parse_method_name("/Health/Check")
        assert service == "Health"
        assert method == "Check"

    def test_parse_method_name_malformed(self):
        """Handle malformed method names."""
        service, method = _parse_method_name("invalid")
        assert service == "unknown"
        assert method == "unknown"

    def test_parse_method_name_empty(self):
        """Handle empty method names."""
        service, method = _parse_method_name("")
        assert service == "unknown"
        assert method == "unknown"


class TestRpcMetrics:
    """Tests for RPC metrics recording."""

    def test_record_rpc_request(self):
        """Record RPC request increments counter."""
        # Get initial value
        initial = RPC_REQUESTS.labels(chain="ethereum", method="eth_call")._value.get()

        record_rpc_request("ethereum", "eth_call")

        # Verify increment
        new_value = RPC_REQUESTS.labels(chain="ethereum", method="eth_call")._value.get()
        assert new_value == initial + 1

    def test_record_rpc_latency(self):
        """Record RPC latency updates histogram."""
        # Record a latency
        record_rpc_latency("arbitrum", 0.05)

        # Verify histogram has samples (check sum increased)
        # Note: histograms track sum of all observations
        assert RPC_LATENCY.labels(chain="arbitrum")._sum.get() >= 0.05

    def test_record_rpc_request_multiple_chains(self):
        """Record requests for multiple chains."""
        record_rpc_request("base", "eth_getBalance")
        record_rpc_request("optimism", "eth_getBalance")

        # Both chains should have counts
        base_count = RPC_REQUESTS.labels(chain="base", method="eth_getBalance")._value.get()
        optimism_count = RPC_REQUESTS.labels(chain="optimism", method="eth_getBalance")._value.get()

        assert base_count >= 1
        assert optimism_count >= 1


class TestIntegrationMetrics:
    """Tests for integration metrics recording."""

    def test_record_integration_request(self):
        """Record integration request increments counter."""
        initial = INTEGRATION_REQUESTS.labels(integration="binance", endpoint="get_ticker")._value.get()

        record_integration_request("binance", "get_ticker")

        new_value = INTEGRATION_REQUESTS.labels(integration="binance", endpoint="get_ticker")._value.get()
        assert new_value == initial + 1

    def test_record_integration_latency(self):
        """Record integration latency updates histogram."""
        record_integration_latency("coingecko", "get_price", 0.25)

        # Verify histogram has the observation
        assert INTEGRATION_LATENCY.labels(integration="coingecko", endpoint="get_price")._sum.get() >= 0.25

    def test_record_integration_multiple_integrations(self):
        """Record requests for multiple integrations."""
        record_integration_request("binance", "get_klines")
        record_integration_request("coingecko", "get_markets")
        record_integration_request("thegraph", "query")

        # All integrations should have counts
        assert INTEGRATION_REQUESTS.labels(integration="binance", endpoint="get_klines")._value.get() >= 1
        assert INTEGRATION_REQUESTS.labels(integration="coingecko", endpoint="get_markets")._value.get() >= 1
        assert INTEGRATION_REQUESTS.labels(integration="thegraph", endpoint="query")._value.get() >= 1


class TestGatewayMetrics:
    """Tests for gateway-level metrics."""

    def test_request_count_labels(self):
        """Request count has correct labels."""
        # This verifies the metric was created with expected labels
        REQUEST_COUNT.labels(service="TestService", method="TestMethod", status="ok").inc()
        assert REQUEST_COUNT.labels(service="TestService", method="TestMethod", status="ok")._value.get() >= 1

    def test_request_latency_buckets(self):
        """Request latency histogram has reasonable buckets."""
        # Observe values at different bucket boundaries
        REQUEST_LATENCY.labels(service="BucketTest", method="Test").observe(0.001)
        REQUEST_LATENCY.labels(service="BucketTest", method="Test").observe(0.01)
        REQUEST_LATENCY.labels(service="BucketTest", method="Test").observe(0.1)
        REQUEST_LATENCY.labels(service="BucketTest", method="Test").observe(1.0)

        # Verify observations were recorded by checking the sum
        total_sum = REQUEST_LATENCY.labels(service="BucketTest", method="Test")._sum.get()
        assert total_sum >= 1.111  # 0.001 + 0.01 + 0.1 + 1.0

    def test_active_requests_gauge(self):
        """Active requests gauge can increase and decrease."""
        ACTIVE_REQUESTS.labels(service="GaugeTest").set(0)

        ACTIVE_REQUESTS.labels(service="GaugeTest").inc()
        assert ACTIVE_REQUESTS.labels(service="GaugeTest")._value.get() == 1

        ACTIVE_REQUESTS.labels(service="GaugeTest").inc()
        assert ACTIVE_REQUESTS.labels(service="GaugeTest")._value.get() == 2

        ACTIVE_REQUESTS.labels(service="GaugeTest").dec()
        assert ACTIVE_REQUESTS.labels(service="GaugeTest")._value.get() == 1

    def test_error_count_labels(self):
        """Error count has correct labels."""
        ERROR_COUNT.labels(service="ErrorTest", method="Test", error_type="ValidationError").inc()
        assert ERROR_COUNT.labels(service="ErrorTest", method="Test", error_type="ValidationError")._value.get() >= 1


class TestMetricsServer:
    """Tests for metrics HTTP server."""

    def test_server_init(self):
        """Metrics server initializes with port."""
        server = MetricsServer(port=9091)
        assert server.port == 9091
        assert server._server is None
        assert server._thread is None

    def test_server_start_stop(self):
        """Metrics server starts and stops cleanly."""
        port = get_free_port()
        server = MetricsServer(port=port)
        server.start()

        # Server should be running
        assert server._server is not None
        assert server._thread is not None
        assert server._thread.is_alive()

        server.stop()

        # Give thread time to stop
        time.sleep(0.1)
        # Thread may still be alive briefly, but server should be shutdown

    def test_port_conflict_falls_back_to_ephemeral(self):
        """When the configured port is busy, server binds to an ephemeral port."""
        # Occupy a port
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("", 0))
        busy_port = blocker.getsockname()[1]
        blocker.listen(1)

        try:
            server = MetricsServer(port=busy_port)
            server.start()
            try:
                # Server should have picked a different port
                assert server.port != busy_port
                assert server.port > 0
                assert server._thread.is_alive()
            finally:
                server.stop()
        finally:
            blocker.close()


class TestMetricsRegistry:
    """Tests for custom metrics registry."""

    def test_gateway_registry_isolation(self):
        """Gateway metrics use separate registry."""
        # GATEWAY_REGISTRY should be separate from default REGISTRY
        assert GATEWAY_REGISTRY is not REGISTRY

    def test_metrics_registered(self):
        """All expected metrics are registered."""
        # Check that our metrics are in the gateway registry by collecting all metric names
        metric_names = set()
        for metric in GATEWAY_REGISTRY.collect():
            metric_names.add(metric.name)

        # Core metrics
        assert "gateway_requests" in metric_names or "gateway_requests_total" in metric_names
        assert "gateway_request_latency_seconds" in metric_names
        assert "gateway_errors" in metric_names or "gateway_errors_total" in metric_names
        assert "gateway_active_requests" in metric_names

        # Integration metrics
        assert "gateway_integration_requests" in metric_names or "gateway_integration_requests_total" in metric_names
        assert "gateway_integration_latency_seconds" in metric_names

        # RPC metrics
        assert "gateway_rpc_requests" in metric_names or "gateway_rpc_requests_total" in metric_names
        assert "gateway_rpc_latency_seconds" in metric_names
