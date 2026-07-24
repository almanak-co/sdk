"""Tests for gateway metrics module."""

import asyncio
import socket
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
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
    MetricsInterceptor,
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


class TestInterceptorWrapperErrorPaths:
    """VIB-3293: verify interceptor wrappers never raise UnboundLocalError on
    early-raise paths (especially asyncio.CancelledError, which is a
    BaseException subclass under Python 3.8+ and bypasses `except Exception`).

    The bug was observed in production when an upstream RPC DEADLINE_EXCEEDED
    timeout cancelled the gRPC handler mid-await, leaving `status` unbound
    when the `finally` block emitted metrics.
    """

    @pytest.fixture
    def interceptor(self) -> MetricsInterceptor:
        return MetricsInterceptor()

    def _read_status_count(self, service: str, method: str, status: str) -> float:
        return REQUEST_COUNT.labels(service=service, method=method, status=status)._value.get()

    def test_unary_unary_cancelled_error_labels_as_error(self, interceptor):
        """CancelledError bypasses `except Exception` — finally must still emit status='error'."""

        async def behavior(request, context):
            raise asyncio.CancelledError()

        wrapped = interceptor._wrap_unary_unary(behavior, "RpcService", "Call")

        service, method = "RpcService", "Call"
        before_error = self._read_status_count(service, method, "error")
        before_ok = self._read_status_count(service, method, "ok")

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(wrapped(object(), object()))

        after_error = self._read_status_count(service, method, "error")
        after_ok = self._read_status_count(service, method, "ok")

        # Must be labelled as error, NOT silently mislabelled ok.
        assert after_error == before_error + 1
        assert after_ok == before_ok

    def test_unary_unary_exception_labels_as_error(self, interceptor):
        """Normal exception goes through `except Exception` and still labels as error."""

        async def behavior(request, context):
            raise RuntimeError("boom")

        wrapped = interceptor._wrap_unary_unary(behavior, "RpcService", "Call")

        service, method = "RpcService", "Call"
        before = self._read_status_count(service, method, "error")

        with pytest.raises(RuntimeError):
            asyncio.run(wrapped(object(), object()))

        assert self._read_status_count(service, method, "error") == before + 1

    def test_unary_unary_success_labels_as_ok(self, interceptor):
        """Happy path still labels as ok (did not regress the default)."""

        async def behavior(request, context):
            return "result"

        wrapped = interceptor._wrap_unary_unary(behavior, "RpcService", "Call")

        service, method = "RpcService", "Call"
        before_ok = self._read_status_count(service, method, "ok")
        before_err = self._read_status_count(service, method, "error")

        result = asyncio.run(wrapped(object(), object()))
        assert result == "result"

        assert self._read_status_count(service, method, "ok") == before_ok + 1
        assert self._read_status_count(service, method, "error") == before_err

    def test_stream_unary_cancelled_error_labels_as_error(self, interceptor):
        """Same guarantee for stream-unary path (e.g. ExecutionService.Execute
        style handlers that consume a request stream)."""

        async def behavior(request_iterator, context):
            raise asyncio.CancelledError()

        wrapped = interceptor._wrap_stream_unary(behavior, "ExecutionService", "Execute")

        service, method = "ExecutionService", "Execute"
        before_error = self._read_status_count(service, method, "error")
        before_ok = self._read_status_count(service, method, "ok")

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(wrapped(object(), object()))

        assert self._read_status_count(service, method, "error") == before_error + 1
        assert self._read_status_count(service, method, "ok") == before_ok

    def test_stream_unary_base_exception_labels_as_error(self, interceptor):
        """A non-Exception BaseException (e.g. SystemExit from an upstream
        watchdog) must still emit an 'error' metric, not crash the `finally`."""

        class _FakeBaseException(BaseException):
            pass

        async def behavior(request_iterator, context):
            raise _FakeBaseException("not an Exception subclass")

        wrapped = interceptor._wrap_stream_unary(behavior, "ExecutionService", "Execute")

        service, method = "ExecutionService", "Execute"
        before_error = self._read_status_count(service, method, "error")

        with pytest.raises(_FakeBaseException):
            asyncio.run(wrapped(object(), object()))

        assert self._read_status_count(service, method, "error") == before_error + 1

    def test_unary_stream_cancelled_mid_stream_labels_as_error(self, interceptor):
        """Cancellation after a few yields must NOT be silently labelled ok."""

        async def behavior(request, context):
            yield 1
            yield 2
            raise asyncio.CancelledError()

        wrapped = interceptor._wrap_unary_stream(behavior, "MarketService", "StreamPrices")

        service, method = "MarketService", "StreamPrices"
        before_error = self._read_status_count(service, method, "error")
        before_ok = self._read_status_count(service, method, "ok")

        async def _drain():
            collected = []
            async for item in wrapped(object(), object()):
                collected.append(item)
            return collected

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(_drain())

        assert self._read_status_count(service, method, "error") == before_error + 1
        assert self._read_status_count(service, method, "ok") == before_ok

    def test_unary_stream_clean_completion_labels_as_ok(self, interceptor):
        """Stream that completes cleanly still labels as ok."""

        async def behavior(request, context):
            yield 1
            yield 2

        wrapped = interceptor._wrap_unary_stream(behavior, "MarketService", "StreamPrices")

        service, method = "MarketService", "StreamPrices"
        before_ok = self._read_status_count(service, method, "ok")

        async def _drain():
            collected = []
            async for item in wrapped(object(), object()):
                collected.append(item)
            return collected

        items = asyncio.run(_drain())
        assert items == [1, 2]
        assert self._read_status_count(service, method, "ok") == before_ok + 1

    def test_stream_stream_cancelled_mid_stream_labels_as_error(self, interceptor):
        """Bidirectional streams must label mid-stream cancellation as error."""

        async def behavior(request_iterator, context):
            yield 1
            raise asyncio.CancelledError()

        wrapped = interceptor._wrap_stream_stream(behavior, "ExecutionService", "Stream")

        service, method = "ExecutionService", "Stream"
        before_error = self._read_status_count(service, method, "error")
        before_ok = self._read_status_count(service, method, "ok")

        async def _drain():
            items = []
            async for item in wrapped(object(), object()):
                items.append(item)
            return items

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(_drain())

        assert self._read_status_count(service, method, "error") == before_error + 1
        assert self._read_status_count(service, method, "ok") == before_ok

    def test_stream_stream_base_exception_labels_as_error(self, interceptor):
        """Bidirectional streams must emit error metrics for BaseException paths."""

        class _FakeBaseException(BaseException):
            pass

        async def behavior(request_iterator, context):
            raise _FakeBaseException("not an Exception subclass")
            yield

        wrapped = interceptor._wrap_stream_stream(behavior, "ExecutionService", "Stream")

        service, method = "ExecutionService", "Stream"
        before_error = self._read_status_count(service, method, "error")
        before_ok = self._read_status_count(service, method, "ok")

        async def _drain():
            items = []
            async for item in wrapped(object(), object()):
                items.append(item)
            return items

        with pytest.raises(_FakeBaseException):
            asyncio.run(_drain())

        assert self._read_status_count(service, method, "error") == before_error + 1
        assert self._read_status_count(service, method, "ok") == before_ok

    def test_stream_stream_clean_completion_labels_as_ok(self, interceptor):
        """Bidirectional streams that complete cleanly still label as ok."""

        async def behavior(request_iterator, context):
            yield 1
            yield 2

        wrapped = interceptor._wrap_stream_stream(behavior, "ExecutionService", "Stream")

        service, method = "ExecutionService", "Stream"
        before_ok = self._read_status_count(service, method, "ok")
        before_error = self._read_status_count(service, method, "error")

        async def _drain():
            items = []
            async for item in wrapped(object(), object()):
                items.append(item)
            return items

        items = asyncio.run(_drain())
        assert items == [1, 2]
        assert self._read_status_count(service, method, "ok") == before_ok + 1
        assert self._read_status_count(service, method, "error") == before_error


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


class TestInterceptService:
    """Branch coverage for MetricsInterceptor.intercept_service handler dispatch."""

    METHOD = "/almanak.gateway.MarketService/GetPrice"

    @pytest.fixture
    def interceptor(self) -> MetricsInterceptor:
        return MetricsInterceptor()

    def _details(self) -> SimpleNamespace:
        return SimpleNamespace(method=self.METHOD)

    @staticmethod
    def _continuation(handler):
        return AsyncMock(return_value=handler)

    @pytest.mark.asyncio
    async def test_none_handler_passes_through(self, interceptor):
        result = await interceptor.intercept_service(self._continuation(None), self._details())
        assert result is None

    @pytest.mark.asyncio
    async def test_unary_unary_handler_rewrapped_and_still_calls_behavior(self, interceptor):
        calls = []

        async def behavior(request, context):
            calls.append(request)
            return "resp"

        original = grpc.unary_unary_rpc_method_handler(
            behavior,
            request_deserializer="req-deser",
            response_serializer="resp-ser",
        )

        result = await interceptor.intercept_service(self._continuation(original), self._details())

        assert result is not original
        assert result.unary_unary is not behavior
        # Serialization plumbing is preserved from the original handler.
        assert result.request_deserializer == "req-deser"
        assert result.response_serializer == "resp-ser"
        # The wrapped behavior still reaches the original handler.
        response = await result.unary_unary("request-1", MagicMock())
        assert response == "resp"
        assert calls == ["request-1"]

    @pytest.mark.asyncio
    async def test_unary_stream_handler_rewrapped_and_streams(self, interceptor):
        async def behavior(request, context):
            yield "a"
            yield "b"

        original = grpc.unary_stream_rpc_method_handler(
            behavior,
            request_deserializer="req-deser",
            response_serializer="resp-ser",
        )

        result = await interceptor.intercept_service(self._continuation(original), self._details())

        assert result is not original
        assert result.unary_stream is not behavior
        assert result.request_deserializer == "req-deser"
        items = [item async for item in result.unary_stream("req", MagicMock())]
        assert items == ["a", "b"]

    @pytest.mark.asyncio
    async def test_stream_unary_handler_rewrapped_and_consumes_iterator(self, interceptor):
        async def behavior(request_iterator, context):
            return [item async for item in request_iterator]

        original = grpc.stream_unary_rpc_method_handler(
            behavior,
            request_deserializer="req-deser",
            response_serializer="resp-ser",
        )

        result = await interceptor.intercept_service(self._continuation(original), self._details())

        assert result is not original
        assert result.stream_unary is not behavior
        assert result.response_serializer == "resp-ser"

        async def request_iterator():
            yield "x"
            yield "y"

        response = await result.stream_unary(request_iterator(), MagicMock())
        assert response == ["x", "y"]

    @pytest.mark.asyncio
    async def test_stream_stream_handler_rewrapped_and_streams(self, interceptor):
        async def behavior(request_iterator, context):
            async for item in request_iterator:
                yield item.upper()

        original = grpc.stream_stream_rpc_method_handler(
            behavior,
            request_deserializer="req-deser",
            response_serializer="resp-ser",
        )

        result = await interceptor.intercept_service(self._continuation(original), self._details())

        assert result is not original
        assert result.stream_stream is not behavior

        async def request_iterator():
            yield "a"
            yield "b"

        items = [item async for item in result.stream_stream(request_iterator(), MagicMock())]
        assert items == ["A", "B"]

    @pytest.mark.asyncio
    async def test_handler_with_no_recognized_kind_passes_through(self, interceptor):
        bare = SimpleNamespace(
            unary_unary=None,
            unary_stream=None,
            stream_unary=None,
            stream_stream=None,
        )

        result = await interceptor.intercept_service(self._continuation(bare), self._details())

        assert result is bare

    @pytest.mark.asyncio
    async def test_wrapped_unary_unary_records_request_metrics(self, interceptor):
        async def behavior(request, context):
            return "resp"

        original = grpc.unary_unary_rpc_method_handler(behavior)
        before = (
            REQUEST_COUNT.labels(service="MarketService", method="GetPrice", status="ok")._value.get()
        )

        result = await interceptor.intercept_service(self._continuation(original), self._details())
        await result.unary_unary("req", MagicMock())

        after = (
            REQUEST_COUNT.labels(service="MarketService", method="GetPrice", status="ok")._value.get()
        )
        assert after == before + 1
