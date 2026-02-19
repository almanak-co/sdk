"""Gateway metrics for Prometheus monitoring.

This module provides metrics collection for the gateway server including:
- Request counts by service/method
- Request latency histograms
- Error rates by type
- Active connections gauge

Metrics are exposed via HTTP endpoint for Prometheus scraping.
"""

import logging
import time
from collections.abc import Awaitable, Callable
from http.server import HTTPServer, SimpleHTTPRequestHandler
from threading import Thread
from typing import Any

import grpc
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

logger = logging.getLogger(__name__)

# Create a custom registry to avoid conflicts with other prometheus usage
GATEWAY_REGISTRY = CollectorRegistry()

# Request counter - tracks total requests by service and method
REQUEST_COUNT = Counter(
    "gateway_requests_total",
    "Total number of gRPC requests",
    ["service", "method", "status"],
    registry=GATEWAY_REGISTRY,
)

# Request latency histogram - tracks request duration
REQUEST_LATENCY = Histogram(
    "gateway_request_latency_seconds",
    "Request latency in seconds",
    ["service", "method"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=GATEWAY_REGISTRY,
)

# Error counter - tracks errors by service and error type
ERROR_COUNT = Counter(
    "gateway_errors_total",
    "Total number of errors",
    ["service", "method", "error_type"],
    registry=GATEWAY_REGISTRY,
)

# Active connections gauge - tracks concurrent active requests
ACTIVE_REQUESTS = Gauge(
    "gateway_active_requests",
    "Number of currently active requests",
    ["service"],
    registry=GATEWAY_REGISTRY,
)

# Integration-specific metrics
INTEGRATION_REQUESTS = Counter(
    "gateway_integration_requests_total",
    "Total integration API requests",
    ["integration", "endpoint"],
    registry=GATEWAY_REGISTRY,
)

INTEGRATION_LATENCY = Histogram(
    "gateway_integration_latency_seconds",
    "Integration API request latency",
    ["integration", "endpoint"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=GATEWAY_REGISTRY,
)

# RPC proxy metrics
RPC_REQUESTS = Counter(
    "gateway_rpc_requests_total",
    "Total RPC proxy requests",
    ["chain", "method"],
    registry=GATEWAY_REGISTRY,
)

RPC_LATENCY = Histogram(
    "gateway_rpc_latency_seconds",
    "RPC proxy request latency",
    ["chain"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=GATEWAY_REGISTRY,
)

# Token resolution metrics
TOKEN_RESOLUTION_CACHE_HIT = Counter(
    "token_resolution_cache_hit_total",
    "Total token resolution cache hits",
    ["chain", "cache_type"],
    registry=GATEWAY_REGISTRY,
)

TOKEN_RESOLUTION_CACHE_MISS = Counter(
    "token_resolution_cache_miss_total",
    "Total token resolution cache misses",
    ["chain"],
    registry=GATEWAY_REGISTRY,
)

TOKEN_RESOLUTION_ONCHAIN_LOOKUP = Counter(
    "token_resolution_onchain_lookup_total",
    "Total on-chain token lookups via gateway",
    ["chain", "status"],
    registry=GATEWAY_REGISTRY,
)

TOKEN_RESOLUTION_ERROR = Counter(
    "token_resolution_error_total",
    "Total token resolution errors",
    ["chain", "error_type"],
    registry=GATEWAY_REGISTRY,
)

TOKEN_RESOLUTION_LATENCY = Histogram(
    "token_resolution_latency_seconds",
    "Token resolution latency",
    ["chain", "source"],
    buckets=(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0),
    registry=GATEWAY_REGISTRY,
)


def _parse_method_name(full_method: str) -> tuple[str, str]:
    """Parse gRPC method name into service and method components.

    Args:
        full_method: Full method name like "/almanak.gateway.MarketService/GetPrice"

    Returns:
        Tuple of (service_name, method_name)
    """
    # Remove leading slash and split
    parts = full_method.lstrip("/").split("/")
    if len(parts) >= 2:
        service = parts[0].split(".")[-1]  # Extract service name from full path
        method = parts[1]
        return service, method
    return "unknown", "unknown"


class MetricsInterceptor(grpc.aio.ServerInterceptor):
    """gRPC server interceptor that collects request metrics."""

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        """Intercept gRPC calls to collect metrics.

        Args:
            continuation: The next interceptor or handler
            handler_call_details: Details about the call

        Returns:
            The RPC method handler
        """
        service, method = _parse_method_name(handler_call_details.method)

        # Get the original handler
        handler = await continuation(handler_call_details)

        if handler is None:
            return handler

        # Wrap the handler based on its type
        if handler.unary_unary:
            return grpc.unary_unary_rpc_method_handler(
                self._wrap_unary_unary(handler.unary_unary, service, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        elif handler.unary_stream:
            return grpc.unary_stream_rpc_method_handler(
                self._wrap_unary_stream(handler.unary_stream, service, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        elif handler.stream_unary:
            return grpc.stream_unary_rpc_method_handler(
                self._wrap_stream_unary(handler.stream_unary, service, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        elif handler.stream_stream:
            return grpc.stream_stream_rpc_method_handler(
                self._wrap_stream_stream(handler.stream_stream, service, method),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        return handler

    def _wrap_unary_unary(
        self,
        behavior: Callable[..., Awaitable[Any]],
        service: str,
        method: str,
    ) -> Callable[..., Awaitable[Any]]:
        """Wrap a unary-unary handler with metrics collection."""

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            ACTIVE_REQUESTS.labels(service=service).inc()

            try:
                response = await behavior(request, context)
                status = "ok"
                return response
            except Exception as e:
                status = "error"
                error_type = type(e).__name__
                ERROR_COUNT.labels(service=service, method=method, error_type=error_type).inc()
                raise
            finally:
                duration = time.perf_counter() - start_time
                ACTIVE_REQUESTS.labels(service=service).dec()
                REQUEST_COUNT.labels(service=service, method=method, status=status).inc()
                REQUEST_LATENCY.labels(service=service, method=method).observe(duration)

        return wrapper

    def _wrap_unary_stream(
        self,
        behavior: Callable[..., Any],
        service: str,
        method: str,
    ) -> Callable[..., Any]:
        """Wrap a unary-stream handler with metrics collection."""

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            ACTIVE_REQUESTS.labels(service=service).inc()
            status = "ok"

            try:
                async for response in behavior(request, context):
                    yield response
            except Exception as e:
                status = "error"
                error_type = type(e).__name__
                ERROR_COUNT.labels(service=service, method=method, error_type=error_type).inc()
                raise
            finally:
                duration = time.perf_counter() - start_time
                ACTIVE_REQUESTS.labels(service=service).dec()
                REQUEST_COUNT.labels(service=service, method=method, status=status).inc()
                REQUEST_LATENCY.labels(service=service, method=method).observe(duration)

        return wrapper

    def _wrap_stream_unary(
        self,
        behavior: Callable[..., Awaitable[Any]],
        service: str,
        method: str,
    ) -> Callable[..., Awaitable[Any]]:
        """Wrap a stream-unary handler with metrics collection."""

        async def wrapper(request_iterator: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            ACTIVE_REQUESTS.labels(service=service).inc()

            try:
                response = await behavior(request_iterator, context)
                status = "ok"
                return response
            except Exception as e:
                status = "error"
                error_type = type(e).__name__
                ERROR_COUNT.labels(service=service, method=method, error_type=error_type).inc()
                raise
            finally:
                duration = time.perf_counter() - start_time
                ACTIVE_REQUESTS.labels(service=service).dec()
                REQUEST_COUNT.labels(service=service, method=method, status=status).inc()
                REQUEST_LATENCY.labels(service=service, method=method).observe(duration)

        return wrapper

    def _wrap_stream_stream(
        self,
        behavior: Callable[..., Any],
        service: str,
        method: str,
    ) -> Callable[..., Any]:
        """Wrap a stream-stream handler with metrics collection."""

        async def wrapper(request_iterator: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            ACTIVE_REQUESTS.labels(service=service).inc()
            status = "ok"

            try:
                async for response in behavior(request_iterator, context):
                    yield response
            except Exception as e:
                status = "error"
                error_type = type(e).__name__
                ERROR_COUNT.labels(service=service, method=method, error_type=error_type).inc()
                raise
            finally:
                duration = time.perf_counter() - start_time
                ACTIVE_REQUESTS.labels(service=service).dec()
                REQUEST_COUNT.labels(service=service, method=method, status=status).inc()
                REQUEST_LATENCY.labels(service=service, method=method).observe(duration)

        return wrapper


class MetricsHTTPHandler(SimpleHTTPRequestHandler):
    """HTTP handler that serves Prometheus metrics."""

    def do_GET(self) -> None:
        """Handle GET requests for metrics endpoint."""
        if self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest(GATEWAY_REGISTRY))
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default logging."""
        pass


class MetricsServer:
    """HTTP server for exposing Prometheus metrics."""

    def __init__(self, port: int = 9090):
        """Initialize the metrics server.

        Args:
            port: Port to serve metrics on
        """
        self.port = port
        self._server: HTTPServer | None = None
        self._thread: Thread | None = None

    def start(self) -> None:
        """Start the metrics HTTP server in a background thread."""
        self._server = HTTPServer(("", self.port), MetricsHTTPHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(f"Metrics server started on http://0.0.0.0:{self.port}/metrics")

    def stop(self) -> None:
        """Stop the metrics server."""
        if self._server:
            self._server.shutdown()
            logger.info("Metrics server stopped")


# Convenience functions for recording metrics from services


def record_integration_request(integration: str, endpoint: str) -> None:
    """Record an integration API request.

    Args:
        integration: Integration name (binance, coingecko, thegraph)
        endpoint: API endpoint called
    """
    INTEGRATION_REQUESTS.labels(integration=integration, endpoint=endpoint).inc()


def record_integration_latency(integration: str, endpoint: str, duration: float) -> None:
    """Record integration API request latency.

    Args:
        integration: Integration name
        endpoint: API endpoint called
        duration: Request duration in seconds
    """
    INTEGRATION_LATENCY.labels(integration=integration, endpoint=endpoint).observe(duration)


def record_rpc_request(chain: str, method: str) -> None:
    """Record an RPC proxy request.

    Args:
        chain: Chain name
        method: RPC method called
    """
    RPC_REQUESTS.labels(chain=chain, method=method).inc()


def record_rpc_latency(chain: str, duration: float) -> None:
    """Record RPC proxy request latency.

    Args:
        chain: Chain name
        duration: Request duration in seconds
    """
    RPC_LATENCY.labels(chain=chain).observe(duration)


def record_token_resolution_cache_hit(chain: str, cache_type: str) -> None:
    """Record a token resolution cache hit.

    Args:
        chain: Chain name (e.g., "arbitrum", "ethereum")
        cache_type: Cache layer that was hit ("memory", "disk", "static")
    """
    TOKEN_RESOLUTION_CACHE_HIT.labels(chain=chain, cache_type=cache_type).inc()


def record_token_resolution_cache_miss(chain: str) -> None:
    """Record a token resolution cache miss.

    Args:
        chain: Chain name
    """
    TOKEN_RESOLUTION_CACHE_MISS.labels(chain=chain).inc()


def record_token_resolution_onchain_lookup(chain: str, status: str) -> None:
    """Record an on-chain token lookup via gateway.

    Args:
        chain: Chain name
        status: Lookup result ("success", "not_found", "timeout", "error")
    """
    TOKEN_RESOLUTION_ONCHAIN_LOOKUP.labels(chain=chain, status=status).inc()


def record_token_resolution_error(chain: str, error_type: str) -> None:
    """Record a token resolution error.

    Args:
        chain: Chain name
        error_type: Error class name (e.g., "TokenNotFoundError", "InvalidTokenAddressError")
    """
    TOKEN_RESOLUTION_ERROR.labels(chain=chain, error_type=error_type).inc()


def record_token_resolution_latency(chain: str, source: str, duration: float) -> None:
    """Record token resolution latency.

    Args:
        chain: Chain name
        source: Resolution source ("cache", "static", "on_chain", "alias")
        duration: Resolution duration in seconds
    """
    TOKEN_RESOLUTION_LATENCY.labels(chain=chain, source=source).observe(duration)
