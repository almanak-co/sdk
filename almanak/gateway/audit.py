"""Gateway audit logging for security and debugging.

This module provides structured audit logging for all gateway operations:
- Request/response logging with timestamps and latency
- Strategy ID tracking for multi-tenant debugging
- JSON format for easy parsing and analysis
- Configurable log levels

Logs are written to stdout in JSON format for container log aggregation.
"""

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import grpc
import structlog

# Flag to track if structlog has been configured
_structlog_configured = False

# Module-level logger, created lazily after configuration
_audit_logger: structlog.stdlib.BoundLogger | None = None


def configure_structlog() -> None:
    """Configure structlog for JSON output.

    This function is idempotent - it can be called multiple times safely.
    Configuration is only applied on the first call.

    Call this function during application startup or in test setup
    to initialize structlog with the gateway's standard configuration.
    """
    global _structlog_configured
    if _structlog_configured:
        return

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    _structlog_configured = True


def get_audit_logger() -> structlog.stdlib.BoundLogger:
    """Get the audit logger, configuring structlog if needed.

    Returns:
        The configured audit logger instance.
    """
    global _audit_logger
    if _audit_logger is None:
        configure_structlog()
        _audit_logger = structlog.get_logger("gateway.audit")
    return _audit_logger


def reset_structlog_configuration() -> None:
    """Reset the structlog configuration state.

    This is primarily useful for testing to allow reconfiguration.
    """
    global _structlog_configured, _audit_logger
    _structlog_configured = False
    _audit_logger = None


@dataclass
class AuditRecord:
    """Audit record for a gateway operation."""

    timestamp: str
    service: str
    method: str
    strategy_id: str | None
    latency_ms: float
    success: bool
    error_type: str | None = None
    error_message: str | None = None
    request_summary: dict = field(default_factory=dict)
    response_summary: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "method": self.method,
            "strategy_id": self.strategy_id,
            "latency_ms": round(self.latency_ms, 3),
            "success": self.success,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "request": self.request_summary,
            "response": self.response_summary,
            "metadata": self.metadata,
        }


def _parse_method_name(full_method: str) -> tuple[str, str]:
    """Parse gRPC method name into service and method components.

    Args:
        full_method: Full method name like "/almanak.gateway.MarketService/GetPrice"

    Returns:
        Tuple of (service_name, method_name)
    """
    parts = full_method.lstrip("/").split("/")
    if len(parts) >= 2:
        service = parts[0].split(".")[-1]
        method = parts[1]
        return service, method
    return "unknown", "unknown"


def _extract_strategy_id(request: Any) -> str | None:
    """Extract strategy_id from request if present.

    Args:
        request: gRPC request object

    Returns:
        Strategy ID or None
    """
    # Try common field names
    if hasattr(request, "strategy_id"):
        return request.strategy_id or None
    if hasattr(request, "strategyId"):
        return request.strategyId or None
    return None


def _summarize_request(request: Any, max_fields: int = 5) -> dict:
    """Create a summary of request for logging.

    Only includes key fields, not full payload.

    Args:
        request: gRPC request object
        max_fields: Maximum number of fields to include

    Returns:
        Summary dictionary
    """
    summary = {}
    important_fields = [
        "chain",
        "strategy_id",
        "method",
        "symbol",
        "token_id",
        "subgraph_id",
        "wallet_address",
        "address",
    ]

    count = 0
    for field_name in important_fields:
        if count >= max_fields:
            break
        if hasattr(request, field_name):
            value = getattr(request, field_name)
            if value:
                summary[field_name] = str(value)[:100]  # Truncate long values
                count += 1

    return summary


def _summarize_response(response: Any) -> dict:
    """Create a summary of response for logging.

    Args:
        response: gRPC response object

    Returns:
        Summary dictionary
    """
    summary = {}

    # Check for success field
    if hasattr(response, "success"):
        summary["success"] = response.success

    # Check for error field
    if hasattr(response, "error") and response.error:
        summary["has_error"] = True

    return summary


def log_audit_record(record: AuditRecord) -> None:
    """Log an audit record.

    Args:
        record: Audit record to log
    """
    log_data = record.to_dict()
    logger = get_audit_logger()

    if record.success:
        logger.info(
            "gateway_request",
            **log_data,
        )
    else:
        logger.warning(
            "gateway_request_failed",
            **log_data,
        )


class AuditInterceptor(grpc.aio.ServerInterceptor):
    """gRPC server interceptor that provides audit logging."""

    def __init__(self, enabled: bool = True, log_level: str = "info"):
        """Initialize the audit interceptor.

        Args:
            enabled: Whether audit logging is enabled
            log_level: Minimum log level (debug, info, warning, error)
        """
        self.enabled = enabled
        self.log_level = log_level.lower()

        # Set the log level
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
        }
        logging.getLogger("gateway.audit").setLevel(level_map.get(self.log_level, logging.INFO))

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        """Intercept gRPC calls for audit logging.

        Args:
            continuation: The next interceptor or handler
            handler_call_details: Details about the call

        Returns:
            The RPC method handler
        """
        if not self.enabled:
            return await continuation(handler_call_details)

        service, method = _parse_method_name(handler_call_details.method)

        # Skip health checks in audit log
        if service == "Health":
            return await continuation(handler_call_details)

        handler = await continuation(handler_call_details)

        if handler is None:
            return handler

        # Wrap handlers to add audit logging
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
        """Wrap a unary-unary handler with audit logging."""

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            timestamp = datetime.now(UTC).isoformat()
            strategy_id = _extract_strategy_id(request)
            request_summary = _summarize_request(request)

            error_type = None
            error_message = None
            success = True

            try:
                response = await behavior(request, context)
                response_summary = _summarize_response(response)
                return response
            except Exception as e:
                success = False
                error_type = type(e).__name__
                error_message = str(e)[:200]  # Truncate long errors
                response_summary = {}
                raise
            finally:
                latency_ms = (time.perf_counter() - start_time) * 1000

                record = AuditRecord(
                    timestamp=timestamp,
                    service=service,
                    method=method,
                    strategy_id=strategy_id,
                    latency_ms=latency_ms,
                    success=success,
                    error_type=error_type,
                    error_message=error_message,
                    request_summary=request_summary,
                    response_summary=response_summary,
                )
                log_audit_record(record)

        return wrapper

    def _wrap_unary_stream(
        self,
        behavior: Callable[..., Any],
        service: str,
        method: str,
    ) -> Callable[..., Any]:
        """Wrap a unary-stream handler with audit logging."""

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            timestamp = datetime.now(UTC).isoformat()
            strategy_id = _extract_strategy_id(request)
            request_summary = _summarize_request(request)

            error_type = None
            error_message = None
            success = True

            try:
                async for response in behavior(request, context):
                    yield response
            except Exception as e:
                success = False
                error_type = type(e).__name__
                error_message = str(e)[:200]
                raise
            finally:
                latency_ms = (time.perf_counter() - start_time) * 1000

                record = AuditRecord(
                    timestamp=timestamp,
                    service=service,
                    method=method,
                    strategy_id=strategy_id,
                    latency_ms=latency_ms,
                    success=success,
                    error_type=error_type,
                    error_message=error_message,
                    request_summary=request_summary,
                    response_summary={},
                )
                log_audit_record(record)

        return wrapper

    def _wrap_stream_unary(
        self,
        behavior: Callable[..., Awaitable[Any]],
        service: str,
        method: str,
    ) -> Callable[..., Awaitable[Any]]:
        """Wrap a stream-unary handler with audit logging."""

        async def wrapper(request_iterator: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            timestamp = datetime.now(UTC).isoformat()

            error_type = None
            error_message = None
            success = True

            try:
                response = await behavior(request_iterator, context)
                response_summary = _summarize_response(response)
                return response
            except Exception as e:
                success = False
                error_type = type(e).__name__
                error_message = str(e)[:200]
                response_summary = {}
                raise
            finally:
                latency_ms = (time.perf_counter() - start_time) * 1000

                record = AuditRecord(
                    timestamp=timestamp,
                    service=service,
                    method=method,
                    strategy_id=None,  # Can't extract from stream
                    latency_ms=latency_ms,
                    success=success,
                    error_type=error_type,
                    error_message=error_message,
                    request_summary={},
                    response_summary=response_summary,
                )
                log_audit_record(record)

        return wrapper

    def _wrap_stream_stream(
        self,
        behavior: Callable[..., Any],
        service: str,
        method: str,
    ) -> Callable[..., Any]:
        """Wrap a stream-stream handler with audit logging."""

        async def wrapper(request_iterator: Any, context: grpc.aio.ServicerContext) -> Any:
            start_time = time.perf_counter()
            timestamp = datetime.now(UTC).isoformat()

            error_type = None
            error_message = None
            success = True

            try:
                async for response in behavior(request_iterator, context):
                    yield response
            except Exception as e:
                success = False
                error_type = type(e).__name__
                error_message = str(e)[:200]
                raise
            finally:
                latency_ms = (time.perf_counter() - start_time) * 1000

                record = AuditRecord(
                    timestamp=timestamp,
                    service=service,
                    method=method,
                    strategy_id=None,
                    latency_ms=latency_ms,
                    success=success,
                    error_type=error_type,
                    error_message=error_message,
                    request_summary={},
                    response_summary={},
                )
                log_audit_record(record)

        return wrapper
