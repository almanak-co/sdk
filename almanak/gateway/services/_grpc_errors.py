"""Service-side mapping from upstream/integration errors to typed gRPC errors.

Each gateway service that proxies an upstream HTTP integration calls
:func:`set_error_from_upstream` from its exception handler. The helper:

1. Inspects the exception type / code (``IntegrationRateLimitError``,
   ``IntegrationError(code="TIMEOUT")``, ``IntegrationError(code="HTTP_5xx")``,
   etc.).
2. Maps it to a specific gRPC status code (RESOURCE_EXHAUSTED /
   DEADLINE_EXCEEDED / UNAVAILABLE / FAILED_PRECONDITION).
3. Packs ``RetryInfo`` and ``ErrorInfo`` into the ``grpc-status-details-bin``
   trailer using :func:`almanak.framework.grpc.error_details.set_grpc_error`.

VIB-3800.
"""

from __future__ import annotations

import logging
from typing import Any

import grpc

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    DataSourceRateLimited,
    DataSourceTimeout,
    DataSourceUnavailable,
)
from almanak.framework.grpc.error_details import set_grpc_error
from almanak.gateway.integrations.base import IntegrationError, IntegrationRateLimitError

logger = logging.getLogger(__name__)


def set_error_from_upstream(
    context: Any,
    exc: BaseException,
    *,
    upstream: str,
) -> None:
    """Map an integration-layer exception to a typed gRPC error.

    Args:
        context: gRPC servicer context (sync or async).
        exc: The exception raised by the integration / aggregator layer.
        upstream: Upstream identifier (e.g. ``"geckoterminal"``, ``"binance"``,
            ``"price_aggregator"``). Surfaced as ``ErrorInfo.metadata['upstream']``
            for client-side metrics.
    """
    code, message, retry_delay, reason, metadata = _classify(exc, upstream=upstream)

    set_grpc_error(
        context,
        code=code,
        message=message,
        retry_delay_seconds=retry_delay,
        reason=reason,
        upstream=upstream,
        metadata=metadata,
    )


def _classify(  # noqa: C901
    exc: BaseException,
    *,
    upstream: str,
) -> tuple[grpc.StatusCode, str, float | None, str, dict[str, str]]:
    """Classify an exception into typed-error fields.

    Returns ``(code, message, retry_delay_seconds, reason, metadata)``.
    """
    metadata: dict[str, str] = {}
    message = str(exc)

    if isinstance(exc, IntegrationRateLimitError):
        return (
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            message,
            float(exc.retry_after) if exc.retry_after is not None else None,
            "UPSTREAM_RATE_LIMITED",
            metadata,
        )

    # Typed framework-level data errors (raised by aggregators and oracles).
    if isinstance(exc, DataSourceRateLimited):
        return (
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            message,
            float(exc.retry_after) if exc.retry_after is not None else None,
            "UPSTREAM_RATE_LIMITED",
            metadata,
        )

    if isinstance(exc, DataSourceTimeout):
        return (
            grpc.StatusCode.DEADLINE_EXCEEDED,
            message,
            float(exc.retry_after) if exc.retry_after is not None else None,
            "UPSTREAM_TIMEOUT",
            metadata,
        )

    if isinstance(exc, DataSourceUnavailable):
        return (
            grpc.StatusCode.UNAVAILABLE,
            message,
            float(exc.retry_after) if exc.retry_after is not None else None,
            "UPSTREAM_UNAVAILABLE",
            metadata,
        )

    if isinstance(exc, AllDataSourcesFailed):
        # Every primary source failed. The router should treat this as a
        # transient upstream-side outage (UNAVAILABLE), not a gateway bug.
        return (
            grpc.StatusCode.UNAVAILABLE,
            message,
            None,
            "ALL_SOURCES_FAILED",
            metadata,
        )

    if isinstance(exc, IntegrationError):
        code_str = (exc.code or "").upper()
        metadata["integration_code"] = code_str
        if code_str == "TIMEOUT":
            return (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                message,
                None,
                "UPSTREAM_TIMEOUT",
                metadata,
            )
        if code_str == "NETWORK_ERROR":
            return (
                grpc.StatusCode.UNAVAILABLE,
                message,
                None,
                "UPSTREAM_NETWORK_ERROR",
                metadata,
            )
        if code_str.startswith("HTTP_"):
            suffix = code_str.removeprefix("HTTP_")
            # Defensive: handle wildcard codes like HTTP_5XX / HTTP_4XX that
            # an integration might emit when it knows the broad class but not
            # the specific status. Without these branches int(suffix) raises
            # and the caller falls into UPSTREAM_UNKNOWN, breaking the
            # taxonomy.
            if suffix in ("5XX", "5xx"):
                return (
                    grpc.StatusCode.UNAVAILABLE,
                    message,
                    None,
                    "UPSTREAM_HTTP_5XX",
                    metadata,
                )
            if suffix in ("4XX", "4xx"):
                return (
                    grpc.StatusCode.FAILED_PRECONDITION,
                    message,
                    None,
                    "UPSTREAM_HTTP_4XX",
                    metadata,
                )
            try:
                http_status = int(suffix)
            except ValueError:
                http_status = 0
            if 500 <= http_status < 600:
                return (
                    grpc.StatusCode.UNAVAILABLE,
                    message,
                    None,
                    "UPSTREAM_HTTP_5XX",
                    metadata,
                )
            if http_status == 408:
                return (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    message,
                    None,
                    "UPSTREAM_TIMEOUT",
                    metadata,
                )
            if http_status == 429:
                # Should normally be IntegrationRateLimitError; defensive fallback.
                return (
                    grpc.StatusCode.RESOURCE_EXHAUSTED,
                    message,
                    None,
                    "UPSTREAM_RATE_LIMITED",
                    metadata,
                )
            if 400 <= http_status < 500:
                return (
                    grpc.StatusCode.FAILED_PRECONDITION,
                    message,
                    None,
                    "UPSTREAM_HTTP_4XX",
                    metadata,
                )
        # Unknown integration code — treat as upstream-side internal error
        # but flag it as potentially transient (UNAVAILABLE) rather than
        # gateway-side INTERNAL, because the failure originated in the
        # upstream call path.
        return (
            grpc.StatusCode.UNAVAILABLE,
            message,
            None,
            "UPSTREAM_UNKNOWN",
            metadata,
        )

    # Anything else — TimeoutError, asyncio.TimeoutError, generic exception.
    if isinstance(exc, TimeoutError):
        return (
            grpc.StatusCode.DEADLINE_EXCEEDED,
            message or "Upstream timeout",
            None,
            "UPSTREAM_TIMEOUT",
            metadata,
        )

    # Last resort: surface as INTERNAL with a stable, opaque message. This is
    # the "gateway bug" case — the error did not originate at the upstream
    # boundary, so callers should NOT retry indefinitely. We do NOT echo
    # ``str(exc)`` across the trust boundary because it can carry stack
    # frames, secrets, or library internals; the original exception is logged
    # server-side at exception level so operators can correlate by upstream.
    logger.exception("gateway_internal_error upstream=%s", upstream, exc_info=exc)
    return (
        grpc.StatusCode.INTERNAL,
        "Internal gateway error",
        None,
        "GATEWAY_INTERNAL",
        metadata,
    )


__all__ = ["set_error_from_upstream"]
