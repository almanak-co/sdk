"""Shared helpers for classifying framework gRPC failures."""

import re

import grpc

# Transient gRPC status codes that are worth retrying. Permanent codes
# (UNAUTHENTICATED, PERMISSION_DENIED, INVALID_ARGUMENT, UNIMPLEMENTED, ...)
# indicate config or auth defects that will not resolve with more attempts.
TRANSIENT_GRPC_CODES: frozenset[grpc.StatusCode] = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
    }
)

_RETRY_AFTER_RE = re.compile(r"retry after\s+([0-9]+(?:\.[0-9]+)?)s?", re.IGNORECASE)


def get_grpc_status_code(exc: grpc.RpcError) -> grpc.StatusCode | None:
    """Return a gRPC status code when the concrete RpcError exposes one."""
    code_fn = getattr(exc, "code", None)
    if not callable(code_fn):
        return None
    try:
        code = code_fn()
    except Exception:  # noqa: BLE001 - defensive: code() should not raise
        return None
    return code if isinstance(code, grpc.StatusCode) else None


_RETRY_AFTER_MAX_SECONDS: float = 120.0


def is_transient_grpc_error(exc: grpc.RpcError) -> bool:
    """Return whether a gRPC error is retryable by framework orchestration.

    Unknown status codes (code is None) are treated as transient — consistent
    with PR #1676 — because an unrecognised code more likely indicates a
    transport or version mismatch than a permanent business-logic rejection.
    """
    code = get_grpc_status_code(exc)
    return code is None or code in TRANSIENT_GRPC_CODES


def get_grpc_retry_after_seconds(exc: grpc.RpcError) -> float | None:
    """Extract a ``retry after`` hint from gRPC details or string text.

    The returned value is capped at ``_RETRY_AFTER_MAX_SECONDS`` so a buggy
    or adversarial gateway response cannot stall a teardown indefinitely.
    """
    parts: list[str] = []
    details_fn = getattr(exc, "details", None)
    if callable(details_fn):
        try:
            details = details_fn()
        except Exception:  # noqa: BLE001 - defensive: details() should not raise
            details = None
        if details:
            parts.append(str(details))
    parts.append(str(exc))

    for text in parts:
        match = _RETRY_AFTER_RE.search(text)
        if match:
            return min(float(match.group(1)), _RETRY_AFTER_MAX_SECONDS)
    return None
