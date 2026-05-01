"""Typed gRPC error contract — `google.rpc.Status` codec for the gateway boundary.

Background
----------
The 29 Apr 2026 production incident traced to gateway-side error opacity: every
upstream HTTP failure (429s, timeouts, 5xx) collapsed into a generic gRPC
``INTERNAL`` with a stringified message. The router used heuristic string-matching
on those messages to decide whether to retry. Brittle, lossy, and impossible to
extend without regressing existing callers.

This module is the foundation for replacing that heuristic with an explicit
contract:

- The gateway service MAPS upstream failures to specific gRPC status codes
  (``RESOURCE_EXHAUSTED`` / ``DEADLINE_EXCEEDED`` / ``UNAVAILABLE``) and PACKS
  ``google.rpc.RetryInfo`` and ``google.rpc.ErrorInfo`` into the standard
  ``grpc-status-details-bin`` trailing-metadata slot.
- The gateway client UNPACKS that trailer, surfacing ``retry_delay`` and
  ``reason`` to the framework retry policy and breaker.

Wire format
-----------
The ``grpc-status-details-bin`` metadata key carries a serialized
``google.rpc.Status`` message. ``Status.details`` is a ``repeated Any`` field
holding ``RetryInfo`` and ``ErrorInfo`` protos. This is the standard Google API
error model — interoperable with any gRPC client that follows it.

VIB-3800 — typed error contract foundation. VIB-3802 consumes ``retry_delay``
in the router; VIB-3803 consumes ``reason`` in the FailureKind taxonomy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import grpc
from google.protobuf import duration_pb2
from google.rpc import error_details_pb2, status_pb2

_STATUS_DETAILS_KEY = "grpc-status-details-bin"


# google.rpc.Code values (canonical numeric mapping, mirrored by grpc.StatusCode).
# We avoid importing google.rpc.code_pb2 because it's not always shipped — the
# numeric mapping is stable per the gRPC spec.
_STATUS_CODE_TO_INT: dict[grpc.StatusCode, int] = {
    grpc.StatusCode.OK: 0,
    grpc.StatusCode.CANCELLED: 1,
    grpc.StatusCode.UNKNOWN: 2,
    grpc.StatusCode.INVALID_ARGUMENT: 3,
    grpc.StatusCode.DEADLINE_EXCEEDED: 4,
    grpc.StatusCode.NOT_FOUND: 5,
    grpc.StatusCode.ALREADY_EXISTS: 6,
    grpc.StatusCode.PERMISSION_DENIED: 7,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 8,
    grpc.StatusCode.FAILED_PRECONDITION: 9,
    grpc.StatusCode.ABORTED: 10,
    grpc.StatusCode.OUT_OF_RANGE: 11,
    grpc.StatusCode.UNIMPLEMENTED: 12,
    grpc.StatusCode.INTERNAL: 13,
    grpc.StatusCode.UNAVAILABLE: 14,
    grpc.StatusCode.DATA_LOSS: 15,
    grpc.StatusCode.UNAUTHENTICATED: 16,
}

_INT_TO_STATUS_CODE: dict[int, grpc.StatusCode] = {v: k for k, v in _STATUS_CODE_TO_INT.items()}


@dataclass
class StatusDetails:
    """Decoded view of a typed gRPC error trailer.

    Attributes:
        code: Decoded gRPC status code (``UNKNOWN`` if the wire code is unrecognized).
        message: Human-readable message from ``Status.message``.
        retry_delay_seconds: Suggested retry delay (from ``RetryInfo``), or None.
        reason: Stable machine-readable error reason (from ``ErrorInfo.reason``),
            e.g. ``"UPSTREAM_RATE_LIMITED"``. Used by the FailureKind taxonomy.
        domain: ``ErrorInfo.domain`` (e.g. ``"almanak.gateway"``).
        upstream: Upstream identifier (from ``ErrorInfo.metadata['upstream']``),
            e.g. ``"geckoterminal"`` / ``"binance"``. Useful for per-upstream metrics.
        metadata: Full ``ErrorInfo.metadata`` dict (all key/value pairs).
    """

    code: grpc.StatusCode
    message: str
    retry_delay_seconds: float | None = None
    reason: str | None = None
    domain: str | None = None
    upstream: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)


def _seconds_to_duration(seconds: float) -> duration_pb2.Duration:
    """Convert a float seconds value to a ``google.protobuf.Duration``."""
    duration = duration_pb2.Duration()
    duration.FromTimedelta(_seconds_to_timedelta(seconds))
    return duration


def _seconds_to_timedelta(seconds: float):
    from datetime import timedelta

    return timedelta(seconds=max(seconds, 0.0))


def pack_status_details(
    *,
    code: grpc.StatusCode,
    message: str,
    retry_delay_seconds: float | None = None,
    reason: str | None = None,
    domain: str = "almanak.gateway",
    upstream: str | None = None,
    metadata: dict[str, str] | None = None,
) -> tuple[grpc.StatusCode, str, list[tuple[str, bytes]]]:
    """Build the ``(code, message, trailing_metadata)`` triple for a typed gRPC error.

    Pass the result to ``context.set_code(...)``, ``context.set_details(...)``,
    and ``context.set_trailing_metadata(...)`` — or use :func:`set_grpc_error`
    which does all three.

    Args:
        code: gRPC status code (e.g. ``RESOURCE_EXHAUSTED``).
        message: Human-readable message.
        retry_delay_seconds: Optional suggested retry delay. Packed as ``RetryInfo``.
            None means "no advice; caller policy applies."
        reason: Stable machine-readable reason (e.g. ``"UPSTREAM_RATE_LIMITED"``).
        domain: ``ErrorInfo.domain``.
        upstream: Upstream identifier (e.g. ``"geckoterminal"``).
        metadata: Additional ``ErrorInfo.metadata`` entries.

    Returns:
        Tuple of (code, message, trailing_metadata) where trailing_metadata is
        a list of ``(key, bytes)`` tuples ready to pass to gRPC.
    """
    status = status_pb2.Status()
    status.code = _STATUS_CODE_TO_INT.get(code, _STATUS_CODE_TO_INT[grpc.StatusCode.UNKNOWN])
    status.message = message

    if retry_delay_seconds is not None and retry_delay_seconds >= 0:
        retry_info = error_details_pb2.RetryInfo()
        retry_info.retry_delay.CopyFrom(_seconds_to_duration(retry_delay_seconds))
        status.details.add().Pack(retry_info)

    if reason is not None or upstream is not None or metadata:
        error_info = error_details_pb2.ErrorInfo()
        if reason is not None:
            error_info.reason = reason
        error_info.domain = domain
        if upstream is not None:
            error_info.metadata["upstream"] = upstream
        if metadata:
            for key, value in metadata.items():
                error_info.metadata[key] = value
        status.details.add().Pack(error_info)

    trailing: list[tuple[str, bytes]] = [(_STATUS_DETAILS_KEY, status.SerializeToString())]
    return code, message, trailing


def set_grpc_error(
    context: Any,
    *,
    code: grpc.StatusCode,
    message: str,
    retry_delay_seconds: float | None = None,
    reason: str | None = None,
    domain: str = "almanak.gateway",
    upstream: str | None = None,
    metadata: dict[str, str] | None = None,
) -> None:
    """Set a typed error on a gRPC servicer context.

    Convenience wrapper around :func:`pack_status_details` that handles the
    three context calls (``set_code`` / ``set_details`` / ``set_trailing_metadata``).

    Works with both ``grpc.ServicerContext`` and ``grpc.aio.ServicerContext``.
    """
    code, msg, trailing = pack_status_details(
        code=code,
        message=message,
        retry_delay_seconds=retry_delay_seconds,
        reason=reason,
        domain=domain,
        upstream=upstream,
        metadata=metadata,
    )
    context.set_code(code)
    context.set_details(msg)
    context.set_trailing_metadata(trailing)


def unpack_status_details(rpc_error: Any) -> StatusDetails | None:
    """Extract typed status details from a gRPC error.

    Looks for the ``grpc-status-details-bin`` entry in the error's trailing
    metadata, deserializes it as ``google.rpc.Status``, and unpacks any
    ``RetryInfo`` and ``ErrorInfo`` details.

    Args:
        rpc_error: A ``grpc.RpcError`` or any object exposing ``trailing_metadata()``
            that returns metadata in the same shape.

    Returns:
        ``StatusDetails`` if a typed trailer is present, else None.
    """
    trailing_metadata = _safe_trailing_metadata(rpc_error)
    if not trailing_metadata:
        return None

    payload = _find_status_payload(trailing_metadata)
    if not payload:
        # Treat missing AND empty trailer as "no typed details" — `b""`
        # parses to a default Status (code=OK, no details), which would
        # otherwise look like a typed-OK signal and skip the legacy
        # fallback path.
        return None

    try:
        status = status_pb2.Status()
        status.ParseFromString(payload)
    except Exception:
        return None

    code = _INT_TO_STATUS_CODE.get(status.code, grpc.StatusCode.UNKNOWN)
    details = StatusDetails(code=code, message=status.message)

    for any_detail in status.details:
        retry_info = error_details_pb2.RetryInfo()
        if any_detail.Is(retry_info.DESCRIPTOR):
            any_detail.Unpack(retry_info)
            details.retry_delay_seconds = retry_info.retry_delay.seconds + retry_info.retry_delay.nanos / 1e9
            continue

        error_info = error_details_pb2.ErrorInfo()
        if any_detail.Is(error_info.DESCRIPTOR):
            any_detail.Unpack(error_info)
            details.reason = error_info.reason or None
            details.domain = error_info.domain or None
            metadata = dict(error_info.metadata)
            details.metadata = metadata
            details.upstream = metadata.get("upstream")
            continue

    return details


def _safe_trailing_metadata(rpc_error: Any):
    fetch = getattr(rpc_error, "trailing_metadata", None)
    if fetch is None:
        return None
    try:
        return fetch()
    except Exception:
        return None


def _find_status_payload(trailing_metadata: Any) -> bytes | None:
    """Locate the ``grpc-status-details-bin`` payload in a metadata sequence.

    gRPC returns metadata as a sequence of ``(key, value)`` tuples — but
    different code paths normalize the value differently (bytes for `-bin`
    keys per the gRPC spec, but some shims surface str). Tolerate both.
    """
    try:
        items = list(trailing_metadata)
    except TypeError:
        return None

    for entry in items:
        if not entry or len(entry) < 2:
            continue
        key, value = entry[0], entry[1]
        if key != _STATUS_DETAILS_KEY:
            continue
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            # `-bin` trailer values per the gRPC spec are bytes; we only see
            # str if a shim has already decoded them. latin-1 is bijective
            # over 0..255, so a clean encode succeeds. If any character is
            # outside that range, the upstream shim corrupted the payload —
            # fail loud (return None → legacy fallback) rather than silently
            # dropping bytes via errors="ignore" and producing a partial
            # protobuf that ParseFromString may or may not reject.
            try:
                return value.encode("latin-1")
            except UnicodeEncodeError:
                return None
    return None
