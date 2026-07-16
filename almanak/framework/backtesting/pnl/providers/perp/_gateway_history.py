"""Shared gateway client for perp funding-rate history (VIB-4851 Phase D).

Centralizes the ``RateHistoryService.GetFundingRateHistory`` round-trip the
per-venue funding providers share, mirroring the DEX-volume precedent
(``providers/dex/_gateway_volume.py``). The providers in this package hold no
HTTP client and open no socket — the gateway owns all funding-data egress via
each connector's ``GatewayFundingHistoryCapability`` implementation.

Empty ``rate_hourly`` strings (unmeasured points) are SKIPPED, never coerced to
zero — the accounting contract's Empty ≠ Zero rule applies to market data too.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Any

from almanak.framework.data.interfaces import DataSourceUnavailable

logger = logging.getLogger(__name__)

__all__ = [
    "FundingHistoryPoint",
    "MAX_WINDOW_SECONDS",
    "fetch_funding_points",
    "get_connected_gateway_client",
    "run_sync_gateway_call",
]

# Upstream funding-history endpoints cap one response at ~500 hourly entries
# (Hyperliquid Info API; the GMX V2 venue proxies it). The gateway connector
# issues one upstream call per RPC, so windows wider than this are chunked
# client-side to preserve full-range coverage.
MAX_WINDOW_SECONDS = 500 * 3600
_GATEWAY_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="backtest-gateway")


@dataclass(frozen=True)
class FundingHistoryPoint:
    """One decoded funding-rate observation from the gateway."""

    timestamp: int
    rate_hourly: Decimal


async def run_sync_gateway_call(
    func: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run a blocking gateway client call without poisoning the default executor."""
    loop = asyncio.get_running_loop()
    call = partial(func, *args, **kwargs)
    return await loop.run_in_executor(_GATEWAY_EXECUTOR, call)


def get_connected_gateway_client() -> tuple[Any, Any]:
    """Return ``(client, gateway_pb2)`` with the client connected, or raise.

    Same import + connect dance as the lending/TWAP/volume peers
    (``lending_apy._get_connected_gateway_client``).
    """
    try:
        from almanak.framework.gateway_client import get_gateway_client
        from almanak.gateway.proto import gateway_pb2
    except ImportError as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"Gateway client unavailable: {exc}",
            transport=True,
        ) from exc

    client = get_gateway_client()
    if not client.is_connected:
        try:
            client.connect()
        except Exception as exc:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"Gateway connect failed: {exc}",
                transport=True,
            ) from exc
    return client, gateway_pb2


def fetch_funding_points(
    *,
    venue: str,
    market: str,
    chain: str = "",
    start_ts: int,
    end_ts: int,
    max_window_seconds: int = MAX_WINDOW_SECONDS,
) -> list[FundingHistoryPoint]:
    """Fetch and decode funding points for ``[start_ts, end_ts]``.

    Windows wider than ``max_window_seconds`` are split into sequential
    ``GetFundingRateHistory`` RPCs (see :data:`MAX_WINDOW_SECONDS`).

    Args:
        venue: Gateway dispatch key — a connector's
            ``GatewayFundingHistoryCapability.funding_venue()`` (resolve
            protocol identifiers through ``FundingHistoryRegistry.venue_for``).
        market: Market symbol in canonical ``"ETH-USD"`` form; the owning
            connector resolves venue-native coin symbols server-side.
        chain: Chain for on-chain venues; empty for chain-agnostic venues.
        start_ts: Window start (unix seconds, inclusive).
        end_ts: Window end (unix seconds, inclusive).
        max_window_seconds: Per-RPC window cap.

    Returns:
        Decoded points sorted by timestamp ascending. Points whose
        ``rate_hourly`` is empty (unmeasured) are skipped.

    Raises:
        ValueError: When ``max_window_seconds`` is not positive (the chunk
            loop could not make progress).
        DataSourceUnavailable: On transport failure or a gateway-side
            ``success=False`` envelope.
    """
    if max_window_seconds <= 0:
        raise ValueError(f"max_window_seconds must be > 0, got {max_window_seconds}")

    client, gateway_pb2 = get_connected_gateway_client()

    points: list[FundingHistoryPoint] = []
    chunk_start = start_ts
    while chunk_start <= end_ts:
        chunk_end = min(chunk_start + max_window_seconds - 1, end_ts)
        points.extend(
            _fetch_window(
                client,
                gateway_pb2,
                venue=venue,
                market=market,
                chain=chain,
                start_ts=chunk_start,
                end_ts=chunk_end,
            )
        )
        chunk_start = chunk_end + 1

    points.sort(key=lambda p: p.timestamp)
    return points


# gRPC status codes that indicate the CHANNEL is unusable (memoizable for a
# run). Everything else — INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED,
# UNIMPLEMENTED, ... — is a per-request outcome and must stay retryable.
_TRANSPORT_STATUS_NAMES = frozenset({"UNAVAILABLE", "DEADLINE_EXCEEDED"})

# Python exception types that are connectivity failures by construction.
# Deliberately NOT bare OSError: FileNotFoundError/PermissionError are OSError
# subclasses, and a local file/cert bug must not memoize as a gateway outage.
_TRANSPORT_EXCEPTION_TYPES = (ConnectionError, TimeoutError)


def _is_transport_failure(exc: Exception) -> bool:
    """True only when an exception is POSITIVELY a connectivity failure.

    The default for anything unrecognized is NON-transport: memoizing an
    unknown exception (a local decoding bug, a ValueError) as a gateway
    outage silently disables the funding lane for the rest of the run,
    which is far worse than one redundant retry.
    """
    if isinstance(exc, _TRANSPORT_EXCEPTION_TYPES):
        return True
    code = getattr(exc, "code", None)
    if not callable(code):
        return False
    try:
        status = code()
    except Exception:  # noqa: BLE001 — unreadable status is not proof of an outage
        return False
    name = getattr(status, "name", None) or str(status)
    return any(marker in str(name).upper() for marker in _TRANSPORT_STATUS_NAMES)


def _fetch_window(
    client: Any,
    gateway_pb2: Any,
    *,
    venue: str,
    market: str,
    chain: str,
    start_ts: int,
    end_ts: int,
) -> list[FundingHistoryPoint]:
    """Issue one ``GetFundingRateHistory`` RPC and decode its points."""
    request = gateway_pb2.GetFundingRateHistoryRequest(
        venue=venue,
        market=market,
        chain=chain,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    try:
        response = client.rate_history.GetFundingRateHistory(request)
    except Exception as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"GetFundingRateHistory RPC failed: {exc}",
            transport=_is_transport_failure(exc),
        ) from exc
    if not response.success:
        raise DataSourceUnavailable(
            source=response.source or "gateway",
            reason=response.error or "GetFundingRateHistory returned success=false",
        )

    points: list[FundingHistoryPoint] = []
    for proto_point in response.points:
        if proto_point.rate_hourly == "":
            # Unmeasured by the upstream — skip, never substitute zero.
            continue
        try:
            rate = Decimal(proto_point.rate_hourly)
        except (InvalidOperation, ValueError):
            logger.warning(
                "Discarding malformed funding point (venue=%s market=%s ts=%s rate=%r)",
                venue,
                market,
                proto_point.timestamp,
                proto_point.rate_hourly,
            )
            continue
        points.append(FundingHistoryPoint(timestamp=proto_point.timestamp, rate_hourly=rate))
    return points
