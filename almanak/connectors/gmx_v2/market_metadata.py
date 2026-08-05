"""Strategy-side typed client for verified GMX market metadata (VIB-6561)."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import grpc

from almanak.gateway.proto import gateway_pb2


@dataclass(frozen=True)
class ResolvedGmxMarket:
    label: str
    market_token: str
    index_token: str
    index_symbol: str
    index_token_decimals: int
    long_token: str
    long_token_symbol: str
    short_token: str
    short_token_symbol: str


class GmxMarketDiscoveryUnavailable(Exception):
    """The dynamic gateway surface is unavailable; static fallback is allowed."""


class GmxMarketNotFound(Exception):
    """The venue catalogue has no row for this query; static fallback is allowed."""


def resolve_market_via_gateway(gateway_client: Any, *, chain: str, market: str) -> ResolvedGmxMarket:
    """Resolve and verify a GMX market through the shared gateway channel."""
    configured_timeout = getattr(getattr(gateway_client, "config", None), "timeout", 30.0)
    timeout = (
        float(configured_timeout)
        if isinstance(configured_timeout, int | float)
        and not isinstance(configured_timeout, bool)
        and math.isfinite(configured_timeout)
        and configured_timeout > 0
        else 30.0
    )
    stub = getattr(getattr(gateway_client, "market", None), "GetPerpMarket", None)
    if stub is None:
        raise GmxMarketDiscoveryUnavailable(
            "Connected gateway does not expose GetPerpMarket; static fallback is required"
        )
    try:
        response = stub(
            gateway_pb2.GetPerpMarketRequest(protocol="gmx_v2", chain=chain, market=market),
            timeout=timeout,
        )
    except grpc.RpcError as exc:
        if exc.code() is grpc.StatusCode.NOT_FOUND:
            raise GmxMarketNotFound(str(exc)) from exc
        if exc.code() in {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.UNIMPLEMENTED,
            grpc.StatusCode.DEADLINE_EXCEEDED,
        }:
            raise GmxMarketDiscoveryUnavailable(str(exc)) from exc
        raise ValueError(f"GMX market {market!r} was rejected by the gateway: {exc.details()}") from exc
    if not response.success:
        raise ValueError(response.error or f"GMX market {market!r} could not be resolved")
    item = response.market
    if not item.verified:
        raise ValueError(f"Gateway returned unverified GMX market metadata for {market!r}")
    if item.index_token_decimals < 0 or item.index_token_decimals > 30:
        raise ValueError(
            f"Gateway returned invalid index decimals {item.index_token_decimals} for GMX market {market!r}"
        )
    return ResolvedGmxMarket(
        label=item.label,
        market_token=item.market_token,
        index_token=item.index_token,
        index_symbol=item.index_symbol,
        index_token_decimals=item.index_token_decimals,
        long_token=item.long_token,
        long_token_symbol=item.long_token_symbol,
        short_token=item.short_token,
        short_token_symbol=item.short_token_symbol,
    )


__all__ = [
    "GmxMarketDiscoveryUnavailable",
    "GmxMarketNotFound",
    "ResolvedGmxMarket",
    "resolve_market_via_gateway",
]
