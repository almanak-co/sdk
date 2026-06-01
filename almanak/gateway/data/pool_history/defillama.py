"""DefiLlama pool-history provider (POOL-5 / VIB-4753) — FALLBACK, 1d ONLY.

DefiLlama's yields API serves daily TVL / volume time-series. It is the
secondary provider for ``1d`` requests and is **skipped entirely** for
sub-daily resolutions (``1h`` / ``4h``) — labeling daily data as 1h/4h
history would be a silent data-quality failure (UAT card D2.M3 /
``test_defillama_skipped_for_*``).

Two-step lookup:

1. Resolve the pool's DefiLlama ``pool`` id from the ``/pools`` catalog by
   **equality** on the address segment (``rsplit("-", 1)[-1]``) AND the
   chain display name AND (when given) the registry ``defillama_slug``
   project — NOT substring containment (decision #9 must-fix: the framework
   reader's ``history.py:584`` substring match is buggy and can collide a
   short prefix with an unrelated pool). The multi-MB ``/pools`` catalog is
   cached + in-flight-deduped (ported from ``pool_analytics_service``).
2. Fetch ``/chart/{pool_id}`` for the daily series and translate to
   ``PoolSnapshot`` with Empty != Zero decimals.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

import aiohttp

from almanak.gateway.proto import gateway_pb2

from ._base import (
    _CHAIN_TO_LLAMA_DISPLAY,
    _NOT_ATTEMPTED,
    ProviderResult,
    _ProviderError,
    _safe_decimal_str,
    _TokenBucket,
    build_unmeasured_fields,
    is_solana_family,
)

logger = logging.getLogger(__name__)

_YIELDS_API = "https://yields.llama.fi"

#: DefiLlama only carries DAILY series — provider is skipped for sub-daily.
_DEFILLAMA_RESOLUTION = gateway_pb2.Resolution.RESOLUTION_1D

#: Catalog TTL — the ``/pools`` listing is the whole DeFi-yield universe
#: (multi-MB). Cached so N cold callers don't each refetch it. Matches the
#: analytics-service catalog TTL.
_CATALOG_TTL_SECONDS = 60.0


class DefiLlamaPoolHistoryProvider:
    """1d-only fallback pool-history provider backed by DefiLlama yields."""

    name = "defillama"

    def __init__(
        self,
        *,
        session_getter: Callable[[], Any],
        slug_resolver: Callable[[str], str | None],
        rate_limiter: _TokenBucket,
    ) -> None:
        # ``session_getter`` is an async callable returning the shared
        # gateway ``aiohttp.ClientSession`` (mirrors analytics
        # ``_get_http_session``). ``slug_resolver(protocol) -> slug | None``
        # reads the registry ``GatewayDefillamaSlugCapability``.
        self._session_getter = session_getter
        self._slug_resolver = slug_resolver
        self._rate_limiter = rate_limiter
        self._catalog_cache: tuple[list[dict[str, Any]], float] | None = None
        self._catalog_inflight: asyncio.Task[list[dict[str, Any]]] | None = None
        self._cache_lock = threading.Lock()

    async def fetch(
        self,
        *,
        chain: str,
        pool_address: str,
        protocol: str,
        start_ts: int,
        end_ts: int,
        resolution: int,
    ) -> ProviderResult:
        if resolution != _DEFILLAMA_RESOLUTION:
            # Daily-only: do not even hit the catalog for 1h / 4h (the
            # dispatcher's eligibility table already excludes us, but this is
            # the defense-in-depth that makes test_defillama_skipped_* hold
            # even if a caller invokes us directly).
            return _NOT_ATTEMPTED

        llama_chain = _CHAIN_TO_LLAMA_DISPLAY.get(chain)
        if llama_chain is None:
            return _NOT_ATTEMPTED

        # Fallback bucket empty -> local skip (NOT an error; only the PRIMARY
        # raises on an empty bucket per decision #6).
        if not self._rate_limiter.acquire():
            logger.debug("DefiLlama rate-limit bucket empty; skipping this fetch")
            return _NOT_ATTEMPTED

        try:
            pools = await self._get_catalog()
        except (TimeoutError, aiohttp.ClientError, ValueError, _ProviderError) as exc:
            # ValueError covers json.JSONDecodeError on a 200-with-malformed-body:
            # a garbage upstream payload is a provider failure, not an unhandled
            # crash — map it into the _ProviderError taxonomy like any other.
            raise _ProviderError(f"defillama: {exc}") from exc

        pool_id = self._match_pool_id(
            pools,
            chain=chain,
            llama_chain=llama_chain,
            pool_address=pool_address,
            protocol=protocol,
        )
        if pool_id is None:
            return None  # reached upstream, no matching pool: not-found.

        try:
            chart = await self._query_chart(pool_id)
        except (TimeoutError, aiohttp.ClientError, ValueError, _ProviderError) as exc:
            # ValueError covers json.JSONDecodeError on a 200-with-malformed-body:
            # a garbage upstream payload is a provider failure, not an unhandled
            # crash — map it into the _ProviderError taxonomy like any other.
            raise _ProviderError(f"defillama: {exc}") from exc

        snapshots = _chart_to_snapshots(chart, start_ts=start_ts, end_ts=end_ts)
        if not snapshots:
            return None
        return snapshots

    # -- pool-id matching (equality, NOT substring) -----------------------

    def _match_pool_id(
        self,
        pools: list[dict[str, Any]],
        *,
        chain: str,
        llama_chain: str,
        pool_address: str,
        protocol: str,
    ) -> str | None:
        llama_chain_lower = llama_chain.lower()
        llama_project = self._slug_resolver(protocol) if protocol else None
        # EVM pool_address arrives already lowercased by the caller; Solana
        # retains case. DefiLlama lowercases EVM addresses in its pool ids
        # but preserves Solana case.
        target_address = pool_address if is_solana_family(chain) else pool_address.lower()
        for pool in pools:
            pool_id = str(pool.get("pool", ""))
            pool_chain = str(pool.get("chain", "")).lower()
            if pool_chain != llama_chain_lower:
                continue
            # Address segment: substring AFTER the last "-" for EVM-style ids
            # like "arbitrum-0xc6962...", or the whole id for Solana. EQUALITY
            # on the segment — a longer hex containing the requested address
            # as a substring must NOT match (decision #9 must-fix).
            address_segment = pool_id.rsplit("-", 1)[-1]
            if not is_solana_family(chain):
                address_segment = address_segment.lower()
            if address_segment != target_address:
                continue
            if llama_project and str(pool.get("project", "")).lower() != llama_project:
                # Protocol specified but this candidate is a different
                # project — keep looking; never silently merge projects.
                continue
            return pool_id
        return None

    # -- catalog cache (ported from pool_analytics_service) ---------------

    async def _get_catalog(self) -> list[dict[str, Any]]:
        with self._cache_lock:
            entry = self._catalog_cache
            if entry is not None and time.monotonic() - entry[1] <= _CATALOG_TTL_SECONDS:
                return entry[0]
            if self._catalog_inflight is None or self._catalog_inflight.done():
                self._catalog_inflight = asyncio.create_task(self._refresh_catalog())
            inflight = self._catalog_inflight
        return await inflight

    async def _refresh_catalog(self) -> list[dict[str, Any]]:
        try:
            pools = await self._query_pools()
        except BaseException:
            with self._cache_lock:
                self._catalog_inflight = None
            raise
        with self._cache_lock:
            self._catalog_cache = (pools, time.monotonic())
            self._catalog_inflight = None
        return pools

    async def _query_pools(self) -> list[dict[str, Any]]:
        session = await self._session_getter()
        url = f"{_YIELDS_API}/pools"
        async with session.get(url) as response:
            if response.status != 200:
                text = await response.text()
                raise _ProviderError(f"HTTP {response.status}: {text[:200]}")
            data = await response.json()
            # A present-but-null "data" (``{"data": null}``) must coerce to []:
            # ``.get("data", [])`` would return None and crash the downstream
            # iteration with a TypeError.
            result = data.get("data") if isinstance(data, dict) else None
            return result if isinstance(result, list) else []

    async def _query_chart(self, pool_id: str) -> list[dict[str, Any]]:
        session = await self._session_getter()
        url = f"{_YIELDS_API}/chart/{pool_id}"
        async with session.get(url) as response:
            if response.status == 404:
                return []
            if response.status != 200:
                text = await response.text()
                raise _ProviderError(f"HTTP {response.status}: {text[:200]}")
            data = await response.json()
            # A present-but-null "data" (``{"data": null}``) must coerce to []:
            # ``.get("data", [])`` would return None and crash the downstream
            # iteration with a TypeError.
            result = data.get("data") if isinstance(data, dict) else None
            return result if isinstance(result, list) else []


def _chart_to_snapshots(
    chart: list[dict[str, Any]],
    *,
    start_ts: int,
    end_ts: int,
) -> list[gateway_pb2.PoolSnapshot]:
    """Translate the DefiLlama ``/chart`` daily series to ``PoolSnapshot`` rows.

    Each chart point has a ``timestamp`` (ISO-8601 or unix) + ``tvlUsd`` +
    ``volumeUsd1d`` (when present). Rows are aligned to the 1d grid
    (``timestamp - timestamp % 86400``), filtered to the half-open
    ``[start_ts, end_ts)`` window, and ordered ascending. Reserves + fee
    revenue are unmeasured on this series.
    """
    snapshots: list[gateway_pb2.PoolSnapshot] = []
    for point in chart:
        timestamp = _parse_chart_timestamp(point.get("timestamp"))
        if timestamp is None:
            continue
        # Align to the daily grid (DefiLlama timestamps are end-of-day-ish).
        aligned = timestamp - (timestamp % 86400)
        if aligned < int(start_ts) or aligned >= int(end_ts):
            continue
        tvl = _safe_decimal_str(point.get("tvlUsd"))
        volume_24h = _safe_decimal_str(point.get("volumeUsd1d"))
        fee_revenue_24h = ""  # DefiLlama chart carries no daily fee revenue.
        token0_reserve = ""
        token1_reserve = ""
        unmeasured = build_unmeasured_fields(
            tvl=tvl,
            volume_24h=volume_24h,
            fee_revenue_24h=fee_revenue_24h,
            token0_reserve=token0_reserve,
            token1_reserve=token1_reserve,
        )
        snapshots.append(
            gateway_pb2.PoolSnapshot(
                timestamp=aligned,
                tvl=tvl,
                volume_24h=volume_24h,
                fee_revenue_24h=fee_revenue_24h,
                token0_reserve=token0_reserve,
                token1_reserve=token1_reserve,
                unmeasured_fields=unmeasured,
            )
        )
    snapshots.sort(key=lambda s: s.timestamp)
    return snapshots


def _parse_chart_timestamp(value: Any) -> int | None:
    """Parse a DefiLlama chart timestamp (ISO-8601 string or unix int) to unix seconds."""
    if value is None:
        return None
    if isinstance(value, int | float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    # Unix-seconds string.
    try:
        return int(text)
    except ValueError:
        pass
    # ISO-8601 (e.g. "2024-01-01T00:00:00.000Z").
    from datetime import datetime

    try:
        normalized = text.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalized).timestamp())
    except ValueError:
        logger.debug("DefiLlama: dropping chart point with unparseable timestamp %r", value)
        return None


__all__ = ["DefiLlamaPoolHistoryProvider"]
