"""GeckoTerminal pool-history provider (POOL-5 / VIB-4753) — LAST RESORT, all resolutions.

Uses GeckoTerminal's OHLCV endpoint
``/networks/{network}/pools/{address}/ohlcv/{timeframe}`` with
``aggregate`` ∈ {1, 4} to cover 1h / 4h / 1d. OHLCV carries volume only —
TVL, fee revenue, and reserves are unmeasured on this series (Empty != Zero:
they stay ``""`` and are listed in ``unmeasured_fields``).

GeckoTerminal returns newest-first OHLCV lists ``[ts, open, high, low, close,
volume]``; this provider reverses them to ascending and maps ``volume`` to
``volume_24h``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import aiohttp

from almanak.gateway.proto import gateway_pb2

from ._base import (
    _CHAIN_TO_GT_NETWORK,
    _NOT_ATTEMPTED,
    ProviderResult,
    _ProviderError,
    _safe_decimal_str,
    _TokenBucket,
    build_unmeasured_fields,
)

logger = logging.getLogger(__name__)

_GT_API = "https://api.geckoterminal.com/api/v2"

#: GeckoTerminal OHLCV per-call row limit. Windows longer than this MUST be
#: fetched across multiple backward-paginated calls (audit blocker #1) — a
#: single call silently truncates to the most-recent 1000 bars.
_OHLCV_LIMIT = 1000

#: Defensive backward-pagination ceiling (mirrors TheGraph's ``_MAX_PAGES``) so
#: a pathological endpoint returning perpetual full pages can't loop forever.
#: 90d-1h = 2160 bars = 3 pages; this comfortably covers the soft caps.
_GT_MAX_PAGES = 100

#: Resolution -> (timeframe path segment, aggregate). GeckoTerminal exposes
#: ``hour`` + ``day`` timeframes; 4h is ``hour`` aggregated by 4.
_RESOLUTION_TO_OHLCV: dict[int, tuple[str, int]] = {
    gateway_pb2.Resolution.RESOLUTION_1H: ("hour", 1),
    gateway_pb2.Resolution.RESOLUTION_4H: ("hour", 4),
    gateway_pb2.Resolution.RESOLUTION_1D: ("day", 1),
}


class GeckoTerminalPoolHistoryProvider:
    """Last-resort pool-history provider backed by GeckoTerminal OHLCV."""

    name = "geckoterminal"

    def __init__(
        self,
        *,
        session_getter: Callable[[], Any],
        rate_limiter: _TokenBucket,
    ) -> None:
        self._session_getter = session_getter
        self._rate_limiter = rate_limiter

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
        network = _CHAIN_TO_GT_NETWORK.get(chain)
        if network is None:
            return _NOT_ATTEMPTED

        ohlcv_spec = _RESOLUTION_TO_OHLCV.get(resolution)
        if ohlcv_spec is None:
            return _NOT_ATTEMPTED
        timeframe, aggregate = ohlcv_spec

        # Fallback bucket empty -> local skip (only the PRIMARY raises).
        if not self._rate_limiter.acquire():
            logger.debug("GeckoTerminal rate-limit bucket empty; skipping this fetch")
            return _NOT_ATTEMPTED

        try:
            ohlcv = await self._query_ohlcv(
                network=network,
                pool_address=pool_address,
                timeframe=timeframe,
                aggregate=aggregate,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except (TimeoutError, aiohttp.ClientError, ValueError, _ProviderError) as exc:
            # ValueError covers json.JSONDecodeError on a 200-with-malformed-body:
            # a garbage upstream payload is a provider failure, not an unhandled
            # crash — map it into the _ProviderError taxonomy like any other.
            raise _ProviderError(f"geckoterminal: {exc}") from exc

        if ohlcv is None:
            return None  # 404 = not found, not a transport failure.

        snapshots = _ohlcv_to_snapshots(ohlcv, start_ts=start_ts, end_ts=end_ts, resolution=resolution)
        if not snapshots:
            return None
        return snapshots

    async def _query_ohlcv(
        self,
        *,
        network: str,
        pool_address: str,
        timeframe: str,
        aggregate: int,
        start_ts: int,
        end_ts: int,
    ) -> list[list[Any]] | None:
        """Fetch OHLCV across the whole window, paginating backward.

        GeckoTerminal returns up to ``_OHLCV_LIMIT`` candles newest-first,
        ending at ``before_timestamp``. A single call therefore silently
        truncates windows longer than 1000 bars (audit blocker #1) — so we
        page backward via ``before_timestamp`` until the window's ``start_ts``
        is covered, the endpoint runs out of data (short page), or the
        ``_GT_MAX_PAGES`` ceiling is hit. Returns ``None`` only when the pool
        is genuinely not found (404 on the first page) or no data exists.
        """
        session = await self._session_getter()
        url = f"{_GT_API}/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}"
        all_rows: list[list[Any]] = []
        before_timestamp = int(end_ts)
        for _page in range(_GT_MAX_PAGES):
            params = {
                "aggregate": str(aggregate),
                "limit": str(_OHLCV_LIMIT),
                "before_timestamp": str(before_timestamp),
            }
            async with session.get(url, params=params) as response:
                if response.status == 404:
                    # 404 on the first page = pool not found. On a later page
                    # (older data exhausted) keep what we already paged.
                    if not all_rows:
                        return None
                    break
                if response.status != 200:
                    text = await response.text()
                    raise _ProviderError(f"HTTP {response.status}: {text[:200]}")
                data = await response.json()
            if not isinstance(data, dict):
                break
            # Navigate defensively: any level may be present-but-null
            # (``{"data": null}`` / ``{"attributes": null}``), which would
            # otherwise raise AttributeError on the chained ``.get``.
            data_dict = data.get("data")
            attributes = data_dict.get("attributes") if isinstance(data_dict, dict) else None
            rows = attributes.get("ohlcv_list") if isinstance(attributes, dict) else None
            if not isinstance(rows, list) or not rows:
                break
            all_rows.extend(rows)
            oldest = _oldest_ts(rows)
            if oldest is None or oldest <= int(start_ts) or len(rows) < _OHLCV_LIMIT:
                break
            # Next page: candles strictly older than this page's oldest bar.
            before_timestamp = oldest
        return all_rows or None


def _ohlcv_to_snapshots(
    ohlcv: list[list[Any]],
    *,
    start_ts: int,
    end_ts: int,
    resolution: int,
) -> list[gateway_pb2.PoolSnapshot]:
    """Translate GeckoTerminal OHLCV rows to ``PoolSnapshot`` (ascending, windowed).

    Each OHLCV row is ``[timestamp, open, high, low, close, volume]``.
    GeckoTerminal returns newest-first; we reverse to ascending. Only
    ``volume`` is measured (-> ``volume_24h``); TVL / fee / reserves are
    unmeasured on the OHLCV series.
    """
    bucket = _resolution_seconds(resolution)
    # Dedup by aligned timestamp: backward pagination can overlap a bar at the
    # page boundary (``before_timestamp`` may be inclusive), and duplicate
    # timestamps would otherwise inflate the series.
    by_ts: dict[int, gateway_pb2.PoolSnapshot] = {}
    for row in ohlcv:
        if not isinstance(row, list | tuple) or len(row) < 6:
            continue
        try:
            timestamp = int(row[0])
        except (TypeError, ValueError):
            continue
        aligned = timestamp - (timestamp % bucket)
        if aligned < int(start_ts) or aligned >= int(end_ts):
            continue
        volume_24h = _safe_decimal_str(row[5])
        tvl = ""
        fee_revenue_24h = ""
        token0_reserve = ""
        token1_reserve = ""
        unmeasured = build_unmeasured_fields(
            tvl=tvl,
            volume_24h=volume_24h,
            fee_revenue_24h=fee_revenue_24h,
            token0_reserve=token0_reserve,
            token1_reserve=token1_reserve,
        )
        by_ts[aligned] = gateway_pb2.PoolSnapshot(
            timestamp=aligned,
            tvl=tvl,
            volume_24h=volume_24h,
            fee_revenue_24h=fee_revenue_24h,
            token0_reserve=token0_reserve,
            token1_reserve=token1_reserve,
            unmeasured_fields=unmeasured,
        )
    return [by_ts[ts] for ts in sorted(by_ts)]


def _oldest_ts(rows: list[list[Any]]) -> int | None:
    """Return the oldest (min) timestamp in a GeckoTerminal OHLCV page."""
    timestamps: list[int] = []
    for row in rows:
        if isinstance(row, list | tuple) and row:
            try:
                timestamps.append(int(row[0]))
            except (TypeError, ValueError):
                continue
    return min(timestamps) if timestamps else None


def _resolution_seconds(resolution: int) -> int:
    if resolution == gateway_pb2.Resolution.RESOLUTION_1H:
        return 3600
    if resolution == gateway_pb2.Resolution.RESOLUTION_4H:
        return 14400
    if resolution == gateway_pb2.Resolution.RESOLUTION_1D:
        return 86400
    raise ValueError(f"unsupported resolution: {resolution}")


__all__ = ["GeckoTerminalPoolHistoryProvider"]
