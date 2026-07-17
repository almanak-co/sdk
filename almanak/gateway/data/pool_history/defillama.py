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

UUID-id catalog reality (ALM-2940): every ``pool`` id in today's live
``/pools`` catalog is an opaque UUID — the ``chain-0xaddress`` id style the
address-segment matcher was written against no longer appears at all (0 of
~15.4k entries as of 2026-07-15), so step 1 alone is an always-miss. The
matcher therefore falls back to **token-set matching**: resolve the pool's
underlying token addresses via the caller-supplied ``pool_token_resolver``
(the dispatcher backs it with one CoinGecko Onchain pool-info call) and
match catalog entries on (project slug, chain, exact ``underlyingTokens``
set). HONESTY CAVEAT: the free-tier yields catalog exposes no pool address
for UUID-id entries (the address-bearing ``/poolsOld`` endpoint is
paid-tier), so a token-set match asserts pool identity WITHOUT address-level
verification. The guardrails: a registry project slug is REQUIRED (no
cross-project token matching), the token set must match exactly, and the
match is refused when more than one candidate survives (e.g. multiple
Liquidity Book bin-steps of the same pair) — ambiguity yields not-found,
never a guess.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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


@dataclass(frozen=True)
class ResolvedPoolIdentity:
    """A pool's on-chain identity as resolved by the dispatcher's token resolver.

    ``tokens`` is the underlying token-address set (chain-aware casing).
    ``reserve_usd`` is the pool's LIVE reserve from the same CoinGecko
    Onchain pool-info response (``None`` when unmeasured) — used as the
    measured cross-check when several catalog entries share a token set
    (e.g. a Solidly pair's volatile + stable twins).
    """

    tokens: frozenset[str]
    reserve_usd: Decimal | None


#: TVL-consistency band for multi-candidate disambiguation: a candidate is
#: consistent when ``catalog tvlUsd / live reserve_usd`` falls in
#: ``[0.5, 2.0]``. Catalog TVL is refreshed daily and the live reserve moves
#: with prices, so same-day drift beyond 2x means "different pool" far more
#: often than "volatile day"; the aerodrome-v1 WETH-USDC twins this exists
#: for differ by ~300x ($7.6M vAMM vs $24k sAMM).
_TVL_CONSISTENCY_MIN_RATIO = 0.5
_TVL_CONSISTENCY_MAX_RATIO = 2.0


class DefiLlamaPoolHistoryProvider:
    """1d-only fallback pool-history provider backed by DefiLlama yields."""

    name = "defillama"

    def __init__(
        self,
        *,
        session_getter: Callable[[], Any],
        slug_resolver: Callable[[str], str | None],
        rate_limiter: _TokenBucket,
        pool_token_resolver: Callable[[str, str], Awaitable[ResolvedPoolIdentity | None]] | None = None,
    ) -> None:
        # ``session_getter`` is an async callable returning the shared
        # gateway ``aiohttp.ClientSession`` (mirrors analytics
        # ``_get_http_session``). ``slug_resolver(protocol) -> slug | None``
        # reads the registry ``GatewayDefillamaSlugCapability``.
        # ``pool_token_resolver(chain, pool_address)`` is an async callable
        # returning the pool's resolved token identity
        # (``ResolvedPoolIdentity | None``); the dispatcher backs it with a
        # CoinGecko Onchain pool-info lookup. ``None`` (default) disables
        # the UUID-id token-set match path — the legacy address-segment
        # match still runs.
        self._session_getter = session_getter
        self._slug_resolver = slug_resolver
        self._rate_limiter = rate_limiter
        self._pool_token_resolver = pool_token_resolver
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
            # UUID-id catalog fallback (ALM-2940): the address-segment match
            # cannot hit an opaque-UUID pool id; try token-set matching.
            pool_id = await self._match_pool_id_by_tokens(
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

    # -- UUID-id token-set matching (ALM-2940) -----------------------------

    async def _match_pool_id_by_tokens(
        self,
        pools: list[dict[str, Any]],
        *,
        chain: str,
        llama_chain: str,
        pool_address: str,
        protocol: str,
    ) -> str | None:
        """Match a UUID-id catalog entry by (project, chain, underlying-token set).

        The live yields catalog exposes no pool address on UUID-id entries
        (see module docstring), so identity is asserted via the underlying
        token addresses resolved from the pool contract by
        ``pool_token_resolver``. Guardrails, in order:

        * a registry project slug is REQUIRED — matching token pairs across
          all of DeFi would collide on every popular pair;
        * the resolver must return the full token set (both sides of the
          pair; ``None`` on any failure means no match, never a guess);
        * the entry's ``underlyingTokens`` must equal the resolved set
          EXACTLY (chain-aware casing: EVM lowercased, Solana preserved);
        * ONE surviving candidate matches on the token set alone; SEVERAL
          candidates (a Solidly pair's volatile + stable twins, multiple
          Liquidity Book bin-steps) are disambiguated by the MEASURED
          TVL-consistency cross-check — the candidate whose catalog
          ``tvlUsd`` is consistent with the pool's live ``reserve_usd``
          (from the same pool-info response that resolved the tokens) wins,
          and the match is refused unless exactly one candidate is
          consistent. No reserve measurement + several candidates refuses —
          ambiguity is not-found, never "pick one".
        """
        if self._pool_token_resolver is None:
            return None
        llama_project = self._slug_resolver(protocol) if protocol else None
        if not llama_project:
            return None
        try:
            identity = await self._pool_token_resolver(chain, pool_address)
        except Exception as exc:  # noqa: BLE001 — assist path; a resolver crash must not abort the provider chain
            logger.debug("DefiLlama token-set match: resolver failed for %s/%s: %s", chain, pool_address, exc)
            return None
        if identity is None or not identity.tokens or len(identity.tokens) < 2:
            # Single-token / unresolved pools are unmatchable: one token
            # address matches every pool that includes the token.
            return None

        solana = is_solana_family(chain)
        llama_chain_lower = llama_chain.lower()
        candidates: list[dict[str, Any]] = []
        for pool in pools:
            if str(pool.get("chain", "")).lower() != llama_chain_lower:
                continue
            if str(pool.get("project", "")).lower() != llama_project:
                continue
            underlying = pool.get("underlyingTokens")
            if not isinstance(underlying, list) or not underlying:
                continue
            entry_tokens = frozenset(str(t) if solana else str(t).lower() for t in underlying if t)
            if entry_tokens != identity.tokens:
                continue
            if str(pool.get("pool", "")):
                candidates.append(pool)

        # Cross-check catalog TVL against the live reserve whenever one is
        # available — even for a LONE candidate. The free-tier catalog omits
        # many pools, so a same-token-set SIBLING can be the only listed pool
        # while the strategy's actual pool is absent; returning the sibling's
        # history on token-set alone is a silent wrong-pool match under a
        # measured-looking MEDIUM label. With no reserve we cannot cross-check:
        # a lone candidate then stands on token-set alone (the documented
        # address-less caveat), but multiple candidates still refuse.
        has_reserve = identity.reserve_usd is not None and identity.reserve_usd > 0
        if has_reserve or len(candidates) > 1:
            candidates = self._tvl_consistent_candidates(candidates, identity, chain, pool_address, llama_project)

        if len(candidates) == 1:
            pool_id = str(candidates[0].get("pool", ""))
            logger.debug(
                "DefiLlama token-set match: %s/%s -> %s (project=%s)",
                chain,
                pool_address,
                pool_id,
                llama_project,
            )
            return pool_id
        return None

    def _tvl_consistent_candidates(
        self,
        candidates: list[dict[str, Any]],
        identity: ResolvedPoolIdentity,
        chain: str,
        pool_address: str,
        llama_project: str,
    ) -> list[dict[str, Any]]:
        """Narrow same-token-set candidates by catalog-TVL vs live-reserve consistency.

        Returns the consistent subset; the caller matches only when exactly
        one survives. With no reserve measurement every candidate is
        inconsistent (empty list) — refusing beats guessing.
        """
        if identity.reserve_usd is None or identity.reserve_usd <= 0:
            logger.info(
                "DefiLlama token-set match refused for %s/%s: %d candidates in project %s share the "
                "token set and no live reserve measurement is available to disambiguate",
                chain,
                pool_address,
                len(candidates),
                llama_project,
            )
            return []
        consistent: list[dict[str, Any]] = []
        for pool in candidates:
            try:
                tvl = Decimal(str(pool.get("tvlUsd")))
            except (InvalidOperation, ValueError, TypeError):
                continue
            if not tvl.is_finite() or tvl <= 0:
                continue
            ratio = float(tvl / identity.reserve_usd)
            if _TVL_CONSISTENCY_MIN_RATIO <= ratio <= _TVL_CONSISTENCY_MAX_RATIO:
                consistent.append(pool)
        if len(consistent) != 1:
            logger.info(
                "DefiLlama token-set match refused for %s/%s: %d of %d same-token-set candidates in "
                "project %s are TVL-consistent with the live reserve ($%s) — need exactly 1",
                chain,
                pool_address,
                len(consistent),
                len(candidates),
                llama_project,
                identity.reserve_usd,
            )
        return consistent

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
