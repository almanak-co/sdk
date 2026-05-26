"""PoolAnalyticsService implementation - off-chain pool analytics (VIB-4727).

Server-side handler that owns the HTTP egress to DefiLlama (primary) and
GeckoTerminal (fallback). The framework-side ``PoolAnalyticsReader`` is a
thin gRPC client that calls into this service via the gateway sidecar —
strategies in production containers never see ``aiohttp``.

Input validation:
- ``pool_address`` is chain-validated at the entry. EVM expects
  ``^0x[0-9a-f]{40}$``; Solana expects base58 32-44 chars with case
  preserved (lower-casing a Solana address yields a different address).
- ``chain`` must be in the supported chain map. Empty / whitespace-only
  ``pool_address`` or ``chain`` returns INVALID_ARGUMENT.
- DefiLlama matcher uses equality on the address segment of the pool id
  (the segment after the chain-name prefix), NOT substring containment,
  so a short attacker-controlled prefix can't match an unrelated pool.

Error semantics are dual-channel (mirrors ``FundingRateService``):

- status ``OK`` + ``success=True``  → fresh data.
- status ``OK`` + ``success=False`` → degraded / stale data (rare; framework
  may still HOLD on this; not used in v1 — kept for forward-compat).
- non-OK status (``UNAVAILABLE`` / ``INVALID_ARGUMENT`` / ``DEADLINE_EXCEEDED``
  / ``PERMISSION_DENIED``) → framework raises ``DataSourceUnavailable``.

State / scope:

- Mid-flight termination is safe: this RPC writes nothing to
  ``transaction_ledger`` / ``position_events`` / ``portfolio_snapshots``.
- No mode-aware writes (no ``live`` vs ``paper`` divergence).
- Cache is in-memory only on the gateway — no Postgres (the deployed
  Postgres schema is owned by the ``metrics-database`` repo per AGENTS.md
  "Database schema ownership" rule).
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp
import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# =============================================================================
# Provider configuration
# =============================================================================

_YIELDS_API = "https://yields.llama.fi"
_GT_API = "https://api.geckoterminal.com/api/v2"

# Chain → GeckoTerminal network slug
_CHAIN_TO_GT_NETWORK: dict[str, str] = {
    "ethereum": "eth",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon_pos",
    "avalanche": "avax",
    "bsc": "bsc",
    "sonic": "sonic",
    "solana": "solana",
}

# Chain → DefiLlama display name (DefiLlama uses capitalized names)
_CHAIN_TO_LLAMA_DISPLAY: dict[str, str] = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum",
    "base": "Base",
    "optimism": "Optimism",
    "polygon": "Polygon",
    "avalanche": "Avalanche",
    "bsc": "BSC",
    "sonic": "Sonic",
    "solana": "Solana",
}

# Protocol hint → DefiLlama project slug — registry-driven (VIB-4811 / VIB-4817).
#
# Phase 3 (VIB-4811) replaces the hardcoded dispatch dict with a
# derivation from ``GATEWAY_REGISTRY.capability_providers(
# GatewayDefillamaSlugCapability)``. Each connector declares its own
# slug (and any alias variants like ``aerodrome_slipstream``); this
# module composes them at import time. The dispatcher uses the result
# identically to the previous static dict.
#
# VIB-4817 migrated the last two TODO-fallback entries
# (``pancakeswap_v3`` and ``morpho``) onto their connectors —
# ``PancakeSwapV3GatewayConnector`` and ``MorphoVaultGatewayConnector``
# now publish those slugs directly.


def _build_protocol_to_llama() -> dict[str, str]:
    """Compose ``protocol -> defillama_slug`` from the registry.

    Iterates every ``GatewayDefillamaSlugCapability`` provider once and
    unions their canonical slug + alias entries.
    """
    from almanak.connectors._base.gateway_capabilities import (
        GatewayDefillamaSlugCapability,
    )
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    table: dict[str, str] = {}
    # mypy: ``capability_providers`` accepts a ``@runtime_checkable`` Protocol
    # by design — see ``_derive_pool_history_tables`` in
    # ``pool_history_service.py`` for the rationale.
    for connector in GATEWAY_REGISTRY.capability_providers(GatewayDefillamaSlugCapability):  # type: ignore[type-abstract]
        slug = connector.defillama_slug()
        if slug is not None:
            # Normalize protocol key to lowercase — request validation
            # already lowercases ``protocol``; mixed-case connector
            # names would silently miss the lookup. (Gemini code-review.)
            table[str(connector.protocol).lower()] = slug  # type: ignore[attr-defined]
        # Aliases (e.g. ``aerodrome_slipstream`` rides aerodrome,
        # ``morpho`` rides morpho_vault for the morpho-blue project).
        for alias_key, alias_slug in connector.defillama_slug_aliases().items():
            table[alias_key.lower()] = alias_slug
    return table


class _LazyProtocolToLlama(dict[str, str]):
    """Lazy proxy for ``_PROTOCOL_TO_LLAMA`` — built on first access.

    Eager build at module import races against ``_gateway_registry``
    when an entry point lands on ``_gateway_registry`` first and the
    aave_v3 / etc. provider modules transitively pull in
    ``gateway.services.__init__`` (which loads this module) before
    registration finishes.
    """

    __slots__ = ("_built",)

    def __init__(self) -> None:
        super().__init__()
        self._built = False

    def _ensure_built(self) -> None:
        if not self._built:
            super().update(_build_protocol_to_llama())
            self._built = True

    def __contains__(self, key: object) -> bool:
        self._ensure_built()
        return super().__contains__(key)

    def __iter__(self):
        self._ensure_built()
        return super().__iter__()

    def __len__(self) -> int:
        self._ensure_built()
        return super().__len__()

    def __getitem__(self, key: str) -> str:
        self._ensure_built()
        return super().__getitem__(key)

    def __eq__(self, other: object) -> bool:
        self._ensure_built()
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        self._ensure_built()
        return super().__ne__(other)

    def __hash__(self) -> int:  # type: ignore[override]
        raise TypeError("unhashable type: '_LazyProtocolToLlama'")

    def keys(self):
        self._ensure_built()
        return super().keys()

    def values(self):
        self._ensure_built()
        return super().values()

    def items(self):
        self._ensure_built()
        return super().items()

    def get(self, key, default=None):
        self._ensure_built()
        return super().get(key, default)


_PROTOCOL_TO_LLAMA: dict[str, str] = _LazyProtocolToLlama()

# Cache TTL (seconds) — 60s strikes a balance between provider load and
# strategy iteration cadence. Iteration loops typically tick every ~60s so
# a cache hit on the second iteration is what makes per-strategy load
# bearable for the upstream APIs.
_CACHE_TTL_SECONDS = 60.0

# Bound the per-pool caches so unique-key traffic over long uptime can't
# leak memory. 5000 keys × ~250 bytes/record ≈ 1.25 MB worst-case per
# cache. CodeRabbit PR #2389 review thread.
_CACHE_MAX_ENTRIES = 5000

# DefiLlama pools listing rate: 10 req/s per IP per their public docs.
_DEFILLAMA_RATE_PER_S = 10
# GeckoTerminal public tier: 30 req/min.
_GECKOTERMINAL_RATE_PER_MIN = 30

# Address validation regexes — chain-aware. EVM is case-insensitive hex;
# Solana is base58 (case-sensitive) — lower-casing it produces a different
# address.
_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
# Solana base58 alphabet excludes 0, O, I, l. Mint / pool addresses are
# 32-44 chars.
_SOLANA_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def _normalize_pool_address(address: str, chain: str) -> str:
    """Chain-aware address normalization.

    EVM addresses are case-insensitive → lowercased for cache-key stability.
    Solana base58 addresses are case-sensitive → preserve original case.
    Mirrors ``almanak.framework.data.tokens.resolver._normalize_address_for_chain``.
    """
    address = address.strip()
    if chain == "solana":
        return address
    return address.lower()


def _validate_pool_address(address: str, chain: str) -> bool:
    """Return True when ``address`` is a syntactically valid pool address for ``chain``.

    Rejecting malformed input here means the upstream URL (GeckoTerminal
    embeds the address) can never carry an attacker-supplied path / query
    segment.
    """
    if chain == "solana":
        return bool(_SOLANA_BASE58_RE.match(address))
    return bool(_EVM_ADDRESS_RE.match(address))


# Single-key cache for the full DefiLlama catalog. /pools returns the
# whole DeFi-yield universe (multi-MB). Per-pool callers shouldn't each
# trigger a full fetch; we cache the catalog for the same TTL as the
# parsed-record cache. Important #4 from the multi-auditor review on PR
# #2389.
_LLAMA_CATALOG_CACHE_KEY = "__defillama_catalog__"


# =============================================================================
# Decimal helpers — keep "empty ≠ zero" semantics (AGENTS.md Accounting)
# =============================================================================


def _safe_decimal_str(value: Any) -> str:
    """Convert ``value`` to a decimal-as-string, or empty string when unmeasured.

    `None` → ``""`` (unmeasured, per AGENTS.md "Empty ≠ Zero").
    Numeric ``0`` / ``"0"`` → ``"0"`` (measured zero).
    Anything that doesn't parse cleanly → ``""`` plus a debug log; never
    silently substitutes zero.
    """
    if value is None:
        return ""
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        logger.debug("pool_analytics: dropped unparseable decimal %r", value)
        return ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce to ``float`` clamping NaN/Inf; used for percentages."""
    if value is None:
        return default
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default


# =============================================================================
# Token-bucket rate limiter — preserves per-provider isolation
# =============================================================================


class _TokenBucket:
    """Thread-safe token bucket.

    Lifted from the legacy framework reader (VIB-4727: the legacy reader
    becomes a thin gRPC client; this bucket migrates with the egress).
    """

    def __init__(self, rate: int, period: float = 1.0) -> None:
        self._rate = rate
        self._period = period
        self._tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                float(self._rate),
                self._tokens + elapsed * (self._rate / self._period),
            )
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


# =============================================================================
# Provider health metrics
# =============================================================================


@dataclass
class _ProviderMetrics:
    successes: int = 0
    failures: int = 0


# =============================================================================
# Internal pool analytics record (gateway-side; not exposed to strategy)
# =============================================================================


@dataclass(frozen=True)
class _PoolAnalyticsRecord:
    pool_address: str
    chain: str
    protocol: str
    tvl_usd: str = ""
    volume_24h_usd: str = ""
    volume_7d_usd: str = ""
    fee_apr: str = ""
    fee_apy: str = ""
    utilization_rate: str = ""
    token0_weight: str = ""
    token1_weight: str = ""
    source: str = ""
    observed_at: int = 0
    is_live_data: bool = True


@dataclass
class _CacheEntry:
    record: _PoolAnalyticsRecord
    cached_at: float = field(default_factory=time.monotonic)


class _NotAttempted:
    """Sentinel: a provider was skipped locally (e.g. rate-limit bucket empty).

    Distinct from ``None`` (fetched, no match found) and from
    ``_ProviderError`` (transport / upstream failure). CodeRabbit
    PR #2389 review thread.
    """


_NOT_ATTEMPTED = _NotAttempted()


# =============================================================================
# Servicer
# =============================================================================


class PoolAnalyticsServiceServicer(gateway_pb2_grpc.PoolAnalyticsServiceServicer):
    """gRPC servicer for pool analytics.

    Cache layout (split per UAT card D2.M4):
    - ``_public_cache`` keyed by ``(chain, pool_address, protocol_or_empty)`` —
      what callers ask for. Hit-rate metric is on this.
    - ``_raw_cache`` keyed by ``(chain, pool_address, protocol_or_empty, provider)`` —
      the raw provider payload; partitioning prevents shape-collision when
      the fallback path retries.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None
        self._rate_limiter_llama = _TokenBucket(rate=_DEFILLAMA_RATE_PER_S, period=1.0)
        self._rate_limiter_gt = _TokenBucket(rate=_GECKOTERMINAL_RATE_PER_MIN, period=60.0)
        self._metrics: dict[str, _ProviderMetrics] = {
            "defillama": _ProviderMetrics(),
            "geckoterminal": _ProviderMetrics(),
        }
        self._public_cache: dict[tuple[str, str, str], _CacheEntry] = {}
        self._raw_cache: dict[tuple[str, str, str, str], _CacheEntry] = {}
        # DefiLlama full-catalog cache: ``(pools_list, cached_at_monotonic)``.
        # Shared across all per-pool callers so we don't refetch the
        # multi-MB /pools response on every cache miss (Important #4 from
        # the multi-auditor review on PR #2389).
        self._catalog_cache: tuple[list[dict[str, Any]], float] | None = None
        # Single in-flight catalog refresh task — multiple concurrent
        # cold-cache callers share one upstream fetch instead of each
        # firing their own (the lock guards entry into the fetch; the
        # task itself runs unlocked so other readers don't block).
        # CodeRabbit PR #2389 review thread.
        self._catalog_inflight: asyncio.Task[list[dict[str, Any]]] | None = None
        self._cache_lock = threading.Lock()
        logger.debug("Initialized PoolAnalyticsService")

    # -- HTTP session -----------------------------------------------------

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15.0),
                connector=connector,
                headers={"Accept": "application/json"},
            )
        return self._http_session

    async def close(self) -> None:
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None
            logger.info("PoolAnalyticsService closed")

    # -- Health --------------------------------------------------------

    def health(self) -> dict[str, dict[str, int]]:
        """Per-provider success/failure counters. Exposed for tests and ops."""
        return {name: {"successes": m.successes, "failures": m.failures} for name, m in self._metrics.items()}

    # -- gRPC entry point --------------------------------------------

    async def GetPoolAnalytics(
        self,
        request: gateway_pb2.PoolAnalyticsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PoolAnalyticsResponse:
        chain = request.chain.strip().lower()
        protocol = request.protocol.strip().lower()
        # Chain-aware normalize: EVM lowercase, Solana preserve case
        # (base58 is case-sensitive — lowercasing yields a different address).
        pool_address = _normalize_pool_address(request.pool_address, chain)

        if not pool_address or not chain:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("pool_address and chain are required")
            return gateway_pb2.PoolAnalyticsResponse(
                pool_address=pool_address,
                chain=chain,
                protocol=protocol,
                success=False,
                error="pool_address and chain are required",
            )

        if chain not in _CHAIN_TO_LLAMA_DISPLAY and chain not in _CHAIN_TO_GT_NETWORK:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"unsupported chain: {chain}")
            return gateway_pb2.PoolAnalyticsResponse(
                pool_address=pool_address,
                chain=chain,
                protocol=protocol,
                success=False,
                error=f"unsupported chain: {chain}",
            )

        # Syntactic address validation prevents the GeckoTerminal URL
        # template (line ~470) from carrying attacker-supplied path / query
        # segments, and stops the DefiLlama equality matcher from running
        # on garbage input.
        if not _validate_pool_address(pool_address, chain):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            details = f"invalid pool_address for chain {chain!r}: {pool_address!r}"
            context.set_details(details)
            return gateway_pb2.PoolAnalyticsResponse(
                pool_address=pool_address,
                chain=chain,
                protocol=protocol,
                success=False,
                error=details,
            )

        public_key = (chain, pool_address, protocol)
        cached = self._cache_get_public(public_key)
        if cached is not None:
            return self._record_to_response(cached, is_live_data=False)

        errors: list[str] = []

        # DefiLlama first.
        try:
            record = await self._fetch_from_defillama(chain, pool_address, protocol)
            if isinstance(record, _NotAttempted):
                # Local rate-limit skip — NOT an upstream failure and NOT
                # a "not found" miss. Don't pollute the error string with
                # "defillama: not found" (that would mislead operators
                # investigating UNAVAILABLE responses). CodeRabbit
                # PR #2389 review thread.
                logger.debug("DefiLlama skipped (local rate limit) for %s on %s", pool_address, chain)
            elif record is not None:
                self._metrics["defillama"].successes += 1
                self._cache_put(public_key, "defillama", record)
                return self._record_to_response(record, is_live_data=True)
            else:
                errors.append("defillama: not found")
        except _ProviderError as e:
            self._metrics["defillama"].failures += 1
            errors.append(f"defillama: {e}")
            logger.debug("DefiLlama pool analytics failed for %s on %s: %s", pool_address, chain, e)

        # GeckoTerminal fallback.
        try:
            record = await self._fetch_from_geckoterminal(chain, pool_address, protocol)
            if record is not None:
                self._metrics["geckoterminal"].successes += 1
                self._cache_put(public_key, "geckoterminal", record)
                return self._record_to_response(record, is_live_data=True)
            errors.append("geckoterminal: not found")
        except _ProviderError as e:
            self._metrics["geckoterminal"].failures += 1
            errors.append(f"geckoterminal: {e}")
            logger.debug(
                "GeckoTerminal pool analytics failed for %s on %s: %s",
                pool_address,
                chain,
                e,
            )

        # Both providers exhausted. Hard-fail with UNAVAILABLE so the
        # framework raises DataSourceUnavailable. Per D3.F6: NEVER return a
        # success=True envelope with empty/zero analytics.
        joined = "; ".join(errors) or "all providers exhausted"
        logger.warning(
            "PoolAnalytics not found for %s on %s (protocol=%s): %s",
            pool_address,
            chain,
            protocol or "unspecified",
            joined,
        )
        context.set_code(grpc.StatusCode.UNAVAILABLE)
        context.set_details(joined)
        return gateway_pb2.PoolAnalyticsResponse(
            pool_address=pool_address,
            chain=chain,
            protocol=protocol,
            success=False,
            error=joined,
        )

    # -- DefiLlama provider --------------------------------------------------

    async def _fetch_from_defillama(
        self,
        chain: str,
        pool_address: str,
        protocol: str,
    ) -> _PoolAnalyticsRecord | _NotAttempted | None:
        # Local rate-limit hit is NOT an upstream failure AND NOT a
        # "not found" miss — return the ``_NOT_ATTEMPTED`` sentinel so
        # the caller (`GetPoolAnalytics`) can distinguish it from the
        # legitimate "fetched-and-no-match" case (``None``) and the
        # transport-failure case (``_ProviderError``). Health metrics
        # don't spuriously spike DefiLlama failures. CodeRabbit
        # PR #2389 review threads (Important #5 + the local-skip
        # collapse follow-up).
        if not self._rate_limiter_llama.acquire():
            logger.debug("DefiLlama rate-limit bucket empty; skipping this fetch")
            return _NOT_ATTEMPTED

        llama_chain = _CHAIN_TO_LLAMA_DISPLAY.get(chain)
        if llama_chain is None:
            raise _ProviderError(f"unsupported chain: {chain}")

        try:
            pools = await self._get_defillama_catalog()
        except (TimeoutError, aiohttp.ClientError, _ProviderError) as e:
            raise _ProviderError(str(e)) from e

        # For EVM chains, DefiLlama's `pool` id format is
        # `"<chain>-<pool_address>"` (lowercase address). For Solana it's
        # the base58 address as-is. Match the ADDRESS SEGMENT explicitly
        # (split on the last "-") rather than substring containment — a
        # short attacker-controlled prefix could otherwise collide with an
        # unrelated pool's id. Important #3 / #6 from the multi-auditor
        # review on PR #2389.
        llama_chain_lower = llama_chain.lower()
        llama_project = _PROTOCOL_TO_LLAMA.get(protocol) if protocol else None
        # EVM pool_address is already lowercase via _normalize_pool_address;
        # Solana retains case. DefiLlama lowercases EVM addresses in its
        # pool ids but preserves Solana case.
        target_address = pool_address if chain == "solana" else pool_address.lower()
        match: dict[str, Any] | None = None
        for pool in pools:
            pool_id = str(pool.get("pool", ""))
            pool_chain = str(pool.get("chain", "")).lower()
            if pool_chain != llama_chain_lower:
                continue
            # Address segment: substring AFTER the last "-" for EVM-style
            # ids like "arbitrum-0xc6962...", or the whole id for Solana.
            address_segment = pool_id.rsplit("-", 1)[-1]
            if chain != "solana":
                address_segment = address_segment.lower()
            if address_segment != target_address:
                continue
            if llama_project and str(pool.get("project", "")).lower() != llama_project:
                # Protocol was specified but this candidate is from a
                # different project — keep looking; do not silently merge.
                continue
            match = pool
            break

        if match is None:
            return None  # Caller treats None as "not found", not as failure.

        return _parse_llama_pool(match, pool_address, chain, protocol or str(match.get("project", "")))

    async def _get_defillama_catalog(self) -> list[dict[str, Any]]:
        """Return the DefiLlama /pools catalog, cached for ``_CACHE_TTL_SECONDS``.

        DefiLlama's /pools endpoint returns the entire DeFi-yield universe
        (multi-MB JSON, tens of thousands of pools). Without catalog-level
        caching, every per-pool cache miss would re-fetch it, which blows
        the upstream rate limit when N strategies poll their pools every
        TTL window. The per-pool ``_PoolAnalyticsRecord`` cache and this
        catalog cache share the same TTL.

        Cold-cache concurrency: when N callers race into an empty/expired
        catalog, only ONE upstream fetch runs — the others await the
        same in-flight task. The lock guards entry into the fetch; the
        task itself runs unlocked so other readers don't block. CodeRabbit
        PR #2389 review thread (cold-cache fanout).
        """
        with self._cache_lock:
            entry = self._catalog_cache
            if entry is not None and time.monotonic() - entry[1] <= _CACHE_TTL_SECONDS:
                return entry[0]
            # Cache cold/expired — either join the in-flight fetch or
            # create one. The lock is dropped before await so other
            # callers can also see and join the same task.
            if self._catalog_inflight is None or self._catalog_inflight.done():
                self._catalog_inflight = asyncio.create_task(self._refresh_catalog())
            inflight = self._catalog_inflight
        return await inflight

    async def _refresh_catalog(self) -> list[dict[str, Any]]:
        """Fetch the DefiLlama catalog and update the cache.

        Wrapped so all callers awaiting ``_catalog_inflight`` see the
        same return value (or exception). On success the cache is
        repopulated; on failure the in-flight slot is cleared so the
        next caller retries instead of being permanently wedged.
        """
        try:
            pools = await self._query_defillama_pools()
        except BaseException:
            with self._cache_lock:
                self._catalog_inflight = None
            raise
        with self._cache_lock:
            self._catalog_cache = (pools, time.monotonic())
            self._catalog_inflight = None
        return pools

    async def _query_defillama_pools(self) -> list[dict[str, Any]]:
        session = await self._get_http_session()
        url = f"{_YIELDS_API}/pools"
        async with session.get(url) as response:
            if response.status != 200:
                text = await response.text()
                raise _ProviderError(f"HTTP {response.status}: {text[:200]}")
            data = await response.json()
            return data.get("data", [])

    # -- GeckoTerminal provider ----------------------------------------------

    async def _fetch_from_geckoterminal(
        self,
        chain: str,
        pool_address: str,
        protocol: str,
    ) -> _PoolAnalyticsRecord | None:
        if not self._rate_limiter_gt.acquire():
            raise _ProviderError("rate limited")

        network = _CHAIN_TO_GT_NETWORK.get(chain)
        if network is None:
            raise _ProviderError(f"unsupported chain: {chain}")

        try:
            payload = await self._query_geckoterminal_pool(network, pool_address)
        except (TimeoutError, aiohttp.ClientError, _ProviderError) as e:
            raise _ProviderError(str(e)) from e

        if payload is None:
            return None  # 404 = not found, not a transport failure.

        return _parse_gt_pool(payload, pool_address, chain, protocol)

    async def _query_geckoterminal_pool(
        self,
        network: str,
        pool_address: str,
    ) -> dict[str, Any] | None:
        session = await self._get_http_session()
        url = f"{_GT_API}/networks/{network}/pools/{pool_address}"
        async with session.get(url) as response:
            if response.status == 404:
                return None
            if response.status != 200:
                text = await response.text()
                raise _ProviderError(f"HTTP {response.status}: {text[:200]}")
            return await response.json()

    # -- Cache helpers --------------------------------------------------

    def _cache_get_public(
        self,
        key: tuple[str, str, str],
    ) -> _PoolAnalyticsRecord | None:
        with self._cache_lock:
            entry = self._public_cache.get(key)
            if entry is None:
                return None
            if time.monotonic() - entry.cached_at > _CACHE_TTL_SECONDS:
                del self._public_cache[key]
                return None
            return entry.record

    def _cache_put(
        self,
        public_key: tuple[str, str, str],
        provider: str,
        record: _PoolAnalyticsRecord,
    ) -> None:
        with self._cache_lock:
            now = time.monotonic()
            # Evict expired entries on write so unique-key traffic over
            # long uptime can't leak memory (expired-on-read alone leaves
            # never-re-read keys forever). CodeRabbit PR #2389 review thread.
            self._evict_expired_locked(now)
            self._public_cache[public_key] = _CacheEntry(record=record, cached_at=now)
            chain, pool_address, protocol = public_key
            raw_key = (chain, pool_address, protocol, provider)
            self._raw_cache[raw_key] = _CacheEntry(record=record, cached_at=now)
            # Hard cap as second line of defense if a burst of unique
            # keys lands all within one TTL window. FIFO eviction —
            # iteration order on dict preserves insertion order.
            while len(self._public_cache) > _CACHE_MAX_ENTRIES:
                self._public_cache.pop(next(iter(self._public_cache)))
            while len(self._raw_cache) > _CACHE_MAX_ENTRIES:
                self._raw_cache.pop(next(iter(self._raw_cache)))

    def _evict_expired_locked(self, now: float) -> None:
        """Drop entries older than ``_CACHE_TTL_SECONDS``. Caller holds
        ``_cache_lock``. O(n) over current cache size; called on writes
        only so amortized cost stays low."""
        self._public_cache = {k: v for k, v in self._public_cache.items() if now - v.cached_at <= _CACHE_TTL_SECONDS}
        self._raw_cache = {k: v for k, v in self._raw_cache.items() if now - v.cached_at <= _CACHE_TTL_SECONDS}

    # -- Proto-shape helpers -------------------------------------------

    def _record_to_response(
        self,
        record: _PoolAnalyticsRecord,
        *,
        is_live_data: bool,
    ) -> gateway_pb2.PoolAnalyticsResponse:
        return gateway_pb2.PoolAnalyticsResponse(
            pool_address=record.pool_address,
            chain=record.chain,
            protocol=record.protocol,
            tvl_usd=record.tvl_usd,
            volume_24h_usd=record.volume_24h_usd,
            volume_7d_usd=record.volume_7d_usd,
            fee_apr=record.fee_apr,
            fee_apy=record.fee_apy,
            utilization_rate=record.utilization_rate,
            token0_weight=record.token0_weight,
            token1_weight=record.token1_weight,
            source=record.source,
            observed_at=record.observed_at,
            is_live_data=is_live_data,
            success=True,
        )


# =============================================================================
# Provider error (internal)
# =============================================================================


class _ProviderError(Exception):
    """Raised inside a provider path on a transport / rate-limit / parse failure.

    The servicer translates this into the dual-channel response envelope
    plus a non-OK gRPC status when both providers are exhausted.
    """


# =============================================================================
# Provider payload parsers (pure functions for unit testability)
# =============================================================================


def _parse_llama_pool(
    pool: dict[str, Any],
    pool_address: str,
    chain: str,
    protocol: str,
) -> _PoolAnalyticsRecord:
    """Translate a DefiLlama pool dict into the internal record shape.

    Each field is either populated from the payload or stays empty —
    never substitutes 0 for missing data.
    """
    tvl_usd = _safe_decimal_str(pool.get("tvlUsd"))
    apy_base = pool.get("apyBase")
    apy_total = pool.get("apy")
    vol_24h = _safe_decimal_str(pool.get("volumeUsd1d"))
    vol_7d = _safe_decimal_str(pool.get("volumeUsd7d"))

    fee_apr = _safe_decimal_str(apy_base) if apy_base is not None else ""
    fee_apy = _safe_decimal_str(apy_total) if apy_total is not None else fee_apr

    return _PoolAnalyticsRecord(
        pool_address=pool_address,
        chain=chain,
        protocol=protocol or str(pool.get("project", "")),
        tvl_usd=tvl_usd,
        volume_24h_usd=vol_24h,
        volume_7d_usd=vol_7d,
        fee_apr=fee_apr,
        fee_apy=fee_apy,
        utilization_rate="",
        token0_weight="",
        token1_weight="",
        source="defillama",
        observed_at=int(time.time()),
        is_live_data=True,
    )


def _parse_gt_pool(
    data: dict[str, Any],
    pool_address: str,
    chain: str,
    protocol: str,
) -> _PoolAnalyticsRecord:
    """Translate a GeckoTerminal pool response into the internal record shape."""
    attrs = data.get("data", {}).get("attributes", {}) if isinstance(data, dict) else {}

    tvl_raw = attrs.get("reserve_in_usd")
    tvl_usd = _safe_decimal_str(tvl_raw)
    vol_24h_raw = attrs.get("volume_usd", {}).get("h24") if isinstance(attrs.get("volume_usd"), dict) else None
    vol_24h_usd = _safe_decimal_str(vol_24h_raw)
    vol_7d_usd = ""  # GeckoTerminal doesn't expose 7d volume directly.

    pool_fee = _safe_float(attrs.get("pool_fee"))
    tvl_num = _safe_float(tvl_raw)
    vol_24h_num = _safe_float(vol_24h_raw)
    fee_apr_num = 0.0
    if pool_fee > 0 and tvl_num > 0:
        # Annualize the daily-fee yield: (daily_volume * fee_rate * 365) / TVL * 100.
        fee_apr_num = (vol_24h_num * pool_fee * 365) / tvl_num * 100
    fee_apy_num = ((1 + fee_apr_num / 365 / 100) ** 365 - 1) * 100 if fee_apr_num > 0 else 0.0
    fee_apr = _safe_decimal_str(fee_apr_num) if pool_fee > 0 and tvl_num > 0 else ""
    fee_apy = _safe_decimal_str(fee_apy_num) if fee_apr_num > 0 else ""

    detected_protocol = protocol or str(attrs.get("dex_id", ""))

    return _PoolAnalyticsRecord(
        pool_address=pool_address,
        chain=chain,
        protocol=detected_protocol,
        tvl_usd=tvl_usd,
        volume_24h_usd=vol_24h_usd,
        volume_7d_usd=vol_7d_usd,
        fee_apr=fee_apr,
        fee_apy=fee_apy,
        utilization_rate="",
        token0_weight="",
        token1_weight="",
        source="geckoterminal",
        observed_at=int(time.time()),
        is_live_data=True,
    )


__all__ = [
    "PoolAnalyticsServiceServicer",
]
