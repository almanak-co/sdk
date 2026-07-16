"""PoolAnalyticsService implementation - off-chain pool analytics (VIB-4727).

Server-side handler that owns the HTTP egress to CoinGecko Onchain — the
sole external pool-analytics lane. The framework-side ``PoolAnalyticsReader``
is a thin gRPC client that calls into this service via the gateway sidecar —
strategies in production containers never see ``aiohttp``.

Provider history: the service originally tried a catalog-matching provider
first with CoinGecko Onchain as fallback. The matcher lane was structurally
dead — the upstream catalog keys pools by opaque UUIDs, not addresses, so an
address-equality match could never hit. CoinGecko
Onchain is address-keyed and verified end-to-end, so it is now the primary
and only lane. The org deliberately runs the paid CoinGecko key here
(CoinGecko acquired GeckoTerminal; Onchain is the same data behind paid
limits) — do NOT reintroduce a keyless GeckoTerminal client.

API-key resolution (gateway settings conventions):

1. ``GatewaySettings.coingecko_api_key`` — populated by the pydantic env
   loader from ``ALMANAK_GATEWAY_COINGECKO_API_KEY`` (the gateway-canonical
   name; deployed containers inject this).
2. Fallback: ``_get_gateway_api_key("COINGECKO_API_KEY")`` — checks
   ``ALMANAK_GATEWAY_COINGECKO_API_KEY`` first, then bare
   ``COINGECKO_API_KEY`` (local-dev convenience).

A missing key is non-fatal: the provider raises an internal error naming the
env vars, and the RPC surfaces the standard UNAVAILABLE not-found envelope.

Input validation:
- ``pool_address`` is chain-validated at the entry. EVM expects
  ``^0x[0-9a-f]{40}$``; Solana expects base58 32-44 chars with case
  preserved (lower-casing a Solana address yields a different address).
- ``chain`` must be in the supported chain map. Empty / whitespace-only
  ``pool_address`` or ``chain`` returns INVALID_ARGUMENT.

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
from almanak.gateway.data._history_common import (
    _CHAIN_TO_GT_NETWORK,
    coingecko_onchain_api_base,
    coingecko_onchain_headers,
    is_solana_family,
)
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# =============================================================================
# Provider configuration
# =============================================================================

_COINGECKO_ONCHAIN_SOURCE = "coingecko_onchain"

# The chain-name map (``_CHAIN_TO_GT_NETWORK``), the CoinGecko Onchain API
# base / header helpers, and the ``is_solana_family`` helper are imported
# from the shared ``almanak/gateway/data/_history_common`` home so this
# service and the pool-history providers agree on chain spelling and API
# plumbing without duplicating the literals (coupling-ratchet canonical
# home — blueprint 22).

#: Backward-compatible module-local alias — the chain-family chokepoint used to
#: be defined here as ``_is_solana_family`` (W3 / VIB-4855). The implementation
#: now lives in ``_history_common.is_solana_family``; the alias keeps the
#: pre-existing import surface stable.
_is_solana_family = is_solana_family

# Cache TTL (seconds) — 60s strikes a balance between provider load and
# strategy iteration cadence. Iteration loops typically tick every ~60s so
# a cache hit on the second iteration is what makes per-strategy load
# bearable for the upstream API.
_CACHE_TTL_SECONDS = 60.0
# Negative results (provider failure / schema garbage) are cached briefly so
# retry storms cannot drain the shared CoinGecko bucket, but recover fast.
_NEGATIVE_CACHE_TTL_SECONDS = 30.0
# CoinGecko Onchain token-pools upstream paging (atomic fetch bound).
_TOKEN_POOLS_UPSTREAM_PAGE_SIZE = 20
_TOKEN_POOLS_MAX_UPSTREAM_PAGES = 5
# Hard cap on distinct token-pools cache keys (kilobytes of protobufs, but
# unbounded key space is unbounded key space).
_TOKEN_POOLS_CACHE_MAX_ENTRIES = 2048

# Bound the per-pool caches so unique-key traffic over long uptime can't
# leak memory. 5000 keys × ~250 bytes/record ≈ 1.25 MB worst-case per
# cache.
_CACHE_MAX_ENTRIES = 5000

# CoinGecko Onchain bucket: keep the historical 30 req/min public budget as
# the conservative local throttle.
_COINGECKO_ONCHAIN_RATE_PER_MIN = 30

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
    if is_solana_family(chain):
        return address
    return address.lower()


def _validate_pool_address(address: str, chain: str) -> bool:
    """Return True when ``address`` is a syntactically valid pool address for ``chain``.

    Rejecting malformed input here means the upstream URL (CoinGecko Onchain
    embeds the address) can never carry an attacker-supplied path / query
    segment.
    """
    if is_solana_family(chain):
        return bool(_SOLANA_BASE58_RE.match(address))
    return bool(_EVM_ADDRESS_RE.match(address))


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
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        logger.debug("pool_analytics: dropped unparseable decimal %r", value)
        return ""
    # Decimal accepts "NaN"/"Infinity" — non-finite readings are unmeasured,
    # never serialized onto the wire (Empty != Zero, and NaN poisons every
    # downstream comparison).
    if not parsed.is_finite():
        logger.debug("pool_analytics: dropped non-finite decimal %r", value)
        return ""
    return str(parsed)


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


# =============================================================================
# Servicer
# =============================================================================


class PoolAnalyticsServiceServicer(gateway_pb2_grpc.PoolAnalyticsServiceServicer):
    """gRPC servicer for pool analytics.

    Cache layout (split per UAT card D2.M4):
    - ``_public_cache`` keyed by ``(chain, pool_address, protocol_or_empty)`` —
      what callers ask for. Hit-rate metric is on this.
    - ``_raw_cache`` keyed by ``(chain, pool_address, protocol_or_empty, provider)`` —
      the raw provider payload; partitioning prevents shape-collision if a
      second provider lane is ever reintroduced.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None
        self._rate_limiter_cg = _TokenBucket(rate=_COINGECKO_ONCHAIN_RATE_PER_MIN, period=60.0)
        self._metrics: dict[str, _ProviderMetrics] = {
            _COINGECKO_ONCHAIN_SOURCE: _ProviderMetrics(),
        }
        self._public_cache: dict[tuple[str, str, str], _CacheEntry] = {}
        self._raw_cache: dict[tuple[str, str, str, str], _CacheEntry] = {}
        # ListTokenPools TTL cache: (chain, token, page) -> (monotonic_at,
        # response_snapshot | None, error). None snapshot = negative entry.
        # page 0 entries are ATOMIC full-fetch snapshots.
        self._token_pools_cache: dict[tuple[str, str, int], tuple[float, Any, str]] = {}
        # Single-flight locks for in-progress token-pools fetches.
        self._token_pools_inflight: dict[tuple[str, str, int], asyncio.Lock] = {}
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

        if chain not in _CHAIN_TO_GT_NETWORK:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"unsupported chain: {chain}")
            return gateway_pb2.PoolAnalyticsResponse(
                pool_address=pool_address,
                chain=chain,
                protocol=protocol,
                success=False,
                error=f"unsupported chain: {chain}",
            )

        # Syntactic address validation prevents the CoinGecko Onchain URL
        # template (``_query_coingecko_onchain_pool``) from carrying
        # attacker-supplied path / query segments.
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

        # CoinGecko Onchain — the sole external lane.
        try:
            record = await self._fetch_from_coingecko_onchain(chain, pool_address, protocol)
            if record is not None:
                self._metrics[_COINGECKO_ONCHAIN_SOURCE].successes += 1
                self._cache_put(public_key, _COINGECKO_ONCHAIN_SOURCE, record)
                return self._record_to_response(record, is_live_data=True)
            errors.append(f"{_COINGECKO_ONCHAIN_SOURCE}: not found")
        except _ProviderError as e:
            self._metrics[_COINGECKO_ONCHAIN_SOURCE].failures += 1
            errors.append(f"{_COINGECKO_ONCHAIN_SOURCE}: {e}")
            logger.debug(
                "CoinGecko Onchain pool analytics failed for %s on %s: %s",
                pool_address,
                chain,
                e,
            )

        # Provider exhausted. Hard-fail with UNAVAILABLE so the framework
        # raises DataSourceUnavailable. Per D3.F6: NEVER return a
        # success=True envelope with empty/zero analytics.
        joined = "; ".join(errors) or "provider exhausted"
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

    async def ListTokenPools(
        self,
        request: gateway_pb2.TokenPoolsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TokenPoolsResponse:
        """Pools where a token is base or quote, with product-distinct dex ids.

        Serves symbolic pool resolution for product-ambiguous venue families
        (Aerodrome/Velodrome classic vs Slipstream): CoinGecko Onchain keys
        every pool with a product-distinct dex id where DexScreener cannot.
        """
        from almanak.core.chains import ChainRegistry

        raw_chain = request.chain.strip()
        descriptor = ChainRegistry.try_resolve(raw_chain)
        chain = descriptor.name.lower() if descriptor is not None else raw_chain.lower()
        token_address = _normalize_pool_address(request.token_address, chain)
        # page 0 (default) = ATOMIC bounded fetch: the gateway pages upstream
        # itself on RAW row counts and returns one consistent snapshot with
        # `complete` set. page >= 1 = single upstream page (compat).
        page = int(request.page or 0)

        def _fail(code: grpc.StatusCode, error: str) -> gateway_pb2.TokenPoolsResponse:
            context.set_code(code)
            context.set_details(error)
            return gateway_pb2.TokenPoolsResponse(chain=chain, token_address=token_address, success=False, error=error)

        if page < 0:
            # The wire contract defines page 0 (atomic) and >= 1 (single
            # upstream page) only; silently mapping malformed input onto the
            # atomic mode would turn garbage into the most expensive request.
            return _fail(grpc.StatusCode.INVALID_ARGUMENT, f"page must be >= 0, got {page}")

        if not token_address or not chain:
            return _fail(grpc.StatusCode.INVALID_ARGUMENT, "token_address and chain are required")
        if chain not in _CHAIN_TO_GT_NETWORK:
            return _fail(grpc.StatusCode.INVALID_ARGUMENT, f"unsupported chain: {chain}")
        if not _validate_pool_address(token_address, chain):
            return _fail(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"invalid token_address for chain {chain!r}: {token_address!r}",
            )

        # Bounded TTL cache (positive AND negative): symbolic resolution
        # retries per open/prewarm, and every uncached call spends the shared
        # 30/min CoinGecko bucket — repeated unresolved lookups must not
        # throttle unrelated consumers.
        cache_key = (chain, token_address, page)
        now = time.monotonic()
        with self._cache_lock:
            cached = self._token_pools_cache.get(cache_key)
            if cached is not None and now - cached[0] <= (
                _CACHE_TTL_SECONDS if cached[1] is not None else _NEGATIVE_CACHE_TTL_SECONDS
            ):
                if cached[1] is not None:
                    response = gateway_pb2.TokenPoolsResponse()
                    response.CopyFrom(cached[1])
                    return response
                return _fail(grpc.StatusCode.UNAVAILABLE, cached[2] or "coingecko_onchain: cached failure")

        def _fail_cached(error: str) -> gateway_pb2.TokenPoolsResponse:
            with self._cache_lock:
                self._evict_token_pools_locked(time.monotonic())
                self._token_pools_cache[cache_key] = (time.monotonic(), None, error)
            return _fail(grpc.StatusCode.UNAVAILABLE, error)

        # SINGLE-FLIGHT: concurrent identical misses share one upstream fetch
        # instead of double-spending the paid rate budget.
        flight = self._token_pools_inflight.get(cache_key)
        if flight is None:
            flight = asyncio.Lock()
            self._token_pools_inflight[cache_key] = flight
        async with flight:
            # The registry entry must survive until the work completes —
            # popping before the upstream await lets a second caller
            # miss the entry, mint its own lock, and double-spend the
            # shared CoinGecko budget (CodeRabbit find, #3271). Waiters
            # acquire the same lock, then hit the re-checked cache.
            try:
                # Re-check the cache: a concurrent holder may have filled it.
                now = time.monotonic()
                with self._cache_lock:
                    cached = self._token_pools_cache.get(cache_key)
                    if cached is not None and now - cached[0] <= (
                        _CACHE_TTL_SECONDS if cached[1] is not None else _NEGATIVE_CACHE_TTL_SECONDS
                    ):
                        if cached[1] is not None:
                            response = gateway_pb2.TokenPoolsResponse()
                            response.CopyFrom(cached[1])
                            return response
                        return _fail(grpc.StatusCode.UNAVAILABLE, cached[2] or "coingecko_onchain: cached failure")

                try:
                    pools, complete = await self._fetch_token_pools_upstream(chain, token_address, page)
                except _RateLimitedError:
                    # Rate-limit pressure is transient: fail WITHOUT
                    # negative-caching, or one burst would blank the lane for the
                    # negative-TTL window.
                    return _fail(grpc.StatusCode.UNAVAILABLE, "coingecko_onchain: rate limited")
                except (TimeoutError, aiohttp.ClientError, ValueError, TypeError, AttributeError, _ProviderError) as e:
                    self._metrics[_COINGECKO_ONCHAIN_SOURCE].failures += 1
                    return _fail_cached(f"{_COINGECKO_ONCHAIN_SOURCE}: {e}")

                self._metrics[_COINGECKO_ONCHAIN_SOURCE].successes += 1
                response = gateway_pb2.TokenPoolsResponse(
                    chain=chain,
                    token_address=token_address,
                    pools=pools,
                    source=_COINGECKO_ONCHAIN_SOURCE,
                    observed_at=int(time.time()),
                    success=True,
                    complete=complete,
                )
                with self._cache_lock:
                    self._evict_token_pools_locked(time.monotonic())
                    snapshot = gateway_pb2.TokenPoolsResponse()
                    snapshot.CopyFrom(response)
                    self._token_pools_cache[cache_key] = (time.monotonic(), snapshot, "")
                return response
            finally:
                self._token_pools_inflight.pop(cache_key, None)

    async def _fetch_token_pools_upstream(
        self,
        chain: str,
        token_address: str,
        page: int,
    ) -> tuple[list[gateway_pb2.TokenPoolRow], bool]:
        """Fetch token pools upstream; page 0 = bounded atomic multi-page.

        Returns ``(pools, complete)``. Continuation decides on the RAW
        upstream row count — filtered counts under-read full pages (a
        sanitized junk row must not truncate the search). Payload navigation
        and row parsing stay INSIDE the provider boundary: a 200 whose body
        is a list / malformed relationship object raises ``_ProviderError``
        (structured UNAVAILABLE), never AttributeError surfaced as UNKNOWN.
        Raises ``_RateLimitedError`` on local bucket exhaustion (transient —
        the caller must not negative-cache it).
        """
        if not self._coingecko_api_key:
            raise _ProviderError(
                "CoinGecko Onchain API requires a valid COINGECKO_API_KEY for token pools; "
                "set ALMANAK_GATEWAY_COINGECKO_API_KEY on the gateway"
            )
        network = _CHAIN_TO_GT_NETWORK[chain]
        url = f"{self._coingecko_onchain_api_base}/networks/{network}/tokens/{token_address}/pools"
        pages = [page] if page >= 1 else list(range(1, _TOKEN_POOLS_MAX_UPSTREAM_PAGES + 1))
        session = await self._get_http_session()
        pools: list[gateway_pb2.TokenPoolRow] = []
        for upstream_page in pages:
            if not self._rate_limiter_cg.acquire():
                raise _RateLimitedError()
            async with session.get(
                url, params={"page": str(upstream_page)}, headers=self._coingecko_onchain_headers
            ) as response:
                if response.status == 404:
                    payload: dict[str, Any] | None = None
                elif response.status != 200:
                    text = await response.text()
                    raise _ProviderError(f"HTTP {response.status}: {text[:200]}")
                else:
                    payload = await response.json()
            if payload is not None and not isinstance(payload, dict):
                raise _ProviderError(f"token-pools returned non-object payload: {str(payload)[:120]}")
            rows = (payload or {}).get("data")
            if payload is not None and not isinstance(rows, list):
                raise _ProviderError(f"token-pools payload missing data list: {str(payload)[:120]}")
            raw_count = len(rows or [])
            pools.extend(
                parsed
                for parsed in (_parse_token_pool_row(row, network, chain) for row in rows or [])
                if parsed is not None
            )
            if raw_count < _TOKEN_POOLS_UPSTREAM_PAGE_SIZE:
                return pools, True
        # Bound exhausted without a short page: NOT known-complete.
        return pools, False

    def _evict_token_pools_locked(self, now: float) -> None:
        """Sweep expired token-pools entries; hard-cap the key space.

        Caller holds ``_cache_lock``. Expired entries go first; if the cap is
        still exceeded (distinct live keys), the oldest entries are dropped.
        """
        self._token_pools_cache = {
            k: v
            for k, v in self._token_pools_cache.items()
            if now - v[0] <= (_CACHE_TTL_SECONDS if v[1] is not None else _NEGATIVE_CACHE_TTL_SECONDS)
        }
        overflow = len(self._token_pools_cache) - (_TOKEN_POOLS_CACHE_MAX_ENTRIES - 1)
        if overflow > 0:
            for key in sorted(self._token_pools_cache, key=lambda k: self._token_pools_cache[k][0])[:overflow]:
                del self._token_pools_cache[key]

    # -- CoinGecko Onchain provider ------------------------------------------

    async def _fetch_from_coingecko_onchain(
        self,
        chain: str,
        pool_address: str,
        protocol: str,
    ) -> _PoolAnalyticsRecord | None:
        if not self._rate_limiter_cg.acquire():
            raise _ProviderError("rate limited")

        network = _CHAIN_TO_GT_NETWORK.get(chain)
        if network is None:
            raise _ProviderError(f"unsupported chain: {chain}")

        try:
            payload = await self._query_coingecko_onchain_pool(network, pool_address)
        except (TimeoutError, aiohttp.ClientError, ValueError, _ProviderError) as e:
            # ValueError covers json.JSONDecodeError on a 200-with-malformed-body:
            # a garbage upstream payload is a provider failure, not an unhandled
            # crash — map it into the _ProviderError taxonomy like any other so
            # GetPoolAnalytics returns its structured UNAVAILABLE, never a stack trace.
            raise _ProviderError(str(e)) from e

        if payload is None:
            return None  # 404 = not found, not a transport failure.

        return _parse_coingecko_onchain_pool(payload, pool_address, chain, protocol)

    async def _query_coingecko_onchain_pool(
        self,
        network: str,
        pool_address: str,
    ) -> dict[str, Any] | None:
        if not self._coingecko_api_key:
            raise _ProviderError(
                "CoinGecko Onchain API requires a valid COINGECKO_API_KEY for pool data; "
                "set ALMANAK_GATEWAY_COINGECKO_API_KEY on the gateway"
            )

        session = await self._get_http_session()
        url = f"{self._coingecko_onchain_api_base}/networks/{network}/pools/{pool_address}"
        async with session.get(url, headers=self._coingecko_onchain_headers) as response:
            if response.status == 404:
                return None
            if response.status != 200:
                text = await response.text()
                if response.status == 401:
                    raise _ProviderError(
                        "CoinGecko Onchain API requires a valid COINGECKO_API_KEY for pool data; "
                        "the key may be missing, invalid, or expired; HTTP 401"
                    )
                raise _ProviderError(f"HTTP {response.status}: {text[:200]}")
            return await response.json()

    @property
    def _coingecko_api_key(self) -> str | None:
        key = getattr(self.settings, "coingecko_api_key", None)
        return key or _get_gateway_api_key("COINGECKO_API_KEY")

    @property
    def _coingecko_onchain_api_base(self) -> str:
        return coingecko_onchain_api_base(self._coingecko_api_key)

    @property
    def _coingecko_onchain_headers(self) -> dict[str, str]:
        return coingecko_onchain_headers(self._coingecko_api_key)

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
            # never-re-read keys forever).
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


class _RateLimitedError(Exception):
    """Local token bucket exhausted — transient, never negative-cached."""


class _ProviderError(Exception):
    """Raised inside the provider path on a transport / rate-limit / parse failure.

    The servicer translates this into the dual-channel response envelope
    plus a non-OK gRPC status when the provider is exhausted.
    """


# =============================================================================
# Provider payload parser (pure function for unit testability)
# =============================================================================


def _token_address_from_relationship_id(token_id: Any, network: str, chain: str) -> str:
    """Extract the address from a relationship id like ``"base_0xabc..."``.

    The id format is ``{network}_{address}`` and NETWORK IDS THEMSELVES
    CONTAIN UNDERSCORES (``polygon_pos``) — splitting at the first
    underscore mangles those chains' addresses ("pos_0x…"). Strip the exact
    requested network prefix instead, then normalize chain-aware (EVM
    lowercases; Solana-family base58 is case-sensitive and must be
    preserved). A row from a different network than requested yields ``""``.
    """
    if not isinstance(token_id, str):
        return ""
    prefix = f"{network}_"
    if not token_id.startswith(prefix):
        return ""
    address = token_id[len(prefix) :]
    if not address:
        return ""
    normalized = _normalize_pool_address(address, chain)
    # Syntactic validation, chain-aware: a malformed identity is omitted
    # (""), never surfaced as a matchable token address.
    return normalized if _validate_pool_address(normalized, chain) else ""


def _parse_token_pool_row(row: Any, network: str, chain: str) -> gateway_pb2.TokenPoolRow | None:
    """One CoinGecko Onchain token-pools row -> ``TokenPoolRow`` (None = skip).

    Money stays decimal-as-string (Empty != Zero): an absent or non-numeric
    ``reserve_in_usd`` is ``""`` — the CLIENT decides how to rank unmeasured
    reserves, the wire never invents a zero. Rows whose pool address fails
    the chain's syntactic validation are skipped: a non-address identity
    must never become a selectable "product-exact" pool.
    """
    if not isinstance(row, dict):
        return None
    attributes = row.get("attributes") or {}
    relationships = row.get("relationships") or {}
    address = _normalize_pool_address(str(attributes.get("address") or ""), chain)
    if not address or not _validate_pool_address(address, chain):
        return None
    dex = ((relationships.get("dex") or {}).get("data") or {}).get("id")
    return gateway_pb2.TokenPoolRow(
        pool_address=address,
        dex_id=str(dex or "").lower(),
        name=str(attributes.get("name") or ""),
        reserve_usd=_safe_decimal_str(attributes.get("reserve_in_usd")),
        base_token_address=_token_address_from_relationship_id(
            ((relationships.get("base_token") or {}).get("data") or {}).get("id"), network, chain
        ),
        quote_token_address=_token_address_from_relationship_id(
            ((relationships.get("quote_token") or {}).get("data") or {}).get("id"), network, chain
        ),
    )


def _parse_coingecko_onchain_pool(
    data: dict[str, Any],
    pool_address: str,
    chain: str,
    protocol: str,
) -> _PoolAnalyticsRecord:
    """Translate a CoinGecko Onchain pool response into the internal record shape."""
    attrs = data.get("data", {}).get("attributes", {}) if isinstance(data, dict) else {}

    tvl_raw = attrs.get("reserve_in_usd")
    tvl_usd = _safe_decimal_str(tvl_raw)
    vol_24h_raw = attrs.get("volume_usd", {}).get("h24") if isinstance(attrs.get("volume_usd"), dict) else None
    vol_24h_usd = _safe_decimal_str(vol_24h_raw)
    vol_7d_usd = ""  # CoinGecko Onchain doesn't expose 7d volume directly.

    pool_fee_raw = attrs.get("pool_fee_percentage", attrs.get("pool_fee"))
    pool_fee = (
        _safe_float(pool_fee_raw) / 100 if attrs.get("pool_fee_percentage") is not None else _safe_float(pool_fee_raw)
    )
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
        source=_COINGECKO_ONCHAIN_SOURCE,
        observed_at=int(time.time()),
        is_live_data=True,
    )


__all__ = [
    "PoolAnalyticsServiceServicer",
]
