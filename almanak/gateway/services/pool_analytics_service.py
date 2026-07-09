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


class _ProviderError(Exception):
    """Raised inside the provider path on a transport / rate-limit / parse failure.

    The servicer translates this into the dual-channel response envelope
    plus a non-OK gRPC status when the provider is exhausted.
    """


# =============================================================================
# Provider payload parser (pure function for unit testability)
# =============================================================================


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
