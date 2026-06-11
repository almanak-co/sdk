"""Multi-provider OHLCV router with CEX/DEX classification.

Routes OHLCV requests to the appropriate provider chain based on instrument
classification:

- **DeFi-native pairs**: GeckoTerminal -> DeFi Llama -> Binance
  Tokens that have GeckoTerminal pool data but no known CEX symbol.

- **Major CEX-listed tokens**: Binance -> CoinGecko -> DeFi Llama
  Tokens with known CEX symbols in CEX_SYMBOL_MAP.

When a CEX source (e.g. Binance) is used for a DeFi strategy, the
DataMeta.confidence is reduced to 0.7 and a warning is logged to flag
the CEX/DEX basis risk.

Finalized candles (>24h old) are cached to disk; recent candles are
tagged provisional and refreshed on next access.

Example:
    from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter

    router = OHLCVRouter()
    router.register_provider(gecko_provider)
    router.register_provider(binance_wrapper)

    envelope = router.get_ohlcv(
        token="WETH",
        chain="arbitrum",
        timeframe="1h",
        limit=100,
    )
    candles = envelope.value
    print(envelope.meta.source)  # e.g. "geckoterminal"
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from almanak.core.chains import DEFAULT_CHAIN
from almanak.framework.data.interfaces import (
    DataSourceRateLimited,
    DataSourceTimeout,
    DataSourceUnavailable,
    OHLCVCandle,
)
from almanak.framework.data.models import (
    CEX_SYMBOL_MAP,
    OHLCV_PROXY_MAP,
    DataClassification,
    DataEnvelope,
    DataMeta,
    Instrument,
    resolve_instrument,
)
from almanak.framework.data.routing.config import DataProvider

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retry policy (VIB-3447 + VIB-3802)
# ---------------------------------------------------------------------------

# Max additional attempts beyond the first for the primary DEX provider.
# Total attempts = 1 + _MAX_PRIMARY_RETRIES.
_MAX_PRIMARY_RETRIES = 2
_RETRY_BASE_DELAY = 0.25  # seconds
_RETRY_MAX_DELAY = 2.0  # seconds cap (default full-jitter backoff)

# Cap on upstream-advised retry delay (RetryInfo). Adversarial or misconfigured
# upstreams can return very long delays; we honor the advice but cap so a
# single retry can never stretch beyond the framework's iteration budget.
# Values longer than this are breaker territory, not retry territory — that's
# what VIB-3803's exposure-aware breaker is for.
_UPSTREAM_RETRY_MAX_DELAY = 5.0  # seconds

# Legacy substring heuristic. VIB-3802 prefers typed exceptions
# (DataSourceRateLimited / DataSourceTimeout / DataSourceUnavailable populated
# from RetryInfo), but we keep this as a fallback for paths that haven't been
# migrated to the typed-error contract yet.
_TRANSIENT_ERROR_HINTS: frozenset[str] = frozenset(
    [
        "statuscode.internal",
        "statuscode.unavailable",
        "statuscode.resource_exhausted",
        "statuscode.deadline_exceeded",
        "timeout",
        "timed out",
        "temporarily unavailable",
        "rate limit",
        "429",
        "resource exhausted",
        "service unavailable",
        "connection reset",
        "connection refused",
        "connection aborted",
    ]
)


def _is_retryable_exc(exc: Exception) -> bool:
    """Return True if *exc* should trigger a retry.

    Decision order:

    1. **Typed VIB-3800 signals** — :class:`DataSourceRateLimited` and
       :class:`DataSourceTimeout` always retry. :class:`DataSourceUnavailable`
       retries iff the upstream populated ``retry_after`` (i.e. ``RetryInfo``
       was present on the trailer); the gateway only sets that when the failure
       is transient.
    2. **Legacy substring heuristic** — for non-typed exceptions and untagged
       :class:`DataSourceUnavailable` instances, fall back to matching against
       ``_TRANSIENT_ERROR_HINTS``. This preserves the existing behavior where
       a DSU like "Pool not found" or "unsupported chain" (raised from
       client-side validation, not from an upstream error) is NOT retried.

    The legacy path will narrow as more upstream services migrate to the typed
    contract.
    """
    if isinstance(exc, DataSourceRateLimited | DataSourceTimeout):
        return True

    if isinstance(exc, DataSourceUnavailable):
        if exc.retry_after is not None:
            return True
        s = exc.reason.lower()
    else:
        s = str(exc).lower()
    return any(hint in s for hint in _TRANSIENT_ERROR_HINTS)


# Back-compat alias — external callers still import the legacy name.
_is_transient_exc = _is_retryable_exc


def _extract_upstream_retry_delay(exc: Exception | None) -> float | None:
    """Read the upstream-advised retry delay (seconds) from a typed exception.

    Returns None when the exception doesn't carry ``retry_after`` (legacy
    path) or the value is missing / unusable, so the caller falls back to
    full-jitter backoff.
    """
    if exc is None:
        return None
    retry_after = getattr(exc, "retry_after", None)
    if isinstance(retry_after, int | float) and retry_after >= 0:
        return float(retry_after)
    return None


def _retry_delay_for(exc: Exception | None, attempt: int) -> float:
    """Compute the delay before the next retry.

    Honors :class:`google.rpc.RetryInfo` (surfaced as ``exc.retry_after``)
    when the upstream provided one, capped at ``_UPSTREAM_RETRY_MAX_DELAY``
    so a single retry can't stretch the iteration budget. Falls back to
    full-jitter exponential backoff when no advice is available.

    Args:
        exc: The exception from the most recent failed attempt.
        attempt: The attempt index we are about to start (1-based).
    """
    upstream_delay = _extract_upstream_retry_delay(exc)
    if upstream_delay is not None:
        return min(upstream_delay, _UPSTREAM_RETRY_MAX_DELAY)
    return _backoff_delay(attempt)


def _backoff_delay(attempt: int) -> float:
    """Full-jitter exponential backoff delay.  *attempt* is 1-based."""
    cap = min(_RETRY_BASE_DELAY * (2**attempt), _RETRY_MAX_DELAY)
    return random.uniform(0, cap)


# DEX-side providers that get bounded retries on transient failures.
# CEX providers (binance, coingecko) are excluded: their failures are almost
# always deterministic ("unknown token"), so retrying wastes latency budget.
_RETRYABLE_PROVIDERS: frozenset[str] = frozenset(["geckoterminal", "defillama", "coingecko_dex"])


def _error_text(exc: Exception | None) -> str:
    """Return the informative portion of an exception for error reason strings.

    For DataSourceUnavailable, uses exc.reason directly to avoid embedding the
    "Data source '...' unavailable: " boilerplate into composed reason strings,
    which would otherwise re-introduce the word "unavailable" and confuse
    downstream transient/permanent hint matching.
    """
    if exc is None:
        return "no registered OHLCV provider available"
    if isinstance(exc, DataSourceUnavailable):
        return exc.reason
    return str(exc)


# ---------------------------------------------------------------------------
# CEX/DEX classification
# ---------------------------------------------------------------------------

# All base symbols that have a known CEX mapping (for any exchange)
_CEX_KNOWN_BASES: frozenset[str] = frozenset(base for (_exchange, base, _quote) in CEX_SYMBOL_MAP)


def classify_instrument(instrument: Instrument) -> str:
    """Classify an instrument as "cex_primary" or "defi_primary".

    If the instrument's base token has a known CEX symbol mapping in
    CEX_SYMBOL_MAP, it's classified as CEX-primary (major token).
    Otherwise, it's DeFi-native.

    Args:
        instrument: Canonical Instrument to classify.

    Returns:
        "cex_primary" or "defi_primary".
    """
    if instrument.base in _CEX_KNOWN_BASES:
        return "cex_primary"
    return "defi_primary"


# Provider chain ordering per classification.
#
# INVARIANT (VIB-4847): every name here MUST be registered by the OHLCV
# factory. A name listed but never constructed is a "phantom tier" — the
# router walks past it on every miss and the chain silently degrades to one
# fewer provider than advertised. ``assert_provider_chains_registered`` (in
# ``factory.py``) enforces this at build time + in unit tests. If a provider
# is intentionally not-yet-wired, REMOVE it from the chain rather than leaving
# it dangling.
#
# ``defillama`` was removed here (VIB-4847) because no gateway-backed DeFi
# Llama OHLCV provider exists yet (tracked on VIB-3448). When that provider
# ships, re-add it to both chains AND register it in the factory in the same
# change so the invariant stays satisfied.
_PROVIDER_CHAINS: dict[str, list[str]] = {
    "cex_primary": ["binance", "coingecko"],
    "defi_primary": ["geckoterminal", "binance"],
}


def provider_names_in_chains() -> set[str]:
    """Return the set of distinct provider names referenced by any chain.

    Single source of truth for the provider-chain ↔ registry invariant guard
    (VIB-4847). The factory asserts every name returned here is registered.
    """
    return {name for chain in _PROVIDER_CHAINS.values() for name in chain}


# ---------------------------------------------------------------------------
# Disk cache for finalized candles
# ---------------------------------------------------------------------------

# Candles older than this are considered finalized and cached immutably
_FINALIZATION_AGE = timedelta(hours=24)

# ---------------------------------------------------------------------------
# Upstream-staleness guard (ALM-2697)
# ---------------------------------------------------------------------------
#
# Without this guard, a CEX upstream that has silently stopped returning fresh
# data — e.g. Binance after the MATIC -> POL ticker rebrand still answering
# `MATICUSDT` requests with last-month klines — will hand back a fully populated
# response. ``_split_by_finality`` then classifies *every* candle as "finalized"
# (because they are all >24h old), the disk cache writes them, and on every
# subsequent iteration the router serves the same byte-identical bag. RSI on a
# 5-minute timeframe ends up frozen forever.
#
# We treat a fetch as stale when the youngest candle's *start-time* (i.e. the
# candle's ``timestamp`` field — see ``OHLCVCandle``) is more than
# ``_STALE_TIMEFRAME_MULTIPLE * timeframe`` behind wall-clock. Start-time is a
# stricter signal than close-time (a candle's close-time is start-time +
# timeframe), so any check that passes start-time also passes close-time —
# this is intentional safety margin, not a bug. Two timeframes is generous
# enough to absorb upstream propagation delay and weekend gaps on longer
# intervals, while still catching "MATICUSDT hasn't traded in 2+ days but
# Binance is happy to keep echoing 5m klines from June".
#
# Mapping is duplicated from ``data.qa.test_definitions.cex_historical`` rather
# than imported to keep the router free of QA-test transitive imports — the QA
# suite pulls in pandas + grading machinery this hot path doesn't need. Keep
# the two in sync; the keys mirror ``GatewayOHLCVProvider._SUPPORTED_TIMEFRAMES``.

_TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
}

# How many full timeframes the youngest candle may lag wall-clock before the
# upstream is judged stale. Two is the smallest value that doesn't false-alarm
# on a normal 5m fetch where the most recent in-progress candle hasn't closed
# yet (lag < 1 timeframe) and the previous closed candle is still <2 timeframes
# old. Three+ would let a >10-minute outage slide on 5m, which is the regime
# where RSI stops moving.
_STALE_TIMEFRAME_MULTIPLE = 2

# Floor on the staleness budget. For ``1m`` this would otherwise be only 120s,
# which is too tight given upstream propagation jitter on a healthy feed. We
# never claim a feed is stale until at least this many seconds have passed
# since the youngest candle.
_STALE_MIN_BUDGET = timedelta(seconds=300)

# VIB-4875: on-chain DEX OHLCV sources where "no trade in an interval" is the
# *correct* representation of a quiet market — not a dead feed. For a quiet
# pool the newest real candle legitimately lags wall-clock (GeckoTerminal only
# emits a bucket when a swap occurs, and ``include_empty_intervals`` backfills
# only *interior* gaps, never past the last trade). For these sources we (1)
# forward-fill flat candles up to the current bucket so indicators get a
# continuous, current series, and (2) apply a relaxed staleness budget that
# doubles as the "dead pool" horizon. CEX sources (binance/coingecko) are
# deliberately excluded — there, a stale response means a dead/rebranded ticker
# (the ALM-2697 case) and must still be rejected on the strict budget.
_DEX_QUIET_POOL_PROVIDERS = frozenset({"geckoterminal"})

# Relaxed staleness multiple for DEX quiet-pool sources. This is the dead-pool
# horizon: a DEX pool with no trade for more than ``_DEX_STALE_TIMEFRAME_MULTIPLE``
# timeframes is presumed dead/illiquid and is NOT forward-filled (the staleness
# guard then rejects it). It also bounds the number of synthetic forward-fill
# candles to at most this many. 24x the timeframe (e.g. 24h on 1h, 24m on 1m).
_DEX_STALE_TIMEFRAME_MULTIPLE = 24

# Confidence ceiling stamped on a response that carries synthetic forward-filled
# candles — lower than a live trade, mirroring the CEX/DEX basis-risk haircut.
_DEX_FORWARD_FILL_CONFIDENCE = 0.6


# Default disk cache directory — use home if writable, fall back to system temp
def _resolve_cache_dir() -> Path:
    candidates = [
        Path.home() / ".almanak" / "data_cache" / "ohlcv",
        Path(tempfile.gettempdir())
        / f".almanak-{os.getuid() if hasattr(os, 'getuid') else os.getpid()}"
        / "data_cache"
        / "ohlcv",
    ]
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            if os.access(candidate, os.W_OK):
                return candidate
        except OSError:
            continue
    return candidates[-1]


_DEFAULT_CACHE_DIR = _resolve_cache_dir()


@dataclass(frozen=True)
class _CachedCandle:
    """Serializable candle representation for disk cache."""

    timestamp_iso: str
    open: str
    high: str
    low: str
    close: str
    volume: str | None


def _candle_to_dict(candle: OHLCVCandle) -> dict[str, Any]:
    return {
        "timestamp": candle.timestamp.isoformat(),
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume) if candle.volume is not None else None,
    }


def _dict_to_candle(d: dict[str, Any]) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime.fromisoformat(d["timestamp"]),
        open=Decimal(d["open"]),
        high=Decimal(d["high"]),
        low=Decimal(d["low"]),
        close=Decimal(d["close"]),
        volume=Decimal(d["volume"]) if d.get("volume") is not None else None,
    )


class _OHLCVDiskCache:
    """Simple JSON-based disk cache for finalized OHLCV candles.

    Only candles older than _FINALIZATION_AGE are written to disk.
    Recent (provisional) candles are never persisted.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir or _DEFAULT_CACHE_DIR

    def _key_path(self, cache_key: str) -> Path:
        safe_name = hashlib.sha256(cache_key.encode()).hexdigest()[:32]
        return self._cache_dir / f"{safe_name}.json"

    def get(self, cache_key: str) -> list[OHLCVCandle] | None:
        """Load finalized candles from disk, or None if not cached."""
        path = self._key_path(cache_key)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text())
            # Validate checksum
            payload = raw.get("candles", [])
            stored_checksum = raw.get("checksum", "")
            computed = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            if computed != stored_checksum:
                logger.warning("Disk cache checksum mismatch for %s, evicting", cache_key)
                path.unlink(missing_ok=True)
                return None
            return [_dict_to_candle(c) for c in payload]
        except Exception:
            logger.debug("Failed to read disk cache for %s", cache_key, exc_info=True)
            return None

    def put(self, cache_key: str, candles: list[OHLCVCandle]) -> None:
        """Write finalized candles to disk with sha256 checksum."""
        if not candles:
            return
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        payload = [_candle_to_dict(c) for c in candles]
        checksum = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        data = {
            "candles": payload,
            "checksum": checksum,
            "cached_at": datetime.now(UTC).isoformat(),
        }
        path = self._key_path(cache_key)
        path.write_text(json.dumps(data))
        logger.debug("Wrote %d finalized candles to disk cache: %s", len(candles), cache_key)

    def evict(self, cache_key: str) -> None:
        """Remove a cached entry by key.

        Used by the staleness guard (ALM-2697) when a previously-cached
        snapshot is detected as poisoned by a now-silent upstream.
        ``missing_ok=True`` keeps the operation idempotent so a concurrent
        eviction by another process is not an error.
        """
        path = self._key_path(cache_key)
        path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# OHLCV Router
# ---------------------------------------------------------------------------


@dataclass
class OHLCVRouter:
    """Routes OHLCV requests to providers with CEX/DEX classification.

    Classifies instruments and routes to the appropriate provider chain.
    Applies CEX/DEX basis warnings, disk caches finalized candles, and
    tags recent candles as provisional.

    Attributes:
        default_chain: Default chain for instrument resolution.
        disk_cache_dir: Optional disk cache directory override.
    """

    default_chain: str = DEFAULT_CHAIN
    disk_cache_dir: Path | None = None
    _providers: dict[str, DataProvider] = field(default_factory=dict, init=False, repr=False)
    _disk_cache: _OHLCVDiskCache = field(init=False, repr=False)
    _proxy_warned: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        self._disk_cache = _OHLCVDiskCache(self.disk_cache_dir)

    def register_provider(self, provider: DataProvider) -> None:
        """Register a data provider for OHLCV routing.

        Args:
            provider: A DataProvider implementation (e.g. GeckoTerminalOHLCVProvider).
        """
        self._providers[provider.name] = provider
        logger.debug("ohlcv_router_registered provider=%s", provider.name)

    def _consume_disk_cache(
        self,
        disk_key: str,
        limit: int,
        timeframe: str,
        now: datetime,
    ) -> DataEnvelope | None:
        """Disk-cache lookup with the ALM-2697 staleness guard at read time.

        Returns a fresh-cache-hit envelope when the cached entry is recent
        enough; otherwise None — either no cache entry, the entry is shorter
        than ``limit``, or the entry is stale and has been evicted. Caller
        falls through to the provider chain.
        """
        cached = self._disk_cache.get(disk_key)
        if cached is None or len(cached) < limit:
            return None

        cached_stale, cached_lag = _is_upstream_stale(cached, timeframe, now)
        if cached_stale:
            cached_lag_s = cached_lag.total_seconds() if cached_lag is not None else float("nan")
            logger.warning(
                "ohlcv_disk_cache_stale_evict key=%s lag_s=%.0f budget_s=%.0f timeframe=%s",
                disk_key,
                cached_lag_s,
                _staleness_budget(timeframe).total_seconds(),
                timeframe,
            )
            self._disk_cache.evict(disk_key)
            return None

        logger.debug("ohlcv_disk_cache_hit key=%s count=%d", disk_key, len(cached))
        return DataEnvelope(
            value=cached[-limit:],
            meta=DataMeta(
                source="disk_cache",
                observed_at=now,
                finality="off_chain",
                staleness_ms=0,
                latency_ms=0,
                confidence=1.0,
                cache_hit=True,
            ),
            classification=DataClassification.INFORMATIONAL,
        )

    @staticmethod
    def _record_miss(
        primary_name: str | None,
        primary_error: Exception | None,
        provider_name: str,
        miss: Exception,
    ) -> tuple[str | None, Exception | None]:
        """Update primary-provider tracking when a miss occurs.

        The first retryable (DEX) provider's most-recent failure becomes the
        "primary" error reported in the final raised ``DataSourceUnavailable``.
        Subsequent failures of that same provider update the terminal error;
        failures of non-retryable providers (CEX fallback) leave the primary
        tracking untouched. Caller still updates its own ``last_error``
        separately so the most-recent-across-all-providers view is preserved.
        """
        if provider_name in _RETRYABLE_PROVIDERS and (primary_name is None or primary_name == provider_name):
            return provider_name, miss
        return primary_name, primary_error

    @staticmethod
    def _log_and_sleep_retry_backoff(
        provider_name: str,
        instrument: Instrument,
        attempt: int,
        max_attempts: int,
        last_provider_exc: Exception | None,
    ) -> None:
        """Sleep the inter-attempt delay and emit the structured retry log.

        Honors any ``RetryInfo`` advisory the upstream attached to its last
        failure (VIB-3802); falls back to ``_backoff_delay(attempt)`` when no
        advisory is present.
        """
        advised = _extract_upstream_retry_delay(last_provider_exc)
        delay = min(advised, _UPSTREAM_RETRY_MAX_DELAY) if advised is not None else _backoff_delay(attempt)
        log_kind = "ohlcv_retry_upstream_advised" if advised is not None else "ohlcv_retry"
        logger.info(
            "%s provider=%s instrument=%s attempt=%d/%d delay_s=%.2f advised_s=%s",
            log_kind,
            provider_name,
            instrument.pair,
            attempt + 1,
            max_attempts,
            delay,
            f"{advised:.2f}" if advised is not None else "none",
        )
        time.sleep(delay)

    def _try_proxy_fallback(
        self,
        instrument: Instrument,
        target_chain: str,
        timeframe: str,
        limit: int,
        pool_address: str | None,
        force_provider: str | None,
        quote: str | None,
    ) -> DataEnvelope[list[OHLCVCandle]] | None:
        """Wrapped-token proxy retry: if every provider failed and the token
        has a known 1:1 unwrapped equivalent (``OHLCV_PROXY_MAP``), refetch
        under the proxy symbol and tag provenance. Returns ``None`` when no
        proxy is registered.
        """
        from dataclasses import replace

        proxy_symbol = OHLCV_PROXY_MAP.get(instrument.base)
        if not proxy_symbol:
            return None

        original_symbol = instrument.base
        if original_symbol not in self._proxy_warned:
            self._proxy_warned.add(original_symbol)
            logger.warning(
                "ohlcv_proxy_fallback using %s as proxy for %s (1:1 wrapped native, no direct data source)",
                proxy_symbol,
                original_symbol,
            )
        else:
            logger.debug(
                "ohlcv_proxy_fallback using %s as proxy for %s",
                proxy_symbol,
                original_symbol,
            )

        # Build Instrument directly to bypass _canonicalize_symbol which would
        # otherwise map MNT -> WMNT (the token we're falling back FROM).
        proxy_instrument = Instrument(
            base=proxy_symbol,
            quote=instrument.quote,
            chain=instrument.chain,
            venue=instrument.venue,
        )
        proxy_envelope = self.get_ohlcv(
            token=proxy_instrument,
            chain=target_chain,
            timeframe=timeframe,
            limit=limit,
            pool_address=pool_address,
            force_provider=force_provider,
            quote=quote,
            _is_proxy_retry=True,
        )
        return DataEnvelope(
            value=proxy_envelope.value,
            meta=replace(proxy_envelope.meta, proxy_source=original_symbol),
            classification=proxy_envelope.classification,
        )

    @staticmethod
    def _build_stale_response_miss(
        candles: list[OHLCVCandle],
        timeframe: str,
        provider_name: str,
        instrument: Instrument,
        target_chain: str,
        now: datetime,
        *,
        is_dex: bool = False,
    ) -> DataSourceUnavailable | None:
        """ALM-2697: upstream-staleness guard for fresh provider responses.

        An upstream that has silently stopped tracking a symbol (Binance +
        MATICUSDT post-rebrand is the canonical case) will keep returning
        fully-populated klines whose newest candle is days old. Without this
        gate, every candle gets classified as "finalized" (>24h),
        ``_split_by_finality`` writes the bag to the disk cache, and every
        subsequent iteration serves the same byte-identical snapshot — RSI
        freezes.

        Returns the typed ``DataSourceUnavailable`` to record as a miss when
        the response is stale (caller breaks to the next provider in the
        chain), or ``None`` when the response is fresh. Logs the structured
        ``ohlcv_upstream_stale`` warning at the staleness verdict so callers
        do not need to.
        """
        is_stale, lag = _is_upstream_stale(candles, timeframe, now, is_dex=is_dex)
        if not is_stale:
            return None

        budget = _staleness_budget(timeframe, is_dex=is_dex)
        lag_seconds = lag.total_seconds() if lag is not None else float("nan")
        budget_seconds = budget.total_seconds()
        logger.warning(
            "ohlcv_upstream_stale provider=%s instrument=%s timeframe=%s lag_s=%.0f budget_s=%.0f candles=%d",
            provider_name,
            instrument.pair,
            timeframe,
            lag_seconds,
            budget_seconds,
            len(candles),
        )
        return DataSourceUnavailable(
            source=provider_name,
            reason=(
                f"{provider_name} returned stale OHLCV for {instrument.pair} on "
                f"{target_chain}: youngest candle is {lag_seconds:.0f}s behind "
                f"wall-clock (budget {budget_seconds:.0f}s for timeframe {timeframe}); "
                "treating as provider miss"
            ),
        )

    @staticmethod
    def _apply_dex_forward_fill(
        candles: list[OHLCVCandle],
        provider_name: str,
        timeframe: str,
        instrument: Instrument,
        now: datetime,
    ) -> tuple[list[OHLCVCandle], int]:
        """VIB-4875: trailing-edge forward-fill for DEX quiet-pool sources.

        No-op for non-DEX providers (see ``_DEX_QUIET_POOL_PROVIDERS``). Applied
        *before* the staleness guard so a quiet-but-live pool yields a continuous,
        current series instead of a false ``ohlcv_upstream_stale`` miss. Returns
        ``(candles, n_synth)`` — the (possibly extended) ascending list and the
        count of synthetic buckets appended (0 = no fill). The caller needs the
        count, not just a boolean, to keep synthetic buckets out of the finalized
        disk cache. Logs the structured ``ohlcv_dex_forward_fill`` line at the
        fill verdict so the caller does not need to.
        """
        if provider_name not in _DEX_QUIET_POOL_PROVIDERS:
            return candles, 0
        filled, n_synth = _forward_fill_dex_candles(candles, timeframe, now)
        if n_synth:
            logger.info(
                "ohlcv_dex_forward_fill provider=%s instrument=%s timeframe=%s synth=%d",
                provider_name,
                instrument.pair,
                timeframe,
                n_synth,
            )
        return filled, n_synth

    def _forward_fill_and_trim(
        self,
        candles: list[OHLCVCandle],
        provider_name: str,
        timeframe: str,
        instrument: Instrument,
        now: datetime,
        limit: int,
    ) -> tuple[list[OHLCVCandle], int]:
        """Forward-fill a quiet DEX pool, then clamp to the requested ``limit``.

        ``limit`` is a maximum (``MarketSnapshot.ohlcv`` contract): synthetic
        forward-fill buckets must not silently widen the lookback window past
        what was requested. Keep the newest ``limit`` buckets — the synthetic
        tail is anchored at ``now``, so trimming drops the oldest *real* candles
        first and the series stays current (VIB-4875). Returns the (possibly
        trimmed) candle list and the surviving synthetic-bucket count.
        """
        candles, n_synth = self._apply_dex_forward_fill(candles, provider_name, timeframe, instrument, now)
        if len(candles) > limit:
            candles = candles[-limit:]
            n_synth = min(n_synth, len(candles))
        return candles, n_synth

    def _build_success_envelope(
        self,
        *,
        candles: list[OHLCVCandle],
        n_synth: int,
        provider_name: str,
        instrument: Instrument,
        base_confidence: float,
        latency_ms: int,
        attempt: int,
        disk_key: str,
        now: datetime,
    ) -> DataEnvelope[list[OHLCVCandle]]:
        """Assemble the success-path envelope for a fetched OHLCV response.

        Applies the confidence haircuts (CEX/DEX basis risk and forward-fill),
        splits finality, persists finalized *real* candles to the disk cache,
        and stamps ``DataMeta``. Extracted from ``get_ohlcv`` to keep that
        method's complexity bounded.

        Synthetic forward-fill buckets are NEVER finalized: for longer timeframes
        a synthetic bucket can age past the 24h finalization cutoff, but it is a
        no-trade placeholder, not immutable history — persisting it to the disk
        cache would later serve it as ``source="disk_cache"`` confidence=1.0
        immutable OHLCV. Split only the real candles; the synthetic tail (always
        the last ``n_synth``) stays provisional (VIB-4875).
        """
        forward_filled = n_synth > 0

        # CEX source used for a DeFi-native pair carries basis risk.
        is_cex_source = provider_name in ("binance", "coingecko")
        has_basis_risk = is_cex_source and classify_instrument(instrument) == "defi_primary"

        confidence = base_confidence
        if has_basis_risk:
            confidence = min(confidence, 0.7)
            logger.warning(
                "cex_dex_basis_warning instrument=%s provider=%s confidence=%.2f "
                "reason='CEX source used for DeFi-native pair, basis risk'",
                instrument.pair,
                provider_name,
                confidence,
            )
        if forward_filled:
            confidence = min(confidence, _DEX_FORWARD_FILL_CONFIDENCE)

        real_candles = candles[:-n_synth] if n_synth else candles
        finalized, provisional = _split_by_finality(real_candles, now)
        if n_synth:
            provisional = provisional + candles[-n_synth:]

        if finalized:
            self._disk_cache.put(disk_key, finalized)

        meta = DataMeta(
            source=provider_name,
            observed_at=now,
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=confidence,
            cache_hit=False,
            forward_filled=forward_filled,
        )

        logger.info(
            "ohlcv_fetched provider=%s instrument=%s attempt=%d candles=%d finalized=%d provisional=%d",
            provider_name,
            instrument.pair,
            attempt + 1,
            len(candles),
            len(finalized),
            len(provisional),
        )

        return DataEnvelope(
            value=candles,
            meta=meta,
            classification=DataClassification.INFORMATIONAL,
        )

    def get_ohlcv(  # noqa: C901
        self,
        token: str | Instrument,
        chain: str | None = None,
        timeframe: str = "1h",
        limit: int = 100,
        *,
        pool_address: str | None = None,
        force_provider: str | None = None,
        quote: str | None = None,
        _is_proxy_retry: bool = False,
    ) -> DataEnvelope[list[OHLCVCandle]]:
        """Fetch OHLCV candles with multi-provider routing.

        Classifies the instrument as CEX-primary or DeFi-primary and
        routes through the appropriate provider chain.

        Args:
            token: Token symbol, "BASE/QUOTE" string, or Instrument.
            chain: Chain name (default: self.default_chain).
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d).
            limit: Number of candles.
            pool_address: Explicit pool address for DEX providers.
            force_provider: Force a specific provider (bypass classification).

        Returns:
            DataEnvelope[list[OHLCVCandle]] with meta.source and finality tags.

        Raises:
            DataSourceUnavailable: If all providers fail.
        """
        target_chain = (chain or self.default_chain).lower()
        instrument = (
            resolve_instrument(token, target_chain, quote=quote) if not isinstance(token, Instrument) else token
        )

        # Determine provider chain
        if force_provider:
            provider_chain = [force_provider]
        else:
            classification = classify_instrument(instrument)
            provider_chain = _PROVIDER_CHAINS.get(classification, ["binance"])
            logger.debug(
                "ohlcv_classified instrument=%s classification=%s providers=%s",
                instrument.pair,
                classification,
                provider_chain,
            )

        # Disk cache lookup, with the ALM-2697 staleness guard baked in. Returns
        # a DataEnvelope on a fresh cache hit; otherwise the cache is missing,
        # too short, or stale (and has been evicted) and we fall through to the
        # provider chain.
        addr = (pool_address or "auto").lower()
        disk_key = f"{instrument.base}:{instrument.quote}:{target_chain}:{timeframe}:{limit}:{addr}"
        now = datetime.now(UTC)
        if (cache_hit := self._consume_disk_cache(disk_key, limit, timeframe, now)) is not None:
            return cache_hit

        # Try each provider in chain.
        # - primary_provider_name / primary_error: tracks the terminal (most recent)
        #   failure of the first retryable DEX provider attempted. Updated on every
        #   failure of that same provider so the final error reflects its actual last
        #   known state rather than a stale transient from an early retry.
        # - last_error: most recent failure across all providers (often the futile
        #   CEX "unknown token" fallback). Both are used to compose the raised DSU.
        primary_provider_name: str | None = None
        primary_error: Exception | None = None
        last_error: Exception | None = None

        for provider_name in provider_chain:
            provider = self._providers.get(provider_name)
            if provider is None:
                if last_error is None:
                    # Record a sentinel so that if every provider is unregistered
                    # the final error message is informative rather than "None".
                    last_error = DataSourceUnavailable(
                        source=provider_name,
                        reason=f"{provider_name} provider is not registered",
                    )
                logger.debug("ohlcv_provider_not_registered provider=%s", provider_name)
                continue

            # Give DEX providers bounded retries; CEX paths get one attempt only
            # because their failures are almost always deterministic (no symbol).
            max_attempts = (1 + _MAX_PRIMARY_RETRIES) if provider_name in _RETRYABLE_PROVIDERS else 1

            # Tracks the exception from the most recent failed attempt against
            # this provider, so the next iteration's backoff can honor any
            # upstream-advised RetryInfo (VIB-3802).
            last_provider_exc: Exception | None = None

            for attempt in range(max_attempts):
                if attempt > 0:
                    self._log_and_sleep_retry_backoff(
                        provider_name, instrument, attempt, max_attempts, last_provider_exc
                    )

                start = time.monotonic()
                try:
                    envelope = provider.fetch(
                        token=instrument.base,
                        quote=instrument.quote,
                        chain=target_chain,
                        timeframe=timeframe,
                        limit=limit,
                        pool_address=pool_address or "",
                    )
                    latency_ms = int((time.monotonic() - start) * 1000)
                    candles: list[OHLCVCandle] = envelope.value

                    if not candles:
                        miss = DataSourceUnavailable(
                            source=provider_name,
                            reason=f"{provider_name} returned no OHLCV candles for {instrument.pair} on {target_chain}",
                        )
                        last_error = miss
                        primary_provider_name, primary_error = self._record_miss(
                            primary_provider_name, primary_error, provider_name, miss
                        )
                        logger.debug("ohlcv_empty_result provider=%s", provider_name)
                        break  # treat empty result as a provider miss, skip to next

                    now = datetime.now(UTC)

                    # VIB-4875: quiet-pool trailing-edge forward-fill for DEX
                    # sources, applied BEFORE the staleness guard so a quiet
                    # (but live) pool yields a continuous, current series rather
                    # than a false "stale upstream" miss. `limit` is honored
                    # (it is a maximum) inside the helper.
                    candles, n_synth = self._forward_fill_and_trim(
                        candles, provider_name, timeframe, instrument, now, limit
                    )

                    # ALM-2697: upstream-staleness guard. Stale response →
                    # provider miss; never cached, never returned. DEX sources
                    # use the relaxed dead-pool budget (VIB-4875). The helper
                    # logs the structured warning; we just record the miss and
                    # break out to the next provider.
                    if (
                        stale_miss := self._build_stale_response_miss(
                            candles,
                            timeframe,
                            provider_name,
                            instrument,
                            target_chain,
                            now,
                            is_dex=provider_name in _DEX_QUIET_POOL_PROVIDERS,
                        )
                    ) is not None:
                        last_error = stale_miss
                        primary_provider_name, primary_error = self._record_miss(
                            primary_provider_name, primary_error, provider_name, stale_miss
                        )
                        break  # fall through to next provider in the chain

                    return self._build_success_envelope(
                        candles=candles,
                        n_synth=n_synth,
                        provider_name=provider_name,
                        instrument=instrument,
                        base_confidence=envelope.meta.confidence,
                        latency_ms=latency_ms,
                        attempt=attempt,
                        disk_key=disk_key,
                        now=now,
                    )

                except Exception as exc:
                    last_error = exc
                    last_provider_exc = exc
                    primary_provider_name, primary_error = self._record_miss(
                        primary_provider_name, primary_error, provider_name, exc
                    )

                    elapsed = int((time.monotonic() - start) * 1000)

                    # Retry on transient errors if we have attempts left
                    if attempt < max_attempts - 1 and _is_retryable_exc(exc):
                        logger.warning(
                            "ohlcv_retry_scheduled provider=%s instrument=%s attempt=%d/%d error=%s elapsed_ms=%d",
                            provider_name,
                            instrument.pair,
                            attempt + 1,
                            max_attempts,
                            exc,
                            elapsed,
                        )
                        continue  # retry this provider

                    logger.warning(
                        "ohlcv_provider_failed provider=%s instrument=%s attempt=%d/%d error=%s elapsed_ms=%d",
                        provider_name,
                        instrument.pair,
                        attempt + 1,
                        max_attempts,
                        exc,
                        elapsed,
                    )
                    break  # move to next provider

        # Wrapped token proxy fallback: if all providers failed and the
        # token has a known unwrapped equivalent, retry with the proxy.
        if (
            not _is_proxy_retry
            and (
                proxy_envelope := self._try_proxy_fallback(
                    instrument, target_chain, timeframe, limit, pool_address, force_provider, quote
                )
            )
            is not None
        ):
            return proxy_envelope

        # Lead with the primary (DEX) error so logs point at the actionable
        # cause, not the trailing CEX "unknown token" from a known-futile path.
        # Use _error_text() to extract raw reason text instead of str(exc) so
        # DSU boilerplate ("Data source '...' unavailable:") is never re-embedded
        # into the composed reason, which would confuse downstream hint matching.
        if primary_error is not None and primary_error is not last_error:
            reason = (
                f"All providers failed for {instrument.pair} on {target_chain}"
                f" — primary: {_error_text(primary_error)}; last: {_error_text(last_error)}"
            )
        else:
            reason = f"All providers failed for {instrument.pair} on {target_chain}: {_error_text(last_error)}"
        raise DataSourceUnavailable(source="ohlcv_router", reason=reason)


def _split_by_finality(
    candles: list[OHLCVCandle],
    now: datetime,
) -> tuple[list[OHLCVCandle], list[OHLCVCandle]]:
    """Split candles into finalized (>24h old) and provisional (recent).

    Args:
        candles: Sorted list of candles.
        now: Current UTC time.

    Returns:
        Tuple of (finalized, provisional) candle lists.
    """
    cutoff = now - _FINALIZATION_AGE
    finalized: list[OHLCVCandle] = []
    provisional: list[OHLCVCandle] = []

    for candle in candles:
        if candle.timestamp < cutoff:
            finalized.append(candle)
        else:
            provisional.append(candle)

    return finalized, provisional


def _staleness_budget(timeframe: str, *, is_dex: bool = False) -> timedelta:
    """Return the wall-clock lag budget for the youngest candle on *timeframe*.

    See module-level ``_STALE_TIMEFRAME_MULTIPLE`` / ``_STALE_MIN_BUDGET``
    notes for rationale. Unknown timeframes fall back to 1 hour, matching
    the router's default ``timeframe="1h"`` so the floor remains meaningful
    even when a caller passes an unsupported interval.

    When ``is_dex`` is set (a DEX quiet-pool source, see
    ``_DEX_QUIET_POOL_PROVIDERS``) the relaxed ``_DEX_STALE_TIMEFRAME_MULTIPLE``
    is used instead — this is the dead-pool horizon: a quiet pool may legitimately
    lag wall-clock by many timeframes without the feed being broken (VIB-4875).
    """
    seconds = _TIMEFRAME_SECONDS.get(timeframe, 3600)
    multiple = _DEX_STALE_TIMEFRAME_MULTIPLE if is_dex else _STALE_TIMEFRAME_MULTIPLE
    budget = timedelta(seconds=seconds * multiple)
    return max(budget, _STALE_MIN_BUDGET)


def _is_upstream_stale(
    candles: list[OHLCVCandle],
    timeframe: str,
    now: datetime,
    *,
    is_dex: bool = False,
) -> tuple[bool, timedelta | None]:
    """Detect whether an upstream OHLCV response is stale relative to *now*.

    A response is judged stale when its youngest candle's *start-time* (the
    candle's ``timestamp`` field) lags wall-clock by more than the budget
    returned by ``_staleness_budget``. Start-time is a stricter signal than
    close-time (close-time = start-time + timeframe), so any check passing
    start-time also passes close-time — the extra margin is intentional.
    Empty responses are *not* flagged here — the router has a separate
    "empty result" path (treated as a provider miss) and we don't want
    those collapsed onto the same code branch, since the actionable signal
    differs (empty = "upstream said nothing", stale = "upstream said
    something old").

    Returns:
        ``(is_stale, lag)`` where ``lag`` is ``now - youngest_candle_ts``
        when the input is non-empty, otherwise ``None``. Callers use the
        lag value for log diagnostics; the boolean is the gate.
    """
    if not candles:
        return False, None

    # Candles are produced in ascending order by every provider in this
    # codebase; defend against sloppy upstreams by taking the explicit max
    # rather than trusting [-1].
    youngest = max(candle.timestamp for candle in candles)
    lag = now - youngest
    return lag > _staleness_budget(timeframe, is_dex=is_dex), lag


def _forward_fill_dex_candles(
    candles: list[OHLCVCandle],
    timeframe: str,
    now: datetime,
) -> tuple[list[OHLCVCandle], int]:
    """Trailing-edge forward-fill for a quiet on-chain DEX pool (VIB-4875).

    A DEX OHLCV source only emits a bucket when a swap occurs, so a genuinely
    quiet pool returns a newest candle that lags wall-clock — which the
    staleness guard would otherwise reject as a dead feed, stranding the
    strategy in ``DATA_ERROR``. For a DEX source that is the *correct* on-chain
    state ("price didn't move because nothing traded"), so we synthesise flat
    candles (carry the last close, zero volume) from the youngest real candle
    up to the current wall-clock bucket. This advances the newest timestamp so
    the guard passes AND hands indicators (RSI/MACD/ATR) a continuous, current
    series.

    Bounded by the dead-pool horizon: if the gap exceeds the DEX staleness
    budget the pool is presumed dead/illiquid and is left untouched (the guard
    then rejects it). This caps the synthetic-candle count at
    ``_DEX_STALE_TIMEFRAME_MULTIPLE``. For longer timeframes that horizon spans
    more than 24h (e.g. ``1d`` → 24 days), so synthetic buckets *can* age past
    the finalization cutoff; the caller therefore excludes the synthetic tail
    from finalized disk caching explicitly rather than relying on recency.

    Returns ``(candles, n_synth)`` — the (possibly extended) ascending candle
    list and the number of synthetic candles appended (0 when no fill applied).
    """
    if not candles:
        return candles, 0

    tf_seconds = _TIMEFRAME_SECONDS.get(timeframe, 3600)
    step = timedelta(seconds=tf_seconds)
    youngest = max(candles, key=lambda c: c.timestamp)
    gap = now - youngest.timestamp

    # Already current (the in-progress / just-closed bucket is present), or so
    # stale the pool is presumed dead — in both cases, no forward-fill.
    if gap <= step or gap > _staleness_budget(timeframe, is_dex=True):
        return candles, 0

    # Fill up to the current wall-clock bucket start (floor(now / tf)).
    current_bucket = datetime.fromtimestamp((int(now.timestamp()) // tf_seconds) * tf_seconds, tz=UTC)
    last_close = youngest.close
    synthetic: list[OHLCVCandle] = []
    stamp = youngest.timestamp + step
    while stamp <= current_bucket:
        synthetic.append(
            OHLCVCandle(
                timestamp=stamp,
                open=last_close,
                high=last_close,
                low=last_close,
                close=last_close,
                volume=Decimal(0),
            )
        )
        stamp += step

    if not synthetic:
        return candles, 0
    return candles + synthetic, len(synthetic)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "OHLCVRouter",
    "classify_instrument",
    "provider_names_in_chains",
]
