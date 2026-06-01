"""Generic two-tier cache for gateway history services (POOL-4 / VIB-4752).

Implements the two-tier dual cache from PoolX.md §D6 — public cache keyed
without provider, raw cache keyed with provider, bounded LRU on entries
AND bytes (inherited audit row #7 extended), in-flight asyncio.Task dedup
(inherited row #8), and TTL-based finality semantics (PoolX.md §D4).

Generic over value type ``V`` so ``RateHistoryService`` (VIB-4747) can
reuse it with a different proto message. Generic over key type ``K``
so callers can pass whatever Hashable tuple matches the cache tier
(7-tuple public, 8-tuple raw).

Inherited audit rows enforced here:

* **#7 (extended)**: Both bounds — ``_CACHE_MAX_ENTRIES`` AND
  ``_CACHE_MAX_BYTES`` — are honoured. Eviction by either dimension
  bumps its own counter; ``health()`` exposes both so a runaway
  upstream can be diagnosed.
* **#8**: In-flight dedup. N concurrent identical ``get_or_fetch``
  callers share ONE upstream invocation through a single
  ``asyncio.Future`` slot. A cancelled fetcher propagates the
  cancellation to all awaiters (consistent with VIB-4727's pattern).
* **#11 (per-DTO Empty != Zero)**: out of scope for the cache; the
  responses themselves carry ``unmeasured_fields``.

Finality semantics (PoolX.md §D4):

* ``finality_band="provisional"`` → short TTL (60s default). A row
  within the per-provider finality cutoff is provisional; it MAY be
  re-fetched cheaply.
* ``finality_band="finalized"`` → long TTL (24h default). The row
  is durable and won't change.

POOL-6 (VIB-4754) will re-promote provisional entries to finalized
when the row ages past the cutoff. The cache exposes ``get`` /
``put`` semantics that make that re-promotion possible (the key is
stable across the band flip); the actual re-promotion logic is
POOL-6's.

The cache instance does NOT itself talk to providers — POOL-5
(VIB-4753) wires the dispatcher as the ``fetcher`` argument to
``get_or_fetch``. POOL-4 lands the cache primitives only.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Public TTL constants (PoolX.md §D4)
# -----------------------------------------------------------------------------

#: Provisional TTL — short, because a trailing-bar row within the per-provider
#: finality cutoff is allowed to revise on subsequent fetches.
DEFAULT_PROVISIONAL_TTL_SECONDS: float = 60.0

#: Finalized TTL — long, because rows older than the per-provider finality
#: cutoff are durable. Re-fetching them burns quota for no value.
DEFAULT_FINALIZED_TTL_SECONDS: float = 86400.0

#: Default cap on cache ENTRIES (rows). Mirrors VIB-4727's analytics cache
#: ceiling so the two services scale identically.
DEFAULT_MAX_ENTRIES: int = 5000

#: Default cap on cache RESIDENT BYTES. 64 MiB is comfortable for a long-
#: uptime gateway running multiple strategies; bounded so unique-key traffic
#: can't leak memory.
DEFAULT_MAX_BYTES: int = 64 * 1024 * 1024

# Env-var keys for the cache caps. Bound + validated on
# :class:`almanak.gateway.core.settings.GatewaySettings`
# (``pool_history_cache_max_entries`` / ``pool_history_cache_max_bytes``).
# Listed here for diff-ability and operator discoverability; this module
# does NOT auto-read env (callers pass a ``GatewaySettings`` instance to
# ``load_max_entries_from_settings`` / ``load_max_bytes_from_settings`` so
# tests can construct caches with explicit values).
ENV_MAX_ENTRIES: str = "ALMANAK_GATEWAY_POOL_HISTORY_CACHE_MAX_ENTRIES"
ENV_MAX_BYTES: str = "ALMANAK_GATEWAY_POOL_HISTORY_CACHE_MAX_BYTES"


# -----------------------------------------------------------------------------
# Finality band — explicit Literal-style for typo safety
# -----------------------------------------------------------------------------

FINALITY_PROVISIONAL: str = "provisional"
FINALITY_FINALIZED: str = "finalized"
_VALID_FINALITY_BANDS: frozenset[str] = frozenset({FINALITY_PROVISIONAL, FINALITY_FINALIZED})


# -----------------------------------------------------------------------------
# Cache entry record
# -----------------------------------------------------------------------------


@dataclass
class _CacheEntry[V]:
    """One row in the cache. Mutable so finality re-promotion (POOL-6) can
    flip the band in place without disturbing LRU position or key."""

    value: V
    expires_at: float  # monotonic clock; compared with the cache's clock fn
    size_bytes: int
    finality_band: str


# -----------------------------------------------------------------------------
# HistoryCache
# -----------------------------------------------------------------------------


class HistoryCache[K: Hashable, V]:
    """Bounded LRU cache with TTL + in-flight dedup.

    Two are typically instantiated per service:

    * ``public_cache`` — provider-OMITTED key (7-tuple for pool history).
      The "user-visible" cache. ``get_or_fetch`` lives here.
    * ``raw_cache``    — provider-INCLUDED key (8-tuple). Per-provider
      response store; the dispatcher writes here so successful provider
      responses survive even when a different provider wins the public
      cache race. Uses ``partition_extractor`` to track entries-by-provider
      for ``health()``.

    Single-threaded asyncio model: the cache is shared across coroutines
    on one event loop. A ``threading.Lock`` guards mutations so the
    semantics are also correct if a future caller invokes from a worker
    thread (mirrors VIB-4727's belt-and-suspenders pattern).
    """

    def __init__(
        self,
        *,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        max_bytes: int = DEFAULT_MAX_BYTES,
        size_estimator: Callable[[V], int],
        provisional_ttl_seconds: float = DEFAULT_PROVISIONAL_TTL_SECONDS,
        finalized_ttl_seconds: float = DEFAULT_FINALIZED_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        partition_extractor: Callable[[K], str] | None = None,
        repromoter: Callable[[V], str | None] | None = None,
        name: str = "history_cache",
    ) -> None:
        if max_entries <= 0:
            raise ValueError(f"max_entries must be > 0, got {max_entries}")
        if max_bytes <= 0:
            raise ValueError(f"max_bytes must be > 0, got {max_bytes}")
        if provisional_ttl_seconds <= 0:
            raise ValueError("provisional_ttl_seconds must be > 0")
        if finalized_ttl_seconds <= 0:
            raise ValueError("finalized_ttl_seconds must be > 0")
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._size_estimator = size_estimator
        self._provisional_ttl = provisional_ttl_seconds
        self._finalized_ttl = finalized_ttl_seconds
        self._clock = clock
        self._partition_extractor = partition_extractor
        # POOL-6 (VIB-4754) finality re-promotion hook. Called UNDER ``_lock``
        # with the cached value when a ``provisional`` entry's TTL has expired;
        # returns the new finality band if the value has aged past the
        # provider's finality cutoff (and mutates the value's ``finalized_only``
        # in place), or None to evict + miss as normal. MUST be fast and MUST
        # NOT re-enter the cache (the non-reentrant lock is held). Only the
        # public cache wires this; the raw cache does not re-promote.
        self._repromoter = repromoter
        self._name = name
        self._entries: OrderedDict[K, _CacheEntry[V]] = OrderedDict()
        self._bytes_resident: int = 0
        # Counters exposed via ``stats()`` and consumed by the servicer's
        # ``health()``. Names mirror the locked schema in
        # ``pool_history_service.py``.
        self._hits: int = 0
        self._misses: int = 0
        self._evictions_by_entries: int = 0
        self._evictions_by_bytes: int = 0
        self._inflight_dedup_hits: int = 0
        self._entries_by_partition: dict[str, int] = {}
        # State lock for the cache dict. Asyncio is cooperative within a
        # single event loop, but the lock also defends against the rare
        # worker-thread caller.
        self._lock = threading.Lock()
        # In-flight futures keyed by the SAME key used for ``get`` — this
        # is set/cleared inside ``get_or_fetch``. The dict itself is
        # guarded by ``_lock``; the futures are awaited without holding
        # the lock so callers don't serialize on a slow fetcher.
        self._inflight: dict[K, asyncio.Future[V]] = {}

    # -- Public API: synchronous get / put --------------------------------

    def get(self, key: K) -> V | None:
        """Return cached value (touching LRU) or None on miss / expiry.

        Expired entries are evicted on access and counted toward ``misses``
        (not ``evictions_*``, because TTL eviction is "natural decay",
        not pressure-driven displacement).
        """
        now = self._clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return None
            if now > entry.expires_at:
                # TTL expired. Before evicting, try finality re-promotion: a
                # provisional entry whose data has aged past the cutoff is now
                # finalized + durable, so we flip its band + extend its TTL in
                # place rather than evicting + re-fetching (D3.F9).
                if self._try_repromote_locked(key, entry, now):
                    return entry.value
                self._remove_locked(key)
                self._misses += 1
                return None
            # Touch LRU position.
            self._entries.move_to_end(key, last=True)
            self._hits += 1
            return entry.value

    def _try_repromote_locked(self, key: K, entry: _CacheEntry[V], now: float) -> bool:
        """Re-promote an expired ``provisional`` entry to ``finalized`` in place.

        Caller MUST hold ``self._lock``. Returns True iff the entry was
        re-promoted (band flipped, TTL extended, LRU touched, value mutated by
        the repromoter) — in which case the caller serves ``entry.value`` as a
        hit. The mutation is in place (no remove/insert), so the key never
        leaves the dict and a concurrent reader during the flip gets the entry,
        never a miss-then-insert (D3.F9 atomicity).

        Only provisional entries are eligible. The repromoter decides finality
        from the value (e.g. ``now - newest_row_ts > provider_cutoff``); a None
        return means "still provisional — let it expire and re-fetch."

        Trade-off (PoolX.md §D4): re-promotion serves the value as captured at
        its last fetch. A revision that landed in the gap since then is missed —
        accepted because once a row ages past the finality cutoff it no longer
        revises, and the alternative (always re-fetch) burns provider quota.
        """
        if self._repromoter is None or entry.finality_band != FINALITY_PROVISIONAL:
            return False
        new_band = self._repromoter(entry.value)
        if new_band is None or new_band == FINALITY_PROVISIONAL:
            return False
        # The guard above already returned for None / provisional, and
        # ``finalized`` is the only other valid band, so a re-promotion always
        # extends to the finalized TTL.
        ttl = self._finalized_ttl
        entry.finality_band = new_band
        entry.expires_at = now + ttl
        self._entries.move_to_end(key, last=True)
        self._hits += 1
        return True

    def put(self, key: K, value: V, finality_band: str) -> None:
        """Insert / replace ``key`` with ``value``. Evicts as needed.

        ``finality_band`` selects the TTL (``provisional`` ≈ 60s,
        ``finalized`` ≈ 24h by default; both knobs are construction args).
        """
        if finality_band not in _VALID_FINALITY_BANDS:
            raise ValueError(f"finality_band must be one of {sorted(_VALID_FINALITY_BANDS)}, got {finality_band!r}")
        size_bytes = self._size_estimator(value)
        if size_bytes < 0:
            raise ValueError(f"size_estimator returned negative bytes: {size_bytes}")
        ttl = self._provisional_ttl if finality_band == FINALITY_PROVISIONAL else self._finalized_ttl
        expires_at = self._clock() + ttl
        entry = _CacheEntry(value=value, expires_at=expires_at, size_bytes=size_bytes, finality_band=finality_band)
        with self._lock:
            # If replacing, account the old entry's bytes / partition first.
            if key in self._entries:
                self._remove_locked(key)
            self._entries[key] = entry
            self._bytes_resident += size_bytes
            if self._partition_extractor is not None:
                partition = self._partition_extractor(key)
                self._entries_by_partition[partition] = self._entries_by_partition.get(partition, 0) + 1
            # Evict oldest until under both caps.
            while len(self._entries) > self._max_entries:
                evicted_key = next(iter(self._entries))
                self._remove_locked(evicted_key)
                self._evictions_by_entries += 1
            while self._bytes_resident > self._max_bytes and self._entries:
                evicted_key = next(iter(self._entries))
                self._remove_locked(evicted_key)
                self._evictions_by_bytes += 1

    def invalidate(self, key: K) -> bool:
        """Drop ``key`` from the cache. Returns True if it was present."""
        with self._lock:
            if key in self._entries:
                self._remove_locked(key)
                return True
            return False

    # -- Public API: async dedup ------------------------------------------

    async def get_or_fetch(
        self,
        key: K,
        fetcher: Callable[[], Awaitable[tuple[V, str]]],
    ) -> V:
        """Get from cache or dedupe concurrent cold fetches (inherited #8).

        ``fetcher`` is an async callable returning ``(value, finality_band)``.
        N concurrent callers for the same key share ONE invocation; the
        first caller drives the fetch, subsequent callers ``await`` the
        same future and count toward ``inflight_dedup_hits``.

        Behaviour:

        * Cache hit -> return cached value (no fetcher call).
        * Cache miss + no inflight -> create future, call fetcher
          (without holding the lock), populate cache, set future result.
        * Cache miss + inflight exists -> await the existing future;
          counts as an ``inflight_dedup_hit``.
        * If the fetcher raises (or is cancelled), the exception
          propagates to ALL awaiters. The cache is not poisoned —
          the inflight slot is cleared in ``finally``.
        """
        # Fast path: cache hit.
        cached = self.get(key)
        if cached is not None:
            return cached

        # Lock the inflight check + creation atomically. Re-check the
        # cache inside the lock to close the window between the fast-path
        # ``self.get(key)`` above and acquiring the lock — a peer may have
        # finished its fetch and populated ``_entries`` in that window;
        # without this check the current task would redundantly trigger a
        # second fetch instead of returning the peer's value.
        #
        # We reach into ``self._entries`` directly here rather than calling
        # ``self.get()`` because ``self._lock`` is non-reentrant
        # (``threading.Lock``) — re-entering would deadlock. The hit /
        # miss counter accounting mirrors ``self.get()``'s logic so the
        # locked re-check is observationally equivalent to a real cache
        # hit (LRU touch + ``cache_hits++``).
        with self._lock:
            now = self._clock()
            entry = self._entries.get(key)
            if entry is not None:
                if now <= entry.expires_at:
                    # Locked cache hit — touch LRU position and bump
                    # hits to match ``self.get()`` semantics.
                    self._entries.move_to_end(key, last=True)
                    self._hits += 1
                    return entry.value
                # TTL expired — try finality re-promotion before evicting
                # (matches ``self.get()``); a re-promotable provisional entry is
                # served as a hit rather than re-fetched (D3.F9).
                if self._try_repromote_locked(key, entry, now):
                    return entry.value
                self._remove_locked(key)
                self._misses += 1

            existing = self._inflight.get(key)
            if existing is None:
                # We are the lead caller. Create future, register, drop
                # the lock; we'll fill it below. ``get_running_loop()`` is
                # the modern preferred API (``get_event_loop()`` is
                # deprecated for the no-running-loop case); inside an
                # ``async`` method a loop is always running.
                loop = asyncio.get_running_loop()
                future: asyncio.Future[V] = loop.create_future()
                self._inflight[key] = future
                lead = True
            else:
                # A peer is already fetching. We dedupe.
                future = existing
                lead = False
                self._inflight_dedup_hits += 1

        if not lead:
            # We're a peer; await the lead caller's future. Any
            # exception propagates verbatim.
            return await future

        # Lead caller path: invoke fetcher, populate cache, settle future.
        # ``try/finally`` (not ``try/except/else``) is mandatory here: if
        # ``self.put()`` raises after a successful fetch (e.g. a buggy
        # ``size_estimator`` throws), the inflight slot MUST still be
        # cleared and the future MUST be settled, otherwise every awaiter
        # for ``key`` hangs forever. ``BaseException`` (catching
        # KeyboardInterrupt / CancelledError too) is the right
        # generalisation, but ``future.set_exception`` only accepts
        # ``Exception``; for non-``Exception`` cases we cancel the future
        # so awaiters see ``CancelledError`` rather than a ``TypeError``
        # from ``set_exception``.
        try:
            value, finality_band = await fetcher()
            # Populate cache before settling the future so peer awaiters
            # see a cache hit on their retry path (defense-in-depth).
            self.put(key, value, finality_band)
            if not future.done():
                future.set_result(value)
            return value
        except BaseException as exc:
            if not future.done():
                if isinstance(exc, Exception):
                    future.set_exception(exc)
                    # The lead caller re-raises ``exc`` below; it never awaits
                    # its OWN future (only deduped peers do, at the branch
                    # above). With no peers, the set exception is GC'd
                    # unretrieved and asyncio logs "Future exception was never
                    # retrieved" on EVERY failure path — loud noise once a
                    # caller (e.g. PoolHistoryService, VIB-4753) raises through
                    # ``get_or_fetch`` on provider exhaustion. Retrieve it here
                    # to clear the flag; a peer's ``await future`` still
                    # re-raises ``exc`` verbatim.
                    future.exception()
                else:
                    future.cancel()
            raise
        finally:
            with self._lock:
                self._inflight.pop(key, None)

    # -- Stats / health ---------------------------------------------------

    def stats(self) -> dict[str, int]:
        """Return a counter snapshot. Mirrors the per-RPC cache slice of
        the locked ``health()`` schema in ``pool_history_service.py``.
        Safe to call concurrently; returns a copy."""
        with self._lock:
            return {
                "entries_resident": len(self._entries),
                "bytes_resident": self._bytes_resident,
                "cache_hits": self._hits,
                "cache_misses": self._misses,
                "cache_evictions_by_entries": self._evictions_by_entries,
                "cache_evictions_by_bytes": self._evictions_by_bytes,
                "inflight_dedup_hits": self._inflight_dedup_hits,
                "inflight_pending": len(self._inflight),
            }

    @property
    def entries_resident(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def bytes_resident(self) -> int:
        with self._lock:
            return self._bytes_resident

    @property
    def entries_by_partition(self) -> dict[str, int]:
        """Return per-partition entry counts (defensive copy).

        Empty when the cache was constructed without ``partition_extractor``
        (e.g. the public cache). Populated for the raw cache where the
        extractor pulls ``provider`` from the 8-tuple key.
        """
        with self._lock:
            return dict(self._entries_by_partition)

    # -- Internal helpers --------------------------------------------------

    def _remove_locked(self, key: K) -> None:
        """Remove ``key`` from the cache, updating bytes + partition
        counts. MUST be called with ``self._lock`` held."""
        entry = self._entries.pop(key, None)
        if entry is None:
            return
        self._bytes_resident -= entry.size_bytes
        if self._partition_extractor is not None:
            partition = self._partition_extractor(key)
            current = self._entries_by_partition.get(partition, 0)
            if current <= 1:
                self._entries_by_partition.pop(partition, None)
            else:
                self._entries_by_partition[partition] = current - 1


# -----------------------------------------------------------------------------
# Settings loaders (called by the servicer at __init__ time)
# -----------------------------------------------------------------------------


def load_max_entries_from_settings(settings: object) -> int:
    """Return the configured cache-max-entries from ``GatewaySettings``.

    Validation (typo / non-positive fallback) lives in
    :func:`GatewaySettings._validate_pool_history_cache_caps`; this loader
    is the boundary between settings binding and cache construction so
    tests can swap in a synthetic settings object.

    ``settings`` is a :class:`almanak.gateway.core.settings.GatewaySettings`
    instance (typed at the caller; not imported here to keep the module
    chain-agnostic, mirroring the ``get_soft_cap_seconds`` pattern in
    ``_history_common``).
    """
    value = getattr(settings, "pool_history_cache_max_entries", DEFAULT_MAX_ENTRIES)
    return int(value) if value else DEFAULT_MAX_ENTRIES


def load_max_bytes_from_settings(settings: object) -> int:
    """Return the configured cache-max-bytes from ``GatewaySettings``.

    Validation (typo / non-positive fallback) lives in
    :func:`GatewaySettings._validate_pool_history_cache_caps`; see
    :func:`load_max_entries_from_settings` for the rationale.
    """
    value = getattr(settings, "pool_history_cache_max_bytes", DEFAULT_MAX_BYTES)
    return int(value) if value else DEFAULT_MAX_BYTES


# -----------------------------------------------------------------------------
# Key types: pool history specific
# -----------------------------------------------------------------------------
#
# Defined here (rather than inlined in the servicer) so the rates work in
# VIB-4747 can mirror the pattern with its own key shape and reuse the
# ``HistoryCache`` class verbatim.

#: Public-cache key (7 fields). Provider OMITTED — see PoolX.md §D6.
PoolHistoryPublicKey = tuple[str, str, str, int, int, int, str]
#                            chain  pool   protocol  start_ts  end_ts  resolution  finality_band

#: Raw-cache key (8 fields). Provider is the 8th dimension.
PoolHistoryRawKey = tuple[str, str, str, int, int, int, str, str]
#                         chain  pool   protocol  start_ts  end_ts  resolution  finality_band  provider


def make_public_key(
    *,
    chain: str,
    pool_address: str,
    protocol: str,
    start_ts: int,
    end_ts: int,
    resolution: int,
    finality_band: str,
) -> PoolHistoryPublicKey:
    """Build a normalized public-cache key. Chain + protocol are
    lowercased; pool_address is taken as-is (caller is responsible for
    chain-aware normalization — see ``_history_common.normalize_pool_address``)."""
    return (
        chain.lower(),
        pool_address,
        protocol.lower(),
        int(start_ts),
        int(end_ts),
        int(resolution),
        finality_band,
    )


def make_raw_key(
    *,
    chain: str,
    pool_address: str,
    protocol: str,
    start_ts: int,
    end_ts: int,
    resolution: int,
    finality_band: str,
    provider: str,
) -> PoolHistoryRawKey:
    """Build a normalized raw-cache key (public key + ``provider``)."""
    return (
        chain.lower(),
        pool_address,
        protocol.lower(),
        int(start_ts),
        int(end_ts),
        int(resolution),
        finality_band,
        provider,
    )


def extract_provider_from_raw_key(key: PoolHistoryRawKey) -> str:
    """Pull the provider out of an 8-tuple raw-cache key. Used as the
    ``partition_extractor`` for the raw cache so
    ``entries_by_partition`` gives a per-provider count."""
    return key[7]
