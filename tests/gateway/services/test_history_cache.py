"""D2.M4 cache tests for PoolHistoryService (VIB-4752 / POOL-4).

Maps to the umbrella UAT card at ``docs/internal/uat-cards/VIB-4728.md``:
- D2.M4 (cache hit + LRU eviction by entries + LRU eviction by bytes +
  in-flight dedup + raw-cache provider partition)
- D3.F3 (cache-key collision across all six D6 dimensions) — partially
  tested here at the cache primitive level; the full key-collision
  matrix lands in ``test_pool_history_service.py`` once POOL-5 wires
  the dispatcher.

The cache is generic over ``(K, V)``. Most tests exercise it with the
real ``gateway_pb2.PoolHistoryResponse`` value type so any byte-size
estimator change (e.g. ``ByteSize`` vs ``SerializeToString``) is
caught here. A small handful use a dummy ``int`` value to make
size-based eviction trivially predictable.
"""

from __future__ import annotations

import asyncio

import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services._history_cache import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_ENTRIES,
    ENV_MAX_BYTES,
    ENV_MAX_ENTRIES,
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    HistoryCache,
    PoolHistoryPublicKey,
    PoolHistoryRawKey,
    extract_provider_from_raw_key,
    load_max_bytes_from_settings,
    load_max_entries_from_settings,
    make_public_key,
    make_raw_key,
)


# ============================================================================
# Test fixtures
# ============================================================================


def _snapshot_response(*, rows: int = 1, source: str = "the_graph") -> gateway_pb2.PoolHistoryResponse:
    """Build a realistic-looking response for cache size tests."""
    resp = gateway_pb2.PoolHistoryResponse(
        source=source,
        success=True,
        finalized_only=True,
    )
    for i in range(rows):
        resp.snapshots.add(
            timestamp=1_700_000_000 + i * 3600,
            tvl="1000000.0",
            volume_24h="500000.0",
            fee_revenue_24h="1500.0",
            token0_reserve="100.0",
            token1_reserve="200.0",
        )
    return resp


class _FakeClock:
    """Deterministic clock for TTL tests. ``advance(seconds)`` moves it."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# Public key for several tests (all 7 fields).
def _public_key(
    *,
    chain: str = "arbitrum",
    pool: str = "0xc6962004f452be9203591991d15f6b388e09e8d0",
    protocol: str = "uniswap_v3",
    start: int = 1_700_000_000,
    end: int = 1_700_604_800,
    resolution: int = gateway_pb2.Resolution.RESOLUTION_1H,
    band: str = FINALITY_FINALIZED,
) -> PoolHistoryPublicKey:
    return make_public_key(
        chain=chain,
        pool_address=pool,
        protocol=protocol,
        start_ts=start,
        end_ts=end,
        resolution=resolution,
        finality_band=band,
    )


def _raw_key(
    *,
    chain: str = "arbitrum",
    pool: str = "0xc6962004f452be9203591991d15f6b388e09e8d0",
    protocol: str = "uniswap_v3",
    start: int = 1_700_000_000,
    end: int = 1_700_604_800,
    resolution: int = gateway_pb2.Resolution.RESOLUTION_1H,
    band: str = FINALITY_FINALIZED,
    provider: str = "the_graph",
) -> PoolHistoryRawKey:
    return make_raw_key(
        chain=chain,
        pool_address=pool,
        protocol=protocol,
        start_ts=start,
        end_ts=end,
        resolution=resolution,
        finality_band=band,
        provider=provider,
    )


def _public_cache(**kwargs) -> HistoryCache:
    defaults: dict = {
        "max_entries": 10,
        "max_bytes": 64 * 1024 * 1024,
        "size_estimator": lambda v: v.ByteSize() if hasattr(v, "ByteSize") else 1,
        "name": "test_public",
    }
    defaults.update(kwargs)
    return HistoryCache(**defaults)


def _raw_cache(**kwargs) -> HistoryCache:
    defaults: dict = {
        "max_entries": 10,
        "max_bytes": 64 * 1024 * 1024,
        "size_estimator": lambda v: v.ByteSize() if hasattr(v, "ByteSize") else 1,
        "partition_extractor": extract_provider_from_raw_key,
        "name": "test_raw",
    }
    defaults.update(kwargs)
    return HistoryCache(**defaults)


# ============================================================================
# Basic put / get
# ============================================================================


def test_get_returns_none_on_miss():
    cache = _public_cache()
    assert cache.get(_public_key()) is None
    assert cache.stats()["cache_misses"] == 1
    assert cache.stats()["cache_hits"] == 0


def test_put_then_get_returns_value():
    cache = _public_cache()
    response = _snapshot_response(rows=5)
    key = _public_key()
    cache.put(key, response, FINALITY_FINALIZED)
    got = cache.get(key)
    assert got is response  # identity — no copy
    assert cache.stats()["cache_hits"] == 1
    assert cache.stats()["entries_resident"] == 1


def test_put_replaces_existing_entry():
    cache = _public_cache()
    key = _public_key()
    cache.put(key, _snapshot_response(rows=1), FINALITY_FINALIZED)
    cache.put(key, _snapshot_response(rows=5), FINALITY_FINALIZED)
    assert cache.entries_resident == 1
    assert len(cache.get(key).snapshots) == 5


def test_put_rejects_unknown_finality_band():
    cache = _public_cache()
    with pytest.raises(ValueError, match="finality_band"):
        cache.put(_public_key(), _snapshot_response(), "unknown_band")


def test_invalidate_removes_entry():
    cache = _public_cache()
    key = _public_key()
    cache.put(key, _snapshot_response(), FINALITY_FINALIZED)
    assert cache.invalidate(key) is True
    assert cache.get(key) is None
    assert cache.invalidate(key) is False  # idempotent


# ============================================================================
# TTL: provisional vs finalized
# ============================================================================


def test_provisional_ttl_expires():
    clock = _FakeClock()
    cache = _public_cache(
        provisional_ttl_seconds=60.0,
        finalized_ttl_seconds=86400.0,
        clock=clock,
    )
    cache.put(_public_key(), _snapshot_response(), FINALITY_PROVISIONAL)
    assert cache.get(_public_key()) is not None
    clock.advance(61.0)
    assert cache.get(_public_key()) is None
    # TTL expiry counts as a miss, not as an entries-cap eviction.
    stats = cache.stats()
    assert stats["cache_evictions_by_entries"] == 0
    assert stats["cache_evictions_by_bytes"] == 0
    assert stats["entries_resident"] == 0


def test_finalized_ttl_is_long():
    """A finalized entry survives well past the provisional 60s window."""
    clock = _FakeClock()
    cache = _public_cache(
        provisional_ttl_seconds=60.0,
        finalized_ttl_seconds=86400.0,
        clock=clock,
    )
    cache.put(_public_key(), _snapshot_response(), FINALITY_FINALIZED)
    clock.advance(3600.0)  # 1 hour
    assert cache.get(_public_key()) is not None


def test_finalized_ttl_eventually_expires():
    clock = _FakeClock()
    cache = _public_cache(finalized_ttl_seconds=10.0, clock=clock)
    cache.put(_public_key(), _snapshot_response(), FINALITY_FINALIZED)
    clock.advance(11.0)
    assert cache.get(_public_key()) is None


# ============================================================================
# LRU eviction by entries (UAT card D2.M4 + Codex Round-2 fix #3)
# ============================================================================


def test_lru_eviction_by_entries():
    """Insert 4 keys with ``max_entries=3``. The oldest (least recently
    used) MUST be evicted on the 4th put."""
    cache = _public_cache(max_entries=3)
    keys = [_public_key(start=1_700_000_000 + i * 86400) for i in range(4)]
    for k in keys:
        cache.put(k, _snapshot_response(), FINALITY_FINALIZED)
    # K1 (the first inserted, oldest) must be evicted.
    assert cache.get(keys[0]) is None
    # K2, K3, K4 retained.
    for k in keys[1:]:
        assert cache.get(k) is not None
    assert cache.stats()["cache_evictions_by_entries"] == 1
    assert cache.stats()["cache_evictions_by_bytes"] == 0


def test_lru_eviction_uses_access_order():
    """A get() on an old key bumps its LRU position; subsequent put()
    evicts a DIFFERENT key."""
    cache = _public_cache(max_entries=3)
    keys = [_public_key(start=1_700_000_000 + i * 86400) for i in range(4)]
    # Put K0, K1, K2.
    for k in keys[:3]:
        cache.put(k, _snapshot_response(), FINALITY_FINALIZED)
    # Touch K0 — it becomes most-recently-used.
    assert cache.get(keys[0]) is not None
    # Put K3 — evicts K1 (now the oldest), NOT K0.
    cache.put(keys[3], _snapshot_response(), FINALITY_FINALIZED)
    assert cache.get(keys[0]) is not None, "K0 should survive — it was touched"
    assert cache.get(keys[1]) is None, "K1 should have been evicted"


# ============================================================================
# LRU eviction by bytes (UAT card D2.M4 + Codex Round-2 fix #3)
# ============================================================================


def test_lru_eviction_by_bytes_uses_dedicated_counter():
    """When ONLY the bytes bound trips, eviction is classed as ``by_bytes``
    not ``by_entries``. A buggy implementation that bumps the wrong
    counter would silently break monitoring."""
    # Use a fake int value type with a tiny estimator so we can control
    # byte sizes precisely.
    cache: HistoryCache[int, int] = HistoryCache(
        max_entries=1000,  # very generous; bytes will trip first
        max_bytes=200,
        size_estimator=lambda v: int(v),  # value IS its byte cost
        name="test_bytes",
    )
    cache.put(1, 150, FINALITY_FINALIZED)  # bytes = 150
    cache.put(2, 100, FINALITY_FINALIZED)  # +100 -> total 250 > 200; evict K1
    # K1 should have been evicted by BYTES (not entries).
    assert cache.get(1) is None
    assert cache.get(2) == 100
    stats = cache.stats()
    assert stats["cache_evictions_by_bytes"] == 1
    assert stats["cache_evictions_by_entries"] == 0
    assert stats["bytes_resident"] == 100


def test_bytes_resident_decrements_on_eviction():
    cache: HistoryCache[int, int] = HistoryCache(
        max_entries=3,
        max_bytes=10_000,
        size_estimator=lambda v: int(v),
        name="test_bytes_decr",
    )
    cache.put(1, 100, FINALITY_FINALIZED)
    cache.put(2, 200, FINALITY_FINALIZED)
    cache.put(3, 300, FINALITY_FINALIZED)
    assert cache.bytes_resident == 600
    cache.put(4, 400, FINALITY_FINALIZED)  # entries cap evicts K1 (100)
    assert cache.bytes_resident == 200 + 300 + 400


def test_size_estimator_is_called_on_each_put():
    """Anti-bypass: the estimator is invoked on every put so a future
    cache-key with a hugely-different size is accounted correctly."""
    calls: list[int] = []

    def estimator(v: int) -> int:
        calls.append(v)
        return v

    cache: HistoryCache[int, int] = HistoryCache(
        max_entries=10,
        max_bytes=10_000,
        size_estimator=estimator,
        name="test_size_called",
    )
    cache.put(1, 100, FINALITY_FINALIZED)
    cache.put(2, 200, FINALITY_FINALIZED)
    cache.put(1, 50, FINALITY_FINALIZED)  # replace -> estimator called again
    assert calls == [100, 200, 50]


def test_negative_size_estimator_raises():
    cache: HistoryCache[int, int] = HistoryCache(
        max_entries=10,
        max_bytes=10_000,
        size_estimator=lambda v: -1,
        name="test_neg",
    )
    with pytest.raises(ValueError, match="negative bytes"):
        cache.put(1, 100, FINALITY_FINALIZED)


# ============================================================================
# In-flight dedup (inherited audit row #8)
# ============================================================================


@pytest.mark.asyncio
async def test_inflight_dedup_shared_fetch():
    """Two concurrent ``get_or_fetch`` callers for the same key result
    in ONE fetcher invocation; both receive the same value."""
    fetch_count = 0

    async def fetcher() -> tuple[gateway_pb2.PoolHistoryResponse, str]:
        nonlocal fetch_count
        fetch_count += 1
        await asyncio.sleep(0.01)  # let the peer task arrive
        return _snapshot_response(rows=3), FINALITY_FINALIZED

    cache = _public_cache()
    key = _public_key()
    results = await asyncio.gather(
        cache.get_or_fetch(key, fetcher),
        cache.get_or_fetch(key, fetcher),
        cache.get_or_fetch(key, fetcher),
    )
    assert fetch_count == 1, "only the lead caller should have called the fetcher"
    # All three saw the same value.
    assert all(len(r.snapshots) == 3 for r in results)
    # Dedup counter: 2 peers used the lead's future.
    assert cache.stats()["inflight_dedup_hits"] == 2
    # Cache populated.
    assert cache.entries_resident == 1


@pytest.mark.asyncio
async def test_inflight_dedup_cache_hit_avoids_fetcher():
    """If the value is already cached, ``get_or_fetch`` returns it
    without invoking the fetcher."""
    fetch_count = 0

    async def fetcher() -> tuple[gateway_pb2.PoolHistoryResponse, str]:
        nonlocal fetch_count
        fetch_count += 1
        return _snapshot_response(), FINALITY_FINALIZED

    cache = _public_cache()
    key = _public_key()
    cache.put(key, _snapshot_response(rows=2), FINALITY_FINALIZED)
    got = await cache.get_or_fetch(key, fetcher)
    assert fetch_count == 0
    assert len(got.snapshots) == 2


@pytest.mark.asyncio
async def test_inflight_dedup_propagates_exception():
    """If the lead's fetcher raises, ALL awaiters see the exception
    and the inflight slot is cleared so a retry can succeed."""
    attempt = {"n": 0}

    async def failing_fetcher() -> tuple[gateway_pb2.PoolHistoryResponse, str]:
        attempt["n"] += 1
        await asyncio.sleep(0.01)
        raise RuntimeError("upstream provider failed")

    cache = _public_cache()
    key = _public_key()
    with pytest.raises(RuntimeError, match="upstream provider failed"):
        await asyncio.gather(
            cache.get_or_fetch(key, failing_fetcher),
            cache.get_or_fetch(key, failing_fetcher),
        )
    # Inflight slot cleared — a retry can fire.
    async def working_fetcher() -> tuple[gateway_pb2.PoolHistoryResponse, str]:
        return _snapshot_response(rows=1), FINALITY_FINALIZED

    got = await cache.get_or_fetch(key, working_fetcher)
    assert got is not None
    assert cache.entries_resident == 1


@pytest.mark.asyncio
async def test_inflight_dedup_put_failure_does_not_hang_awaiters():
    """Regression: if ``put()`` raises AFTER a successful fetcher call
    (e.g. a buggy ``size_estimator``), the inflight slot must still be
    cleared and the awaiters must still be settled with the exception —
    otherwise peer tasks hang forever. Implemented via ``try/finally``
    in ``get_or_fetch``."""

    def bad_estimator(_msg) -> int:
        raise RuntimeError("estimator boom")

    cache = HistoryCache[PoolHistoryPublicKey, gateway_pb2.PoolHistoryResponse](
        max_entries=10,
        max_bytes=10_000,
        size_estimator=bad_estimator,
        name="put-fail",
    )

    async def fetcher() -> tuple[gateway_pb2.PoolHistoryResponse, str]:
        return _snapshot_response(rows=1), FINALITY_FINALIZED

    key = _public_key()
    # Lead and peer both raise; lead from put(), peer from awaiting the
    # future that the lead's finally-block settles. Without try/finally
    # this hangs forever.
    with pytest.raises(RuntimeError, match="estimator boom"):
        await asyncio.wait_for(
            asyncio.gather(
                cache.get_or_fetch(key, fetcher),
                cache.get_or_fetch(key, fetcher),
            ),
            timeout=2.0,
        )

    # Inflight slot cleared so a follow-up retry can fire (no leaked future).
    assert key not in cache._inflight


@pytest.mark.asyncio
async def test_get_or_fetch_locked_recheck_returns_peer_value(monkeypatch):
    """Regression: when a peer populates the cache between the fast-path
    ``self.get()`` check and the inflight-lock acquisition, the locked
    re-check must return the peer's value WITHOUT triggering a redundant
    fetch."""

    cache = _public_cache()
    key = _public_key()
    peer_value = _snapshot_response(rows=2)

    # Force the fast-path ``self.get()`` to MISS (simulating the race
    # window: the fast-path read happened BEFORE the peer's put), then
    # let the locked re-check see the populated entry.
    real_get = cache.get
    call_count = {"n": 0}

    def get_then_populate(k):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # First call from get_or_fetch fast-path — return None so we
            # fall through to the locked re-check.
            # Populate the cache so the locked re-check sees the entry.
            cache.put(k, peer_value, FINALITY_FINALIZED)
            return None
        return real_get(k)

    monkeypatch.setattr(cache, "get", get_then_populate)

    async def fetcher_should_not_run() -> tuple[gateway_pb2.PoolHistoryResponse, str]:
        raise AssertionError("fetcher must not be invoked after locked re-check hit")

    got = await cache.get_or_fetch(key, fetcher_should_not_run)
    assert got is peer_value
    # Locked re-check counts as a real hit (mirrors self.get() semantics).
    assert cache.stats()["cache_hits"] >= 1


@pytest.mark.asyncio
async def test_inflight_dedup_distinct_keys_run_concurrently():
    """Different keys do NOT share the inflight slot — concurrent
    distinct-key fetches each invoke the fetcher."""
    fetch_count = 0

    async def fetcher() -> tuple[gateway_pb2.PoolHistoryResponse, str]:
        nonlocal fetch_count
        fetch_count += 1
        await asyncio.sleep(0.01)
        return _snapshot_response(), FINALITY_FINALIZED

    cache = _public_cache()
    k1 = _public_key(start=1_700_000_000)
    k2 = _public_key(start=1_700_100_000)
    await asyncio.gather(
        cache.get_or_fetch(k1, fetcher),
        cache.get_or_fetch(k2, fetcher),
    )
    assert fetch_count == 2
    assert cache.stats()["inflight_dedup_hits"] == 0


# ============================================================================
# Raw cache provider partition (UAT card D2.M4 + Codex Round-3 fix #3)
# ============================================================================


def test_raw_cache_provider_partition():
    """The raw cache key includes provider; ``entries_by_partition``
    counts entries per provider for ``health()``."""
    cache = _raw_cache()
    cache.put(_raw_key(provider="the_graph"), _snapshot_response(source="the_graph"), FINALITY_FINALIZED)
    cache.put(_raw_key(provider="defillama"), _snapshot_response(source="defillama"), FINALITY_FINALIZED)
    cache.put(_raw_key(provider="defillama", start=1_700_500_000),
              _snapshot_response(source="defillama"), FINALITY_FINALIZED)
    by_provider = cache.entries_by_partition
    assert by_provider == {"the_graph": 1, "defillama": 2}


def test_raw_cache_partition_count_decrements_on_invalidate():
    cache = _raw_cache()
    k = _raw_key(provider="the_graph")
    cache.put(k, _snapshot_response(), FINALITY_FINALIZED)
    assert cache.entries_by_partition == {"the_graph": 1}
    cache.invalidate(k)
    assert cache.entries_by_partition == {}


def test_raw_cache_partition_count_decrements_on_eviction():
    """Eviction by entries cap drops the partition count."""
    cache = _raw_cache(max_entries=2)
    cache.put(_raw_key(provider="the_graph"), _snapshot_response(), FINALITY_FINALIZED)
    cache.put(_raw_key(provider="defillama"), _snapshot_response(), FINALITY_FINALIZED)
    cache.put(_raw_key(provider="geckoterminal", start=1_700_500_000),
              _snapshot_response(), FINALITY_FINALIZED)
    # The_graph (oldest) should have been evicted.
    assert "the_graph" not in cache.entries_by_partition
    assert cache.entries_by_partition == {"defillama": 1, "geckoterminal": 1}


def test_public_cache_has_no_partition_tracking():
    """Without a ``partition_extractor`` the cache exposes an empty
    ``entries_by_partition`` (no per-key inspection)."""
    cache = _public_cache()
    cache.put(_public_key(), _snapshot_response(), FINALITY_FINALIZED)
    assert cache.entries_by_partition == {}


# ============================================================================
# Key constructors normalize for cache-key stability
# ============================================================================


def test_make_public_key_normalizes_chain_and_protocol():
    k1 = make_public_key(
        chain="Arbitrum",
        pool_address="0xC6962004F452BE9203591991D15F6B388E09E8D0",
        protocol="Uniswap_V3",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
    )
    k2 = make_public_key(
        chain="arbitrum",
        pool_address="0xC6962004F452BE9203591991D15F6B388E09E8D0",  # callers normalize first
        protocol="uniswap_v3",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
    )
    assert k1 == k2


def test_make_public_and_raw_keys_share_first_seven_fields():
    """The raw key is the public key + provider — the first 7 fields
    MUST match so cache lookups can crosswalk between tiers."""
    pub = make_public_key(
        chain="arbitrum",
        pool_address="0xabc",
        protocol="uniswap_v3",
        start_ts=10,
        end_ts=20,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
    )
    raw = make_raw_key(
        chain="arbitrum",
        pool_address="0xabc",
        protocol="uniswap_v3",
        start_ts=10,
        end_ts=20,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
        provider="the_graph",
    )
    assert raw[:7] == pub
    assert extract_provider_from_raw_key(raw) == "the_graph"


def test_make_public_key_distinguishes_each_dimension():
    """A regression that hashes a different dimension into the same
    cache slot would silently collide — test that each dimension
    produces a distinct key. (Mirrors D3.F3's 6-dimension check at
    the cache-primitive level.)"""
    base = dict(
        chain="arbitrum",
        pool_address="0xabc",
        protocol="uniswap_v3",
        start_ts=10,
        end_ts=20,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
        finality_band=FINALITY_FINALIZED,
    )
    base_key = make_public_key(**base)
    seen = {base_key}
    for field, override in [
        ("chain", "ethereum"),
        ("pool_address", "0xdef"),
        ("protocol", "aerodrome"),
        ("start_ts", 11),
        ("end_ts", 21),
        ("resolution", gateway_pb2.Resolution.RESOLUTION_4H),
        ("finality_band", FINALITY_PROVISIONAL),
    ]:
        variant = dict(base)
        variant[field] = override
        k = make_public_key(**variant)
        assert k not in seen, f"varying {field} did not change the key"
        seen.add(k)
    assert len(seen) == 8  # baseline + 7 variants


# ============================================================================
# Settings loaders + defaults
# ============================================================================
#
# Validation (typo / non-positive fallback) lives on ``GatewaySettings`` —
# we drive these tests by setting env vars that ``GatewaySettings`` reads at
# construction so the loader and the validator are exercised end-to-end.


def test_load_max_entries_uses_default_when_unset(monkeypatch):
    monkeypatch.delenv(ENV_MAX_ENTRIES, raising=False)
    settings = GatewaySettings()
    assert load_max_entries_from_settings(settings) == DEFAULT_MAX_ENTRIES


def test_load_max_bytes_uses_default_when_unset(monkeypatch):
    monkeypatch.delenv(ENV_MAX_BYTES, raising=False)
    settings = GatewaySettings()
    assert load_max_bytes_from_settings(settings) == DEFAULT_MAX_BYTES


def test_load_max_entries_parses_env_override(monkeypatch):
    monkeypatch.setenv(ENV_MAX_ENTRIES, "42")
    settings = GatewaySettings()
    assert load_max_entries_from_settings(settings) == 42


@pytest.mark.parametrize("bad", ["", "   ", "not-a-number", "0", "-1"])
def test_load_max_entries_falls_back_on_invalid(monkeypatch, bad: str):
    monkeypatch.setenv(ENV_MAX_ENTRIES, bad)
    settings = GatewaySettings()
    assert load_max_entries_from_settings(settings) == DEFAULT_MAX_ENTRIES


@pytest.mark.parametrize("bad", ["", "   ", "not-a-number", "0", "-100"])
def test_load_max_bytes_falls_back_on_invalid(monkeypatch, bad: str):
    monkeypatch.setenv(ENV_MAX_BYTES, bad)
    settings = GatewaySettings()
    assert load_max_bytes_from_settings(settings) == DEFAULT_MAX_BYTES


# ============================================================================
# Construction guards
# ============================================================================


@pytest.mark.parametrize("bad_max_entries", [0, -1, -100])
def test_construct_rejects_nonpositive_max_entries(bad_max_entries: int):
    with pytest.raises(ValueError, match="max_entries"):
        HistoryCache(
            max_entries=bad_max_entries, max_bytes=1024, size_estimator=lambda v: 1, name="bad"
        )


@pytest.mark.parametrize("bad_max_bytes", [0, -1, -100])
def test_construct_rejects_nonpositive_max_bytes(bad_max_bytes: int):
    with pytest.raises(ValueError, match="max_bytes"):
        HistoryCache(
            max_entries=10, max_bytes=bad_max_bytes, size_estimator=lambda v: 1, name="bad"
        )


# ============================================================================
# stats() shape lock
# ============================================================================


def test_stats_shape_is_locked():
    """The cache exposes a fixed set of stat keys. Adding a new key
    requires a downstream ``health()`` update in
    ``pool_history_service.py``, so the cache's stats shape is a
    contract — pin it here."""
    cache = _public_cache()
    stats = cache.stats()
    assert set(stats.keys()) == {
        "entries_resident",
        "bytes_resident",
        "cache_hits",
        "cache_misses",
        "cache_evictions_by_entries",
        "cache_evictions_by_bytes",
        "inflight_dedup_hits",
        "inflight_pending",
    }
    for v in stats.values():
        assert isinstance(v, int)
