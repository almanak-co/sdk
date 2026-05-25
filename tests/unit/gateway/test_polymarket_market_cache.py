"""Bounded-TTL cache for GammaMarket lookups (issue #1957).

Without this cache, every BUY / SELL through ``CreateAndPostOrder`` pays one
Gamma round-trip on the critical path because the V2 ``build_limit_order``
path needs the market's ``tick_size`` / ``min_size`` / ``neg_risk``. The cache
makes burst-of-orders for the same token re-use a single fetch, while keeping
metadata fresh enough that a Polymarket-admin tick-size change propagates
within ~1 minute.

Coverage:

  1. Cache miss -> upstream fetch -> result populated.
  2. Cache hit  -> NO upstream fetch.
  3. TTL expiry -> upstream re-fetch.
  4. TTL=0 (env opt-out) -> cache disabled, every call fetches.
  5. LRU bound -> oldest entries evicted when capacity exceeded.
  6. Defensive invalidation on tick-size / min-size errors.
  7. Single-flight: concurrent fetches for the same token coalesce on one
     upstream round-trip (the canonical issue #1957 burst-of-orders case).
  8. Non-shape errors do NOT invalidate the cache (PolymarketAPIError with a
     generic message must not evict, otherwise transient upstream noise
     defeats the whole point of caching).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.polymarket import (
    GammaMarket,
    PolymarketAPIError,
    PolymarketMinimumOrderError,
)
from almanak.framework.connectors.polymarket.exceptions import (
    PolymarketInvalidTickSizeError,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.connectors.polymarket.gateway.service import (
    POLYMARKET_MARKET_CACHE_MAX_ENTRIES,
    POLYMARKET_MARKET_CACHE_TTL_DEFAULT_SECONDS,
    POLYMARKET_MARKET_CACHE_TTL_MAX_SECONDS,
    PolymarketServiceServicer,
    _read_market_cache_ttl_seconds,
)


# =============================================================================
# Fixtures
# =============================================================================


def _make_market(token_id: str, tick: str = "0.01", min_size: str = "5") -> GammaMarket:
    """Build a minimal valid GammaMarket carrying ``token_id`` for tests."""
    return GammaMarket(
        id=f"market-{token_id}",
        condition_id="0x" + "ab" * 32,
        question=f"Question for {token_id}?",
        slug=f"slug-{token_id}",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.5"), Decimal("0.5")],
        clob_token_ids=[token_id, "other"],
        volume=Decimal("0"),
        liquidity=Decimal("0"),
        active=True,
        closed=False,
        enable_order_book=True,
        order_price_min_tick_size=Decimal(tick),
        order_min_size=Decimal(min_size),
    )


@pytest.fixture
def settings() -> MagicMock:
    s = MagicMock(spec=GatewaySettings)
    s.private_key = "0x" + "ab" * 32
    s.polymarket_private_key = None
    s.eoa_address = "0x" + "00" * 19 + "01"
    s.polymarket_wallet_address = None
    s.safe_address = None
    s.safe_mode = None
    s.polymarket_api_key = "k"
    s.polymarket_secret = "c2VjcmV0"
    s.polymarket_passphrase = "p"
    s.polymarket_market_cache_ttl_seconds = POLYMARKET_MARKET_CACHE_TTL_DEFAULT_SECONDS
    return s


@pytest.fixture
def servicer(settings: MagicMock) -> PolymarketServiceServicer:
    """Servicer with cache TTL forced to the default 60s via typed settings."""
    return PolymarketServiceServicer(settings=settings)


# =============================================================================
# 1. TTL parsing
# =============================================================================


class TestReadTtl:
    def test_default_when_unset(self) -> None:
        assert _read_market_cache_ttl_seconds() == POLYMARKET_MARKET_CACHE_TTL_DEFAULT_SECONDS

    def test_explicit_zero_disables(self) -> None:
        assert _read_market_cache_ttl_seconds(SimpleNamespace(polymarket_market_cache_ttl_seconds=0)) == 0.0

    def test_negative_clamped_to_zero(self) -> None:
        assert _read_market_cache_ttl_seconds(SimpleNamespace(polymarket_market_cache_ttl_seconds=-5)) == 0.0

    def test_invalid_falls_back_to_default(self) -> None:
        assert (
            _read_market_cache_ttl_seconds(SimpleNamespace(polymarket_market_cache_ttl_seconds="not-a-number"))
            == POLYMARKET_MARKET_CACHE_TTL_DEFAULT_SECONDS
        )

    def test_custom_value(self) -> None:
        assert _read_market_cache_ttl_seconds(SimpleNamespace(polymarket_market_cache_ttl_seconds=120)) == 120.0

    @pytest.mark.parametrize("raw", ["inf", "Infinity", "-inf", "nan", "NaN", "1e309"])
    def test_non_finite_falls_back_to_default(self, raw: str) -> None:
        """``float("inf")`` parses without ValueError and would silently
        produce a *permanent* cache (every expiry check returns False).
        ``float("nan")`` is even worse — comparisons all return False so
        the cache reads as enabled AND never expires. Both must reject."""
        assert (
            _read_market_cache_ttl_seconds(SimpleNamespace(polymarket_market_cache_ttl_seconds=raw))
            == POLYMARKET_MARKET_CACHE_TTL_DEFAULT_SECONDS
        )

    def test_clamped_to_max(self) -> None:
        """A typo like '86400000' (intended seconds, actually ms) must not
        produce a 1000-day stale window."""
        assert (
            _read_market_cache_ttl_seconds(SimpleNamespace(polymarket_market_cache_ttl_seconds=86_400_000))
            == POLYMARKET_MARKET_CACHE_TTL_MAX_SECONDS
        )


# =============================================================================
# 2. Cache miss -> fetch -> populated
# =============================================================================


class TestMissThenHit:
    @pytest.mark.asyncio
    async def test_miss_populates_then_hit_skips_upstream(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        token = "tok-A"
        market = _make_market(token)
        client = MagicMock()
        # asyncio.to_thread will call client.get_markets — count invocations.
        client.get_markets = MagicMock(return_value=[market])

        first = await servicer._fetch_market_for_token(client, token)
        assert first is market
        assert client.get_markets.call_count == 1

        # Second call must NOT touch upstream.
        second = await servicer._fetch_market_for_token(client, token)
        assert second is market
        assert client.get_markets.call_count == 1


# =============================================================================
# 3. TTL expiry forces re-fetch
# =============================================================================


class TestTtlExpiry:
    @pytest.mark.asyncio
    async def test_expired_entry_re_fetched(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        token = "tok-B"
        m1 = _make_market(token, tick="0.01")
        m2 = _make_market(token, tick="0.001")  # admin tightened tick mid-TTL
        client = MagicMock()
        client.get_markets = MagicMock(side_effect=[[m1], [m2]])

        # Force a tiny TTL by patching the per-instance attr directly. The env
        # parse path is exercised separately in TestReadTtl.
        servicer._market_cache_ttl_seconds = 0.05

        first = await servicer._fetch_market_for_token(client, token)
        assert first is m1
        # Sleep past the TTL window.
        await asyncio.sleep(0.1)
        second = await servicer._fetch_market_for_token(client, token)
        assert second is m2
        assert client.get_markets.call_count == 2


# =============================================================================
# 4. TTL=0 disables caching entirely
# =============================================================================


class TestDisabledCache:
    @pytest.mark.asyncio
    async def test_ttl_zero_means_every_call_fetches(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        token = "tok-C"
        market = _make_market(token)
        client = MagicMock()
        client.get_markets = MagicMock(return_value=[market])

        servicer._market_cache_ttl_seconds = 0.0

        await servicer._fetch_market_for_token(client, token)
        await servicer._fetch_market_for_token(client, token)
        await servicer._fetch_market_for_token(client, token)
        assert client.get_markets.call_count == 3
        # Cache stays empty.
        assert len(servicer._market_cache) == 0


# =============================================================================
# 5. LRU bound
# =============================================================================


class TestLruBound:
    @pytest.mark.asyncio
    async def test_oldest_evicted_when_over_capacity(
        self, servicer: PolymarketServiceServicer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Shrink the cap so we don't have to insert 512 entries.
        monkeypatch.setattr(
            "almanak.connectors.polymarket.gateway.service.POLYMARKET_MARKET_CACHE_MAX_ENTRIES",
            3,
        )

        client = MagicMock()
        client.get_markets = MagicMock(side_effect=lambda f: [_make_market(f.clob_token_ids[0])])

        # Insert 4 distinct tokens with cap=3 -> oldest must be evicted.
        for tok in ("a", "b", "c", "d"):
            await servicer._fetch_market_for_token(client, tok)

        assert "a" not in servicer._market_cache
        assert {"b", "c", "d"} <= set(servicer._market_cache.keys())

    @pytest.mark.asyncio
    async def test_lru_touch_on_hit(self, servicer: PolymarketServiceServicer, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "almanak.connectors.polymarket.gateway.service.POLYMARKET_MARKET_CACHE_MAX_ENTRIES",
            3,
        )
        client = MagicMock()
        client.get_markets = MagicMock(side_effect=lambda f: [_make_market(f.clob_token_ids[0])])

        for tok in ("a", "b", "c"):
            await servicer._fetch_market_for_token(client, tok)
        # Touch "a" so it becomes most-recently-used.
        await servicer._fetch_market_for_token(client, "a")
        # Insert "d"; the LRU victim should now be "b", NOT "a".
        await servicer._fetch_market_for_token(client, "d")
        assert "b" not in servicer._market_cache
        assert "a" in servicer._market_cache


# =============================================================================
# 6. Defensive invalidation
# =============================================================================


class TestInvalidationOnShapeError:
    @pytest.mark.parametrize(
        "exc",
        [
            PolymarketInvalidTickSizeError(price="0.005", tick_size="0.01"),
            PolymarketMinimumOrderError(size="3", minimum="5"),
            PolymarketAPIError("order breaks minimum tick size rule: 0.001"),
            PolymarketAPIError("invalid amount for a marketable BUY order ($0.30), min size: $1"),
        ],
    )
    def test_shape_errors_classified(self, exc: BaseException) -> None:
        assert PolymarketServiceServicer._is_market_shape_error(exc) is True

    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("price is not a number"),
            PolymarketAPIError("rate limited"),
            PolymarketAPIError("temporary upstream error"),
            RuntimeError("unrelated"),
            # Tightened regex no longer matches loose substrings — these
            # would have evicted under the original pattern but must not.
            PolymarketAPIError("permission denied for order size class"),
            PolymarketAPIError("approval failed: min order budget exceeded"),
            PolymarketAPIError("ticksize sensor offline"),
            PolymarketAPIError("min size of approval pool reached"),
        ],
    )
    def test_non_shape_errors_not_classified(self, exc: BaseException) -> None:
        assert PolymarketServiceServicer._is_market_shape_error(exc) is False

    def test_invalidate_drops_entry(self, servicer: PolymarketServiceServicer) -> None:
        servicer._cache_put_market("tok-X", _make_market("tok-X"))
        assert "tok-X" in servicer._market_cache
        servicer._invalidate_market_cache("tok-X")
        assert "tok-X" not in servicer._market_cache

    def test_invalidate_unknown_is_noop(self, servicer: PolymarketServiceServicer) -> None:
        # Must not raise — eviction has to be safe even when the cache is empty.
        servicer._invalidate_market_cache("never-cached")


# =============================================================================
# 7. Single-flight coalescing
# =============================================================================


class TestSingleFlight:
    @pytest.mark.asyncio
    async def test_concurrent_first_fetches_coalesce(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """The canonical issue #1957 case: many concurrent BUY/SELLs for the
        same token must produce exactly one Gamma round-trip, not N."""
        import threading

        token = "tok-burst"
        market = _make_market(token)

        call_count = 0
        gate = threading.Event()

        def slow_get_markets(_filters: object) -> list[GammaMarket]:
            nonlocal call_count
            call_count += 1
            # Block the worker thread until the test releases the gate.
            # This keeps the first fetch in-flight long enough that every
            # other coroutine has time to queue up on the per-token lock.
            assert gate.wait(timeout=2.0), "gate never released"
            return [market]

        client = MagicMock()
        client.get_markets = slow_get_markets

        # Fire 8 concurrent fetches.
        tasks = [
            asyncio.create_task(servicer._fetch_market_for_token(client, token))
            for _ in range(8)
        ]
        # Yield several times so all 8 tasks reach _acquire_market_lock and
        # queue behind the in-flight one. asyncio.sleep(0) yields once;
        # we need a tiny wall-clock wait to let asyncio.to_thread dispatch.
        await asyncio.sleep(0.05)
        # Release the upstream call.
        gate.set()
        results = await asyncio.gather(*tasks)

        assert all(r is market for r in results)
        # Exactly one upstream call — the rest hit the cache via the
        # double-check after lock acquisition.
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_lock_eviction_skips_held_locks(
        self,
        servicer: PolymarketServiceServicer,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression: when the per-token locks dict reaches the cap and
        every entry is held by an in-flight fetch, we must NOT pop a held
        lock (which would let two concurrent fetches for the same token
        each create their own brand-new lock and both hit upstream).

        Set the cap to 2, hold both locks, then trigger a 3rd distinct
        token's acquisition. The dict is allowed to exceed the cap rather
        than corrupt single-flight; the originals must remain identifiable.
        """
        monkeypatch.setattr(
            "almanak.connectors.polymarket.gateway.service.POLYMARKET_MARKET_CACHE_MAX_ENTRIES",
            2,
        )
        # Manually populate two held locks (simulating in-flight fetches).
        held_a = asyncio.Lock()
        await held_a.acquire()
        held_b = asyncio.Lock()
        await held_b.acquire()
        servicer._market_locks["a"] = held_a
        servicer._market_locks["b"] = held_b

        try:
            # Now request a 3rd distinct token's lock. Eviction must skip
            # both held locks; the dict is allowed to grow to 3 rather
            # than break single-flight.
            new_lock = await servicer._acquire_market_lock("c")
            assert new_lock is not held_a, "must not return a held lock"
            assert new_lock is not held_b, "must not return a held lock"
            assert servicer._market_locks["a"] is held_a, "held lock 'a' must still be tracked"
            assert servicer._market_locks["b"] is held_b, "held lock 'b' must still be tracked"
            assert servicer._market_locks["c"] is new_lock
        finally:
            held_a.release()
            held_b.release()

    @pytest.mark.asyncio
    async def test_lock_eviction_drops_unheld_when_cap_reached(
        self,
        servicer: PolymarketServiceServicer,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When eviction has a choice (some locks held, some not), it
        must drop an unheld one rather than uselessly grow the dict."""
        monkeypatch.setattr(
            "almanak.connectors.polymarket.gateway.service.POLYMARKET_MARKET_CACHE_MAX_ENTRIES",
            2,
        )
        held = asyncio.Lock()
        await held.acquire()
        unheld = asyncio.Lock()
        servicer._market_locks["held"] = held
        servicer._market_locks["unheld"] = unheld

        try:
            await servicer._acquire_market_lock("new")
            assert "held" in servicer._market_locks, "held lock must survive eviction"
            assert "unheld" not in servicer._market_locks, "unheld lock should have been evicted"
            assert "new" in servicer._market_locks
            assert len(servicer._market_locks) == 2
        finally:
            held.release()

    @pytest.mark.asyncio
    async def test_concurrent_distinct_tokens_do_not_block_each_other(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Per-token locks must not cross-block: two fetches for *different*
        tokens should run in parallel, not serialise on a global lock."""
        import threading

        markets = {
            "tok-X": _make_market("tok-X"),
            "tok-Y": _make_market("tok-Y"),
        }
        in_flight = threading.Semaphore(0)
        proceed = threading.Event()

        def slow_get_markets(filters: object) -> list[GammaMarket]:
            tok = filters.clob_token_ids[0]  # type: ignore[attr-defined]
            in_flight.release()
            assert proceed.wait(timeout=2.0), "proceed never set"
            return [markets[tok]]

        client = MagicMock()
        client.get_markets = slow_get_markets

        t1 = asyncio.create_task(servicer._fetch_market_for_token(client, "tok-X"))
        t2 = asyncio.create_task(servicer._fetch_market_for_token(client, "tok-Y"))

        # Both upstream calls must reach the worker thread BEFORE either
        # finishes — i.e. they're truly in-flight in parallel.
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: (in_flight.acquire(timeout=2.0) and in_flight.acquire(timeout=2.0))
        )
        proceed.set()
        r1, r2 = await asyncio.gather(t1, t2)
        assert r1 is markets["tok-X"]
        assert r2 is markets["tok-Y"]


# =============================================================================
# 8. Cache survives transient upstream noise
# =============================================================================


class TestCacheSurvivesNonShapeErrors:
    """A non-shape PolymarketAPIError (rate limit, transient 500, etc.) must
    NOT evict the cache entry — the whole point of the cache is to absorb
    upstream noise without re-paying the round-trip on the next order."""

    @pytest.mark.asyncio
    async def test_generic_api_error_does_not_invalidate(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        token = "tok-noise"
        servicer._cache_put_market(token, _make_market(token))
        assert token in servicer._market_cache

        # Simulate the CreateAndPostOrder exception path: only invalidate
        # when _is_market_shape_error says so.
        exc = PolymarketAPIError("temporary upstream error")
        if servicer._is_market_shape_error(exc):
            servicer._invalidate_market_cache(token)
        assert token in servicer._market_cache, "rate-limit-style errors must not evict"


# =============================================================================
# 9. Eviction is scoped to the upstream order call, not the whole try block
# =============================================================================


class TestEvictionScope:
    """The defensive eviction must fire only when ``client.create_and_post_order``
    rejects with a shape error. A coincidentally-matching error from
    ``_ensure_wallet_ready`` or anywhere else in the RPC body must NOT
    evict — the cached market wasn't the cause and a perf regression on
    the next order is unjustified."""

    def test_invalidation_helper_can_be_called_from_anywhere(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Sanity: _invalidate_market_cache is the chokepoint and is safe
        to call without holding any lock — covered by the broader scoping
        test which exercises the wired call site."""
        servicer._cache_put_market("scoped", _make_market("scoped"))
        servicer._invalidate_market_cache("scoped")
        assert "scoped" not in servicer._market_cache


# =============================================================================
# 10. Module-level cap is not zero
# =============================================================================


def test_max_entries_is_sane() -> None:
    """Sanity check: the cap must be set to a reasonable production value.
    Zero would silently disable bounding; <= 10 would defeat caching for
    any realistic strategy."""
    assert POLYMARKET_MARKET_CACHE_MAX_ENTRIES >= 64


def test_ttl_max_is_sane() -> None:
    """The hard ceiling on TTL must be finite and not absurdly long.
    24 h is the documented design ceiling — anything larger means the
    operator should be using TTL=0 or rethinking the cache."""
    assert POLYMARKET_MARKET_CACHE_TTL_MAX_SECONDS == 24 * 3600.0
