"""Tests for the process-wide shared rate-limit module (ALM-2943 phase 3).

Pins the consolidation contract:
- Named buckets are shared process-wide: two call sites requesting the same
  name draw from ONE bucket (combined rate cannot exceed the cap).
- Thread safety: concurrent acquires never over-issue tokens.
- Preserved rates: DefiLlamaProvider and YieldAggregator keep their
  historical 10 req/s config, now against a single shared "defillama" budget.
- Stricter-rate semantics: a re-registration with a lower rate tightens the
  bucket; a higher rate never loosens it.
"""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

from almanak.framework.data.ratelimit import TokenBucket, get_bucket, reset_buckets

# ---------------------------------------------------------------------------
# TokenBucket basics
# ---------------------------------------------------------------------------


class TestTokenBucket:
    def test_initial_capacity_and_exhaustion(self):
        bucket = TokenBucket(rate=10, period=1.0)
        for _ in range(10):
            assert bucket.acquire() is True
        assert bucket.acquire() is False

    def test_refill_after_period(self):
        bucket = TokenBucket(rate=10, period=1.0)
        for _ in range(10):
            bucket.acquire()
        assert bucket.acquire() is False

        with patch("almanak.framework.data.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = bucket._last_refill + 1.0
            assert bucket.acquire() is True

    def test_invalid_config_rejected(self):
        with pytest.raises(ValueError):
            TokenBucket(rate=0)
        with pytest.raises(ValueError):
            TokenBucket(rate=10, period=0.0)

    def test_tighten_only_tightens(self):
        bucket = TokenBucket(rate=10, period=1.0)
        bucket.tighten(rate=20, period=1.0)  # looser: ignored
        assert bucket.rate == 10
        bucket.tighten(rate=5, period=1.0)  # stricter: adopted
        assert bucket.rate == 5
        # Capacity clamps immediately: only 5 tokens available now
        successes = sum(1 for _ in range(10) if bucket.acquire())
        assert successes == 5


# ---------------------------------------------------------------------------
# Shared registry
# ---------------------------------------------------------------------------


class TestSharedRegistry:
    def test_same_name_returns_same_bucket(self):
        a = get_bucket("provider-x", rate=10, period=1.0)
        b = get_bucket("provider-x", rate=10, period=1.0)
        assert a is b

    def test_different_names_are_independent(self):
        a = get_bucket("provider-x", rate=10, period=1.0)
        b = get_bucket("provider-y", rate=10, period=1.0)
        assert a is not b

    def test_two_call_sites_draw_from_one_budget(self):
        """Draining via one handle rate-limits the other call site."""
        site_a = get_bucket("shared-upstream", rate=10, period=1.0)
        site_b = get_bucket("shared-upstream", rate=10, period=1.0)

        # Both sites together can only issue 10 requests, not 20.
        total = 0
        for _ in range(10):
            if site_a.acquire():
                total += 1
            if site_b.acquire():
                total += 1
        assert total == 10
        assert site_a.acquire() is False
        assert site_b.acquire() is False

    def test_stricter_rate_wins(self):
        bucket = get_bucket("tighten-me", rate=10, period=1.0)
        again = get_bucket("tighten-me", rate=4, period=1.0)
        assert again is bucket
        assert bucket.rate == 4

    def test_looser_rate_never_loosens(self):
        bucket = get_bucket("keep-strict", rate=4, period=1.0)
        get_bucket("keep-strict", rate=100, period=1.0)
        assert bucket.rate == 4

    def test_reset_buckets_drops_state(self):
        a = get_bucket("ephemeral", rate=10)
        reset_buckets()
        b = get_bucket("ephemeral", rate=10)
        assert a is not b


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_acquire_never_over_issues(self):
        """4 threads hammering one bucket: total grants == capacity, no more."""
        bucket = get_bucket("threaded-upstream", rate=100, period=3600.0)
        grants = []
        lock = threading.Lock()
        barrier = threading.Barrier(4)

        def worker():
            barrier.wait()
            local = sum(1 for _ in range(50) if bucket.acquire())
            with lock:
                grants.append(local)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 200 attempts against a capacity-100 bucket with negligible refill
        # (period=1h => ~0.03 tokens/s): exactly 100 grants across all threads.
        assert sum(grants) == 100

    def test_registry_returns_single_instance_under_contention(self):
        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(4)

        def worker():
            barrier.wait()
            b = get_bucket("contended", rate=10, period=1.0)
            with lock:
                results.append(b)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len({id(b) for b in results}) == 1


# ---------------------------------------------------------------------------
# Preserved rates for the consolidated DeFi Llama call sites
# ---------------------------------------------------------------------------


class TestDefiLlamaConsolidation:
    def test_provider_and_aggregator_share_defillama_bucket(self):
        from almanak.framework.data.providers.defillama_provider import DefiLlamaProvider
        from almanak.framework.data.yields.aggregator import YieldAggregator

        provider = DefiLlamaProvider()
        aggregator = YieldAggregator()

        assert provider._rate_limiter is aggregator._rate_limiter
        assert provider._rate_limiter is get_bucket("defillama")

    def test_defillama_rate_preserved_at_10_per_second(self):
        from almanak.framework.data.providers.defillama_provider import DefiLlamaProvider
        from almanak.framework.data.yields.aggregator import YieldAggregator

        DefiLlamaProvider()
        YieldAggregator()

        bucket = get_bucket("defillama")
        assert bucket.rate == 10
        assert bucket.period == 1.0

    def test_custom_stricter_provider_rate_tightens_shared_bucket(self):
        from almanak.framework.data.providers.defillama_provider import DefiLlamaProvider

        DefiLlamaProvider(rate_limit=10)
        DefiLlamaProvider(rate_limit=5)

        assert get_bucket("defillama").rate == 5
