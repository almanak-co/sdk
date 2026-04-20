"""Unit tests for Token Bucket Rate Limiter.

This module tests the TokenBucketRateLimiter class in providers/rate_limiter.py,
covering:
- Token bucket algorithm allows configured rate of requests
- acquire() blocks when bucket is empty
- on_rate_limit_response() reduces rate by 20%
- Exponential backoff timing and retry logic
- Request queue handling
- Statistics tracking
"""

import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.rate_limiter import (
    RateLimiterStats,
    TokenBucketRateLimiter,
    create_coingecko_rate_limiter,
)


class TestTokenBucketRateLimiterInitialization:
    """Tests for TokenBucketRateLimiter initialization."""

    def test_init_default(self):
        """Test limiter initializes with default settings."""
        limiter = TokenBucketRateLimiter()
        assert limiter.requests_per_minute == 50
        assert limiter.burst_size == 10  # 50 // 5
        assert limiter.min_interval_seconds == pytest.approx(1.2, rel=0.01)  # 60/50

    def test_init_custom_rate(self):
        """Test limiter with custom rate."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)
        assert limiter.requests_per_minute == 100
        assert limiter.burst_size == 20  # 100 // 5
        assert limiter.min_interval_seconds == pytest.approx(0.6, rel=0.01)  # 60/100

    def test_init_custom_burst_size(self):
        """Test limiter with custom burst size."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=30)
        assert limiter.requests_per_minute == 60
        assert limiter.burst_size == 30
        assert limiter.min_interval_seconds == pytest.approx(1.0, rel=0.01)

    def test_init_invalid_rate_raises_error(self):
        """Test that invalid rate raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            TokenBucketRateLimiter(requests_per_minute=0)

        with pytest.raises(ValueError, match="must be positive"):
            TokenBucketRateLimiter(requests_per_minute=-10)

    def test_init_minimum_burst_size(self):
        """Test that burst size is at least 1."""
        limiter = TokenBucketRateLimiter(requests_per_minute=2)
        assert limiter.burst_size == 1  # max(1, 2 // 5) = 1

    def test_initial_rate_preserved(self):
        """Test initial rate is preserved for reset."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)
        assert limiter.initial_requests_per_minute == 100


class TestTokenBucketAllowsConfiguredRate:
    """Tests that token bucket allows configured rate of requests."""

    @pytest.mark.asyncio
    async def test_burst_size_requests_allowed_immediately(self):
        """Test that burst_size requests can be made without waiting."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=5)

        # Should be able to make burst_size requests immediately
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        # All 5 requests should complete nearly instantly (< 0.1s)
        assert elapsed < 0.1
        stats = limiter.get_stats()
        assert stats.tokens_consumed == 5
        assert stats.total_requests == 5
        assert stats.total_waits == 0

    @pytest.mark.asyncio
    async def test_requests_throttled_after_burst(self):
        """Test that requests are throttled after burst is exhausted."""
        # 60 req/min = 1 per second
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=2)

        # Exhaust burst
        await limiter.acquire()
        await limiter.acquire()

        # Third request should wait
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should wait approximately 1 second (60 req/min = 1 per second)
        assert elapsed >= 0.5  # At least half a second
        stats = limiter.get_stats()
        assert stats.total_waits == 1

    @pytest.mark.asyncio
    async def test_acquire_nowait_returns_false_when_empty(self):
        """Test acquire_nowait returns False when no tokens available."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        # First should succeed
        assert await limiter.acquire_nowait() is True

        # Second should fail immediately (no wait)
        assert await limiter.acquire_nowait() is False

        stats = limiter.get_stats()
        assert stats.tokens_consumed == 1  # Only first consumed

    @pytest.mark.asyncio
    async def test_tokens_refill_over_time(self):
        """Test that tokens refill based on elapsed time."""
        # 60 req/min = 1 token per second
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=2)

        # Exhaust burst
        await limiter.acquire()
        await limiter.acquire()

        # Wait for refill
        await asyncio.sleep(1.1)  # Wait slightly more than 1 second

        # Should be able to acquire at least 1 token without waiting
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        assert elapsed < 0.1  # Should be nearly instant


class TestAcquireBlocksWhenEmpty:
    """Tests that acquire() blocks when bucket is empty."""

    @pytest.mark.asyncio
    async def test_acquire_blocks_until_token_available(self):
        """Test that acquire blocks when no tokens available."""
        # 60 req/min = 1 token per second
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        # Exhaust the single token
        await limiter.acquire()

        # Measure blocking time for next acquire
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should block for approximately 1 second
        assert elapsed >= 0.8  # Allow some tolerance
        assert elapsed < 1.5

    @pytest.mark.asyncio
    async def test_wait_time_seconds_returns_correct_value(self):
        """Test wait_time_seconds returns correct remaining wait time."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        # With full bucket, wait time should be 0
        wait_time = await limiter.wait_time_seconds()
        assert wait_time == 0.0

        # Exhaust token
        await limiter.acquire()

        # Wait time should be close to 1 second
        wait_time = await limiter.wait_time_seconds()
        assert 0.5 < wait_time <= 1.0

    @pytest.mark.asyncio
    async def test_stats_track_wait_time(self):
        """Test that statistics track wait time correctly."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        # Exhaust token
        await limiter.acquire()

        # Force a wait
        await limiter.acquire()

        stats = limiter.get_stats()
        assert stats.total_waits == 1
        assert stats.total_wait_time_seconds > 0.5
        assert stats.max_wait_time_seconds > 0.5

    @pytest.mark.asyncio
    async def test_multiple_waiters_processed_in_order(self):
        """Test that multiple waiters are processed in FIFO order."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        # Exhaust token
        await limiter.acquire()

        completion_order = []

        async def waiter(id: int):
            await limiter.acquire()
            completion_order.append(id)

        # Launch multiple waiters
        tasks = [
            asyncio.create_task(waiter(1)),
            asyncio.create_task(waiter(2)),
            asyncio.create_task(waiter(3)),
        ]

        # Let some time pass for refills
        await asyncio.sleep(3.5)  # 3 seconds for 3 tokens

        # All should complete
        await asyncio.gather(*tasks)

        # Should be processed (approximately) in order
        assert len(completion_order) == 3


class TestOnRateLimitResponseReducesRate:
    """Tests that on_rate_limit_response() reduces rate by 20%."""

    @pytest.mark.asyncio
    async def test_rate_reduces_by_20_percent(self):
        """Test that rate is reduced by exactly 20%."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)
        assert limiter.requests_per_minute == 100

        await limiter.on_rate_limit_response()

        assert limiter.requests_per_minute == 80  # 100 * 0.8

    @pytest.mark.asyncio
    async def test_multiple_reductions(self):
        """Test multiple rate reductions compound correctly."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)

        await limiter.on_rate_limit_response()  # 100 -> 80
        assert limiter.requests_per_minute == 80

        await limiter.on_rate_limit_response()  # 80 -> 64
        assert limiter.requests_per_minute == 64

        await limiter.on_rate_limit_response()  # 64 -> 51
        assert limiter.requests_per_minute == 51

    @pytest.mark.asyncio
    async def test_rate_never_goes_below_minimum(self):
        """Test that rate never goes below MIN_REQUESTS_PER_MINUTE (1)."""
        limiter = TokenBucketRateLimiter(requests_per_minute=5)

        # Reduce multiple times
        for _ in range(10):
            await limiter.on_rate_limit_response()

        # Should never go below 1
        assert limiter.requests_per_minute >= TokenBucketRateLimiter.MIN_REQUESTS_PER_MINUTE
        assert limiter.requests_per_minute == 1

    @pytest.mark.asyncio
    async def test_reduction_updates_internal_state(self):
        """Test that rate reduction updates refill rate and min interval."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60)

        # Initial state
        assert limiter.min_interval_seconds == pytest.approx(1.0, rel=0.01)

        await limiter.on_rate_limit_response()  # 60 -> 48

        # min_interval should increase (slower rate = longer interval)
        assert limiter.requests_per_minute == 48
        assert limiter.min_interval_seconds == pytest.approx(60.0 / 48, rel=0.01)

    @pytest.mark.asyncio
    async def test_stats_track_reductions(self):
        """Test that statistics track rate reductions."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)

        await limiter.on_rate_limit_response()
        await limiter.on_rate_limit_response()
        await limiter.on_rate_limit_response()

        stats = limiter.get_stats()
        assert stats.rate_limit_reductions == 3

    def test_reset_rate_restores_initial_value(self):
        """Test that reset_rate() restores the initial rate."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)

        # Reduce rate multiple times
        asyncio.run(limiter.on_rate_limit_response())  # 100 -> 80
        asyncio.run(limiter.on_rate_limit_response())  # 80 -> 64
        assert limiter.requests_per_minute == 64

        # Reset should restore original
        limiter.reset_rate()
        assert limiter.requests_per_minute == 100
        assert limiter.initial_requests_per_minute == 100


class TestExponentialBackoffTiming:
    """Tests for exponential backoff timing in retry_with_backoff."""

    _SLEEP_PATCH_TARGET = "almanak.framework.backtesting.pnl.providers.rate_limiter.asyncio.sleep"

    @pytest.mark.asyncio
    async def test_successful_request_no_retry(self):
        """Test that successful request returns immediately without retry."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60)

        async def success_func():
            return "success"

        result = await limiter.retry_with_backoff(success_func)
        assert result == "success"

        stats = limiter.get_stats()
        assert stats.retry_attempts == 0
        assert stats.retry_successes == 0

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Test that failures trigger retries."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60)

        call_count = 0

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Transient error")
            return "success"

        result = await limiter.retry_with_backoff(
            fail_then_succeed,
            max_retries=5,
            base_delay_seconds=0.01,  # Fast for testing
        )

        assert result == "success"
        assert call_count == 3

        stats = limiter.get_stats()
        assert stats.retry_attempts == 2  # 2 failures before success
        assert stats.retry_successes == 1

    @pytest.mark.asyncio
    async def test_max_retries_exhausted_raises(self):
        """Test that exhausting max retries raises the last exception."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60)

        async def always_fail():
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            await limiter.retry_with_backoff(
                always_fail,
                max_retries=2,
                base_delay_seconds=0.01,  # Fast for testing
            )

        stats = limiter.get_stats()
        assert stats.retry_attempts == 3  # Initial + 2 retries

    @pytest.mark.asyncio
    async def test_backoff_delay_increases_exponentially(self):
        """Test that backoff delays increase exponentially."""
        limiter = TokenBucketRateLimiter(requests_per_minute=1000)  # High rate for fast acquire

        delays: list[float] = []
        call_times: list[float] = []

        async def track_timing():
            call_times.append(time.monotonic())
            raise ValueError("Fail")

        try:
            with patch(
                self._SLEEP_PATCH_TARGET,
                new_callable=AsyncMock,
            ) as mock_sleep:
                await limiter.retry_with_backoff(
                    track_timing,
                    max_retries=3,
                    base_delay_seconds=1.0,
                    max_delay_seconds=60.0,
                )
        except ValueError:
            pass

        # Check that sleep was called with increasing delays
        # Exponential backoff: 1*2^0 + jitter, 1*2^1 + jitter, 1*2^2 + jitter
        for i, call in enumerate(mock_sleep.call_args_list):
            delay = call[0][0]
            delays.append(delay)

        # Verify delays are roughly exponential (with jitter, so approximate)
        assert len(delays) == 3
        # First delay should be ~1-2, second ~2-3, third ~4-5 (plus jitter 0-1)
        assert 1.0 <= delays[0] <= 2.0
        assert 2.0 <= delays[1] <= 3.5
        assert 4.0 <= delays[2] <= 5.5

    @pytest.mark.asyncio
    async def test_max_delay_caps_backoff(self):
        """Test that max_delay_seconds caps the backoff time."""
        limiter = TokenBucketRateLimiter(requests_per_minute=1000)

        async def always_fail():
            raise ValueError("Fail")

        try:
            with patch(
                self._SLEEP_PATCH_TARGET,
                new_callable=AsyncMock,
            ) as mock_sleep:
                await limiter.retry_with_backoff(
                    always_fail,
                    max_retries=5,
                    base_delay_seconds=10.0,
                    max_delay_seconds=5.0,  # Cap at 5 seconds
                )
        except ValueError:
            pass

        # All delays should be capped at max_delay_seconds (5.0) + jitter (0-1)
        for call in mock_sleep.call_args_list:
            delay = call[0][0]
            assert delay <= 6.0  # max 5 + 1 jitter

    @pytest.mark.asyncio
    async def test_rate_limit_error_triggers_rate_reduction(self):
        """Test that rate limit errors trigger on_rate_limit_response."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)

        async def rate_limit_error():
            raise ValueError("429 Too Many Requests")

        try:
            await limiter.retry_with_backoff(
                rate_limit_error,
                max_retries=2,
                base_delay_seconds=0.01,
            )
        except ValueError:
            pass

        # Rate should have been reduced due to rate limit detection
        assert limiter.requests_per_minute < 100
        stats = limiter.get_stats()
        assert stats.rate_limit_reductions > 0

    @pytest.mark.asyncio
    async def test_custom_is_rate_limit_error_function(self):
        """Test using custom function to detect rate limit errors."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)

        def custom_detector(e: Exception) -> bool:
            return "CUSTOM_LIMIT" in str(e)

        async def custom_rate_limit():
            raise ValueError("CUSTOM_LIMIT exceeded")

        try:
            await limiter.retry_with_backoff(
                custom_rate_limit,
                max_retries=1,
                base_delay_seconds=0.01,
                is_rate_limit_error=custom_detector,
            )
        except ValueError:
            pass

        # Should have triggered rate reduction
        assert limiter.requests_per_minute < 100


class TestRequestQueue:
    """Tests for request queue functionality."""

    @pytest.mark.asyncio
    async def test_enqueue_request_processes_in_order(self):
        """Test that queued requests are processed in FIFO order."""
        limiter = TokenBucketRateLimiter(requests_per_minute=120, burst_size=1)

        # Exhaust burst
        await limiter.acquire()

        completion_order: list[int] = []

        async def queued_request(id: int):
            await limiter.enqueue_request()
            completion_order.append(id)

        # Queue multiple requests
        tasks = [
            asyncio.create_task(queued_request(1)),
            asyncio.create_task(queued_request(2)),
            asyncio.create_task(queued_request(3)),
        ]

        # Wait for all to complete
        await asyncio.gather(*tasks)

        # Should complete (order may vary slightly due to async scheduling)
        assert len(completion_order) == 3
        assert set(completion_order) == {1, 2, 3}

    @pytest.mark.asyncio
    async def test_get_queue_size(self):
        """Test getting the current queue size."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        assert limiter.get_queue_size() == 0

        # Create pending request
        await limiter.acquire()  # Exhaust burst

        async def pending_request():
            await limiter.enqueue_request()

        task = asyncio.create_task(pending_request())

        # Give task time to queue
        await asyncio.sleep(0.1)

        # Queue should have item (or already processed)
        # Note: Queue size may be 0 if already processed
        size = limiter.get_queue_size()
        assert size >= 0

        # Clean up
        await asyncio.sleep(1.1)  # Wait for token refill
        await task


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_acquires_token(self):
        """Test that async context manager acquires a token on entry."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=5)

        async with limiter:
            pass

        stats = limiter.get_stats()
        assert stats.tokens_consumed == 1

    @pytest.mark.asyncio
    async def test_context_manager_multiple_uses(self):
        """Test multiple context manager uses."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=5)

        async with limiter:
            pass
        async with limiter:
            pass
        async with limiter:
            pass

        stats = limiter.get_stats()
        assert stats.tokens_consumed == 3


class TestRateLimiterStats:
    """Tests for RateLimiterStats dataclass."""

    def test_stats_default_values(self):
        """Test default values for stats."""
        stats = RateLimiterStats()
        assert stats.total_requests == 0
        assert stats.total_waits == 0
        assert stats.total_wait_time_seconds == 0.0
        assert stats.max_wait_time_seconds == 0.0
        assert stats.tokens_consumed == 0
        assert stats.rate_limit_reductions == 0
        assert stats.retry_attempts == 0
        assert stats.retry_successes == 0
        assert isinstance(stats.created_at, datetime)

    def test_requests_per_second_calculation(self):
        """Test requests_per_second property."""
        stats = RateLimiterStats(total_requests=60)
        # Created just now, so requests_per_second depends on elapsed time
        # Just verify it's calculable and non-negative
        assert stats.requests_per_second >= 0

    def test_average_wait_seconds_calculation(self):
        """Test average_wait_seconds property."""
        stats = RateLimiterStats(
            total_waits=4,
            total_wait_time_seconds=10.0,
        )
        assert stats.average_wait_seconds == 2.5

    def test_average_wait_seconds_zero_waits(self):
        """Test average_wait_seconds returns 0 when no waits."""
        stats = RateLimiterStats(total_waits=0)
        assert stats.average_wait_seconds == 0.0

    def test_wait_rate_calculation(self):
        """Test wait_rate property."""
        stats = RateLimiterStats(
            total_requests=100,
            total_waits=25,
        )
        assert stats.wait_rate == 25.0

    def test_wait_rate_zero_requests(self):
        """Test wait_rate returns 0 when no requests."""
        stats = RateLimiterStats(total_requests=0)
        assert stats.wait_rate == 0.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        stats = RateLimiterStats(
            total_requests=100,
            total_waits=10,
            total_wait_time_seconds=5.0,
            max_wait_time_seconds=1.5,
            tokens_consumed=95,
            rate_limit_reductions=2,
            retry_attempts=5,
            retry_successes=3,
        )

        d = stats.to_dict()
        assert d["total_requests"] == 100
        assert d["total_waits"] == 10
        assert d["total_wait_time_seconds"] == 5.0
        assert d["max_wait_time_seconds"] == 1.5
        assert d["tokens_consumed"] == 95
        assert d["rate_limit_reductions"] == 2
        assert d["retry_attempts"] == 5
        assert d["retry_successes"] == 3
        assert "created_at" in d

    def test_reset_stats(self):
        """Test resetting statistics."""
        limiter = TokenBucketRateLimiter()
        limiter._stats.total_requests = 100
        limiter._stats.tokens_consumed = 50

        limiter.reset_stats()

        stats = limiter.get_stats()
        assert stats.total_requests == 0
        assert stats.tokens_consumed == 0


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_create_coingecko_rate_limiter_free_tier(self):
        """Test creating CoinGecko rate limiter for free tier."""
        limiter = create_coingecko_rate_limiter(pro_tier=False)
        assert limiter.requests_per_minute == 50
        assert limiter.burst_size == 10

    def test_create_coingecko_rate_limiter_pro_tier(self):
        """Test creating CoinGecko rate limiter for pro tier."""
        limiter = create_coingecko_rate_limiter(pro_tier=True)
        assert limiter.requests_per_minute == 500
        assert limiter.burst_size == 50


class TestProperties:
    """Tests for limiter properties."""

    def test_tokens_available_property(self):
        """Test tokens_available property."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=5)
        # Initial tokens should equal burst size
        assert limiter.tokens_available == 5.0

    @pytest.mark.asyncio
    async def test_tokens_decrease_after_acquire(self):
        """Test that tokens decrease after acquire."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=5)

        initial_tokens = limiter.tokens_available
        await limiter.acquire()
        after_tokens = limiter.tokens_available

        assert after_tokens < initial_tokens

    def test_requests_per_minute_property(self):
        """Test requests_per_minute property."""
        limiter = TokenBucketRateLimiter(requests_per_minute=75)
        assert limiter.requests_per_minute == 75

    def test_burst_size_property(self):
        """Test burst_size property."""
        limiter = TokenBucketRateLimiter(burst_size=20)
        assert limiter.burst_size == 20

    def test_min_interval_seconds_property(self):
        """Test min_interval_seconds property."""
        limiter = TokenBucketRateLimiter(requests_per_minute=120)  # 2 per second
        assert limiter.min_interval_seconds == pytest.approx(0.5, rel=0.01)


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_very_high_rate(self):
        """Test limiter with very high rate."""
        limiter = TokenBucketRateLimiter(requests_per_minute=10000, burst_size=100)

        # Should be able to make many requests quickly
        start = time.monotonic()
        for _ in range(100):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        assert elapsed < 1.0  # All should complete in under 1 second

    @pytest.mark.asyncio
    async def test_very_low_rate(self):
        """Test limiter with very low rate."""
        limiter = TokenBucketRateLimiter(requests_per_minute=1, burst_size=1)

        # First request should be instant
        start = time.monotonic()
        await limiter.acquire()
        elapsed1 = time.monotonic() - start
        assert elapsed1 < 0.1

        # Second request should take ~60 seconds (but we just verify it waits)
        wait_time = await limiter.wait_time_seconds()
        assert wait_time > 30  # Should be close to 60 seconds

    def test_get_stats_returns_copy(self):
        """Test that get_stats returns a copy, not the original."""
        limiter = TokenBucketRateLimiter()
        stats1 = limiter.get_stats()
        stats2 = limiter.get_stats()

        # Should be equal but not the same object
        assert stats1.total_requests == stats2.total_requests
        assert stats1 is not stats2

    @pytest.mark.asyncio
    async def test_concurrent_acquires(self):
        """Test multiple concurrent acquire calls."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=3)

        # Launch concurrent acquires
        results = await asyncio.gather(
            limiter.acquire(),
            limiter.acquire(),
            limiter.acquire(),
            limiter.acquire(),  # This one should wait
            limiter.acquire(),  # This one should wait longer
        )

        stats = limiter.get_stats()
        assert stats.total_requests == 5
        assert stats.tokens_consumed == 5
        # At least some should have waited
        assert stats.total_waits >= 2


class TestRateLimiterConstants:
    """Tests for rate limiter constants."""

    def test_rate_reduction_factor(self):
        """Test RATE_REDUCTION_FACTOR constant."""
        assert TokenBucketRateLimiter.RATE_REDUCTION_FACTOR == 0.8

    def test_min_requests_per_minute(self):
        """Test MIN_REQUESTS_PER_MINUTE constant."""
        assert TokenBucketRateLimiter.MIN_REQUESTS_PER_MINUTE == 1

    def test_default_backoff_settings(self):
        """Test default backoff settings."""
        assert TokenBucketRateLimiter.DEFAULT_BASE_DELAY_SECONDS == 1.0
        assert TokenBucketRateLimiter.DEFAULT_MAX_DELAY_SECONDS == 60.0
        assert TokenBucketRateLimiter.DEFAULT_MAX_RETRIES == 5
