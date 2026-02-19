"""Unit tests for TokenBucketRateLimiter.

This module tests the TokenBucketRateLimiter class, covering:
- Basic initialization and configuration
- Token acquisition and blocking behavior
- Rate limit enforcement
- Statistics tracking
- Context manager usage
- Edge cases and error handling
"""

import asyncio
import time
from datetime import datetime

import pytest

from almanak.framework.backtesting.pnl.providers.rate_limiter import (
    RateLimiterStats,
    TokenBucketRateLimiter,
    create_coingecko_rate_limiter,
)


class TestRateLimiterStats:
    """Tests for RateLimiterStats dataclass."""

    def test_default_stats(self):
        """Test default stats are initialized to zero."""
        stats = RateLimiterStats()
        assert stats.total_requests == 0
        assert stats.total_waits == 0
        assert stats.total_wait_time_seconds == 0.0
        assert stats.max_wait_time_seconds == 0.0
        assert stats.tokens_consumed == 0
        assert isinstance(stats.created_at, datetime)

    def test_requests_per_second_no_time(self):
        """Test requests per second when just created."""
        stats = RateLimiterStats()
        # Just created, very little time elapsed
        assert stats.requests_per_second >= 0.0

    def test_average_wait_no_waits(self):
        """Test average wait is zero when no waits occurred."""
        stats = RateLimiterStats(total_requests=10, total_waits=0)
        assert stats.average_wait_seconds == 0.0

    def test_average_wait_with_waits(self):
        """Test average wait calculation."""
        stats = RateLimiterStats(
            total_requests=10,
            total_waits=5,
            total_wait_time_seconds=2.5,
        )
        assert stats.average_wait_seconds == 0.5

    def test_wait_rate_no_requests(self):
        """Test wait rate is zero when no requests."""
        stats = RateLimiterStats()
        assert stats.wait_rate == 0.0

    def test_wait_rate_with_waits(self):
        """Test wait rate calculation."""
        stats = RateLimiterStats(total_requests=100, total_waits=25)
        assert stats.wait_rate == 25.0

    def test_to_dict(self):
        """Test stats convert to dictionary correctly."""
        stats = RateLimiterStats(
            total_requests=100,
            total_waits=10,
            total_wait_time_seconds=5.0,
            max_wait_time_seconds=1.5,
            tokens_consumed=100,
        )
        result = stats.to_dict()

        assert result["total_requests"] == 100
        assert result["total_waits"] == 10
        assert result["total_wait_time_seconds"] == 5.0
        assert result["max_wait_time_seconds"] == 1.5
        assert result["average_wait_seconds"] == 0.5
        assert result["wait_rate_percent"] == 10.0
        assert result["tokens_consumed"] == 100
        assert "created_at" in result


class TestTokenBucketRateLimiterInit:
    """Tests for TokenBucketRateLimiter initialization."""

    def test_init_default_values(self):
        """Test default initialization values."""
        limiter = TokenBucketRateLimiter()
        assert limiter.requests_per_minute == 50
        assert limiter.burst_size == 10  # 50 // 5
        assert limiter.min_interval_seconds == pytest.approx(1.2, rel=0.01)

    def test_init_custom_requests_per_minute(self):
        """Test custom requests per minute."""
        limiter = TokenBucketRateLimiter(requests_per_minute=100)
        assert limiter.requests_per_minute == 100
        assert limiter.burst_size == 20  # 100 // 5

    def test_init_custom_burst_size(self):
        """Test custom burst size."""
        limiter = TokenBucketRateLimiter(requests_per_minute=50, burst_size=5)
        assert limiter.burst_size == 5

    def test_init_zero_requests_raises(self):
        """Test zero requests per minute raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TokenBucketRateLimiter(requests_per_minute=0)
        assert "must be positive" in str(exc_info.value)

    def test_init_negative_requests_raises(self):
        """Test negative requests per minute raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            TokenBucketRateLimiter(requests_per_minute=-10)
        assert "must be positive" in str(exc_info.value)

    def test_init_starts_with_full_bucket(self):
        """Test bucket starts full."""
        limiter = TokenBucketRateLimiter(burst_size=10)
        assert limiter.tokens_available == 10.0


class TestTokenBucketRateLimiterAcquire:
    """Tests for acquire() method."""

    @pytest.mark.asyncio
    async def test_acquire_consumes_token(self):
        """Test acquire consumes one token."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=10)
        initial_tokens = limiter.tokens_available

        await limiter.acquire()

        # Token consumed (approximately, due to refill)
        assert limiter.tokens_available < initial_tokens

    @pytest.mark.asyncio
    async def test_acquire_tracks_stats(self):
        """Test acquire updates statistics."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=10)

        await limiter.acquire()

        stats = limiter.get_stats()
        assert stats.total_requests == 1
        assert stats.tokens_consumed == 1

    @pytest.mark.asyncio
    async def test_acquire_multiple_no_wait_when_tokens_available(self):
        """Test multiple acquires don't wait when tokens available."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=5)

        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should complete almost instantly (< 0.5s for 5 acquires)
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_acquire_blocks_when_no_tokens(self):
        """Test acquire blocks when no tokens available."""
        # Very low rate: 1 request per 2 seconds
        limiter = TokenBucketRateLimiter(requests_per_minute=30, burst_size=1)

        # First acquire should be immediate
        await limiter.acquire()

        # Second acquire should wait
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        # Should have waited approximately 2 seconds
        assert elapsed >= 1.5

    @pytest.mark.asyncio
    async def test_acquire_tracks_wait_time(self):
        """Test acquire tracks wait time in stats."""
        # 1 request per second, burst of 1
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        # First acquire is immediate
        await limiter.acquire()

        # Second acquire will wait
        await limiter.acquire()

        stats = limiter.get_stats()
        assert stats.total_waits >= 1
        assert stats.total_wait_time_seconds > 0


class TestTokenBucketRateLimiterAcquireNowait:
    """Tests for acquire_nowait() method."""

    @pytest.mark.asyncio
    async def test_acquire_nowait_succeeds_with_tokens(self):
        """Test acquire_nowait returns True when tokens available."""
        limiter = TokenBucketRateLimiter(burst_size=5)

        result = await limiter.acquire_nowait()

        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_nowait_fails_without_tokens(self):
        """Test acquire_nowait returns False when no tokens."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)

        # Consume the only token
        await limiter.acquire()

        # Try to acquire without waiting
        result = await limiter.acquire_nowait()

        assert result is False

    @pytest.mark.asyncio
    async def test_acquire_nowait_no_blocking(self):
        """Test acquire_nowait never blocks."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)
        await limiter.acquire()  # Consume token

        start = time.monotonic()
        await limiter.acquire_nowait()
        elapsed = time.monotonic() - start

        # Should return immediately (< 50ms)
        assert elapsed < 0.05


class TestTokenBucketRateLimiterWaitTime:
    """Tests for wait_time_seconds() method."""

    @pytest.mark.asyncio
    async def test_wait_time_zero_when_tokens_available(self):
        """Test wait time is zero when tokens available."""
        limiter = TokenBucketRateLimiter(burst_size=5)

        wait_time = await limiter.wait_time_seconds()

        assert wait_time == 0.0

    @pytest.mark.asyncio
    async def test_wait_time_positive_when_no_tokens(self):
        """Test wait time is positive when no tokens."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=1)
        await limiter.acquire()  # Consume token

        wait_time = await limiter.wait_time_seconds()

        assert wait_time > 0.0


class TestTokenBucketRateLimiterContextManager:
    """Tests for async context manager usage."""

    @pytest.mark.asyncio
    async def test_context_manager_acquires_token(self):
        """Test context manager acquires token on entry."""
        limiter = TokenBucketRateLimiter(burst_size=5)

        async with limiter:
            stats = limiter.get_stats()
            assert stats.tokens_consumed >= 1

    @pytest.mark.asyncio
    async def test_context_manager_returns_limiter(self):
        """Test context manager returns the limiter instance."""
        limiter = TokenBucketRateLimiter()

        async with limiter as ctx:
            assert ctx is limiter


class TestTokenBucketRateLimiterStats:
    """Tests for statistics and reset."""

    @pytest.mark.asyncio
    async def test_get_stats_returns_copy(self):
        """Test get_stats returns a copy, not the original."""
        limiter = TokenBucketRateLimiter()
        await limiter.acquire()

        stats1 = limiter.get_stats()
        await limiter.acquire()
        stats2 = limiter.get_stats()

        assert stats1.tokens_consumed == 1
        assert stats2.tokens_consumed == 2

    def test_reset_stats(self):
        """Test reset_stats clears all statistics."""
        limiter = TokenBucketRateLimiter()
        limiter._stats.total_requests = 100
        limiter._stats.tokens_consumed = 50

        limiter.reset_stats()

        stats = limiter.get_stats()
        assert stats.total_requests == 0
        assert stats.tokens_consumed == 0


class TestTokenBucketRateLimiterRefill:
    """Tests for token refill behavior."""

    @pytest.mark.asyncio
    async def test_tokens_refill_over_time(self):
        """Test tokens are refilled over time."""
        # 60 requests per minute = 1 token per second
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=2)

        # Consume all tokens
        await limiter.acquire()
        await limiter.acquire()

        # Wait for refill
        await asyncio.sleep(1.1)  # Slightly more than 1 second

        # Should have at least 1 token now
        result = await limiter.acquire_nowait()
        assert result is True

    @pytest.mark.asyncio
    async def test_bucket_capped_at_burst_size(self):
        """Test token count is capped at burst size."""
        limiter = TokenBucketRateLimiter(requests_per_minute=6000, burst_size=5)

        # Wait to accumulate tokens
        await asyncio.sleep(0.1)

        # Should still be capped at burst size
        assert limiter.tokens_available <= 5


class TestCreateCoingeckoRateLimiter:
    """Tests for create_coingecko_rate_limiter factory function."""

    def test_free_tier(self):
        """Test free tier configuration."""
        limiter = create_coingecko_rate_limiter(pro_tier=False)

        assert limiter.requests_per_minute == 50
        assert limiter.burst_size == 10

    def test_pro_tier(self):
        """Test pro tier configuration."""
        limiter = create_coingecko_rate_limiter(pro_tier=True)

        assert limiter.requests_per_minute == 500
        assert limiter.burst_size == 50

    def test_default_is_free_tier(self):
        """Test default is free tier."""
        limiter = create_coingecko_rate_limiter()

        assert limiter.requests_per_minute == 50


class TestRateLimiterConcurrency:
    """Tests for concurrent access."""

    @pytest.mark.asyncio
    async def test_concurrent_acquires_are_serialized(self):
        """Test concurrent acquires are properly serialized."""
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=5)

        # Launch 10 concurrent acquires
        tasks = [limiter.acquire() for _ in range(10)]
        await asyncio.gather(*tasks)

        stats = limiter.get_stats()
        assert stats.tokens_consumed == 10
        assert stats.total_requests == 10

    @pytest.mark.asyncio
    async def test_concurrent_acquires_with_waiting(self):
        """Test concurrent acquires properly wait when needed."""
        limiter = TokenBucketRateLimiter(requests_per_minute=120, burst_size=2)

        start = time.monotonic()

        # Launch 4 concurrent acquires, only 2 tokens in bucket
        tasks = [limiter.acquire() for _ in range(4)]
        await asyncio.gather(*tasks)

        elapsed = time.monotonic() - start

        # Should have waited for tokens to refill
        stats = limiter.get_stats()
        assert stats.total_waits >= 2  # At least 2 waits
        assert elapsed >= 0.5  # Waited for refill


class TestRateLimiterIntegration:
    """Integration tests for real-world usage patterns."""

    @pytest.mark.asyncio
    async def test_burst_then_steady_rate(self):
        """Test initial burst followed by steady rate."""
        # 60 requests per minute = 1 per second
        limiter = TokenBucketRateLimiter(requests_per_minute=60, burst_size=3)

        # Burst: 3 requests immediately
        for _ in range(3):
            result = await limiter.acquire_nowait()
            assert result is True

        # Now must wait for steady rate
        result = await limiter.acquire_nowait()
        assert result is False

    @pytest.mark.asyncio
    async def test_stats_accuracy_after_many_requests(self):
        """Test stats remain accurate after many requests."""
        limiter = TokenBucketRateLimiter(requests_per_minute=600, burst_size=10)

        for _ in range(20):
            await limiter.acquire()

        stats = limiter.get_stats()
        assert stats.total_requests == 20
        assert stats.tokens_consumed == 20
        # Some waits should have occurred after burst
        assert stats.total_waits >= 10

    @pytest.mark.asyncio
    async def test_rate_enforcement(self):
        """Test that rate is actually enforced over time."""
        # 120 requests per minute = 2 per second
        limiter = TokenBucketRateLimiter(requests_per_minute=120, burst_size=2)

        start = time.monotonic()

        # Make 6 requests (should take ~2 seconds after burst)
        for _ in range(6):
            await limiter.acquire()

        elapsed = time.monotonic() - start

        # 2 burst + 4 @ 2/sec = ~2 seconds minimum
        assert elapsed >= 1.5
