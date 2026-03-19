"""Unit tests for DataCache class.

This module tests the DataCache class, covering:
- Cache write and read operations
- Cache invalidation by TTL
- Cache miss behavior
- Batch operations
- Key-value caching
- Cache warming
- Statistics tracking
- Filesystem fallback paths
"""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.cache import CacheKey, CacheStats, DataCache, OHLCVData


class TestCacheKeyDataclass:
    """Tests for CacheKey dataclass."""

    def test_create_cache_key(self):
        """Test creating a CacheKey."""
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        assert key.token == "ETH"
        assert key.timestamp == datetime(2024, 1, 1, 12, 0, 0)
        assert key.interval == "1h"

    def test_cache_key_hashable(self):
        """Test CacheKey is hashable for use in dicts."""
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        # Should be usable as dict key
        d = {key: "value"}
        assert d[key] == "value"

    def test_cache_key_equality(self):
        """Test CacheKey equality comparison."""
        key1 = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        key2 = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        key3 = CacheKey(
            token="BTC",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        assert key1 == key2
        assert key1 != key3

    def test_cache_key_inequality_with_non_key(self):
        """Test CacheKey inequality with non-CacheKey objects."""
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        assert key != "not a key"
        assert key != 123
        assert key is not None


class TestOHLCVDataDataclass:
    """Tests for OHLCVData dataclass."""

    def test_create_ohlcv_data(self):
        """Test creating OHLCVData."""
        data = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
            volume=Decimal("1000000"),
        )
        assert data.open == Decimal("3000")
        assert data.high == Decimal("3100")
        assert data.low == Decimal("2950")
        assert data.close == Decimal("3050")
        assert data.volume == Decimal("1000000")

    def test_create_ohlcv_data_without_volume(self):
        """Test creating OHLCVData without volume."""
        data = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
        )
        assert data.volume is None

    def test_ohlcv_to_dict(self):
        """Test OHLCVData serialization."""
        data = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
            volume=Decimal("1000000"),
        )
        d = data.to_dict()
        assert d["open"] == "3000"
        assert d["high"] == "3100"
        assert d["low"] == "2950"
        assert d["close"] == "3050"
        assert d["volume"] == "1000000"

    def test_ohlcv_to_dict_without_volume(self):
        """Test OHLCVData serialization without volume."""
        data = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
        )
        d = data.to_dict()
        assert d["volume"] is None

    def test_ohlcv_from_dict(self):
        """Test OHLCVData deserialization."""
        d = {
            "open": "3000",
            "high": "3100",
            "low": "2950",
            "close": "3050",
            "volume": "1000000",
        }
        data = OHLCVData.from_dict(d)
        assert data.open == Decimal("3000")
        assert data.high == Decimal("3100")
        assert data.low == Decimal("2950")
        assert data.close == Decimal("3050")
        assert data.volume == Decimal("1000000")

    def test_ohlcv_from_dict_without_volume(self):
        """Test OHLCVData deserialization without volume."""
        d = {
            "open": "3000",
            "high": "3100",
            "low": "2950",
            "close": "3050",
            "volume": None,
        }
        data = OHLCVData.from_dict(d)
        assert data.volume is None

    def test_ohlcv_roundtrip(self):
        """Test OHLCVData roundtrip serialization."""
        original = OHLCVData(
            open=Decimal("3000.12345"),
            high=Decimal("3100.99"),
            low=Decimal("2950.01"),
            close=Decimal("3050.50"),
            volume=Decimal("1000000.123"),
        )
        restored = OHLCVData.from_dict(original.to_dict())
        assert original == restored


class TestCacheStatsDataclass:
    """Tests for CacheStats dataclass."""

    def test_create_cache_stats(self):
        """Test creating CacheStats with defaults."""
        stats = CacheStats()
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.expired == 0
        assert stats.total_entries == 0

    def test_hit_rate_calculation(self):
        """Test hit rate calculation."""
        stats = CacheStats(hits=75, misses=25)
        assert stats.hit_rate() == 0.75

    def test_hit_rate_zero_requests(self):
        """Test hit rate when no requests made."""
        stats = CacheStats()
        assert stats.hit_rate() == 0.0

    def test_cache_stats_to_dict(self):
        """Test CacheStats serialization."""
        stats = CacheStats(hits=10, misses=5, expired=2, total_entries=100)
        d = stats.to_dict()
        assert d["hits"] == 10
        assert d["misses"] == 5
        assert d["expired"] == 2
        assert d["total_entries"] == 100
        assert d["hit_rate"] == pytest.approx(0.666666, rel=0.001)


class TestDataCacheInitialization:
    """Tests for DataCache initialization."""

    def test_init_in_memory(self):
        """Test creating in-memory cache."""
        cache = DataCache(":memory:")
        assert cache.db_path == ":memory:"
        assert cache._is_memory is True
        assert cache._memory_conn is not None

    def test_init_default_ttl(self):
        """Test default TTL is 0 (no expiration)."""
        cache = DataCache(":memory:")
        assert cache.ttl_seconds == 0

    def test_init_custom_ttl(self):
        """Test custom TTL setting."""
        cache = DataCache(":memory:", ttl_seconds=3600)
        assert cache.ttl_seconds == 3600

    def test_init_creates_tables(self):
        """Test initialization creates database tables."""
        cache = DataCache(":memory:")
        # Verify tables exist by counting entries (should be 0)
        assert cache.count() == 0
        # KV cache should also work
        assert cache.get_kv("nonexistent") is None


class TestCacheWriteAndRead:
    """Tests for cache write and read operations."""

    def test_set_and_get(self):
        """Test basic set and get operations."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
            volume=Decimal("1000000"),
        )

        cache.set(key, data)
        result = cache.get(key)

        assert result is not None
        assert result.open == data.open
        assert result.high == data.high
        assert result.low == data.low
        assert result.close == data.close
        assert result.volume == data.volume

    def test_set_updates_existing(self):
        """Test set updates existing entry with same key."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data1 = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
        )
        data2 = OHLCVData(
            open=Decimal("4000"),
            high=Decimal("4100"),
            low=Decimal("3950"),
            close=Decimal("4050"),
        )

        cache.set(key, data1)
        cache.set(key, data2)  # Update same key

        result = cache.get(key)
        assert result.open == Decimal("4000")
        assert cache.count() == 1  # Should still be one entry

    def test_get_increments_hit_stats(self):
        """Test get increments hit count on cache hit."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
        )

        cache.set(key, data)
        cache.reset_stats()

        cache.get(key)
        cache.get(key)
        cache.get(key)

        assert cache.stats.hits == 3
        assert cache.stats.misses == 0

    def test_set_without_volume(self):
        """Test set and get without volume."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2950"),
            close=Decimal("3050"),
        )

        cache.set(key, data)
        result = cache.get(key)

        assert result is not None
        assert result.volume is None

    def test_multiple_tokens(self):
        """Test caching multiple tokens."""
        cache = DataCache(":memory:")
        ts = datetime(2024, 1, 1, 12, 0, 0)

        eth_key = CacheKey(token="ETH", timestamp=ts, interval="1h")
        btc_key = CacheKey(token="BTC", timestamp=ts, interval="1h")

        eth_data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        btc_data = OHLCVData(
            open=Decimal("50000"), high=Decimal("51000"), low=Decimal("49000"), close=Decimal("50500")
        )

        cache.set(eth_key, eth_data)
        cache.set(btc_key, btc_data)

        assert cache.get(eth_key).close == Decimal("3050")
        assert cache.get(btc_key).close == Decimal("50500")
        assert cache.count() == 2

    def test_multiple_intervals(self):
        """Test caching multiple intervals for same token."""
        cache = DataCache(":memory:")
        ts = datetime(2024, 1, 1, 12, 0, 0)

        key_1h = CacheKey(token="ETH", timestamp=ts, interval="1h")
        key_4h = CacheKey(token="ETH", timestamp=ts, interval="4h")

        data_1h = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        data_4h = OHLCVData(
            open=Decimal("2900"), high=Decimal("3200"), low=Decimal("2850"), close=Decimal("3100")
        )

        cache.set(key_1h, data_1h)
        cache.set(key_4h, data_4h)

        assert cache.get(key_1h).close == Decimal("3050")
        assert cache.get(key_4h).close == Decimal("3100")
        assert cache.count() == 2


class TestCacheMissBehavior:
    """Tests for cache miss behavior."""

    def test_get_nonexistent_returns_none(self):
        """Test get returns None for nonexistent key."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        result = cache.get(key)
        assert result is None

    def test_get_nonexistent_increments_miss_stats(self):
        """Test get increments miss count on cache miss."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )

        cache.get(key)
        cache.get(key)

        assert cache.stats.hits == 0
        assert cache.stats.misses == 2

    def test_get_wrong_timestamp_returns_none(self):
        """Test get returns None for wrong timestamp."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        wrong_key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 13, 0, 0),  # Different hour
            interval="1h",
        )
        assert cache.get(wrong_key) is None

    def test_get_wrong_interval_returns_none(self):
        """Test get returns None for wrong interval."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        wrong_key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="4h",  # Different interval
        )
        assert cache.get(wrong_key) is None

    def test_get_wrong_token_returns_none(self):
        """Test get returns None for wrong token."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        wrong_key = CacheKey(
            token="BTC",  # Different token
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        assert cache.get(wrong_key) is None


class TestCacheInvalidationByTTL:
    """Tests for cache invalidation by TTL."""

    def test_expired_entry_returns_none(self):
        """Test expired entries return None."""
        cache = DataCache(":memory:", ttl_seconds=1)  # 1 second TTL
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        # Verify it's there initially
        assert cache.get(key) is not None

        # Wait for TTL to expire
        import time

        time.sleep(1.1)

        # Now it should be expired
        result = cache.get(key)
        assert result is None

    def test_expired_entry_increments_expired_stats(self):
        """Test expired entries increment expired counter."""
        cache = DataCache(":memory:", ttl_seconds=1)
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)
        cache.reset_stats()

        import time

        time.sleep(1.1)

        cache.get(key)
        assert cache.stats.expired == 1
        assert cache.stats.misses == 1  # Expired counts as a miss

    def test_no_ttl_entries_never_expire(self):
        """Test entries never expire when TTL is 0."""
        cache = DataCache(":memory:", ttl_seconds=0)  # No TTL
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        # Should not expire
        result = cache.get(key)
        assert result is not None

    def test_invalidate_expired_removes_old_entries(self):
        """Test invalidate_expired removes old entries."""
        cache = DataCache(":memory:", ttl_seconds=1)
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)
        assert cache.count() == 1

        import time

        time.sleep(1.1)

        deleted = cache.invalidate_expired()
        assert deleted == 1
        assert cache.count() == 0

    def test_invalidate_expired_no_effect_without_ttl(self):
        """Test invalidate_expired has no effect when TTL is 0."""
        cache = DataCache(":memory:", ttl_seconds=0)
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        deleted = cache.invalidate_expired()
        assert deleted == 0
        assert cache.count() == 1

    def test_get_expired_count(self):
        """Test get_expired_count returns correct count."""
        cache = DataCache(":memory:", ttl_seconds=1)

        # Add some entries
        for i in range(5):
            key = CacheKey(
                token="ETH",
                timestamp=datetime(2024, 1, 1, i, 0, 0),
                interval="1h",
            )
            data = OHLCVData(
                open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
            )
            cache.set(key, data)

        assert cache.get_expired_count() == 0

        import time

        time.sleep(1.1)

        assert cache.get_expired_count() == 5

    def test_contains_respects_ttl(self):
        """Test contains method respects TTL."""
        cache = DataCache(":memory:", ttl_seconds=1)
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)
        assert cache.contains(key) is True

        import time

        time.sleep(1.1)

        assert cache.contains(key) is False

    def test_contains_ignores_ttl_when_check_disabled(self):
        """Test contains ignores TTL when check_ttl=False."""
        cache = DataCache(":memory:", ttl_seconds=1)
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        import time

        time.sleep(1.1)

        # With check_ttl=True (default), should be expired
        assert cache.contains(key, check_ttl=True) is False
        # With check_ttl=False, should still exist
        assert cache.contains(key, check_ttl=False) is True


class TestBatchOperations:
    """Tests for batch operations."""

    def test_set_batch(self):
        """Test batch set operation."""
        cache = DataCache(":memory:")
        items = []
        for i in range(10):
            key = CacheKey(
                token="ETH",
                timestamp=datetime(2024, 1, 1, i, 0, 0),
                interval="1h",
            )
            data = OHLCVData(
                open=Decimal(f"{3000 + i}"),
                high=Decimal(f"{3100 + i}"),
                low=Decimal(f"{2950 + i}"),
                close=Decimal(f"{3050 + i}"),
            )
            items.append((key, data))

        count = cache.set_batch(items)
        assert count == 10
        assert cache.count() == 10

    def test_set_batch_empty_list(self):
        """Test batch set with empty list."""
        cache = DataCache(":memory:")
        count = cache.set_batch([])
        assert count == 0

    def test_get_range(self):
        """Test get_range retrieves correct entries."""
        cache = DataCache(":memory:")

        # Add entries for multiple hours
        for i in range(24):
            key = CacheKey(
                token="ETH",
                timestamp=datetime(2024, 1, 1, i, 0, 0),
                interval="1h",
            )
            data = OHLCVData(
                open=Decimal(f"{3000 + i}"),
                high=Decimal(f"{3100 + i}"),
                low=Decimal(f"{2950 + i}"),
                close=Decimal(f"{3050 + i}"),
            )
            cache.set(key, data)

        # Get range for hours 5-10
        results = cache.get_range(
            token="ETH",
            interval="1h",
            start=datetime(2024, 1, 1, 5, 0, 0),
            end=datetime(2024, 1, 1, 10, 0, 0),
        )

        assert len(results) == 6  # Hours 5, 6, 7, 8, 9, 10
        assert results[0][0] == datetime(2024, 1, 1, 5, 0, 0)
        assert results[-1][0] == datetime(2024, 1, 1, 10, 0, 0)

    def test_get_range_no_filters(self):
        """Test get_range without start/end filters."""
        cache = DataCache(":memory:")

        for i in range(5):
            key = CacheKey(
                token="ETH",
                timestamp=datetime(2024, 1, 1, i, 0, 0),
                interval="1h",
            )
            data = OHLCVData(
                open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
            )
            cache.set(key, data)

        results = cache.get_range(token="ETH", interval="1h")
        assert len(results) == 5


class TestDeleteAndClear:
    """Tests for delete and clear operations."""

    def test_delete(self):
        """Test delete removes specific entry."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)
        assert cache.count() == 1

        deleted = cache.delete(key)
        assert deleted is True
        assert cache.count() == 0
        assert cache.get(key) is None

    def test_delete_nonexistent(self):
        """Test delete returns False for nonexistent key."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        deleted = cache.delete(key)
        assert deleted is False

    def test_clear_all(self):
        """Test clear removes all entries."""
        cache = DataCache(":memory:")

        for token in ["ETH", "BTC", "ARB"]:
            for i in range(3):
                key = CacheKey(
                    token=token,
                    timestamp=datetime(2024, 1, 1, i, 0, 0),
                    interval="1h",
                )
                data = OHLCVData(
                    open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
                )
                cache.set(key, data)

        assert cache.count() == 9

        deleted = cache.clear()
        assert deleted == 9
        assert cache.count() == 0

    def test_clear_by_token(self):
        """Test clear with token filter."""
        cache = DataCache(":memory:")

        for token in ["ETH", "BTC"]:
            for i in range(3):
                key = CacheKey(
                    token=token,
                    timestamp=datetime(2024, 1, 1, i, 0, 0),
                    interval="1h",
                )
                data = OHLCVData(
                    open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
                )
                cache.set(key, data)

        assert cache.count() == 6

        deleted = cache.clear(token="ETH")
        assert deleted == 3
        assert cache.count() == 3
        assert cache.count(token="BTC") == 3

    def test_clear_by_interval(self):
        """Test clear with interval filter."""
        cache = DataCache(":memory:")

        for interval in ["1h", "4h"]:
            for i in range(3):
                key = CacheKey(
                    token="ETH",
                    timestamp=datetime(2024, 1, 1, i, 0, 0),
                    interval=interval,
                )
                data = OHLCVData(
                    open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
                )
                cache.set(key, data)

        assert cache.count() == 6

        deleted = cache.clear(interval="1h")
        assert deleted == 3
        assert cache.count() == 3


class TestKeyValueCache:
    """Tests for key-value cache operations."""

    def test_set_kv_and_get_kv(self):
        """Test basic key-value operations."""
        cache = DataCache(":memory:")
        cache.set_kv("my_key", "my_value")
        assert cache.get_kv("my_key") == "my_value"

    def test_get_kv_nonexistent(self):
        """Test get_kv returns None for nonexistent key."""
        cache = DataCache(":memory:")
        assert cache.get_kv("nonexistent") is None

    def test_set_kv_updates_existing(self):
        """Test set_kv updates existing key."""
        cache = DataCache(":memory:")
        cache.set_kv("key", "value1")
        cache.set_kv("key", "value2")
        assert cache.get_kv("key") == "value2"

    def test_set_json_and_get_json(self):
        """Test JSON key-value operations."""
        cache = DataCache(":memory:")
        data = {"name": "ETH", "price": 3000, "active": True}
        cache.set_json("my_json", data)
        result = cache.get_json("my_json")
        assert result == data

    def test_get_json_nonexistent(self):
        """Test get_json returns None for nonexistent key."""
        cache = DataCache(":memory:")
        assert cache.get_json("nonexistent") is None

    def test_set_json_list(self):
        """Test JSON with list value."""
        cache = DataCache(":memory:")
        data = [1, 2, 3, "four", {"five": 5}]
        cache.set_json("my_list", data)
        assert cache.get_json("my_list") == data


class TestCacheStatistics:
    """Tests for cache statistics tracking."""

    def test_stats_initial_values(self):
        """Test initial statistics values."""
        cache = DataCache(":memory:")
        assert cache.stats.hits == 0
        assert cache.stats.misses == 0
        assert cache.stats.expired == 0
        assert cache.stats.total_entries == 0

    def test_stats_updates_on_operations(self):
        """Test statistics update on cache operations."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )

        # Set and verify total entries
        cache.set(key, data)
        assert cache.stats.total_entries == 1

        # Get (hit)
        cache.get(key)
        assert cache.stats.hits == 1

        # Get nonexistent (miss)
        nonexistent_key = CacheKey(
            token="BTC",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        cache.get(nonexistent_key)
        assert cache.stats.misses == 1

    def test_reset_stats(self):
        """Test resetting statistics."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)
        cache.get(key)

        assert cache.stats.hits == 1

        cache.reset_stats()
        assert cache.stats.hits == 0
        assert cache.stats.misses == 0
        assert cache.stats.expired == 0

    def test_hit_rate_calculation(self):
        """Test hit rate calculation in stats."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        # 3 hits
        cache.get(key)
        cache.get(key)
        cache.get(key)

        # 1 miss
        nonexistent_key = CacheKey(
            token="BTC",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        cache.get(nonexistent_key)

        # Hit rate: 3 / 4 = 0.75
        assert cache.stats.hit_rate() == pytest.approx(0.75)


class TestCacheWarmingSynchronous:
    """Tests for synchronous cache warming."""

    def test_warm_cache_sync(self):
        """Test synchronous cache warming with pre-loaded data."""
        cache = DataCache(":memory:")

        # Prepare data
        eth_data = [
            (
                datetime(2024, 1, 1, i, 0, 0),
                OHLCVData(
                    open=Decimal(f"{3000 + i}"),
                    high=Decimal(f"{3100 + i}"),
                    low=Decimal(f"{2950 + i}"),
                    close=Decimal(f"{3050 + i}"),
                ),
            )
            for i in range(24)
        ]

        data = {"ETH": eth_data}

        count = cache.warm_cache_sync(
            tokens=["ETH"],
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            data=data,
            interval="1h",
        )

        assert count == 24
        assert cache.count() == 24

    def test_warm_cache_sync_missing_token(self):
        """Test warm_cache_sync handles missing token gracefully."""
        cache = DataCache(":memory:")

        eth_data = [
            (
                datetime(2024, 1, 1, 0, 0, 0),
                OHLCVData(
                    open=Decimal("3000"),
                    high=Decimal("3100"),
                    low=Decimal("2950"),
                    close=Decimal("3050"),
                ),
            )
        ]

        data = {"ETH": eth_data}

        # Request BTC which is not in data
        count = cache.warm_cache_sync(
            tokens=["ETH", "BTC"],  # BTC not in data
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            data=data,
            interval="1h",
        )

        assert count == 1  # Only ETH was cached
        assert cache.count() == 1

    def test_warm_cache_sync_tokens_stored_uppercase(self):
        """Test warm_cache_sync stores tokens in uppercase regardless of input case."""
        cache = DataCache(":memory:")

        eth_data = [
            (
                datetime(2024, 1, 1, 0, 0, 0),
                OHLCVData(
                    open=Decimal("3000"),
                    high=Decimal("3100"),
                    low=Decimal("2950"),
                    close=Decimal("3050"),
                ),
            )
        ]

        # Provide data with uppercase key (matching what implementation expects)
        data = {"ETH": eth_data}

        # Request with lowercase token - should be converted to uppercase
        count = cache.warm_cache_sync(
            tokens=["eth"],
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            data=data,
            interval="1h",
        )

        assert count == 1
        # Verify it's stored with uppercase key
        key = CacheKey(token="ETH", timestamp=datetime(2024, 1, 1, 0, 0, 0), interval="1h")
        assert cache.get(key) is not None


class TestCacheWarmingAsync:
    """Tests for async cache warming."""

    @pytest.mark.asyncio
    async def test_warm_cache_async(self):
        """Test async cache warming with mock provider."""
        cache = DataCache(":memory:")

        # Create mock provider
        mock_provider = MagicMock()
        mock_ohlcv = MagicMock()
        mock_ohlcv.timestamp = datetime(2024, 1, 1, 0, 0, 0)
        mock_ohlcv.open = Decimal("3000")
        mock_ohlcv.high = Decimal("3100")
        mock_ohlcv.low = Decimal("2950")
        mock_ohlcv.close = Decimal("3050")
        mock_ohlcv.volume = Decimal("1000000")

        mock_provider.get_ohlcv = AsyncMock(return_value=[mock_ohlcv])

        count = await cache.warm_cache(
            tokens=["ETH"],
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            provider=mock_provider,
            interval="1h",
        )

        assert count == 1
        assert cache.count() == 1

        # Verify provider was called
        mock_provider.get_ohlcv.assert_called_once()

    @pytest.mark.asyncio
    async def test_warm_cache_async_provider_returns_empty(self):
        """Test async cache warming when provider returns empty data."""
        cache = DataCache(":memory:")

        mock_provider = MagicMock()
        mock_provider.get_ohlcv = AsyncMock(return_value=[])

        count = await cache.warm_cache(
            tokens=["ETH"],
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            provider=mock_provider,
            interval="1h",
        )

        assert count == 0
        assert cache.count() == 0

    @pytest.mark.asyncio
    async def test_warm_cache_async_provider_error(self):
        """Test async cache warming handles provider errors gracefully."""
        cache = DataCache(":memory:")

        mock_provider = MagicMock()
        mock_provider.get_ohlcv = AsyncMock(side_effect=Exception("Provider error"))

        # Should not raise, but return 0
        count = await cache.warm_cache(
            tokens=["ETH"],
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            provider=mock_provider,
            interval="1h",
        )

        assert count == 0


class TestCacheCount:
    """Tests for count operations."""

    def test_count_all(self):
        """Test count returns total entries."""
        cache = DataCache(":memory:")

        for token in ["ETH", "BTC"]:
            for i in range(3):
                key = CacheKey(
                    token=token,
                    timestamp=datetime(2024, 1, 1, i, 0, 0),
                    interval="1h",
                )
                data = OHLCVData(
                    open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
                )
                cache.set(key, data)

        assert cache.count() == 6

    def test_count_by_token(self):
        """Test count with token filter."""
        cache = DataCache(":memory:")

        for token in ["ETH", "BTC"]:
            for i in range(3):
                key = CacheKey(
                    token=token,
                    timestamp=datetime(2024, 1, 1, i, 0, 0),
                    interval="1h",
                )
                data = OHLCVData(
                    open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
                )
                cache.set(key, data)

        assert cache.count(token="ETH") == 3
        assert cache.count(token="BTC") == 3

    def test_count_by_interval(self):
        """Test count with interval filter."""
        cache = DataCache(":memory:")

        for interval in ["1h", "4h"]:
            for i in range(3):
                key = CacheKey(
                    token="ETH",
                    timestamp=datetime(2024, 1, 1, i, 0, 0),
                    interval=interval,
                )
                data = OHLCVData(
                    open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
                )
                cache.set(key, data)

        assert cache.count(interval="1h") == 3
        assert cache.count(interval="4h") == 3


class TestCacheClose:
    """Tests for cache close operation."""

    def test_close_memory_cache(self):
        """Test closing in-memory cache."""
        cache = DataCache(":memory:")
        key = CacheKey(
            token="ETH",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            interval="1h",
        )
        data = OHLCVData(
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2950"), close=Decimal("3050")
        )
        cache.set(key, data)

        cache.close()
        assert cache._memory_conn is None

    def test_close_is_idempotent(self):
        """Test closing cache multiple times is safe."""
        cache = DataCache(":memory:")
        cache.close()
        cache.close()  # Should not raise
        assert cache._memory_conn is None


class TestTTLProperty:
    """Tests for TTL property."""

    def test_get_ttl_seconds(self):
        """Test getting TTL value."""
        cache = DataCache(":memory:", ttl_seconds=3600)
        assert cache.ttl_seconds == 3600

    def test_set_ttl_seconds(self):
        """Test setting TTL value."""
        cache = DataCache(":memory:")
        cache.ttl_seconds = 7200
        assert cache.ttl_seconds == 7200


class TestDataCacheFilesystemFallback:
    """Tests for DataCache filesystem fallback when home dir is not writable."""

    def test_default_path_uses_home_dir(self, tmp_path):
        """Default (None) resolves to ~/.almanak/cache/data_cache.db."""
        fake_home = tmp_path / "home"
        expected = str(fake_home / ".almanak" / "cache" / "data_cache.db")
        with patch.object(Path, "home", return_value=fake_home):
            cache = DataCache()
        assert cache.db_path == expected

    def test_fallback_to_tmp_when_home_not_writable(self):
        """Falls back to /tmp when home directory mkdir raises OSError."""
        original_mkdir = Path.mkdir

        def selective_mkdir(self, *args, **kwargs):
            if ".almanak" in str(self) and "/tmp" not in str(self):
                raise OSError("Read-only file system")
            return original_mkdir(self, *args, **kwargs)

        with patch.object(Path, "mkdir", selective_mkdir):
            cache = DataCache()
        assert "/tmp/.almanak/cache/data_cache.db" in cache.db_path

    def test_explicit_path_bypasses_fallback(self):
        """Explicit db_path is used directly without fallback."""
        cache = DataCache(":memory:")
        assert cache.db_path == ":memory:"
