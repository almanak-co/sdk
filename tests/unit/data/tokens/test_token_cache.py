"""Unit tests for TokenCacheManager class.

This module tests the TokenCacheManager class, covering:
- Cache key generation
- Cache write and read operations (get/put)
- Memory cache with LRU eviction
- Disk cache persistence
- Thread-safe access
- Async-safe wrapper methods
- Cache statistics tracking
"""

import asyncio
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.core.enums import Chain
from almanak.framework.data.tokens.cache import TokenCacheManager, cache_key
from almanak.framework.data.tokens.models import BridgeType, ResolvedToken


def make_resolved_token(
    symbol: str = "USDC",
    address: str = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    chain: Chain = Chain.ARBITRUM,
    decimals: int = 6,
) -> ResolvedToken:
    """Create a ResolvedToken for testing."""
    return ResolvedToken(
        symbol=symbol,
        address=address,
        decimals=decimals,
        chain=chain,
        chain_id=42161 if chain == Chain.ARBITRUM else 1,
        name=f"{symbol} Token",
        coingecko_id=symbol.lower(),
        is_stablecoin=symbol in ("USDC", "USDT", "DAI"),
        is_native=False,
        is_wrapped_native=symbol == "WETH",
        canonical_symbol=symbol,
        bridge_type=BridgeType.NATIVE,
        source="static",
        is_verified=True,
        resolved_at=datetime.now(),
    )


class TestCacheKeyFunction:
    """Tests for cache_key() function."""

    def test_cache_key_by_address(self):
        """Test cache key generation by address."""
        key = cache_key("arbitrum", address="0xABCD1234")
        assert key == "arbitrum:0xabcd1234"  # Lowercase chain and address

    def test_cache_key_by_symbol(self):
        """Test cache key generation by symbol."""
        key = cache_key("arbitrum", symbol="usdc")
        assert key == "arbitrum:USDC"  # Lowercase chain, uppercase symbol

    def test_cache_key_normalizes_chain(self):
        """Test cache key normalizes chain to lowercase."""
        key1 = cache_key("ARBITRUM", symbol="USDC")
        key2 = cache_key("arbitrum", symbol="USDC")
        assert key1 == key2 == "arbitrum:USDC"

    def test_cache_key_normalizes_address(self):
        """Test cache key normalizes address to lowercase."""
        key1 = cache_key("arbitrum", address="0xABCD")
        key2 = cache_key("arbitrum", address="0xabcd")
        assert key1 == key2 == "arbitrum:0xabcd"

    def test_cache_key_normalizes_symbol(self):
        """Test cache key normalizes symbol to uppercase."""
        key1 = cache_key("arbitrum", symbol="usdc")
        key2 = cache_key("arbitrum", symbol="USDC")
        assert key1 == key2 == "arbitrum:USDC"

    def test_cache_key_requires_address_or_symbol(self):
        """Test cache_key raises when neither address nor symbol provided."""
        with pytest.raises(ValueError, match="Must specify either address or symbol"):
            cache_key("arbitrum")

    def test_cache_key_rejects_both_address_and_symbol(self):
        """Test cache_key raises when both address and symbol provided."""
        with pytest.raises(ValueError, match="Cannot specify both address and symbol"):
            cache_key("arbitrum", address="0x1234", symbol="USDC")


class TestTokenCacheManagerInitialization:
    """Tests for TokenCacheManager initialization."""

    def test_init_default_config(self):
        """Test default configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)
            assert cache._max_size == 10000
            assert len(cache) == 0

    def test_init_custom_max_size(self):
        """Test custom max size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file, max_size=100)
            assert cache._max_size == 100

    def test_init_creates_parent_directories(self):
        """Test cache file parent directories are created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "nested" / "dir" / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)
            token = make_resolved_token()
            cache.put(token)
            assert cache_file.parent.exists()


class TestCachePutAndGet:
    """Tests for cache put and get operations."""

    def test_put_and_get_by_address(self):
        """Test storing and retrieving token by address."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            result = cache.get("arbitrum", address=token.address)
            assert result is not None
            assert result.symbol == token.symbol
            assert result.decimals == token.decimals

    def test_put_and_get_by_symbol(self):
        """Test storing and retrieving token by symbol."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            result = cache.get("arbitrum", symbol="USDC")
            assert result is not None
            assert result.address == token.address

    def test_get_returns_none_for_missing(self):
        """Test get returns None for missing token."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            result = cache.get("arbitrum", symbol="NONEXISTENT")
            assert result is None

    def test_put_updates_existing(self):
        """Test put updates existing token."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token1 = make_resolved_token(symbol="USDC", decimals=6)
            cache.put(token1)

            # Update with same address, different decimals (simulating correction)
            token2 = make_resolved_token(symbol="USDC", decimals=8)
            cache.put(token2)

            result = cache.get("arbitrum", symbol="USDC")
            assert result.decimals == 8

    def test_get_normalizes_chain_case(self):
        """Test get normalizes chain case."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            result = cache.get("ARBITRUM", symbol="USDC")
            assert result is not None

    def test_get_normalizes_symbol_case(self):
        """Test get normalizes symbol case."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            result = cache.get("arbitrum", symbol="usdc")
            assert result is not None


class TestMemoryCacheLayer:
    """Tests for memory cache behavior."""

    def test_memory_cache_hit_is_fast(self):
        """Test memory cache hit is fast (<1ms target)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            # Warm up
            cache.get("arbitrum", symbol="USDC")

            # Measure memory hit time
            start = time.perf_counter()
            for _ in range(100):
                cache.get("arbitrum", symbol="USDC")
            elapsed = (time.perf_counter() - start) / 100 * 1000  # ms per lookup

            assert elapsed < 1.0, f"Memory cache hit took {elapsed:.3f}ms, target <1ms"

    def test_memory_cache_tracks_stats(self):
        """Test memory cache tracks hit statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            # Get multiple times (should be memory hits)
            for _ in range(5):
                cache.get("arbitrum", symbol="USDC")

            stats = cache.stats()
            assert stats["memory_hits"] >= 5
            assert stats["misses"] == 0


class TestDiskCacheLayer:
    """Tests for disk cache persistence."""

    def test_disk_cache_persists_across_instances(self):
        """Test disk cache persists tokens across cache instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"

            # First instance: write token
            cache1 = TokenCacheManager(cache_file=cache_file)
            token = make_resolved_token()
            cache1.put(token)

            # Second instance: read token (should come from disk)
            cache2 = TokenCacheManager(cache_file=cache_file)
            result = cache2.get("arbitrum", symbol="USDC")

            assert result is not None
            assert result.symbol == "USDC"
            assert result.decimals == 6

    def test_disk_cache_promotes_to_memory(self):
        """Test disk cache hit promotes token to memory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"

            # Write to disk via first instance
            cache1 = TokenCacheManager(cache_file=cache_file)
            token = make_resolved_token()
            cache1.put(token)

            # New instance starts with empty memory
            cache2 = TokenCacheManager(cache_file=cache_file)
            assert len(cache2._memory) == 0

            # First get should load from disk
            cache2.get("arbitrum", symbol="USDC")

            # Now memory should have the token
            assert len(cache2._memory) > 0

            # Second get should be memory hit
            stats_before = cache2.stats()
            cache2.get("arbitrum", symbol="USDC")
            stats_after = cache2.stats()

            assert stats_after["memory_hits"] > stats_before["memory_hits"]

    def test_disk_cache_handles_corrupted_file(self):
        """Test disk cache handles corrupted JSON file gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"

            # Write corrupted JSON
            cache_file.write_text("{ invalid json }")

            # Should not raise, starts with empty cache
            cache = TokenCacheManager(cache_file=cache_file)
            assert cache.get("arbitrum", symbol="USDC") is None

    def test_flush_writes_memory_to_disk(self):
        """Test flush() writes all memory entries to disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            tokens = [
                make_resolved_token(symbol="USDC", address="0x1111"),
                make_resolved_token(symbol="WETH", address="0x2222"),
            ]
            for token in tokens:
                cache.put(token)

            cache.flush()

            # Verify disk file has data
            assert cache_file.exists()
            content = cache_file.read_text()
            assert "USDC" in content
            assert "WETH" in content


class TestLRUEviction:
    """Tests for LRU cache eviction."""

    def test_eviction_when_max_size_reached(self):
        """Test cache evicts oldest entries when max size is reached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file, max_size=5)

            # Add 10 tokens (each token creates 2 entries: by address and by symbol)
            # With max_size=5, we expect eviction
            for i in range(10):
                token = make_resolved_token(
                    symbol=f"TOKEN{i}",
                    address=f"0x{i:040x}",
                )
                cache.put(token)

            # Memory should not exceed max_size
            assert len(cache._memory) <= 5

    def test_lru_evicts_least_recently_used(self):
        """Test LRU evicts least recently used entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file, max_size=4)

            # Add 2 tokens (4 entries total: 2 by address, 2 by symbol)
            token1 = make_resolved_token(symbol="TOKEN1", address="0x1111")
            token2 = make_resolved_token(symbol="TOKEN2", address="0x2222")
            cache.put(token1)
            cache.put(token2)

            # Access token1 to make it more recently used
            cache.get("arbitrum", symbol="TOKEN1")

            # Add more tokens to trigger eviction
            token3 = make_resolved_token(symbol="TOKEN3", address="0x3333")
            cache.put(token3)

            # token2 should be evicted first (least recently used in memory)
            # But it should still be in disk cache
            stats = cache.stats()
            assert stats["evictions"] > 0

    def test_eviction_stats_tracking(self):
        """Test eviction statistics are tracked."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file, max_size=2)

            # Add tokens to force evictions
            for i in range(5):
                token = make_resolved_token(
                    symbol=f"TOKEN{i}",
                    address=f"0x{i:040x}",
                )
                cache.put(token)

            stats = cache.stats()
            assert stats["evictions"] > 0


class TestThreadSafety:
    """Tests for thread-safe access."""

    def test_concurrent_reads(self):
        """Test concurrent read access from multiple threads."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            results = []
            errors = []

            def reader():
                try:
                    for _ in range(100):
                        result = cache.get("arbitrum", symbol="USDC")
                        results.append(result is not None)
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=reader) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not errors
            assert all(results)

    def test_concurrent_writes(self):
        """Test concurrent write access from multiple threads."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            errors = []

            def writer(thread_id: int):
                try:
                    for i in range(10):
                        token = make_resolved_token(
                            symbol=f"TOKEN{thread_id}_{i}",
                            address=f"0x{thread_id:020x}{i:020x}",
                        )
                        cache.put(token)
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not errors

    def test_concurrent_read_write(self):
        """Test concurrent read and write access."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            errors = []
            stop_event = threading.Event()

            def writer():
                i = 0
                while not stop_event.is_set():
                    try:
                        token = make_resolved_token(
                            symbol=f"TOKEN{i}",
                            address=f"0x{i:040x}",
                        )
                        cache.put(token)
                        i += 1
                    except Exception as e:
                        errors.append(e)

            def reader():
                while not stop_event.is_set():
                    try:
                        cache.get("arbitrum", symbol="TOKEN0")
                    except Exception as e:
                        errors.append(e)

            write_thread = threading.Thread(target=writer)
            read_threads = [threading.Thread(target=reader) for _ in range(3)]

            write_thread.start()
            for t in read_threads:
                t.start()

            time.sleep(0.5)  # Run for 500ms
            stop_event.set()

            write_thread.join()
            for t in read_threads:
                t.join()

            assert not errors


class TestAsyncSafeWrappers:
    """Tests for async-safe wrapper methods."""

    @pytest.mark.asyncio
    async def test_get_async(self):
        """Test async get operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            result = await cache.get_async("arbitrum", symbol="USDC")
            assert result is not None
            assert result.symbol == "USDC"

    @pytest.mark.asyncio
    async def test_put_async(self):
        """Test async put operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            await cache.put_async(token)

            result = cache.get("arbitrum", symbol="USDC")
            assert result is not None

    @pytest.mark.asyncio
    async def test_remove_async(self):
        """Test async remove operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            removed = await cache.remove_async("arbitrum", symbol="USDC")
            assert removed is True

            result = cache.get("arbitrum", symbol="USDC")
            assert result is None

    @pytest.mark.asyncio
    async def test_concurrent_async_access(self):
        """Test concurrent async access."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            async def reader():
                for _ in range(10):
                    await cache.get_async("arbitrum", symbol="USDC")

            # Run 5 concurrent readers
            await asyncio.gather(*[reader() for _ in range(5)])


class TestCacheRemoveAndClear:
    """Tests for remove and clear operations."""

    def test_remove_by_symbol(self):
        """Test removing token by symbol."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            removed = cache.remove("arbitrum", symbol="USDC")
            assert removed is True

            result = cache.get("arbitrum", symbol="USDC")
            assert result is None

    def test_remove_by_address(self):
        """Test removing token by address."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            removed = cache.remove("arbitrum", address=token.address)
            assert removed is True

    def test_remove_nonexistent(self):
        """Test removing nonexistent token returns False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            removed = cache.remove("arbitrum", symbol="NONEXISTENT")
            assert removed is False

    def test_clear(self):
        """Test clearing all cache entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            # Add multiple tokens
            for i in range(5):
                token = make_resolved_token(
                    symbol=f"TOKEN{i}",
                    address=f"0x{i:040x}",
                )
                cache.put(token)

            cache.clear()

            assert len(cache) == 0
            assert cache.size()[0] == 0  # memory
            assert cache.size()[1] == 0  # disk


class TestCacheStatistics:
    """Tests for cache statistics."""

    def test_stats_initial_values(self):
        """Test initial statistics values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            stats = cache.stats()
            assert stats["memory_hits"] == 0
            assert stats["disk_hits"] == 0
            assert stats["misses"] == 0
            assert stats["evictions"] == 0

    def test_stats_memory_hits(self):
        """Test memory hit statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            # Multiple gets should increment memory hits
            for _ in range(5):
                cache.get("arbitrum", symbol="USDC")

            stats = cache.stats()
            assert stats["memory_hits"] >= 5

    def test_stats_misses(self):
        """Test miss statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            for _ in range(3):
                cache.get("arbitrum", symbol="NONEXISTENT")

            stats = cache.stats()
            assert stats["misses"] == 3

    def test_size_returns_memory_and_disk_counts(self):
        """Test size() returns both memory and disk counts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            memory_size, disk_size = cache.size()
            assert memory_size > 0
            assert disk_size > 0


class TestCacheContains:
    """Tests for __contains__ and __len__ methods."""

    def test_len_returns_memory_size(self):
        """Test __len__ returns memory cache size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            assert len(cache) == 0

            token = make_resolved_token()
            cache.put(token)

            # put() creates entries for both address and symbol
            assert len(cache) == 2

    def test_contains_checks_memory(self):
        """Test __contains__ checks memory cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            token = make_resolved_token()
            cache.put(token)

            key = cache_key("arbitrum", symbol="USDC")
            assert key in cache


class TestCacheKeyMethod:
    """Tests for instance cache_key() method."""

    def test_instance_cache_key_method(self):
        """Test instance method delegates to module function."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = Path(tmpdir) / "token_cache.json"
            cache = TokenCacheManager(cache_file=cache_file)

            key1 = cache.cache_key("arbitrum", symbol="USDC")
            key2 = cache_key("arbitrum", symbol="USDC")

            assert key1 == key2


class TestResolveCacheFile:
    """Tests for TokenCacheManager._resolve_cache_file fallback logic."""

    def test_none_returns_home_dir_when_writable(self):
        """Default (None) resolves to ~/.almanak/token_cache.json when writable."""
        result = TokenCacheManager._resolve_cache_file(None)
        expected = Path(TokenCacheManager.DEFAULT_CACHE_FILE).expanduser()
        assert result == expected

    def test_none_falls_back_to_tmp_when_home_not_writable(self):
        """Falls back to /tmp when home directory mkdir raises OSError."""
        with patch.object(Path, "mkdir", side_effect=OSError("Read-only file system")):
            result = TokenCacheManager._resolve_cache_file(None)
        assert result == Path("/tmp/.almanak/token_cache.json")

    def test_explicit_path_returned_as_is(self):
        """Explicit cache_file is expanded and returned directly."""
        result = TokenCacheManager._resolve_cache_file("/custom/path/cache.json")
        assert result == Path("/custom/path/cache.json")

    def test_explicit_tilde_path_is_expanded(self):
        """Explicit cache_file with ~ is expanded."""
        result = TokenCacheManager._resolve_cache_file("~/my_cache.json")
        assert result == Path("~/my_cache.json").expanduser()
