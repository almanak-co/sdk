"""Token cache with disk persistence for fast lookups.

This module provides a caching layer for token metadata with both
memory (in-process) and disk (JSON file) persistence. The cache
uses an LRU eviction policy and is thread-safe for concurrent access.

Key Components:
    - TokenCacheManager: Main cache class with memory and disk layers
    - cache_key(): Generate consistent cache keys from chain/address/symbol

Performance Targets:
    - Cache hit lookup: <1ms
    - Disk lookup: <10ms

Example:
    from almanak.framework.data.tokens.cache import TokenCacheManager
    from almanak.framework.data.tokens.models import ResolvedToken

    # Create cache with custom location
    cache = TokenCacheManager(cache_file="~/.almanak/token_cache.json")

    # Store a token
    cache.put(resolved_token)

    # Retrieve by address
    token = cache.get("arbitrum", address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

    # Retrieve by symbol
    token = cache.get("arbitrum", symbol="USDC")
"""

import asyncio
import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any

from almanak.framework.data.tokens.models import ResolvedToken

logger = logging.getLogger(__name__)


def cache_key(chain: str, *, address: str | None = None, symbol: str | None = None) -> str:
    """Generate a consistent cache key from chain and identifier.

    Keys are formatted as 'chain:identifier' where:
    - For addresses: chain:address_lower (e.g., "arbitrum:0xaf88...")
    - For symbols: chain:SYMBOL_UPPER (e.g., "arbitrum:USDC")

    Args:
        chain: Chain name (e.g., "arbitrum", "ethereum")
        address: Token contract address (mutually exclusive with symbol)
        symbol: Token symbol (mutually exclusive with address)

    Returns:
        Cache key string

    Raises:
        ValueError: If neither or both address and symbol are provided

    Example:
        key = cache_key("arbitrum", address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        # Returns: "arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831"

        key = cache_key("arbitrum", symbol="USDC")
        # Returns: "arbitrum:USDC"
    """
    if address is not None and symbol is not None:
        raise ValueError("Cannot specify both address and symbol")
    if address is None and symbol is None:
        raise ValueError("Must specify either address or symbol")

    chain_lower = chain.lower()
    if address is not None:
        return f"{chain_lower}:{address.lower()}"
    else:
        return f"{chain_lower}:{symbol.upper()}"  # type: ignore[union-attr]


class TokenCacheManager:
    """Token cache with memory and disk persistence layers.

    This cache provides fast lookups for resolved tokens with automatic
    persistence to disk. It uses an LRU (Least Recently Used) eviction
    policy when the cache reaches its size limit.

    Resolution order for lookups:
    1. Memory cache (fastest, O(1))
    2. Disk cache (loads from JSON file, promotes to memory on hit)

    Thread Safety:
    - Uses threading.RLock for synchronous access
    - Provides async-safe wrapper methods using asyncio.Lock

    Attributes:
        cache_file: Path to the disk cache JSON file
        max_size: Maximum number of entries (default 10000)

    Example:
        cache = TokenCacheManager()

        # Store tokens
        cache.put(usdc_token)
        cache.put(weth_token)

        # Retrieve tokens
        token = cache.get("arbitrum", address="0x...")
        if token:
            print(f"Found {token.symbol} with {token.decimals} decimals")

        # Force persistence
        cache.flush()
    """

    DEFAULT_CACHE_FILE = "~/.almanak/token_cache.json"
    DEFAULT_MAX_SIZE = 10000

    def __init__(
        self,
        cache_file: str | Path | None = None,
        max_size: int = DEFAULT_MAX_SIZE,
    ) -> None:
        """Initialize the token cache.

        Args:
            cache_file: Path to disk cache file. Defaults to ~/.almanak/token_cache.json
            max_size: Maximum cache entries (default 10000). Uses LRU eviction when full.
        """
        self._cache_file = Path(cache_file or self.DEFAULT_CACHE_FILE).expanduser()
        self._max_size = max_size

        # Memory cache using OrderedDict for LRU ordering
        self._memory: OrderedDict[str, ResolvedToken] = OrderedDict()

        # Thread safety
        self._lock = threading.RLock()
        self._async_lock: asyncio.Lock | None = None

        # Disk cache state
        self._disk_loaded = False
        self._disk_cache: dict[str, dict[str, Any]] = {}

        # Performance tracking
        self._stats = {
            "memory_hits": 0,
            "disk_hits": 0,
            "misses": 0,
            "evictions": 0,
        }

    def _ensure_disk_loaded(self) -> None:
        """Load disk cache if not already loaded. Must be called with lock held."""
        if self._disk_loaded:
            return

        try:
            if self._cache_file.exists():
                with self._cache_file.open("r") as f:
                    data = json.load(f)
                    self._disk_cache = data.get("tokens", {})
                    logger.debug(f"Loaded {len(self._disk_cache)} tokens from disk cache")
            else:
                self._disk_cache = {}
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load disk cache: {e}. Starting with empty cache.")
            self._disk_cache = {}

        self._disk_loaded = True

    def _write_disk_cache(self) -> None:
        """Write disk cache to file. Must be called with lock held."""
        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            with self._cache_file.open("w") as f:
                json.dump(
                    {
                        "version": 1,
                        "updated_at": datetime.now().isoformat(),
                        "tokens": self._disk_cache,
                    },
                    f,
                    indent=2,
                )
        except OSError as e:
            logger.warning(f"Failed to write disk cache: {e}")

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if cache exceeds max size. Must be called with lock held."""
        while len(self._memory) >= self._max_size:
            # Pop oldest item (first item in OrderedDict)
            evicted_key, _ = self._memory.popitem(last=False)
            self._stats["evictions"] += 1
            logger.debug(f"Evicted token from cache: {evicted_key}")

    def cache_key(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> str:
        """Generate cache key. Convenience wrapper around module-level cache_key()."""
        return cache_key(chain, address=address, symbol=symbol)

    def get(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> ResolvedToken | None:
        """Get a token from cache by chain and address or symbol.

        Checks memory cache first, then disk cache. On disk hit,
        promotes the token to memory cache.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
            address: Token contract address
            symbol: Token symbol

        Returns:
            ResolvedToken if found, None otherwise

        Example:
            # Get by address
            token = cache.get("arbitrum", address="0xaf88...")

            # Get by symbol
            token = cache.get("arbitrum", symbol="USDC")
        """
        key = cache_key(chain, address=address, symbol=symbol)

        with self._lock:
            # Check memory cache first
            if key in self._memory:
                # Move to end for LRU ordering
                self._memory.move_to_end(key)
                self._stats["memory_hits"] += 1
                return self._memory[key]

            # Check disk cache
            self._ensure_disk_loaded()
            if key in self._disk_cache:
                start_time = time.perf_counter()
                try:
                    token = ResolvedToken.from_dict(self._disk_cache[key])
                    # Promote to memory
                    self._evict_if_needed()
                    self._memory[key] = token
                    self._stats["disk_hits"] += 1

                    elapsed_ms = (time.perf_counter() - start_time) * 1000
                    if elapsed_ms > 10:
                        logger.debug(f"Disk cache lookup took {elapsed_ms:.2f}ms for {key}")

                    return token
                except (KeyError, ValueError) as e:
                    logger.warning(f"Failed to deserialize cached token {key}: {e}")
                    # Remove corrupted entry
                    del self._disk_cache[key]
                    return None

            self._stats["misses"] += 1
            return None

    def put(self, token: ResolvedToken) -> None:
        """Store a token in both memory and disk cache.

        Creates cache entries for both address and symbol lookups.

        Args:
            token: ResolvedToken to cache

        Example:
            cache.put(resolved_usdc_token)
        """
        with self._lock:
            self._ensure_disk_loaded()

            # Create keys for both address and symbol lookups
            address_key = cache_key(token.chain.value, address=token.address)
            symbol_key = cache_key(token.chain.value, symbol=token.symbol)

            # Serialize token
            token_dict = token.to_dict()

            # Store in memory (with LRU eviction)
            self._evict_if_needed()
            self._memory[address_key] = token
            self._memory.move_to_end(address_key)

            if symbol_key != address_key:
                self._evict_if_needed()
                self._memory[symbol_key] = token
                self._memory.move_to_end(symbol_key)

            # Store in disk cache
            self._disk_cache[address_key] = token_dict
            if symbol_key != address_key:
                self._disk_cache[symbol_key] = token_dict

            # Write to disk
            self._write_disk_cache()

    def remove(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> bool:
        """Remove a token from both memory and disk cache.

        Args:
            chain: Chain name
            address: Token contract address
            symbol: Token symbol

        Returns:
            True if token was found and removed, False otherwise
        """
        key = cache_key(chain, address=address, symbol=symbol)

        with self._lock:
            self._ensure_disk_loaded()

            removed = False
            if key in self._memory:
                del self._memory[key]
                removed = True

            if key in self._disk_cache:
                del self._disk_cache[key]
                self._write_disk_cache()
                removed = True

            return removed

    def clear(self) -> None:
        """Clear both memory and disk cache."""
        with self._lock:
            self._memory.clear()
            self._disk_cache.clear()
            self._disk_loaded = True  # Mark as loaded (empty)
            self._write_disk_cache()
            self._stats = {
                "memory_hits": 0,
                "disk_hits": 0,
                "misses": 0,
                "evictions": 0,
            }

    def flush(self) -> None:
        """Force write memory cache to disk.

        Useful for ensuring persistence before shutdown.
        """
        with self._lock:
            self._ensure_disk_loaded()
            # Sync all memory entries to disk
            for key, token in self._memory.items():
                self._disk_cache[key] = token.to_dict()
            self._write_disk_cache()

    def size(self) -> tuple[int, int]:
        """Get the number of entries in memory and disk cache.

        Returns:
            Tuple of (memory_size, disk_size)
        """
        with self._lock:
            self._ensure_disk_loaded()
            return len(self._memory), len(self._disk_cache)

    def stats(self) -> dict[str, int]:
        """Get cache performance statistics.

        Returns:
            Dict with memory_hits, disk_hits, misses, evictions
        """
        with self._lock:
            return dict(self._stats)

    # Async-safe wrapper methods

    async def _get_async_lock(self) -> asyncio.Lock:
        """Get or create async lock. Lazy initialization for event loop compatibility."""
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    async def get_async(
        self, chain: str, *, address: str | None = None, symbol: str | None = None
    ) -> ResolvedToken | None:
        """Async-safe version of get().

        Args:
            chain: Chain name
            address: Token contract address
            symbol: Token symbol

        Returns:
            ResolvedToken if found, None otherwise
        """
        lock = await self._get_async_lock()
        async with lock:
            # Run synchronous get in thread pool to avoid blocking event loop
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: self.get(chain, address=address, symbol=symbol))

    async def put_async(self, token: ResolvedToken) -> None:
        """Async-safe version of put().

        Args:
            token: ResolvedToken to cache
        """
        lock = await self._get_async_lock()
        async with lock:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self.put(token))

    async def remove_async(self, chain: str, *, address: str | None = None, symbol: str | None = None) -> bool:
        """Async-safe version of remove().

        Args:
            chain: Chain name
            address: Token contract address
            symbol: Token symbol

        Returns:
            True if token was found and removed, False otherwise
        """
        lock = await self._get_async_lock()
        async with lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: self.remove(chain, address=address, symbol=symbol))

    def __len__(self) -> int:
        """Return the number of entries in memory cache."""
        with self._lock:
            return len(self._memory)

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in memory cache."""
        with self._lock:
            return key in self._memory


__all__ = [
    "cache_key",
    "TokenCacheManager",
]
