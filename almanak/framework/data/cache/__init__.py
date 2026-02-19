"""Data Cache Module.

Provides persistent SQLite caching for OHLCV candlestick data,
general-purpose data caching, and versioned historical data caching.

Features:
    - TTL-based cache invalidation
    - Cache warming for pre-loading data ranges
    - Hit rate tracking and statistics
    - Versioned historical data with finality tagging and checksum integrity
"""

from almanak.framework.data.cache.data_cache import (
    CacheKey,
    CacheStats,
    DataCache,
    OHLCVData,
)
from almanak.framework.data.cache.ohlcv_cache import OHLCVCache
from almanak.framework.data.cache.versioned_cache import (
    CacheEntry,
    VersionedDataCache,
)
from almanak.framework.data.cache.versioned_cache import (
    CacheStats as VersionedCacheStats,
)

__all__ = [
    "CacheEntry",
    "CacheKey",
    "CacheStats",
    "DataCache",
    "OHLCVCache",
    "OHLCVData",
    "VersionedCacheStats",
    "VersionedDataCache",
]
