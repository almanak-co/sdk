"""Historical data loading utilities for backtesting.

This module provides utilities for pre-fetching and caching historical data
from various sources (Chainlink oracles, DEX subgraphs, etc.) to support
providers that require PRE_CACHE capability.
"""

from .historical_loader import (
    APYSnapshot,
    CacheMetadata,
    ChainlinkRoundData,
    DataCoverageReport,
    DataGap,
    HistoricalDataLoader,
    PoolVolumeSnapshot,
)

__all__ = [
    "APYSnapshot",
    "CacheMetadata",
    "ChainlinkRoundData",
    "DataCoverageReport",
    "DataGap",
    "HistoricalDataLoader",
    "PoolVolumeSnapshot",
]
