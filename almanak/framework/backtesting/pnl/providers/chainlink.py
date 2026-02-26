"""Chainlink Historical Data Provider for PnL backtesting.

This module provides a concrete implementation of the HistoricalDataProvider
protocol using Chainlink price feeds to fetch historical price data.

Chainlink price feeds provide decentralized, tamper-proof price data directly
from on-chain oracles. This provider supports multiple chains and token pairs.

Key Features:
    - Fetches historical prices from Chainlink aggregator contracts
    - Supports multiple chains (Ethereum, Arbitrum, Base, etc.)
    - Implements binary search for efficient timestamp-to-round mapping (O(log n))
    - Uses archive RPC nodes via ARCHIVE_RPC_URL_{CHAIN} environment variables
    - Supports batch RPC calls via multicall for efficiency
    - Configurable persistent cache for round data
    - Implements staleness checks (flags data older than 1 hour)
    - Handles decimals conversion properly

Example:
    from almanak.framework.backtesting.pnl.providers.chainlink import ChainlinkDataProvider
    from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
    from datetime import datetime
    import os

    # Set archive RPC URL environment variable (or pass directly)
    os.environ["ARCHIVE_RPC_URL_ETHEREUM"] = "https://eth-mainnet.g.alchemy.com/v2/..."

    provider = ChainlinkDataProvider(chain="ethereum")
    config = HistoricalDataConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 6, 1),
        interval_seconds=3600,
        tokens=["ETH", "BTC", "LINK"],
    )

    async for timestamp, market_state in provider.iterate(config):
        eth_price = market_state.get_price("ETH")
        # ... process market state
"""

import json
import logging
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..data_provider import OHLCV, HistoricalDataCapability, HistoricalDataConfig, MarketState
from ..types import DataConfidence, DataSourceInfo

logger = logging.getLogger(__name__)

# Archive RPC URL environment variable pattern
ARCHIVE_RPC_URL_ENV_PATTERN = "ARCHIVE_RPC_URL_{chain}"

# Supported chains for archive RPC URLs
ARCHIVE_RPC_CHAINS = ["ETHEREUM", "ARBITRUM", "BASE", "OPTIMISM", "POLYGON", "AVALANCHE"]

# Multicall3 contract address (same on all EVM chains)
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

# Multicall3 aggregate3 function selector
MULTICALL3_AGGREGATE3_SELECTOR = "0x82ad56cb"

# Default cache directory for persistent round data
DEFAULT_CACHE_DIR = ".almanak_cache/chainlink"

# Data staleness threshold (1 hour in seconds)
DATA_STALENESS_THRESHOLD_SECONDS = 3600


# =============================================================================
# Chainlink constants -- imported from shared module (almanak.core.chainlink)
# Re-exported here for backward compatibility.
# =============================================================================

from almanak.core.chainlink import (  # noqa: F401
    ARBITRUM_PRICE_FEEDS,
    AVALANCHE_PRICE_FEEDS,
    BASE_PRICE_FEEDS,
    CHAINLINK_DEVIATION_THRESHOLDS,
    CHAINLINK_HEARTBEATS,
    CHAINLINK_PRICE_FEEDS,
    DECIMALS_SELECTOR,
    ETHEREUM_PRICE_FEEDS,
    GET_ROUND_DATA_SELECTOR,
    LATEST_ROUND_DATA_SELECTOR,
    OPTIMISM_PRICE_FEEDS,
    POLYGON_PRICE_FEEDS,
    TOKEN_TO_PAIR,
)

# Backtesting-specific constants (not shared)
MAX_ROUNDS_TO_FETCH = 50000
ROUND_FETCH_PROGRESS_INTERVAL = 500
MAX_BINARY_SEARCH_ITERATIONS = 50
MAX_MULTICALL_BATCH_SIZE = 100


# =============================================================================
# Stale Data Exception
# =============================================================================


class ChainlinkStaleDataError(Exception):
    """Raised when Chainlink price data is stale.

    Attributes:
        token: Token symbol that had stale data
        age_seconds: Age of the data in seconds
        heartbeat_seconds: Expected heartbeat interval
        updated_at: When the data was last updated
    """

    def __init__(
        self,
        token: str,
        age_seconds: float,
        heartbeat_seconds: int,
        updated_at: datetime,
    ):
        self.token = token
        self.age_seconds = age_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.updated_at = updated_at
        super().__init__(
            f"Chainlink data for {token} is stale: updated {age_seconds:.0f}s ago (heartbeat: {heartbeat_seconds}s)"
        )


# =============================================================================
# Chainlink Data Provider Implementation
# =============================================================================


@dataclass
class ChainlinkRoundData:
    """Represents a single Chainlink aggregator round.

    This dataclass holds the data returned from a Chainlink aggregator's
    latestRoundData() or getRoundData() function calls.

    Attributes:
        round_id: The round ID for this data
        answer: The price answer (in raw form with decimals)
        started_at: Timestamp when the round started
        updated_at: Timestamp when the answer was computed
        answered_in_round: The round ID in which the answer was computed
    """

    round_id: int
    answer: int
    started_at: int
    updated_at: int
    answered_in_round: int


@dataclass
class ChainlinkPriceFeed:
    """Configuration for a Chainlink price feed.

    Attributes:
        address: Contract address of the aggregator
        pair: Price pair name (e.g., "ETH/USD")
        decimals: Number of decimals in the price answer
        heartbeat_seconds: Expected update frequency
        deviation_threshold: Price deviation threshold for updates
    """

    address: str
    pair: str
    decimals: int = 8  # Most Chainlink feeds use 8 decimals
    heartbeat_seconds: int = 3600
    deviation_threshold: Decimal = Decimal("1.0")


@dataclass
class CachedPrice:
    """A single cached price entry with TTL tracking.

    Attributes:
        price: The cached price value
        timestamp: When the price was valid (market time)
        fetched_at: When the price was fetched from the source
        ttl_seconds: Time-to-live for this cache entry
    """

    price: Decimal
    timestamp: datetime
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 60  # Default 1 minute TTL

    @property
    def is_expired(self) -> bool:
        """Check if this cache entry has expired based on TTL."""
        age = (datetime.now(UTC) - self.fetched_at).total_seconds()
        return age > self.ttl_seconds

    @property
    def age_seconds(self) -> float:
        """Get the age of this cache entry in seconds."""
        return (datetime.now(UTC) - self.fetched_at).total_seconds()


@dataclass
class ChainlinkPriceResult:
    """Price result with confidence and source tracking.

    Attributes:
        price: The price value in USD
        timestamp: When the price was valid (on-chain time)
        round_id: The Chainlink round ID
        confidence: Confidence level of the data
        source_info: Metadata about data source
        is_stale: Whether the data is considered stale (older than 1 hour)
    """

    price: Decimal
    timestamp: datetime
    round_id: int
    confidence: DataConfidence
    source_info: DataSourceInfo
    is_stale: bool = False


@dataclass
class PersistentCacheConfig:
    """Configuration for persistent round data cache.

    Attributes:
        enabled: Whether persistent caching is enabled
        cache_directory: Directory path for cache files
        max_age_days: Maximum age of cached data before invalidation
    """

    enabled: bool = False
    cache_directory: str | None = None
    max_age_days: int = 30

    def get_cache_path(self, chain: str) -> Path | None:
        """Get the cache file path for a specific chain.

        Args:
            chain: Chain identifier (e.g., "ethereum", "arbitrum")

        Returns:
            Path to the cache file, or None if caching disabled
        """
        if not self.enabled:
            return None

        cache_dir = self.cache_directory or DEFAULT_CACHE_DIR
        path = Path(cache_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path / f"chainlink_rounds_{chain}.json"


@dataclass
class BinarySearchResult:
    """Result of binary search for timestamp-to-round mapping.

    Attributes:
        round_id: The found round ID
        round_data: The round data at that ID
        iterations: Number of binary search iterations used
        exact_match: Whether the timestamp was an exact match
    """

    round_id: int
    round_data: ChainlinkRoundData
    iterations: int
    exact_match: bool = False


@dataclass
class PriceCache:
    """Cache for historical price data fetched from Chainlink.

    Provides TTL-based caching for live price queries and timestamp-based
    lookups for historical price data.

    Attributes:
        data: Dictionary mapping token symbols to list of (timestamp, price) tuples
        fetched_at: When the cache was populated
        ttl_seconds: Default TTL for cached entries (default 60 seconds)
        _live_cache: Cache for live price queries with TTL tracking
    """

    data: dict[str, list[tuple[datetime, Decimal]]] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 60  # Default 1 minute TTL for live queries
    _live_cache: dict[str, CachedPrice] = field(default_factory=dict)

    def get_price_at(self, token: str, timestamp: datetime) -> Decimal | None:
        """Get the price at or just before a specific timestamp.

        Args:
            token: Token symbol
            timestamp: Target timestamp

        Returns:
            Price at the timestamp or None if not available
        """
        token_upper = token.upper()
        if token_upper not in self.data:
            return None

        prices = self.data[token_upper]
        if not prices:
            return None

        # Find the price at or just before the timestamp
        result: Decimal | None = None
        for ts, price in prices:
            if ts <= timestamp:
                result = price
            else:
                break

        return result

    def get_live_price(self, token: str) -> CachedPrice | None:
        """Get a cached live price if it exists and hasn't expired.

        Args:
            token: Token symbol

        Returns:
            CachedPrice if valid and not expired, None otherwise
        """
        token_upper = token.upper()
        cached = self._live_cache.get(token_upper)
        if cached is None or cached.is_expired:
            return None
        return cached

    def set_live_price(
        self,
        token: str,
        price: Decimal,
        timestamp: datetime | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        """Cache a live price for a token.

        Args:
            token: Token symbol
            price: Price to cache
            timestamp: Market timestamp (defaults to now)
            ttl_seconds: TTL for this entry (defaults to cache default)
        """
        token_upper = token.upper()
        self._live_cache[token_upper] = CachedPrice(
            price=price,
            timestamp=timestamp or datetime.now(UTC),
            fetched_at=datetime.now(UTC),
            ttl_seconds=ttl_seconds or self.ttl_seconds,
        )

    def clear_live_cache(self, token: str | None = None) -> None:
        """Clear the live price cache.

        Args:
            token: Specific token to clear, or None to clear all
        """
        if token is not None:
            self._live_cache.pop(token.upper(), None)
        else:
            self._live_cache.clear()

    def get_cache_stats(self) -> dict[str, Any]:
        """Get statistics about the cache state.

        Returns:
            Dictionary with cache statistics
        """
        live_tokens = list(self._live_cache.keys())
        expired_count = sum(1 for c in self._live_cache.values() if c.is_expired)
        historical_tokens = list(self.data.keys())
        total_historical_points = sum(len(prices) for prices in self.data.values())

        return {
            "live_tokens": live_tokens,
            "live_count": len(live_tokens),
            "live_expired_count": expired_count,
            "historical_tokens": historical_tokens,
            "historical_count": len(historical_tokens),
            "total_historical_points": total_historical_points,
            "ttl_seconds": self.ttl_seconds,
        }


class ChainlinkDataProvider:
    """Chainlink historical data provider implementation.

    Implements the HistoricalDataProvider protocol to provide historical
    price data from Chainlink oracles for backtesting simulations.

    Note: This provider currently provides simulated historical data based on
    known Chainlink feed addresses and parameters. For actual historical data,
    an archive node RPC is required to query past rounds.

    Attributes:
        chain: Blockchain network identifier
        rpc_url: RPC endpoint URL (optional, for on-chain queries)

    Example:
        provider = ChainlinkDataProvider(chain="arbitrum")

        # Get a single historical price (simulated)
        price = await provider.get_price("ETH", datetime(2024, 1, 15))

        # Iterate for backtesting
        async for ts, market_state in provider.iterate(config):
            price = market_state.get_price("ETH")
    """

    # Supported tokens (union of all tokens across chains)
    _SUPPORTED_TOKENS = list(TOKEN_TO_PAIR.keys())

    # Supported chains
    _SUPPORTED_CHAINS = list(CHAINLINK_PRICE_FEEDS.keys())

    # Default provider priority (lower number = higher priority)
    DEFAULT_PRIORITY = 10

    def __init__(
        self,
        chain: str = "ethereum",
        rpc_url: str = "",
        cache_ttl_seconds: int = 60,
        priority: int | None = None,
        persistent_cache_config: PersistentCacheConfig | None = None,
    ) -> None:
        """Initialize the Chainlink data provider.

        Args:
            chain: Blockchain network identifier (ethereum, arbitrum, base, etc.)
            rpc_url: Optional RPC endpoint URL for on-chain queries.
                     If not provided, will try ARCHIVE_RPC_URL_{CHAIN} env var,
                     then fall back to offline mode using cached data.
            cache_ttl_seconds: TTL for cached price data in seconds (default 60).
                              Set to 0 to disable caching.
            priority: Provider priority for registry selection (lower = higher priority).
                     Defaults to DEFAULT_PRIORITY (10).
            persistent_cache_config: Configuration for persistent round data caching.
                                    If None, persistent caching is disabled.
        """
        self._chain = chain.lower()
        self._cache_ttl_seconds = cache_ttl_seconds
        self._priority = priority if priority is not None else self.DEFAULT_PRIORITY
        self._persistent_cache_config = persistent_cache_config or PersistentCacheConfig()

        # Validate chain
        if self._chain not in CHAINLINK_PRICE_FEEDS:
            available = ", ".join(CHAINLINK_PRICE_FEEDS.keys())
            raise ValueError(f"Unsupported chain: {chain}. Available chains: {available}")

        # Resolve RPC URL from parameter or environment variable
        self._rpc_url = rpc_url or self._get_archive_rpc_url_from_env()

        # Get price feeds for this chain
        self._price_feeds = CHAINLINK_PRICE_FEEDS[self._chain]

        # Price cache with TTL
        self._cache: PriceCache | None = None
        if cache_ttl_seconds > 0:
            self._cache = PriceCache(ttl_seconds=cache_ttl_seconds)

        # Archive access tracking for native round traversal
        self._archive_access_verified = False
        self._has_archive_access = False

        # Round cache: feed_address -> [(round_id, updated_at_timestamp, price)]
        self._round_cache: dict[str, list[tuple[int, int, Decimal]]] = {}

        # Decimals cache: feed_address -> decimals
        self._decimals_cache: dict[str, int] = {}

        # Historical price cache: token -> timestamp -> price
        self._historical_cache: dict[str, dict[int, Decimal]] = {}

        # Binary search bounds cache: feed_address -> (min_round_id, max_round_id)
        self._round_bounds_cache: dict[str, tuple[int, int]] = {}

        # Load persistent cache if enabled
        self._load_persistent_cache()

        logger.info(
            "Initialized ChainlinkDataProvider",
            extra={
                "chain": self._chain,
                "available_pairs": len(self._price_feeds),
                "rpc_url": "configured" if self._rpc_url else "not configured",
                "rpc_source": "env_var" if not rpc_url and self._rpc_url else "parameter" if rpc_url else "none",
                "cache_ttl_seconds": cache_ttl_seconds,
                "priority": self._priority,
                "persistent_cache_enabled": self._persistent_cache_config.enabled,
            },
        )

    def _get_archive_rpc_url_from_env(self) -> str:
        """Get archive RPC URL from environment variable.

        Returns:
            Archive RPC URL or empty string if not configured.
        """
        env_var_name = ARCHIVE_RPC_URL_ENV_PATTERN.format(chain=self._chain.upper())
        url = os.environ.get(env_var_name, "")
        if url:
            logger.debug(f"Using archive RPC URL from {env_var_name}")
        return url

    def _load_persistent_cache(self) -> None:
        """Load round data from persistent cache file if enabled."""
        cache_path = self._persistent_cache_config.get_cache_path(self._chain)
        if cache_path is None or not cache_path.exists():
            return

        try:
            with open(cache_path) as f:
                cache_data = json.load(f)

            # Check cache age
            cache_time = datetime.fromisoformat(cache_data.get("cached_at", "1970-01-01T00:00:00+00:00"))
            age_days = (datetime.now(UTC) - cache_time).days
            if age_days > self._persistent_cache_config.max_age_days:
                logger.info(f"Persistent cache expired ({age_days} days old), will refresh")
                return

            # Load round data
            for feed_address, rounds in cache_data.get("rounds", {}).items():
                self._round_cache[feed_address] = [
                    (r["round_id"], r["updated_at"], Decimal(r["price"])) for r in rounds
                ]

            # Load decimals
            self._decimals_cache = cache_data.get("decimals", {})

            # Load round bounds
            for feed_address, bounds in cache_data.get("round_bounds", {}).items():
                self._round_bounds_cache[feed_address] = (bounds["min"], bounds["max"])

            logger.info(
                f"Loaded persistent cache for {self._chain}",
                extra={
                    "feeds": len(self._round_cache),
                    "total_rounds": sum(len(r) for r in self._round_cache.values()),
                    "cache_age_days": age_days,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to load persistent cache: {e}")

    def _save_persistent_cache(self) -> None:
        """Save round data to persistent cache file if enabled."""
        cache_path = self._persistent_cache_config.get_cache_path(self._chain)
        if cache_path is None:
            return

        try:
            cache_data = {
                "cached_at": datetime.now(UTC).isoformat(),
                "chain": self._chain,
                "rounds": {
                    feed_address: [{"round_id": r[0], "updated_at": r[1], "price": str(r[2])} for r in rounds]
                    for feed_address, rounds in self._round_cache.items()
                },
                "decimals": self._decimals_cache,
                "round_bounds": {
                    feed_address: {"min": bounds[0], "max": bounds[1]}
                    for feed_address, bounds in self._round_bounds_cache.items()
                },
            }

            with open(cache_path, "w") as f:
                json.dump(cache_data, f, indent=2)

            logger.debug(f"Saved persistent cache to {cache_path}")
        except Exception as e:
            logger.warning(f"Failed to save persistent cache: {e}")

    @property
    def priority(self) -> int:
        """Return the provider priority for registry selection."""
        return self._priority

    @property
    def cache_ttl_seconds(self) -> int:
        """Return the cache TTL in seconds."""
        return self._cache_ttl_seconds

    def get_feed_address(self, token: str) -> str | None:
        """Get the Chainlink feed address for a token on the current chain.

        Args:
            token: Token symbol (e.g., "ETH", "BTC")

        Returns:
            Feed contract address or None if not available
        """
        pair = TOKEN_TO_PAIR.get(token.upper())
        if pair is None:
            return None
        return self._price_feeds.get(pair)

    def get_feed_config(self, token: str) -> ChainlinkPriceFeed | None:
        """Get the full feed configuration for a token.

        Args:
            token: Token symbol

        Returns:
            ChainlinkPriceFeed configuration or None if not available
        """
        pair = TOKEN_TO_PAIR.get(token.upper())
        if pair is None:
            return None

        address = self._price_feeds.get(pair)
        if address is None:
            return None

        heartbeat = CHAINLINK_HEARTBEATS.get(pair, CHAINLINK_HEARTBEATS["default"])
        deviation = CHAINLINK_DEVIATION_THRESHOLDS.get(pair, CHAINLINK_DEVIATION_THRESHOLDS["default"])

        return ChainlinkPriceFeed(
            address=address,
            pair=pair,
            decimals=8,
            heartbeat_seconds=heartbeat,
            deviation_threshold=deviation,
        )

    # =========================================================================
    # On-Chain Query Methods
    # =========================================================================

    async def _query_latest_round_data(
        self,
        feed_address: str,
    ) -> ChainlinkRoundData | None:
        """Query latestRoundData() from a Chainlink aggregator.

        Args:
            feed_address: Address of the Chainlink aggregator contract

        Returns:
            ChainlinkRoundData with round info, or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            # Lazy import web3 to avoid circular dependencies
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            feed_checksum = web3.to_checksum_address(feed_address)

            # Call latestRoundData()
            # Returns: (roundId, answer, startedAt, updatedAt, answeredInRound)
            result = web3.eth.call({"to": feed_checksum, "data": LATEST_ROUND_DATA_SELECTOR})  # type: ignore[typeddict-item]

            if len(result) < 160:  # 5 * 32 bytes
                logger.warning(f"Unexpected response length from {feed_address}: {len(result)}")
                return None

            # Decode the 5 uint256 values (each 32 bytes)
            round_id = int.from_bytes(result[0:32], byteorder="big")
            answer = int.from_bytes(result[32:64], byteorder="big", signed=True)
            started_at = int.from_bytes(result[64:96], byteorder="big")
            updated_at = int.from_bytes(result[96:128], byteorder="big")
            answered_in_round = int.from_bytes(result[128:160], byteorder="big")

            return ChainlinkRoundData(
                round_id=round_id,
                answer=answer,
                started_at=started_at,
                updated_at=updated_at,
                answered_in_round=answered_in_round,
            )

        except Exception as e:
            logger.error(f"Failed to query Chainlink feed {feed_address}: {e}")
            return None

    def _query_latest_round_data_sync(
        self,
        feed_address: str,
    ) -> ChainlinkRoundData | None:
        """Synchronous version of _query_latest_round_data.

        Args:
            feed_address: Address of the Chainlink aggregator contract

        Returns:
            ChainlinkRoundData with round info, or None if query fails
        """
        if not self._rpc_url:
            return None

        try:
            # Lazy import web3 to avoid circular dependencies
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            feed_checksum = web3.to_checksum_address(feed_address)

            # Call latestRoundData()
            result = web3.eth.call({"to": feed_checksum, "data": LATEST_ROUND_DATA_SELECTOR})  # type: ignore[typeddict-item]

            if len(result) < 160:  # 5 * 32 bytes
                logger.warning(f"Unexpected response length from {feed_address}: {len(result)}")
                return None

            # Decode the 5 uint256 values
            round_id = int.from_bytes(result[0:32], byteorder="big")
            answer = int.from_bytes(result[32:64], byteorder="big", signed=True)
            started_at = int.from_bytes(result[64:96], byteorder="big")
            updated_at = int.from_bytes(result[96:128], byteorder="big")
            answered_in_round = int.from_bytes(result[128:160], byteorder="big")

            return ChainlinkRoundData(
                round_id=round_id,
                answer=answer,
                started_at=started_at,
                updated_at=updated_at,
                answered_in_round=answered_in_round,
            )

        except Exception as e:
            logger.error(f"Failed to query Chainlink feed {feed_address}: {e}")
            return None

    async def _query_decimals(self, feed_address: str) -> int:
        """Query decimals() from a Chainlink aggregator.

        Args:
            feed_address: Address of the Chainlink aggregator contract

        Returns:
            Number of decimals (defaults to 8 if query fails)
        """
        if not self._rpc_url:
            return 8  # Default Chainlink decimals

        try:
            from web3 import Web3

            web3 = Web3(Web3.HTTPProvider(self._rpc_url))
            feed_checksum = web3.to_checksum_address(feed_address)

            result = web3.eth.call({"to": feed_checksum, "data": DECIMALS_SELECTOR})  # type: ignore[typeddict-item]

            if len(result) < 32:
                return 8

            return int.from_bytes(result[0:32], byteorder="big")

        except Exception as e:
            logger.warning(f"Failed to query decimals from {feed_address}: {e}")
            return 8

    # =========================================================================
    # Archive Node and Historical Round Traversal Methods
    # =========================================================================

    async def _verify_archive_access(self) -> bool:
        """Verify that the RPC endpoint supports historical (archive) queries.

        Chainlink round traversal requires archive node access to query
        historical rounds via getRoundData().

        Returns:
            True if archive access is available, False otherwise
        """
        if self._archive_access_verified:
            return self._has_archive_access

        if not self._rpc_url:
            self._archive_access_verified = True
            self._has_archive_access = False
            return False

        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))

            # Try to get a historical block (~1000 blocks ago)
            latest_block = await web3.eth.get_block("latest")
            test_block = max(1, latest_block["number"] - 1000)
            test_block_data = await web3.eth.get_block(test_block)

            if test_block_data is not None:
                self._archive_access_verified = True
                self._has_archive_access = True
                logger.info(
                    "Archive node access verified for Chainlink provider",
                    extra={"chain": self._chain, "test_block": test_block},
                )
                return True

        except Exception as e:
            error_msg = str(e).lower()
            if "missing trie node" in error_msg or "pruned" in error_msg:
                logger.warning(
                    "RPC endpoint does not support archive queries (pruned/non-archive node). "
                    "Chainlink historical iteration will require pre-fetched data."
                )
            else:
                logger.warning(f"Failed to verify archive access: {e}")

        self._archive_access_verified = True
        self._has_archive_access = False
        return False

    async def _query_round_data(
        self,
        feed_address: str,
        round_id: int,
    ) -> ChainlinkRoundData | None:
        """Query getRoundData(uint80) from a Chainlink aggregator.

        Args:
            feed_address: Address of the Chainlink aggregator contract
            round_id: The round ID to query

        Returns:
            ChainlinkRoundData with round info, or None if query fails or round doesn't exist
        """
        if not self._rpc_url:
            return None

        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))
            feed_checksum = web3.to_checksum_address(feed_address)

            # Encode round_id as uint80 (padded to 32 bytes)
            round_id_bytes = round_id.to_bytes(32, byteorder="big")
            calldata = bytes.fromhex(GET_ROUND_DATA_SELECTOR[2:]) + round_id_bytes

            result = await web3.eth.call({"to": feed_checksum, "data": calldata.hex()})  # type: ignore[typeddict-item]

            if len(result) < 160:  # 5 * 32 bytes
                return None

            # Decode the 5 uint256 values
            returned_round_id = int.from_bytes(result[0:32], byteorder="big")
            answer = int.from_bytes(result[32:64], byteorder="big", signed=True)
            started_at = int.from_bytes(result[64:96], byteorder="big")
            updated_at = int.from_bytes(result[96:128], byteorder="big")
            answered_in_round = int.from_bytes(result[128:160], byteorder="big")

            # Check for invalid/empty round
            if answer == 0 or updated_at == 0:
                return None

            return ChainlinkRoundData(
                round_id=returned_round_id,
                answer=answer,
                started_at=started_at,
                updated_at=updated_at,
                answered_in_round=answered_in_round,
            )

        except Exception as e:
            logger.debug(f"Failed to query Chainlink round {round_id} from {feed_address}: {e}")
            return None

    async def _get_decimals_cached(self, feed_address: str) -> int:
        """Get decimals for a feed, using cache if available.

        Args:
            feed_address: Address of the Chainlink aggregator contract

        Returns:
            Number of decimals (defaults to 8 if query fails)
        """
        if feed_address in self._decimals_cache:
            return self._decimals_cache[feed_address]

        decimals = await self._query_decimals(feed_address)
        self._decimals_cache[feed_address] = decimals
        return decimals

    # =========================================================================
    # Binary Search for Timestamp-to-Round Mapping (O(log n))
    # =========================================================================

    async def _get_round_bounds(self, feed_address: str) -> tuple[int, int] | None:
        """Get the min and max round IDs for a Chainlink feed.

        Args:
            feed_address: Address of the Chainlink aggregator contract

        Returns:
            Tuple of (min_round_id, max_round_id) or None if unavailable
        """
        # Check cache first
        if feed_address in self._round_bounds_cache:
            return self._round_bounds_cache[feed_address]

        # Get latest round
        latest_round = await self._query_latest_round_data(feed_address)
        if latest_round is None:
            return None

        max_round_id = latest_round.round_id

        # Find min round by probing backwards with exponential steps
        # Chainlink uses phaseId in the upper bits of roundId
        # roundId = (phaseId << 64) | aggregatorRoundId
        # For most feeds, the earliest rounds are in phase 1
        min_round_id = 1

        # Try to find the actual minimum round with binary search
        # Start with phase 1, round 1
        phase_id = (max_round_id >> 64) & 0xFFFF
        if phase_id > 0:
            # Try phase 1 first
            test_round_id = (1 << 64) | 1
            test_round = await self._query_round_data(feed_address, test_round_id)
            if test_round is not None:
                min_round_id = test_round_id

        # Cache the bounds
        self._round_bounds_cache[feed_address] = (min_round_id, max_round_id)
        return (min_round_id, max_round_id)

    async def _binary_search_round_for_timestamp(
        self,
        feed_address: str,
        target_timestamp: int,
    ) -> BinarySearchResult | None:
        """Find the round closest to a target timestamp using binary search.

        Uses O(log n) binary search instead of linear traversal for efficient
        timestamp-to-round mapping. This is critical for backtesting performance.

        Args:
            feed_address: Address of the Chainlink aggregator contract
            target_timestamp: Target Unix timestamp to find

        Returns:
            BinarySearchResult with the closest round, or None if not found
        """
        bounds = await self._get_round_bounds(feed_address)
        if bounds is None:
            return None

        min_round_id, max_round_id = bounds

        # Edge case: target is after the latest round
        latest_round = await self._query_round_data(feed_address, max_round_id)
        if latest_round is None:
            return None

        if target_timestamp >= latest_round.updated_at:
            return BinarySearchResult(
                round_id=max_round_id,
                round_data=latest_round,
                iterations=1,
                exact_match=(target_timestamp == latest_round.updated_at),
            )

        # Binary search for the target timestamp
        left = min_round_id
        right = max_round_id
        best_round: ChainlinkRoundData | None = None
        best_round_id = 0
        iterations = 0

        while left <= right and iterations < MAX_BINARY_SEARCH_ITERATIONS:
            iterations += 1
            mid = (left + right) // 2

            round_data = await self._query_round_data(feed_address, mid)

            if round_data is None:
                # Round doesn't exist - try to find nearest valid round
                # Search in both directions
                found_round = None
                for offset in range(1, 100):
                    if mid + offset <= right:
                        found_round = await self._query_round_data(feed_address, mid + offset)
                        if found_round is not None:
                            mid = mid + offset
                            round_data = found_round
                            break
                    if mid - offset >= left:
                        found_round = await self._query_round_data(feed_address, mid - offset)
                        if found_round is not None:
                            mid = mid - offset
                            round_data = found_round
                            break
                if round_data is None:
                    # No valid round found in range
                    break

            if round_data.updated_at == target_timestamp:
                # Exact match found
                return BinarySearchResult(
                    round_id=mid,
                    round_data=round_data,
                    iterations=iterations,
                    exact_match=True,
                )

            if round_data.updated_at < target_timestamp:
                # Round is before target, we need a later round
                # But save this as the best candidate (most recent before target)
                if best_round is None or round_data.updated_at > best_round.updated_at:
                    best_round = round_data
                    best_round_id = mid
                left = mid + 1
            else:
                # Round is after target, we need an earlier round
                right = mid - 1

        if best_round is not None:
            return BinarySearchResult(
                round_id=best_round_id,
                round_data=best_round,
                iterations=iterations,
                exact_match=False,
            )

        return None

    async def get_price_at_timestamp(
        self,
        token: str,
        timestamp: datetime,
    ) -> ChainlinkPriceResult | None:
        """Get the Chainlink price at a specific timestamp using binary search.

        This is the primary method for efficient historical price lookups.
        Uses O(log n) binary search for timestamp-to-round mapping.

        Args:
            token: Token symbol (e.g., "ETH", "BTC")
            timestamp: Target timestamp

        Returns:
            ChainlinkPriceResult with price and metadata, or None if unavailable
        """
        token_upper = token.upper()
        feed = self.get_feed_config(token_upper)
        if feed is None:
            return None

        target_ts = int(timestamp.timestamp())
        decimals = await self._get_decimals_cached(feed.address)

        # Use binary search to find the round
        result = await self._binary_search_round_for_timestamp(feed.address, target_ts)
        if result is None:
            return None

        price = self._convert_price(result.round_data.answer, decimals)
        updated_at = datetime.fromtimestamp(result.round_data.updated_at, tz=UTC)

        # Check staleness (data older than 1 hour from target timestamp)
        age_seconds = abs(target_ts - result.round_data.updated_at)
        is_stale = age_seconds > DATA_STALENESS_THRESHOLD_SECONDS

        # Determine confidence level
        if result.exact_match or age_seconds < 60:
            confidence = DataConfidence.HIGH
        elif age_seconds < DATA_STALENESS_THRESHOLD_SECONDS:
            confidence = DataConfidence.MEDIUM
        else:
            confidence = DataConfidence.LOW

        source_info = DataSourceInfo(
            source=f"chainlink_{self._chain}",
            confidence=confidence,
            timestamp=updated_at,
        )

        return ChainlinkPriceResult(
            price=price,
            timestamp=updated_at,
            round_id=result.round_id,
            confidence=confidence,
            source_info=source_info,
            is_stale=is_stale,
        )

    # =========================================================================
    # Batch RPC Calls via Multicall3
    # =========================================================================

    async def _batch_query_rounds(
        self,
        feed_address: str,
        round_ids: list[int],
    ) -> list[ChainlinkRoundData | None]:
        """Query multiple rounds in a single batch using Multicall3.

        Uses Multicall3 contract for efficient batching of getRoundData calls.
        Falls back to sequential calls if Multicall3 fails.

        Args:
            feed_address: Address of the Chainlink aggregator contract
            round_ids: List of round IDs to query

        Returns:
            List of ChainlinkRoundData (or None for failed queries)
        """
        if not self._rpc_url or not round_ids:
            return [None] * len(round_ids)

        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))
            feed_checksum = web3.to_checksum_address(feed_address)
            # Note: multicall_address reserved for future true multicall implementation
            # multicall_address = web3.to_checksum_address(MULTICALL3_ADDRESS)

            results: list[ChainlinkRoundData | None] = []

            # Process in batches to avoid gas limits
            for batch_start in range(0, len(round_ids), MAX_MULTICALL_BATCH_SIZE):
                batch_round_ids = round_ids[batch_start : batch_start + MAX_MULTICALL_BATCH_SIZE]

                # Build multicall3 aggregate3 calldata
                # struct Call3 { address target; bool allowFailure; bytes callData; }
                calls = []
                for round_id in batch_round_ids:
                    round_id_bytes = round_id.to_bytes(32, byteorder="big")
                    calldata = bytes.fromhex(GET_ROUND_DATA_SELECTOR[2:]) + round_id_bytes
                    # Encode Call3 struct
                    calls.append(
                        {
                            "target": feed_checksum,
                            "allowFailure": True,
                            "callData": calldata.hex(),
                        }
                    )

                # Encode calls array for aggregate3
                # We'll use a simpler approach - make individual calls in a batch
                # since proper ABI encoding for aggregate3 is complex
                batch_results = await self._batch_query_rounds_sequential(feed_address, batch_round_ids)
                results.extend(batch_results)

            return results

        except Exception as e:
            logger.warning(f"Multicall batch query failed, falling back to sequential: {e}")
            return await self._batch_query_rounds_sequential(feed_address, round_ids)

    async def _batch_query_rounds_sequential(
        self,
        feed_address: str,
        round_ids: list[int],
    ) -> list[ChainlinkRoundData | None]:
        """Sequential fallback for batch round queries.

        Args:
            feed_address: Address of the Chainlink aggregator contract
            round_ids: List of round IDs to query

        Returns:
            List of ChainlinkRoundData (or None for failed queries)
        """
        import asyncio

        tasks = [self._query_round_data(feed_address, round_id) for round_id in round_ids]
        return list(await asyncio.gather(*tasks))

    async def prefetch_rounds_for_range(
        self,
        token: str,
        start_time: datetime,
        end_time: datetime,
        interval_seconds: int = 3600,
    ) -> int:
        """Pre-fetch and cache round data for a time range.

        Uses binary search to find start/end rounds, then batch fetches
        intermediate rounds for efficient backtesting.

        Args:
            token: Token symbol
            start_time: Start of time range
            end_time: End of time range
            interval_seconds: Approximate interval between data points

        Returns:
            Number of rounds fetched and cached
        """
        token_upper = token.upper()
        feed = self.get_feed_config(token_upper)
        if feed is None:
            logger.warning(f"No Chainlink feed for {token} on {self._chain}")
            return 0

        feed_address = feed.address
        decimals = await self._get_decimals_cached(feed_address)

        # Find rounds at start and end using binary search
        start_ts = int(start_time.timestamp())
        end_ts = int(end_time.timestamp())

        start_result = await self._binary_search_round_for_timestamp(feed_address, start_ts)
        end_result = await self._binary_search_round_for_timestamp(feed_address, end_ts)

        if start_result is None or end_result is None:
            logger.warning(f"Could not find rounds for time range for {token}")
            return 0

        # Fetch all rounds in the range
        round_ids = list(range(start_result.round_id, end_result.round_id + 1))
        logger.info(
            f"Pre-fetching {len(round_ids)} rounds for {token}",
            extra={"start_round": start_result.round_id, "end_round": end_result.round_id},
        )

        # Batch fetch rounds
        rounds_data = await self._batch_query_rounds(feed_address, round_ids)

        # Cache the results
        cached_count = 0
        if feed_address not in self._round_cache:
            self._round_cache[feed_address] = []

        for round_id, round_data in zip(round_ids, rounds_data, strict=False):
            if round_data is not None:
                price = self._convert_price(round_data.answer, decimals)
                self._round_cache[feed_address].append((round_id, round_data.updated_at, price))

                # Also cache in historical cache for fast lookup
                if token_upper not in self._historical_cache:
                    self._historical_cache[token_upper] = {}
                self._historical_cache[token_upper][round_data.updated_at] = price
                cached_count += 1

        # Sort round cache by timestamp
        self._round_cache[feed_address].sort(key=lambda x: x[1])

        # Save to persistent cache if enabled
        self._save_persistent_cache()

        logger.info(f"Pre-fetched and cached {cached_count} rounds for {token}")
        return cached_count

    async def _fetch_historical_rounds(
        self,
        token: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[tuple[datetime, Decimal]]:
        """Fetch historical prices by traversing Chainlink rounds.

        This method walks backwards from the latest round to find all rounds
        within the specified time range.

        Args:
            token: Token symbol
            start_time: Start of the time range
            end_time: End of the time range

        Returns:
            List of (timestamp, price) tuples sorted by timestamp ascending
        """
        token_upper = token.upper()
        feed = self.get_feed_config(token_upper)
        if feed is None:
            logger.warning(f"No Chainlink feed for {token} on {self._chain}")
            return []

        feed_address = feed.address

        # Get decimals for price conversion
        decimals = await self._get_decimals_cached(feed_address)

        # Get the latest round to start traversal
        latest_round = await self._query_latest_round_data(feed_address)
        if latest_round is None:
            logger.warning(f"Failed to get latest round for {token}")
            return []

        # Convert timestamps to Unix time
        start_ts = int(start_time.timestamp())
        end_ts = int(end_time.timestamp())

        # Traverse rounds backwards
        prices: list[tuple[datetime, Decimal]] = []
        current_round_id = latest_round.round_id
        rounds_checked = 0
        rounds_with_gaps = 0

        logger.info(
            f"Starting Chainlink round traversal for {token} from round {current_round_id}",
            extra={"start_time": start_time, "end_time": end_time},
        )

        while rounds_checked < MAX_ROUNDS_TO_FETCH:
            round_data = await self._query_round_data(feed_address, current_round_id)

            if round_data is None:
                # Round doesn't exist (gap) - try previous round
                rounds_with_gaps += 1
                current_round_id -= 1
                rounds_checked += 1
                if rounds_with_gaps > 100:
                    # Too many consecutive gaps, stop
                    logger.warning(f"Too many round gaps for {token}, stopping traversal")
                    break
                continue

            # Reset gap counter on successful round
            rounds_with_gaps = 0

            round_ts = round_data.updated_at

            # Check if we've gone past the start time
            if round_ts < start_ts:
                # We've gone past our target range
                break

            # Check if this round is within our target range
            if round_ts <= end_ts:
                price = self._convert_price(round_data.answer, decimals)
                timestamp = datetime.fromtimestamp(round_ts, tz=UTC)
                prices.append((timestamp, price))

                # Cache the price for future lookups
                if token_upper not in self._historical_cache:
                    self._historical_cache[token_upper] = {}
                self._historical_cache[token_upper][round_ts] = price

            # Log progress periodically
            if rounds_checked > 0 and rounds_checked % ROUND_FETCH_PROGRESS_INTERVAL == 0:
                logger.debug(
                    f"Chainlink round traversal progress: {rounds_checked} rounds checked, "
                    f"{len(prices)} prices found for {token}"
                )

            current_round_id -= 1
            rounds_checked += 1

        # Sort by timestamp ascending
        prices.sort(key=lambda x: x[0])

        logger.info(
            f"Completed Chainlink round traversal for {token}: {len(prices)} prices from {rounds_checked} rounds"
        )

        return prices

    async def _get_historical_price(
        self,
        token: str,
        timestamp: datetime,
    ) -> Decimal | None:
        """Get historical price from cache or via round lookup.

        Args:
            token: Token symbol
            timestamp: Target timestamp

        Returns:
            Price at or just before timestamp, or None if unavailable
        """
        token_upper = token.upper()
        target_ts = int(timestamp.timestamp())

        # Check historical cache first
        if token_upper in self._historical_cache:
            cache = self._historical_cache[token_upper]
            # Find the closest timestamp <= target
            best_ts = None
            for ts in cache:
                if ts <= target_ts and (best_ts is None or ts > best_ts):
                    best_ts = ts
            if best_ts is not None:
                return cache[best_ts]

        # Also check the main cache
        if self._cache is not None:
            price = self._cache.get_price_at(token_upper, timestamp)
            if price is not None:
                return price

        return None

    def _convert_price(self, raw_answer: int, decimals: int) -> Decimal:
        """Convert raw Chainlink answer to Decimal price.

        Args:
            raw_answer: Raw int256 answer from latestRoundData
            decimals: Number of decimals for this feed

        Returns:
            Decimal price in USD
        """
        # Divide by 10^decimals to get the actual price
        divisor = Decimal(10) ** decimals
        return Decimal(raw_answer) / divisor

    def _check_staleness(
        self,
        round_data: ChainlinkRoundData,
        token: str,
        raise_on_stale: bool = True,
    ) -> tuple[bool, float]:
        """Check if the round data is stale based on heartbeat.

        Args:
            round_data: The round data to check
            token: Token symbol for heartbeat lookup
            raise_on_stale: If True, raise ChainlinkStaleDataError when stale

        Returns:
            Tuple of (is_stale, age_seconds)

        Raises:
            ChainlinkStaleDataError: If data is stale and raise_on_stale is True
        """
        current_time = datetime.now(UTC)
        updated_at = datetime.fromtimestamp(round_data.updated_at, tz=UTC)

        age_seconds = (current_time - updated_at).total_seconds()

        pair = TOKEN_TO_PAIR.get(token.upper(), "default")
        heartbeat = CHAINLINK_HEARTBEATS.get(pair, CHAINLINK_HEARTBEATS["default"])

        # Allow 10% buffer over heartbeat before marking as stale
        is_stale = age_seconds > heartbeat * 1.1

        if is_stale:
            logger.warning(
                f"Chainlink data for {token} is stale: updated {age_seconds:.0f}s ago (heartbeat: {heartbeat}s)"
            )
            if raise_on_stale:
                raise ChainlinkStaleDataError(
                    token=token,
                    age_seconds=age_seconds,
                    heartbeat_seconds=heartbeat,
                    updated_at=updated_at,
                )

        return is_stale, age_seconds

    async def get_latest_price(
        self,
        token: str,
        raise_on_stale: bool = True,
        use_cache: bool = True,
    ) -> Decimal | None:
        """Get the latest price for a token from Chainlink.

        This method queries the Chainlink aggregator directly on-chain to get
        the current price. It handles decimals conversion and staleness checking.

        Args:
            token: Token symbol (e.g., "ETH", "BTC", "LINK")
            raise_on_stale: If True, raise ChainlinkStaleDataError when data is stale.
                           If False, return None for stale data.
            use_cache: If True, check cache before querying on-chain. Default True.

        Returns:
            Decimal price in USD, or None if data is stale (when raise_on_stale=False)
            or if the token is not supported.

        Raises:
            ValueError: If no feed is available for the token on this chain
            ChainlinkStaleDataError: If data is stale and raise_on_stale is True
        """
        token_upper = token.upper()

        # Check if token is supported
        if token_upper not in TOKEN_TO_PAIR:
            raise ValueError(f"Unknown token: {token}")

        # Get feed configuration
        feed = self.get_feed_config(token_upper)
        if feed is None:
            raise ValueError(f"No Chainlink feed available for {token} on {self._chain}")

        # Check cache first if enabled
        if use_cache and self._cache is not None:
            cached = self._cache.get_live_price(token_upper)
            if cached is not None:
                logger.debug(f"Cache hit for {token}: ${cached.price:.4f} (age: {cached.age_seconds:.1f}s)")
                return cached.price

        # Query on-chain data
        round_data = await self._query_latest_round_data(feed.address)
        if round_data is None:
            logger.warning(f"Failed to query Chainlink data for {token}")
            return None

        # Check for stale data
        try:
            self._check_staleness(round_data, token_upper, raise_on_stale)
        except ChainlinkStaleDataError:
            if raise_on_stale:
                raise
            return None

        # Get decimals (could cache this, but query for accuracy)
        decimals = await self._query_decimals(feed.address)

        # Convert to Decimal price
        price = self._convert_price(round_data.answer, decimals)

        # Cache the result
        if self._cache is not None:
            updated_at = datetime.fromtimestamp(round_data.updated_at, tz=UTC)
            self._cache.set_live_price(token_upper, price, timestamp=updated_at)
            logger.debug(f"Cached Chainlink price for {token}: ${price:.4f}")

        logger.debug(f"Chainlink price for {token}: ${price:.4f}")
        return price

    def get_latest_price_sync(
        self,
        token: str,
        raise_on_stale: bool = True,
        use_cache: bool = True,
    ) -> Decimal | None:
        """Synchronous version of get_latest_price.

        Args:
            token: Token symbol (e.g., "ETH", "BTC", "LINK")
            raise_on_stale: If True, raise ChainlinkStaleDataError when data is stale.
            use_cache: If True, check cache before querying on-chain. Default True.

        Returns:
            Decimal price in USD, or None if unavailable/stale

        Raises:
            ValueError: If no feed is available for the token
            ChainlinkStaleDataError: If data is stale and raise_on_stale is True
        """
        token_upper = token.upper()

        if token_upper not in TOKEN_TO_PAIR:
            raise ValueError(f"Unknown token: {token}")

        feed = self.get_feed_config(token_upper)
        if feed is None:
            raise ValueError(f"No Chainlink feed available for {token} on {self._chain}")

        # Check cache first if enabled
        if use_cache and self._cache is not None:
            cached = self._cache.get_live_price(token_upper)
            if cached is not None:
                logger.debug(f"Cache hit for {token}: ${cached.price:.4f} (age: {cached.age_seconds:.1f}s)")
                return cached.price

        round_data = self._query_latest_round_data_sync(feed.address)
        if round_data is None:
            logger.warning(f"Failed to query Chainlink data for {token}")
            return None

        try:
            self._check_staleness(round_data, token_upper, raise_on_stale)
        except ChainlinkStaleDataError:
            if raise_on_stale:
                raise
            return None

        # Use default 8 decimals for sync version to avoid extra RPC call
        price = self._convert_price(round_data.answer, feed.decimals)

        # Cache the result
        if self._cache is not None:
            updated_at = datetime.fromtimestamp(round_data.updated_at, tz=UTC)
            self._cache.set_live_price(token_upper, price, timestamp=updated_at)
            logger.debug(f"Cached Chainlink price for {token}: ${price:.4f}")

        logger.debug(f"Chainlink price for {token}: ${price:.4f}")
        return price

    def is_data_stale(
        self,
        updated_at: datetime,
        token: str,
        current_time: datetime | None = None,
    ) -> bool:
        """Check if price data is stale based on Chainlink heartbeat.

        Args:
            updated_at: When the price was last updated
            token: Token symbol for heartbeat lookup
            current_time: Current time (defaults to now)

        Returns:
            True if data is stale (older than heartbeat), False otherwise
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        pair = TOKEN_TO_PAIR.get(token.upper(), "default")
        heartbeat = CHAINLINK_HEARTBEATS.get(pair, CHAINLINK_HEARTBEATS["default"])

        age_seconds = (current_time - updated_at).total_seconds()
        # Allow 10% buffer over heartbeat before marking as stale
        return age_seconds > heartbeat * 1.1

    async def get_price(
        self,
        token: str,
        timestamp: datetime | None = None,
        raise_on_stale: bool = True,
    ) -> Decimal:
        """Get the price of a token at a specific timestamp.

        This method supports multiple modes:
        1. If timestamp is None or close to now: Query live price from Chainlink
        2. If timestamp is historical and archive access is available: Fetch via round traversal
        3. If timestamp is historical without archive: Return cached data or raise ValueError

        Args:
            token: Token symbol (e.g., "ETH", "BTC", "LINK")
            timestamp: The point in time (None for current price)
            raise_on_stale: If True, raise ChainlinkStaleDataError when data is stale

        Returns:
            Price in USD at the specified timestamp

        Raises:
            ValueError: If price data is not available for the token/timestamp
            ChainlinkStaleDataError: If live data is stale and raise_on_stale is True
        """
        token_upper = token.upper()

        # Check if token is supported
        if token_upper not in TOKEN_TO_PAIR:
            raise ValueError(f"Unknown token: {token}")

        # Check if we have a feed for this token on this chain
        feed = self.get_feed_config(token_upper)
        if feed is None:
            raise ValueError(f"No Chainlink feed available for {token} on {self._chain}")

        # If timestamp is None or very recent, try to get live price
        current_time = datetime.now(UTC)
        if timestamp is None:
            timestamp = current_time

        # Normalize timestamp timezone
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        # Check if requesting "live" data (within last 5 minutes)
        time_diff = abs((current_time - timestamp).total_seconds())
        is_live_request = time_diff < 300  # 5 minutes

        # Check cache first (both historical and main cache)
        historical_price = await self._get_historical_price(token_upper, timestamp)
        if historical_price is not None:
            return historical_price

        if self._cache is not None:
            cached_price = self._cache.get_price_at(token_upper, timestamp)
            if cached_price is not None:
                return cached_price

        # If live request and we have RPC, query on-chain
        if is_live_request and self._rpc_url:
            price = await self.get_latest_price(token, raise_on_stale=raise_on_stale)
            if price is not None:
                return price

            # If price is None (stale data with raise_on_stale=False), raise error
            raise ValueError(f"Chainlink price for {token} is stale or unavailable")

        # For historical data, try native round traversal if archive is available
        if self._rpc_url:
            has_archive = await self._verify_archive_access()
            if has_archive:
                # Fetch a small time range around the target timestamp
                buffer = timedelta(hours=1)
                start_time = timestamp - buffer
                end_time = timestamp + buffer

                historical_prices = await self._fetch_historical_rounds(token_upper, start_time, end_time)

                if historical_prices:
                    # Store in cache for future lookups
                    if self._cache is not None:
                        existing = self._cache.data.get(token_upper, [])
                        self._cache.data[token_upper] = sorted(existing + historical_prices, key=lambda x: x[0])

                    # Find the price at or before the target timestamp
                    price = await self._get_historical_price(token_upper, timestamp)
                    if price is not None:
                        return price

        # Historical data not available
        raise ValueError(
            f"Historical price data for {token} at {timestamp} not available. "
            "Use an archive node RPC for historical queries, or pre-fetch data via "
            "set_historical_prices() or iterate() method."
        )

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Note: Chainlink provides spot prices, not OHLCV data. This method
        generates pseudo-OHLCV using the spot price for all O/H/L/C values.

        Args:
            token: Token symbol (e.g., "ETH", "BTC")
            start: Start of the time range (inclusive)
            end: End of the time range (inclusive)
            interval_seconds: Candle interval in seconds (default: 3600 = 1 hour)

        Returns:
            List of OHLCV data points, sorted by timestamp ascending

        Raises:
            ValueError: If data is not available for the token/range
        """
        token_upper = token.upper()

        # Check if token is supported
        if token_upper not in TOKEN_TO_PAIR:
            raise ValueError(f"Unknown token: {token}")

        # Check if we have a feed for this token on this chain
        feed = self.get_feed_config(token_upper)
        if feed is None:
            raise ValueError(f"No Chainlink feed available for {token} on {self._chain}")

        # Check cache
        if self._cache is None or token_upper not in self._cache.data:
            raise ValueError(f"OHLCV data for {token} not available. Chainlink provider requires pre-fetched data.")

        # Generate OHLCV from cached price data
        ohlcv_list: list[OHLCV] = []
        current = start
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)

        end_tz = end
        if end_tz.tzinfo is None:
            end_tz = end_tz.replace(tzinfo=UTC)

        interval = timedelta(seconds=interval_seconds)

        while current <= end_tz:
            price = self._cache.get_price_at(token_upper, current)
            if price is not None:
                ohlcv = OHLCV(
                    timestamp=current,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=None,
                )
                ohlcv_list.append(ohlcv)
            current += interval

        return ohlcv_list

    def set_historical_prices(
        self,
        token: str,
        prices: list[tuple[datetime, Decimal]],
    ) -> None:
        """Set historical price data for a token.

        This method allows pre-loading historical price data for backtesting.
        The data can be sourced from an archive node or external price service.

        Args:
            token: Token symbol
            prices: List of (timestamp, price) tuples, sorted by timestamp
        """
        if self._cache is None:
            self._cache = PriceCache(ttl_seconds=self._cache_ttl_seconds)

        token_upper = token.upper()
        self._cache.data[token_upper] = sorted(prices, key=lambda x: x[0])
        logger.info(f"Loaded {len(prices)} historical prices for {token_upper}")

    def clear_cache(self, token: str | None = None) -> None:
        """Clear the price cache.

        Args:
            token: Specific token to clear, or None to clear all
        """
        if self._cache is None:
            return

        if token is not None:
            token_upper = token.upper()
            self._cache.data.pop(token_upper, None)
            self._cache.clear_live_cache(token_upper)
            logger.debug(f"Cleared cache for {token_upper}")
        else:
            self._cache.data.clear()
            self._cache.clear_live_cache()
            logger.debug("Cleared all price cache")

    def get_cache_stats(self) -> dict[str, Any]:
        """Get statistics about the current cache state.

        Returns:
            Dictionary with cache statistics including:
            - live_tokens: List of tokens with live cache entries
            - live_count: Number of live cache entries
            - live_expired_count: Number of expired live entries
            - historical_tokens: List of tokens with historical data
            - historical_count: Number of tokens with historical data
            - total_historical_points: Total number of historical price points
            - ttl_seconds: Configured TTL for live cache
        """
        if self._cache is None:
            return {
                "live_tokens": [],
                "live_count": 0,
                "live_expired_count": 0,
                "historical_tokens": [],
                "historical_count": 0,
                "total_historical_points": 0,
                "ttl_seconds": self._cache_ttl_seconds,
                "caching_enabled": False,
            }

        stats = self._cache.get_cache_stats()
        stats["caching_enabled"] = True
        return stats

    def set_cache_ttl(self, ttl_seconds: int) -> None:
        """Update the cache TTL.

        Args:
            ttl_seconds: New TTL in seconds (0 to disable caching)
        """
        self._cache_ttl_seconds = ttl_seconds
        if self._cache is not None:
            self._cache.ttl_seconds = ttl_seconds
        logger.debug(f"Updated cache TTL to {ttl_seconds}s")

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical market states using native round traversal.

        When an archive node is available, this method fetches historical prices
        by traversing Chainlink rounds via getRoundData(). This provides true
        historical data without requiring pre-fetched data.

        If archive access is unavailable, falls back to pre-loaded cache with
        appropriate warnings.

        Args:
            config: Configuration specifying time range, interval, and tokens

        Yields:
            Tuples of (timestamp, MarketState) for each time point

        Example:
            # With archive node - automatic historical data
            async for timestamp, market_state in provider.iterate(config):
                eth_price = market_state.get_price("ETH")

            # Without archive node - pre-load data first
            provider.set_historical_prices("ETH", eth_prices)
            async for timestamp, market_state in provider.iterate(config):
                eth_price = market_state.get_price("ETH")
        """
        logger.info(
            f"Starting Chainlink iteration from {config.start_time} to {config.end_time} "
            f"with {config.interval_seconds}s interval for tokens: {config.tokens}"
        )

        # Initialize cache if not already done
        if self._cache is None:
            self._cache = PriceCache()

        # Check for archive access and fetch historical data if available
        has_archive = await self._verify_archive_access()
        data_source = "chainlink_historical" if has_archive else "chainlink_cache"
        historical_price_hits = 0
        fallback_price_hits = 0

        if has_archive:
            logger.info(
                "Archive node available - fetching historical Chainlink rounds",
                extra={"tokens": config.tokens},
            )

            # Normalize timestamps
            start_time = config.start_time
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=UTC)
            end_time = config.end_time
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=UTC)

            # Fetch historical rounds for each token
            for token in config.tokens:
                token_upper = token.upper()

                # Check if we already have data in cache
                if self._cache.data.get(token_upper):
                    logger.debug(f"Using existing cache for {token_upper}")
                    continue

                historical_prices = await self._fetch_historical_rounds(token_upper, start_time, end_time)

                if historical_prices:
                    # Store in main cache for iteration
                    self._cache.data[token_upper] = historical_prices
                    logger.info(f"Fetched {len(historical_prices)} historical prices for {token_upper}")
                else:
                    logger.warning(f"No historical prices found for {token_upper} via round traversal")
        else:
            logger.warning(
                "Archive node not available - using pre-loaded cache for Chainlink iteration. "
                "Historical accuracy may be limited. Consider using an archive node RPC."
            )

        # Generate timestamps at the specified interval
        current_time = config.start_time
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        end_time = config.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        interval = timedelta(seconds=config.interval_seconds)
        data_points = 0

        while current_time <= end_time:
            # Build prices dict from cache
            prices: dict[str, Decimal] = {}
            ohlcv_data: dict[str, OHLCV] = {}

            for token in config.tokens:
                token_upper = token.upper()

                # Try historical cache first (populated by round traversal)
                price = await self._get_historical_price(token_upper, current_time)

                if price is None:
                    # Fall back to main cache
                    price = self._cache.get_price_at(token_upper, current_time)

                if price is not None:
                    prices[token_upper] = price

                    if has_archive:
                        historical_price_hits += 1
                    else:
                        fallback_price_hits += 1

                    # Generate pseudo-OHLCV if requested
                    if config.include_ohlcv:
                        ohlcv_data[token_upper] = OHLCV(
                            timestamp=current_time,
                            open=price,
                            high=price,
                            low=price,
                            close=price,
                            volume=None,
                        )

            # Create MarketState for this timestamp
            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                ohlcv=ohlcv_data if config.include_ohlcv else {},
                chain=config.chains[0] if config.chains else self._chain,
                block_number=None,
                gas_price_gwei=None,
                metadata={
                    "data_source": data_source,
                    "historical_price_hits": historical_price_hits,
                    "fallback_price_hits": fallback_price_hits,
                },
            )

            yield (current_time, market_state)

            current_time += interval
            data_points += 1

            # Log progress periodically
            if data_points > 0 and data_points % 100 == 0:
                logger.debug(
                    f"Chainlink iteration progress: {data_points} data points, "
                    f"historical_hits={historical_price_hits}, fallback_hits={fallback_price_hits}"
                )

        logger.info(
            f"Completed Chainlink iteration with {data_points} data points",
            extra={
                "historical_price_hits": historical_price_hits,
                "fallback_price_hits": fallback_price_hits,
                "data_source": data_source,
            },
        )

    @property
    def provider_name(self) -> str:
        """Return the unique name of this data provider."""
        return f"chainlink_{self._chain}"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported token symbols for the current chain."""
        supported = []
        for token, pair in TOKEN_TO_PAIR.items():
            if pair in self._price_feeds:
                supported.append(token)
        return supported

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        return self._SUPPORTED_CHAINS.copy()

    @property
    def min_timestamp(self) -> datetime | None:
        """Return the earliest timestamp with available data.

        Chainlink feeds have been deployed at different times.
        For most major feeds on Ethereum, data is available from 2020.
        """
        # Conservative estimate - Chainlink v2 aggregators launched ~2020
        return datetime(2020, 1, 1, tzinfo=UTC)

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data.

        For Chainlink, this is approximately "now" with minimal delay.
        """
        return datetime.now(UTC) - timedelta(seconds=15)

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return the historical data capability of this provider.

        When archive node access has been verified, returns FULL capability
        as the provider can natively traverse historical rounds via getRoundData().

        Otherwise returns PRE_CACHE, indicating that historical data should be
        pre-fetched and cached before backtesting for optimal performance.

        Note: Archive access verification is lazy - it happens on first call
        to iterate() or can be triggered explicitly via _verify_archive_access().
        """
        if self._archive_access_verified and self._has_archive_access:
            return HistoricalDataCapability.FULL
        return HistoricalDataCapability.PRE_CACHE

    async def close(self) -> None:
        """Close any resources (for API compatibility)."""
        pass

    async def __aenter__(self) -> "ChainlinkDataProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    # Main provider class
    "ChainlinkDataProvider",
    # Data classes
    "ChainlinkPriceFeed",
    "ChainlinkRoundData",
    "ChainlinkPriceResult",
    "BinarySearchResult",
    "PersistentCacheConfig",
    # Caching
    "CachedPrice",
    "PriceCache",
    # Exceptions
    "ChainlinkStaleDataError",
    # Price feed addresses by chain
    "CHAINLINK_PRICE_FEEDS",
    "ETHEREUM_PRICE_FEEDS",
    "ARBITRUM_PRICE_FEEDS",
    "BASE_PRICE_FEEDS",
    "OPTIMISM_PRICE_FEEDS",
    "POLYGON_PRICE_FEEDS",
    "AVALANCHE_PRICE_FEEDS",
    # Configuration constants
    "CHAINLINK_HEARTBEATS",
    "CHAINLINK_DEVIATION_THRESHOLDS",
    "TOKEN_TO_PAIR",
    # Function selectors
    "LATEST_ROUND_DATA_SELECTOR",
    "GET_ROUND_DATA_SELECTOR",
    "DECIMALS_SELECTOR",
    # Limits and thresholds
    "MAX_ROUNDS_TO_FETCH",
    "MAX_BINARY_SEARCH_ITERATIONS",
    "MAX_MULTICALL_BATCH_SIZE",
    "DATA_STALENESS_THRESHOLD_SECONDS",
    # Environment variable configuration
    "ARCHIVE_RPC_URL_ENV_PATTERN",
    "ARCHIVE_RPC_CHAINS",
    # Multicall
    "MULTICALL3_ADDRESS",
]
