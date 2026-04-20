"""Historical data loader for pre-fetching provider data.

This module provides a utility class for pre-fetching historical data from
various sources before running backtests. This is necessary for providers
with PRE_CACHE capability that cannot fetch historical data on-demand.

Key Features:
    - Pre-fetch Chainlink oracle round data via archive nodes
    - Pre-fetch DEX pool volume history from subgraphs
    - Pre-fetch lending protocol APY history from subgraphs
    - Configurable date ranges and granularity
    - Progress logging for long-running fetches
    - Persistent disk caching with resume on interruption
    - Data coverage validation and gap reporting

Example:
    from almanak.framework.backtesting.data import HistoricalDataLoader
    from almanak.framework.backtesting.config import BacktestDataConfig
    from datetime import datetime, UTC

    config = BacktestDataConfig(
        enable_persistent_cache=True,
        cache_directory="/path/to/cache",
    )

    loader = HistoricalDataLoader.from_config(
        config=config,
        rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
    )

    # Pre-fetch Chainlink price history
    prices = await loader.fetch_chainlink_history(
        feed_address="0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  # ETH/USD
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
    )

    # Pre-fetch pool volume history
    volumes = await loader.fetch_pool_volume_history(
        pool_address="0x...",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
    )

    # Pre-fetch lending APY history
    apys = await loader.fetch_lending_apy_history(
        protocol="aave_v3",
        markets=["USDC", "WETH", "DAI"],
        chain="ethereum",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
    )

    # Validate data coverage
    report = loader.validate_data_coverage(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
    )
    print(f"Overall coverage: {report.coverage_pct:.1f}%")
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import pickle
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig

logger = logging.getLogger(__name__)


# =============================================================================
# Subgraph Constants
# =============================================================================

# Default Uniswap V3 subgraph endpoints by chain
# These are the hosted service endpoints (free, no API key required)
UNISWAP_V3_SUBGRAPH_ENDPOINTS: dict[str, str] = {
    "ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    "arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one",
    "optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
    "polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
    "base": "https://api.thegraph.com/subgraphs/name/ianlapham/base-v3",
}

# Subgraph query pagination limit
SUBGRAPH_PAGE_SIZE = 1000

# HTTP request timeout for subgraph queries
SUBGRAPH_REQUEST_TIMEOUT_SECONDS = 30

# Rate limit delay between paginated requests
SUBGRAPH_RATE_LIMIT_DELAY_SECONDS = 0.5


# =============================================================================
# Chainlink Function Selectors and Constants
# =============================================================================

# latestRoundData() function selector
# Returns: (roundId, answer, startedAt, updatedAt, answeredInRound)
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"

# getRoundData(uint80 _roundId) function selector
# Returns: (roundId, answer, startedAt, updatedAt, answeredInRound)
GET_ROUND_DATA_SELECTOR = "0x9a6fc8f5"

# decimals() function selector
DECIMALS_SELECTOR = "0x313ce567"

# Progress logging interval (log every N rounds)
PROGRESS_LOG_INTERVAL = 100

# Maximum rounds to fetch in one session (safety limit)
MAX_ROUNDS_TO_FETCH = 50000

# Cache file names
CACHE_METADATA_FILE = "cache_metadata.json"
CACHE_VERSION = "1.0"

# Maximum cache age before re-fetching (7 days)
DEFAULT_MAX_CACHE_AGE_DAYS = 7


# =============================================================================
# Data Coverage Report Types
# =============================================================================


@dataclass
class DataGap:
    """Represents a gap in historical data.

    Attributes:
        start: Start of the gap period
        end: End of the gap period
        gap_duration: Duration of the gap
        data_type: Type of data (chainlink, volume, apy)
        identifier: Asset/pool identifier for this gap
    """

    start: datetime
    end: datetime
    gap_duration: timedelta
    data_type: str
    identifier: str


@dataclass
class DataCoverageReport:
    """Report of data coverage and gaps.

    Attributes:
        total_expected_points: Total data points expected for the period
        total_available_points: Total data points actually available
        coverage_pct: Percentage of data coverage (0-100)
        gaps: List of detected data gaps
        by_source: Coverage breakdown by data source
    """

    total_expected_points: int
    total_available_points: int
    coverage_pct: float
    gaps: list[DataGap]
    by_source: dict[str, float]  # source -> coverage pct


@dataclass
class ChainlinkRoundData:
    """Data from a single Chainlink aggregator round.

    Attributes:
        round_id: The round ID from the aggregator
        answer: The price answer (scaled by decimals)
        started_at: When the round started
        updated_at: When the round was last updated
        answered_in_round: The round ID when the answer was computed
    """

    round_id: int
    answer: Decimal
    started_at: datetime
    updated_at: datetime
    answered_in_round: int


@dataclass
class PoolVolumeSnapshot:
    """Volume snapshot from a DEX pool at a specific time.

    Attributes:
        timestamp: The timestamp of the snapshot
        volume_token0: Volume of token0 in the period
        volume_token1: Volume of token1 in the period
        volume_usd: Total volume in USD
        fee_usd: Fees collected in USD
    """

    timestamp: datetime
    volume_token0: Decimal
    volume_token1: Decimal
    volume_usd: Decimal
    fee_usd: Decimal


@dataclass
class APYSnapshot:
    """APY snapshot from a lending protocol at a specific time.

    Attributes:
        timestamp: The timestamp of the snapshot
        supply_apy: Annual supply APY (e.g., 0.03 = 3%)
        borrow_apy: Annual borrow APY (e.g., 0.05 = 5%)
        market: Market identifier (e.g., "USDC")
        protocol: Protocol identifier (e.g., "aave_v3")
    """

    timestamp: datetime
    supply_apy: Decimal
    borrow_apy: Decimal
    market: str
    protocol: str


@dataclass
class CacheMetadata:
    """Metadata for persistent cache entries.

    Attributes:
        version: Cache format version
        created_at: When the cache entry was created
        source: Data source identifier
        identifier: Asset/pool identifier
        start_time: Start of cached data period
        end_time: End of cached data period
        data_points: Number of data points in cache
        checksum: SHA256 hash of cached data for integrity
    """

    version: str
    created_at: datetime
    source: str
    identifier: str
    start_time: datetime
    end_time: datetime
    data_points: int
    checksum: str


@dataclass
class HistoricalDataLoader:
    """Utility for pre-fetching historical data from various sources.

    This loader is designed to work with data providers that require
    PRE_CACHE capability (like Chainlink). It fetches historical data
    upfront and stores it in memory for efficient access during backtest.

    Features:
        - Pre-fetch Chainlink oracle round data via archive nodes
        - Pre-fetch DEX pool volume history from subgraphs
        - Pre-fetch lending protocol APY history from subgraphs
        - Persistent disk caching for faster repeated backtests
        - Resume interrupted fetches
        - Data coverage validation and gap reporting

    Attributes:
        rpc_url: RPC endpoint URL (requires archive node for historical data)
        subgraph_url: The Graph subgraph URL for pool data (optional)
        cache: In-memory cache of fetched data
        cache_directory: Directory for persistent cache storage (optional)
        enable_persistent_cache: Whether to persist cache to disk
        max_cache_age_days: Maximum age of cached data before re-fetching

    Example:
        # Create with BacktestDataConfig
        config = BacktestDataConfig(
            enable_persistent_cache=True,
            cache_directory="/path/to/cache",
        )
        loader = HistoricalDataLoader.from_config(
            config=config,
            rpc_url="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
        )

        # Or create directly
        loader = HistoricalDataLoader(
            rpc_url="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
            subgraph_url="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
            cache_directory="/path/to/cache",
            enable_persistent_cache=True,
        )

        # Fetch ETH/USD Chainlink history
        eth_prices = await loader.fetch_chainlink_history(
            feed_address="0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 6, 1, tzinfo=UTC),
        )

        # Validate data coverage
        report = loader.validate_data_coverage(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 6, 1, tzinfo=UTC),
        )
    """

    rpc_url: str
    subgraph_url: str | None = None
    cache: dict[str, dict[datetime, Decimal]] = field(default_factory=dict)
    cache_directory: str | None = None
    enable_persistent_cache: bool = False
    max_cache_age_days: int = DEFAULT_MAX_CACHE_AGE_DAYS

    # APY cache (separate from main cache for type safety)
    _apy_cache: dict[str, dict[datetime, APYSnapshot]] = field(default_factory=dict)

    # Cache metadata for tracking what's been fetched
    _cache_metadata: dict[str, CacheMetadata] = field(default_factory=dict)

    # Track partial fetches for resume support
    _partial_fetch_state: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_config(
        cls,
        config: BacktestDataConfig,
        rpc_url: str,
        subgraph_url: str | None = None,
    ) -> HistoricalDataLoader:
        """Create a HistoricalDataLoader from BacktestDataConfig.

        Args:
            config: BacktestDataConfig with cache settings
            rpc_url: RPC endpoint URL (requires archive node)
            subgraph_url: The Graph subgraph URL (optional)

        Returns:
            Configured HistoricalDataLoader instance

        Example:
            config = BacktestDataConfig(
                enable_persistent_cache=True,
                cache_directory="/path/to/cache",
            )
            loader = HistoricalDataLoader.from_config(
                config=config,
                rpc_url="https://eth-mainnet.g.alchemy.com/v2/KEY",
            )
        """
        cache_path = config.get_cache_path()
        return cls(
            rpc_url=rpc_url,
            subgraph_url=subgraph_url,
            cache_directory=str(cache_path) if cache_path else None,
            enable_persistent_cache=config.enable_persistent_cache,
        )

    def __post_init__(self) -> None:
        """Initialize the loader after dataclass init."""
        if self.enable_persistent_cache and self.cache_directory:
            # Ensure cache directory exists
            cache_path = Path(self.cache_directory)
            cache_path.mkdir(parents=True, exist_ok=True)
            # Load existing cache metadata
            self._load_cache_metadata()

    async def fetch_chainlink_history(
        self,
        feed_address: str,
        start_time: datetime,
        end_time: datetime,
        *,
        decimals: int = 8,
    ) -> dict[datetime, Decimal]:
        """Fetch historical prices from a Chainlink aggregator via archive node.

        This method traverses Chainlink round history backwards from the current
        round to build a historical price timeline. Requires an archive node to
        access historical state.

        Args:
            feed_address: The Chainlink aggregator contract address
            start_time: Start of the historical period (inclusive)
            end_time: End of the historical period (inclusive)
            decimals: Number of decimals for the price feed (default 8)

        Returns:
            Dictionary mapping timestamps to prices (in USD, normalized).
            Prices are keyed by their updated_at timestamp from each round.

        Raises:
            ValueError: If start_time > end_time
            ConnectionError: If RPC connection fails or archive data unavailable

        Example:
            prices = await loader.fetch_chainlink_history(
                feed_address="0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 6, 1, tzinfo=UTC),
            )
            # prices = {datetime(2024, 1, 1, 0, 0, 1): Decimal('2000.12'), ...}

        Note:
            - Requires an archive node RPC endpoint
            - Large date ranges may take significant time to fetch
            - Progress is logged at INFO level
            - Data gaps are logged as warnings
        """
        if start_time > end_time:
            raise ValueError(f"start_time ({start_time}) must be <= end_time ({end_time})")

        # Ensure timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        logger.info(
            "Fetching Chainlink history for %s from %s to %s",
            feed_address,
            start_time.isoformat(),
            end_time.isoformat(),
        )

        # Lazy import web3 for RPC calls
        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3
        except ImportError as e:
            raise ImportError(
                "web3 package required for Chainlink history fetching. Install with: pip install web3"
            ) from e

        # Create async web3 instance
        web3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
        feed_checksum = web3.to_checksum_address(feed_address)

        # First, get the latest round data to know where to start
        latest_round_data = await self._query_round_data(
            web3,
            feed_checksum,
            None,  # None means latest
        )
        if latest_round_data is None:
            raise ConnectionError(
                f"Failed to fetch latest round data from {feed_address}. Check RPC connection and feed address."
            )

        # Calculate the divisor for price conversion
        divisor = Decimal(10) ** decimals

        # Results dictionary
        prices: dict[datetime, Decimal] = {}

        # Track statistics for logging
        rounds_processed = 0
        gaps_found = 0
        last_timestamp: datetime | None = None

        # Start from the latest round and work backwards
        current_round_id = latest_round_data.round_id
        logger.info(
            "Starting from round %d (updated at %s)",
            current_round_id,
            latest_round_data.updated_at.isoformat(),
        )

        # Traverse rounds backwards until we pass start_time
        while rounds_processed < MAX_ROUNDS_TO_FETCH:
            # Fetch round data
            round_data = await self._query_round_data(web3, feed_checksum, current_round_id)

            if round_data is None:
                # Round might not exist (gaps in round IDs can happen)
                gaps_found += 1
                if gaps_found > 100:
                    logger.warning(
                        "Too many gaps found (%d), stopping at round %d",
                        gaps_found,
                        current_round_id,
                    )
                    break
                current_round_id -= 1
                continue

            # Get timestamp (already a datetime from _query_round_data)
            round_timestamp = round_data.updated_at

            # Check if we've gone past the start time
            if round_timestamp < start_time:
                logger.info(
                    "Reached start boundary at round %d (timestamp %s)",
                    current_round_id,
                    round_timestamp.isoformat(),
                )
                break

            # Only include rounds within our time range
            if round_timestamp <= end_time:
                # Convert price
                price = Decimal(round_data.answer) / divisor
                prices[round_timestamp] = price

                # Check for data gaps (more than 24 hours between rounds)
                if last_timestamp is not None:
                    gap = last_timestamp - round_timestamp
                    if gap > timedelta(hours=24):
                        logger.warning(
                            "Data gap detected: %s to %s (%s)",
                            round_timestamp.isoformat(),
                            last_timestamp.isoformat(),
                            gap,
                        )
                        gaps_found += 1

                last_timestamp = round_timestamp

            rounds_processed += 1

            # Log progress periodically
            if rounds_processed % PROGRESS_LOG_INTERVAL == 0:
                logger.info(
                    "Progress: processed %d rounds, collected %d prices, current timestamp: %s",
                    rounds_processed,
                    len(prices),
                    round_timestamp.isoformat(),
                )

            # Move to previous round
            # Chainlink round IDs can have phase shifts, so we decrement
            # and handle potential gaps
            current_round_id -= 1

            # Safety check for very old rounds
            if current_round_id <= 0:
                logger.info("Reached round 0, stopping iteration")
                break

        # Cache the results
        cache_key = f"chainlink:{feed_address.lower()}"
        self.cache[cache_key] = prices

        logger.info(
            "Completed Chainlink history fetch: %d prices collected, %d rounds processed, %d gaps found",
            len(prices),
            rounds_processed,
            gaps_found,
        )

        return prices

    async def _query_round_data(
        self,
        web3: Any,  # AsyncWeb3 instance
        feed_address: str,
        round_id: int | None,
    ) -> ChainlinkRoundData | None:
        """Query round data from a Chainlink aggregator.

        Args:
            web3: AsyncWeb3 instance
            feed_address: Checksummed feed contract address
            round_id: Round ID to query, or None for latest

        Returns:
            ChainlinkRoundData or None if query fails
        """
        try:
            if round_id is None:
                # Query latestRoundData()
                call_data = LATEST_ROUND_DATA_SELECTOR
            else:
                # Query getRoundData(uint80 _roundId)
                # Encode the round_id as uint80 (padded to 32 bytes)
                round_id_hex = hex(round_id)[2:].zfill(64)
                call_data = GET_ROUND_DATA_SELECTOR + round_id_hex

            result = await web3.eth.call({"to": feed_address, "data": call_data})

            if len(result) < 160:  # 5 * 32 bytes expected
                return None

            # Decode the 5 values: roundId, answer, startedAt, updatedAt, answeredInRound
            decoded_round_id = int.from_bytes(result[0:32], byteorder="big")
            answer = int.from_bytes(result[32:64], byteorder="big", signed=True)
            started_at = int.from_bytes(result[64:96], byteorder="big")
            updated_at = int.from_bytes(result[96:128], byteorder="big")
            answered_in_round = int.from_bytes(result[128:160], byteorder="big")

            # Invalid round data (answer is 0 or timestamps are 0)
            if answer == 0 or updated_at == 0:
                return None

            return ChainlinkRoundData(
                round_id=decoded_round_id,
                answer=Decimal(answer),
                started_at=datetime.fromtimestamp(started_at, tz=UTC),
                updated_at=datetime.fromtimestamp(updated_at, tz=UTC),
                answered_in_round=answered_in_round,
            )

        except Exception as e:
            logger.debug(
                "Failed to query round %s from %s: %s",
                round_id if round_id is not None else "latest",
                feed_address,
                e,
            )
            return None

    async def fetch_pool_volume_history(
        self,
        pool_address: str,
        start_time: datetime,
        end_time: datetime,
        *,
        interval_hours: int = 24,
        chain: str = "arbitrum",
    ) -> dict[datetime, PoolVolumeSnapshot]:
        """Fetch historical volume data for a DEX pool from subgraph.

        This method queries The Graph subgraph for pool hourly/daily snapshots
        to retrieve historical trading volume and fee data.

        Args:
            pool_address: The DEX pool contract address
            start_time: Start of the historical period (inclusive)
            end_time: End of the historical period (inclusive)
            interval_hours: Granularity of volume data (1 for hourly, 24 for daily)
            chain: Blockchain to query (ethereum, arbitrum, base, optimism, polygon)

        Returns:
            Dictionary mapping timestamps to PoolVolumeSnapshot objects.
            Each snapshot contains volume and fee data for that period.

        Raises:
            ValueError: If start_time > end_time or invalid interval
            ConnectionError: If subgraph connection fails or pool not found

        Example:
            volumes = await loader.fetch_pool_volume_history(
                pool_address="0x...",
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 6, 1, tzinfo=UTC),
                interval_hours=24,  # Daily snapshots
            )
            for ts, snapshot in volumes.items():
                print(f"{ts}: ${snapshot.volume_usd} volume, ${snapshot.fee_usd} fees")

        Note:
            - Requires subgraph_url to be configured or uses default based on chain
            - Some subgraphs may have rate limits
            - Historical data availability depends on subgraph indexing
            - Progress is logged at INFO level
        """
        if start_time > end_time:
            raise ValueError(f"start_time ({start_time}) must be <= end_time ({end_time})")

        if interval_hours not in (1, 24):
            raise ValueError(f"interval_hours must be 1 (hourly) or 24 (daily), got {interval_hours}")

        # Use configured subgraph_url or default based on chain
        subgraph_url = self.subgraph_url
        if not subgraph_url:
            chain_lower = chain.lower()
            if chain_lower not in UNISWAP_V3_SUBGRAPH_ENDPOINTS:
                raise ValueError(
                    f"Unsupported chain: {chain}. Supported chains: {list(UNISWAP_V3_SUBGRAPH_ENDPOINTS.keys())}"
                )
            subgraph_url = UNISWAP_V3_SUBGRAPH_ENDPOINTS[chain_lower]

        # Ensure timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        logger.info(
            "Fetching pool volume history for %s from %s to %s (interval=%dh, chain=%s)",
            pool_address,
            start_time.isoformat(),
            end_time.isoformat(),
            interval_hours,
            chain,
        )

        # Convert times to Unix timestamps
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        # Normalize pool address to lowercase
        pool_address_lower = pool_address.lower()

        # Results dictionary
        volumes: dict[datetime, PoolVolumeSnapshot] = {}

        # Choose query based on interval
        if interval_hours == 24:
            volumes = await self._fetch_pool_day_datas(
                subgraph_url=subgraph_url,
                pool_address=pool_address_lower,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        else:
            # Hourly data uses poolHourDatas
            volumes = await self._fetch_pool_hour_datas(
                subgraph_url=subgraph_url,
                pool_address=pool_address_lower,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )

        # Cache the results
        cache_key = f"volume:{pool_address_lower}"
        # Store volumes as Decimal values for simple cache lookup
        self.cache[cache_key] = {ts: snapshot.volume_usd for ts, snapshot in volumes.items()}

        logger.info(
            "Completed pool volume history fetch: %d snapshots collected",
            len(volumes),
        )

        return volumes

    async def _fetch_pool_day_datas(
        self,
        subgraph_url: str,
        pool_address: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> dict[datetime, PoolVolumeSnapshot]:
        """Fetch daily volume data from poolDayDatas endpoint.

        Args:
            subgraph_url: The subgraph URL to query
            pool_address: Lowercase pool contract address
            start_timestamp: Start Unix timestamp
            end_timestamp: End Unix timestamp

        Returns:
            Dictionary mapping timestamps to PoolVolumeSnapshot objects
        """
        volumes: dict[datetime, PoolVolumeSnapshot] = {}
        last_timestamp = start_timestamp
        pages_fetched = 0

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=SUBGRAPH_REQUEST_TIMEOUT_SECONDS)
        ) as session:
            while True:
                # GraphQL query for poolDayDatas with pagination
                query = f"""
                query GetPoolDayDatas($poolAddress: String!, $startDate: Int!, $endDate: Int!, $skip: Int!) {{
                    poolDayDatas(
                        first: {SUBGRAPH_PAGE_SIZE}
                        skip: $skip
                        where: {{
                            pool: $poolAddress
                            date_gte: $startDate
                            date_lte: $endDate
                        }}
                        orderBy: date
                        orderDirection: asc
                    ) {{
                        id
                        date
                        volumeUSD
                        volumeToken0
                        volumeToken1
                        feesUSD
                        tvlUSD
                    }}
                }}
                """

                variables = {
                    "poolAddress": pool_address,
                    "startDate": last_timestamp,
                    "endDate": end_timestamp,
                    "skip": 0,
                }

                try:
                    data = await self._execute_subgraph_query(session, subgraph_url, query, variables)
                    pool_day_datas = data.get("poolDayDatas", [])

                    if not pool_day_datas:
                        logger.debug("No more poolDayDatas returned, stopping pagination")
                        break

                    for day_data in pool_day_datas:
                        day_timestamp = int(day_data.get("date", 0))
                        snapshot_time = datetime.fromtimestamp(day_timestamp, tz=UTC)

                        volumes[snapshot_time] = PoolVolumeSnapshot(
                            timestamp=snapshot_time,
                            volume_token0=Decimal(str(day_data.get("volumeToken0", "0"))),
                            volume_token1=Decimal(str(day_data.get("volumeToken1", "0"))),
                            volume_usd=Decimal(str(day_data.get("volumeUSD", "0"))),
                            fee_usd=Decimal(str(day_data.get("feesUSD", "0"))),
                        )

                        # Track last timestamp for pagination
                        if day_timestamp > last_timestamp:
                            last_timestamp = day_timestamp

                    pages_fetched += 1

                    # Log progress periodically
                    if pages_fetched % 5 == 0:
                        logger.info(
                            "Progress: fetched %d pages, %d snapshots so far",
                            pages_fetched,
                            len(volumes),
                        )

                    # If we got less than a full page, we're done
                    if len(pool_day_datas) < SUBGRAPH_PAGE_SIZE:
                        break

                    # Move past last timestamp for next query (add 1 second to avoid duplicate)
                    last_timestamp += 1

                    # Rate limit delay between pages
                    await self._apply_rate_limit_delay()

                except aiohttp.ClientError as e:
                    logger.error("Subgraph HTTP error: %s", e)
                    raise ConnectionError(f"Failed to connect to subgraph: {e}") from e

        return volumes

    async def _fetch_pool_hour_datas(
        self,
        subgraph_url: str,
        pool_address: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> dict[datetime, PoolVolumeSnapshot]:
        """Fetch hourly volume data from poolHourDatas endpoint.

        Args:
            subgraph_url: The subgraph URL to query
            pool_address: Lowercase pool contract address
            start_timestamp: Start Unix timestamp
            end_timestamp: End Unix timestamp

        Returns:
            Dictionary mapping timestamps to PoolVolumeSnapshot objects
        """
        volumes: dict[datetime, PoolVolumeSnapshot] = {}
        last_timestamp = start_timestamp
        pages_fetched = 0

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=SUBGRAPH_REQUEST_TIMEOUT_SECONDS)
        ) as session:
            while True:
                # GraphQL query for poolHourDatas with pagination
                query = f"""
                query GetPoolHourDatas($poolAddress: String!, $startHour: Int!, $endHour: Int!, $skip: Int!) {{
                    poolHourDatas(
                        first: {SUBGRAPH_PAGE_SIZE}
                        skip: $skip
                        where: {{
                            pool: $poolAddress
                            periodStartUnix_gte: $startHour
                            periodStartUnix_lte: $endHour
                        }}
                        orderBy: periodStartUnix
                        orderDirection: asc
                    ) {{
                        id
                        periodStartUnix
                        volumeUSD
                        volumeToken0
                        volumeToken1
                        feesUSD
                        tvlUSD
                    }}
                }}
                """

                variables = {
                    "poolAddress": pool_address,
                    "startHour": last_timestamp,
                    "endHour": end_timestamp,
                    "skip": 0,
                }

                try:
                    data = await self._execute_subgraph_query(session, subgraph_url, query, variables)
                    pool_hour_datas = data.get("poolHourDatas", [])

                    if not pool_hour_datas:
                        logger.debug("No more poolHourDatas returned, stopping pagination")
                        break

                    for hour_data in pool_hour_datas:
                        hour_timestamp = int(hour_data.get("periodStartUnix", 0))
                        snapshot_time = datetime.fromtimestamp(hour_timestamp, tz=UTC)

                        volumes[snapshot_time] = PoolVolumeSnapshot(
                            timestamp=snapshot_time,
                            volume_token0=Decimal(str(hour_data.get("volumeToken0", "0"))),
                            volume_token1=Decimal(str(hour_data.get("volumeToken1", "0"))),
                            volume_usd=Decimal(str(hour_data.get("volumeUSD", "0"))),
                            fee_usd=Decimal(str(hour_data.get("feesUSD", "0"))),
                        )

                        # Track last timestamp for pagination
                        if hour_timestamp > last_timestamp:
                            last_timestamp = hour_timestamp

                    pages_fetched += 1

                    # Log progress periodically
                    if pages_fetched % 10 == 0:
                        logger.info(
                            "Progress: fetched %d pages, %d snapshots so far",
                            pages_fetched,
                            len(volumes),
                        )

                    # If we got less than a full page, we're done
                    if len(pool_hour_datas) < SUBGRAPH_PAGE_SIZE:
                        break

                    # Move past last timestamp for next query (add 1 second to avoid duplicate)
                    last_timestamp += 1

                    # Rate limit delay between pages
                    await self._apply_rate_limit_delay()

                except aiohttp.ClientError as e:
                    logger.error("Subgraph HTTP error: %s", e)
                    raise ConnectionError(f"Failed to connect to subgraph: {e}") from e

        return volumes

    async def _execute_subgraph_query(
        self,
        session: aiohttp.ClientSession,
        subgraph_url: str,
        query: str,
        variables: dict[str, Any],
        _retries: int = 0,
    ) -> dict[str, Any]:
        """Execute a GraphQL query against the subgraph.

        Args:
            session: aiohttp ClientSession
            subgraph_url: The subgraph URL to query
            query: GraphQL query string
            variables: Query variables
            _retries: Internal retry counter (do not set manually)

        Returns:
            Query response data

        Raises:
            ConnectionError: If subgraph returns error or rate limit exceeded
        """
        max_retries = 5
        headers = {"Content-Type": "application/json"}
        payload = {"query": query, "variables": variables}

        async with session.post(subgraph_url, json=payload, headers=headers) as response:
            if response.status == 429:
                # Rate limited - check retry limit
                if _retries >= max_retries:
                    raise ConnectionError(f"Subgraph rate limit exceeded after {max_retries} retries")
                # Wait with exponential backoff and retry
                retry_after = response.headers.get("Retry-After", "60")
                wait_seconds = float(retry_after) * (2**_retries)
                logger.warning(
                    "Subgraph rate limited, waiting %s seconds before retry (attempt %d/%d)",
                    wait_seconds,
                    _retries + 1,
                    max_retries,
                )
                await asyncio.sleep(wait_seconds)
                # Retry the request
                return await self._execute_subgraph_query(
                    session, subgraph_url, query, variables, _retries=_retries + 1
                )

            if response.status != 200:
                error_text = await response.text()
                logger.error(
                    "Subgraph request failed: status=%d, body=%s",
                    response.status,
                    error_text[:500],
                )
                raise ConnectionError(f"Subgraph request failed with status {response.status}: {error_text[:200]}")

            data = await response.json()

            if "errors" in data and data["errors"]:
                error_msgs = [e.get("message", str(e)) for e in data["errors"]]
                raise ConnectionError(f"Subgraph query error: {'; '.join(error_msgs)}")

            return data.get("data", {})

    async def _apply_rate_limit_delay(self) -> None:
        """Apply rate limit delay between paginated requests."""
        await asyncio.sleep(SUBGRAPH_RATE_LIMIT_DELAY_SECONDS)

    def get_cached_price(
        self,
        feed_address: str,
        timestamp: datetime,
    ) -> Decimal | None:
        """Get a cached price from previously fetched Chainlink data.

        Finds the closest price at or before the requested timestamp.

        Args:
            feed_address: The Chainlink aggregator contract address
            timestamp: The target timestamp to look up

        Returns:
            The price at or before timestamp, or None if not found.
        """
        cache_key = f"chainlink:{feed_address.lower()}"
        if cache_key not in self.cache:
            return None

        prices = self.cache[cache_key]
        # Find the closest timestamp <= target
        valid_timestamps = [ts for ts in prices if ts <= timestamp]
        if not valid_timestamps:
            return None

        closest = max(valid_timestamps)
        return prices[closest]

    def clear_cache(self) -> None:
        """Clear all cached historical data."""
        self.cache.clear()
        self._apy_cache.clear()
        self._cache_metadata.clear()
        logger.debug("Historical data cache cleared")

    # =========================================================================
    # APY History Fetching
    # =========================================================================

    async def fetch_lending_apy_history(
        self,
        protocol: str,
        markets: list[str],
        chain: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, dict[datetime, APYSnapshot]]:
        """Fetch historical APY data for lending protocol markets.

        This method queries The Graph subgraph for historical supply and borrow
        APY data across multiple markets in a lending protocol.

        Args:
            protocol: Lending protocol (aave_v3, compound_v3, morpho_blue, spark)
            markets: List of market symbols (e.g., ["USDC", "WETH", "DAI"])
            chain: Blockchain to query (ethereum, arbitrum, base, etc.)
            start_time: Start of the historical period (inclusive)
            end_time: End of the historical period (inclusive)

        Returns:
            Dictionary mapping market symbols to their APY snapshots.
            Each snapshot contains supply and borrow APY for that date.

        Raises:
            ValueError: If start_time > end_time or unsupported protocol
            ConnectionError: If subgraph connection fails

        Example:
            apys = await loader.fetch_lending_apy_history(
                protocol="aave_v3",
                markets=["USDC", "WETH", "DAI"],
                chain="ethereum",
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 6, 1, tzinfo=UTC),
            )
            for market, snapshots in apys.items():
                for ts, snap in snapshots.items():
                    print(f"{market} @ {ts}: supply={snap.supply_apy:.4f}, borrow={snap.borrow_apy:.4f}")
        """
        if start_time > end_time:
            raise ValueError(f"start_time ({start_time}) must be <= end_time ({end_time})")

        # Ensure timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        protocol_lower = protocol.lower()
        supported_protocols = ("aave_v3", "compound_v3", "morpho_blue", "spark")
        if protocol_lower not in supported_protocols:
            raise ValueError(f"Unsupported protocol: {protocol}. Supported: {supported_protocols}")

        logger.info(
            "Fetching %s APY history for %d markets on %s from %s to %s",
            protocol,
            len(markets),
            chain,
            start_time.isoformat(),
            end_time.isoformat(),
        )

        results: dict[str, dict[datetime, APYSnapshot]] = {}

        # Check persistent cache first
        for market in markets:
            cache_key = f"apy:{protocol_lower}:{chain}:{market.upper()}"
            cached_data = self._load_from_persistent_cache(cache_key, start_time, end_time)
            if cached_data is not None:
                results[market.upper()] = cached_data
                logger.info("Loaded %s APY from cache (%d data points)", market, len(cached_data))

        # Fetch remaining markets
        markets_to_fetch = [m for m in markets if m.upper() not in results]
        if markets_to_fetch:
            # Route to protocol-specific fetcher
            fetched = await self._fetch_apy_from_subgraph(
                protocol=protocol_lower,
                markets=markets_to_fetch,
                chain=chain,
                start_time=start_time,
                end_time=end_time,
            )
            results.update(fetched)

            # Save to persistent cache
            if self.enable_persistent_cache:
                for market, snapshots in fetched.items():
                    cache_key = f"apy:{protocol_lower}:{chain}:{market}"
                    self._save_to_persistent_cache(cache_key, snapshots, start_time, end_time, f"{protocol}:{chain}")

        # Store in memory cache
        for market, snapshots in results.items():
            cache_key = f"apy:{protocol_lower}:{chain}:{market}"
            self._apy_cache[cache_key] = snapshots

        logger.info(
            "Completed %s APY history fetch: %d markets, %d total snapshots",
            protocol,
            len(results),
            sum(len(s) for s in results.values()),
        )

        return results

    async def _fetch_apy_from_subgraph(
        self,
        protocol: str,
        markets: list[str],
        chain: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, dict[datetime, APYSnapshot]]:
        """Fetch APY data from protocol-specific subgraph.

        Args:
            protocol: Protocol identifier (lowercase)
            markets: List of market symbols to fetch
            chain: Chain identifier
            start_time: Start time (UTC)
            end_time: End time (UTC)

        Returns:
            Dictionary mapping market symbols to APY snapshots
        """
        results: dict[str, dict[datetime, APYSnapshot]] = {}

        # Get subgraph URL for protocol
        subgraph_url = self._get_apy_subgraph_url(protocol, chain)
        if not subgraph_url:
            logger.warning(
                "No subgraph URL for %s on %s, returning empty results",
                protocol,
                chain,
            )
            return results

        # Convert times to day numbers for Messari schema (used by morpho_blue, spark)
        # or timestamps for Aave schema
        start_day = (start_time.date() - date(1970, 1, 1)).days
        end_day = (end_time.date() - date(1970, 1, 1)).days
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        total_markets = len(markets)
        for idx, market in enumerate(markets, 1):
            logger.info(
                "Progress: fetching %s APY (%d/%d markets)",
                market,
                idx,
                total_markets,
            )

            snapshots: dict[datetime, APYSnapshot] = {}

            try:
                if protocol == "aave_v3":
                    snapshots = await self._fetch_aave_v3_apy(
                        subgraph_url, market, chain, start_timestamp, end_timestamp
                    )
                elif protocol == "compound_v3":
                    snapshots = await self._fetch_compound_v3_apy(subgraph_url, market, chain, start_day, end_day)
                elif protocol in ("morpho_blue", "spark"):
                    snapshots = await self._fetch_messari_apy(subgraph_url, market, chain, start_day, end_day, protocol)
            except ConnectionError as e:
                logger.warning("Failed to fetch %s APY for %s: %s", protocol, market, e)

            if snapshots:
                results[market.upper()] = snapshots

            # Rate limit between markets
            if idx < total_markets:
                await self._apply_rate_limit_delay()

        return results

    def _get_apy_subgraph_url(self, protocol: str, chain: str) -> str | None:
        """Get subgraph URL for APY data.

        Args:
            protocol: Protocol identifier
            chain: Chain identifier

        Returns:
            Subgraph URL or None if not supported
        """
        # Import Chain enum for mapping
        from almanak.core.enums import Chain

        try:
            chain_enum = Chain[chain.upper()]
        except KeyError:
            logger.warning("Unsupported chain for APY subgraph: %s", chain)
            return None

        # APY subgraph IDs by protocol and chain
        subgraph_ids: dict[str, dict[Chain, str]] = {
            "aave_v3": {
                Chain.ETHEREUM: "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g",
                Chain.ARBITRUM: "DLuE98kEb5pQNXAcKFQGQgfSQ57Xdou4jnVbAEqMfy3B",
                Chain.OPTIMISM: "DSfLz8oQBUeU5atALgUFQKMTSYV9mZAVYp4noLSXAfvb",
                Chain.POLYGON: "Co2URyXjnxaw8WqxKyVHdirq9Ahhm5vcTs4dMedAq211",
                Chain.BASE: "GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF",
                Chain.AVALANCHE: "2h9woxy8RTjHu1HJsCEnmzpPHFArU33avmUh4f71JpVn",
            },
            "compound_v3": {
                Chain.ETHEREUM: "5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp",
                Chain.ARBITRUM: "Ff7ha9ELmpmg81D6nYxy4t8aGP26dPztqD1LDJNPqjLS",
                Chain.POLYGON: "AaFtUWKfFdj2x8nnE3RxTSJkHwGHvawH3VWFBykCGzLs",
                Chain.BASE: "2tGWMrDha4164KkFAfkU3rDCtuxGb4q1emXmFdLLzJ8x",
            },
            "morpho_blue": {
                Chain.ETHEREUM: "8Lz789DP5VKLXumTMTgygjU2xtuzx8AhbaacgN5PYCAs",
                Chain.BASE: "71ZTy1veF9twER9CLMnPWeLQ7GZcwKsjmygejrgKirqs",
            },
            "spark": {
                Chain.ETHEREUM: "GbKdmBe4ycCYCQLQSjqGg6UHYoYfbyJyq5WrG35pv1si",
            },
        }

        protocol_subgraphs = subgraph_ids.get(protocol, {})
        subgraph_id = protocol_subgraphs.get(chain_enum)

        if not subgraph_id:
            return None

        # Use The Graph Gateway URL pattern
        return f"https://gateway.thegraph.com/api/subgraphs/id/{subgraph_id}"

    async def _fetch_aave_v3_apy(
        self,
        subgraph_url: str,
        market: str,
        chain: str,
        start_timestamp: int,
        end_timestamp: int,
    ) -> dict[datetime, APYSnapshot]:
        """Fetch APY from Aave V3 subgraph using reserveParamsHistoryItems."""
        snapshots: dict[datetime, APYSnapshot] = {}

        # First, find the reserve ID for this market
        reserve_id = await self._find_aave_reserve_id(subgraph_url, market, chain)
        if not reserve_id:
            logger.warning("Could not find Aave V3 reserve for %s on %s", market, chain)
            return snapshots

        # Query reserveParamsHistoryItems
        query = """
        query GetReserveParamsHistory($reserveId: String!, $startTimestamp: Int!, $endTimestamp: Int!) {
            reserveParamsHistoryItems(
                first: 1000
                where: {
                    reserve_: { id: $reserveId }
                    timestamp_gte: $startTimestamp
                    timestamp_lte: $endTimestamp
                }
                orderBy: timestamp
                orderDirection: asc
            ) {
                timestamp
                liquidityRate
                variableBorrowRate
            }
        }
        """

        variables = {
            "reserveId": reserve_id,
            "startTimestamp": start_timestamp,
            "endTimestamp": end_timestamp,
        }

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=SUBGRAPH_REQUEST_TIMEOUT_SECONDS)
        ) as session:
            data = await self._execute_subgraph_query(session, subgraph_url, query, variables)
            items = data.get("reserveParamsHistoryItems", [])

            # RAY units (1e27)
            ray = Decimal("1e27")

            for item in items:
                ts = int(item.get("timestamp", 0))
                snapshot_time = datetime.fromtimestamp(ts, tz=UTC)

                try:
                    supply_apy = Decimal(str(item.get("liquidityRate", "0"))) / ray
                    borrow_apy = Decimal(str(item.get("variableBorrowRate", "0"))) / ray
                except Exception:
                    continue

                snapshots[snapshot_time] = APYSnapshot(
                    timestamp=snapshot_time,
                    supply_apy=supply_apy,
                    borrow_apy=borrow_apy,
                    market=market.upper(),
                    protocol="aave_v3",
                )

        return snapshots

    async def _find_aave_reserve_id(
        self,
        subgraph_url: str,
        market: str,
        chain: str,
    ) -> str | None:
        """Find Aave reserve ID by market symbol."""
        query = """
        query FindReserve($symbol: String!) {
            reserves(where: { symbol: $symbol }, first: 1) {
                id
                symbol
            }
        }
        """

        variables = {"symbol": market.upper()}

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=SUBGRAPH_REQUEST_TIMEOUT_SECONDS)
        ) as session:
            data = await self._execute_subgraph_query(session, subgraph_url, query, variables)
            reserves = data.get("reserves", [])
            if reserves:
                return reserves[0].get("id")
        return None

    async def _fetch_compound_v3_apy(
        self,
        subgraph_url: str,
        market: str,
        chain: str,
        start_day: int,
        end_day: int,
    ) -> dict[datetime, APYSnapshot]:
        """Fetch APY from Compound V3 subgraph using DailyMarketAccounting."""
        snapshots: dict[datetime, APYSnapshot] = {}

        # Compound V3 uses comet addresses as market IDs
        # Query DailyMarketAccounting
        query = """
        query GetDailyMarketAccounting($marketSymbol: String!, $startDay: Int!, $endDay: Int!) {
            dailyMarketAccountings(
                first: 1000
                where: {
                    market_: { configuration_: { symbol_contains_nocase: $marketSymbol } }
                    day_gte: $startDay
                    day_lte: $endDay
                }
                orderBy: day
                orderDirection: asc
            ) {
                day
                supplyApr
                borrowApr
            }
        }
        """

        variables = {
            "marketSymbol": market.upper(),
            "startDay": start_day,
            "endDay": end_day,
        }

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=SUBGRAPH_REQUEST_TIMEOUT_SECONDS)
        ) as session:
            data = await self._execute_subgraph_query(session, subgraph_url, query, variables)
            items = data.get("dailyMarketAccountings", [])

            for item in items:
                day_num = int(item.get("day", 0))
                snapshot_date = date(1970, 1, 1) + timedelta(days=day_num)
                snapshot_time = datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC)

                try:
                    # Compound V3 APR is already in decimal format
                    supply_apy = Decimal(str(item.get("supplyApr", "0")))
                    borrow_apy = Decimal(str(item.get("borrowApr", "0")))
                except Exception:
                    continue

                snapshots[snapshot_time] = APYSnapshot(
                    timestamp=snapshot_time,
                    supply_apy=supply_apy,
                    borrow_apy=borrow_apy,
                    market=market.upper(),
                    protocol="compound_v3",
                )

        return snapshots

    async def _fetch_messari_apy(
        self,
        subgraph_url: str,
        market: str,
        chain: str,
        start_day: int,
        end_day: int,
        protocol: str,
    ) -> dict[datetime, APYSnapshot]:
        """Fetch APY from Messari-schema subgraph (Morpho Blue, Spark)."""
        snapshots: dict[datetime, APYSnapshot] = {}

        # First, find market ID by input token symbol
        market_id = await self._find_messari_market_id(subgraph_url, market)
        if not market_id:
            logger.warning("Could not find %s market for %s", protocol, market)
            return snapshots

        # Query MarketDailySnapshot
        query = """
        query GetMarketDailySnapshots($marketId: String!, $startDay: Int!, $endDay: Int!) {
            marketDailySnapshots(
                first: 1000
                where: {
                    market_: { id: $marketId }
                    days_gte: $startDay
                    days_lte: $endDay
                }
                orderBy: days
                orderDirection: asc
            ) {
                days
                rates {
                    rate
                    side
                }
            }
        }
        """

        variables = {
            "marketId": market_id,
            "startDay": start_day,
            "endDay": end_day,
        }

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=SUBGRAPH_REQUEST_TIMEOUT_SECONDS)
        ) as session:
            data = await self._execute_subgraph_query(session, subgraph_url, query, variables)
            items = data.get("marketDailySnapshots", [])

            for item in items:
                day_num = int(item.get("days", 0))
                snapshot_date = date(1970, 1, 1) + timedelta(days=day_num)
                snapshot_time = datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC)

                supply_apy = Decimal("0")
                borrow_apy = Decimal("0")

                rates = item.get("rates", [])
                for rate_info in rates:
                    try:
                        # Messari rates are percentages, divide by 100
                        rate_value = Decimal(str(rate_info.get("rate", "0"))) / Decimal("100")
                        side = rate_info.get("side", "")
                        if side == "LENDER":
                            supply_apy = rate_value
                        elif side == "BORROWER":
                            borrow_apy = rate_value
                    except Exception:
                        continue

                snapshots[snapshot_time] = APYSnapshot(
                    timestamp=snapshot_time,
                    supply_apy=supply_apy,
                    borrow_apy=borrow_apy,
                    market=market.upper(),
                    protocol=protocol,
                )

        return snapshots

    async def _find_messari_market_id(
        self,
        subgraph_url: str,
        market: str,
    ) -> str | None:
        """Find Messari market ID by input token symbol."""
        query = """
        query FindMarket($symbol: String!) {
            markets(where: { inputToken_: { symbol: $symbol } }, first: 1) {
                id
            }
        }
        """

        variables = {"symbol": market.upper()}

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=SUBGRAPH_REQUEST_TIMEOUT_SECONDS)
        ) as session:
            data = await self._execute_subgraph_query(session, subgraph_url, query, variables)
            markets = data.get("markets", [])
            if markets:
                return markets[0].get("id")
        return None

    # =========================================================================
    # Persistent Cache Management
    # =========================================================================

    def _get_cache_file_path(self, cache_key: str) -> Path | None:
        """Get the file path for a cache entry.

        Args:
            cache_key: The cache key to get path for

        Returns:
            Path to cache file, or None if caching disabled
        """
        if not self.enable_persistent_cache or not self.cache_directory:
            return None

        # Create a safe filename from cache key
        safe_key = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
        return Path(self.cache_directory) / f"historical_{safe_key}.pkl"

    def _load_cache_metadata(self) -> None:
        """Load cache metadata from disk."""
        if not self.cache_directory:
            return

        metadata_path = Path(self.cache_directory) / CACHE_METADATA_FILE
        if metadata_path.exists():
            try:
                with open(metadata_path) as f:
                    raw_metadata = json.load(f)

                # Convert to CacheMetadata objects
                for key, value in raw_metadata.items():
                    self._cache_metadata[key] = CacheMetadata(
                        version=value.get("version", CACHE_VERSION),
                        created_at=datetime.fromisoformat(value["created_at"]),
                        source=value["source"],
                        identifier=value["identifier"],
                        start_time=datetime.fromisoformat(value["start_time"]),
                        end_time=datetime.fromisoformat(value["end_time"]),
                        data_points=value["data_points"],
                        checksum=value["checksum"],
                    )
                logger.debug("Loaded cache metadata with %d entries", len(self._cache_metadata))
            except Exception as e:
                logger.warning("Failed to load cache metadata: %s", e)

    def _save_cache_metadata(self) -> None:
        """Save cache metadata to disk."""
        if not self.cache_directory:
            return

        metadata_path = Path(self.cache_directory) / CACHE_METADATA_FILE
        try:
            raw_metadata = {
                key: {
                    "version": meta.version,
                    "created_at": meta.created_at.isoformat(),
                    "source": meta.source,
                    "identifier": meta.identifier,
                    "start_time": meta.start_time.isoformat(),
                    "end_time": meta.end_time.isoformat(),
                    "data_points": meta.data_points,
                    "checksum": meta.checksum,
                }
                for key, meta in self._cache_metadata.items()
            }
            with open(metadata_path, "w") as f:
                json.dump(raw_metadata, f, indent=2)
        except Exception as e:
            logger.warning("Failed to save cache metadata: %s", e)

    def _load_from_persistent_cache(
        self,
        cache_key: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[datetime, Any] | None:
        """Load data from persistent cache if available and valid.

        Args:
            cache_key: Cache key to load
            start_time: Required start time
            end_time: Required end time

        Returns:
            Cached data or None if not available/valid
        """
        if not self.enable_persistent_cache:
            return None

        # Check metadata
        metadata = self._cache_metadata.get(cache_key)
        if not metadata:
            return None

        # Check if cache covers requested time range
        if metadata.start_time > start_time or metadata.end_time < end_time:
            logger.debug(
                "Cache for %s doesn't cover requested range (%s-%s vs %s-%s)",
                cache_key,
                metadata.start_time.isoformat(),
                metadata.end_time.isoformat(),
                start_time.isoformat(),
                end_time.isoformat(),
            )
            return None

        # Check cache age
        cache_age = datetime.now(UTC) - metadata.created_at
        if cache_age.days > self.max_cache_age_days:
            logger.debug(
                "Cache for %s is too old (%d days)",
                cache_key,
                cache_age.days,
            )
            return None

        # Load actual data
        cache_file = self._get_cache_file_path(cache_key)
        if not cache_file or not cache_file.exists():
            return None

        try:
            with open(cache_file, "rb") as f:
                data = pickle.load(f)

            # Verify checksum
            data_bytes = pickle.dumps(data)
            checksum = hashlib.sha256(data_bytes).hexdigest()
            if checksum != metadata.checksum:
                logger.warning("Cache checksum mismatch for %s, invalidating", cache_key)
                return None

            return data

        except Exception as e:
            logger.warning("Failed to load cache for %s: %s", cache_key, e)
            return None

    def _save_to_persistent_cache(
        self,
        cache_key: str,
        data: dict[datetime, Any],
        start_time: datetime,
        end_time: datetime,
        source: str,
    ) -> None:
        """Save data to persistent cache.

        Args:
            cache_key: Cache key to save under
            data: Data to cache
            start_time: Data start time
            end_time: Data end time
            source: Data source identifier
        """
        if not self.enable_persistent_cache or not self.cache_directory:
            return

        cache_file = self._get_cache_file_path(cache_key)
        if not cache_file:
            return

        try:
            # Serialize and compute checksum
            data_bytes = pickle.dumps(data)
            checksum = hashlib.sha256(data_bytes).hexdigest()

            # Save data
            with open(cache_file, "wb") as f:
                pickle.dump(data, f)

            # Update metadata
            # Extract identifier from cache key (format: "type:protocol:chain:identifier")
            parts = cache_key.split(":")
            identifier = parts[-1] if parts else cache_key

            self._cache_metadata[cache_key] = CacheMetadata(
                version=CACHE_VERSION,
                created_at=datetime.now(UTC),
                source=source,
                identifier=identifier,
                start_time=start_time,
                end_time=end_time,
                data_points=len(data),
                checksum=checksum,
            )
            self._save_cache_metadata()

            logger.debug(
                "Saved %d data points to cache for %s",
                len(data),
                cache_key,
            )

        except Exception as e:
            logger.warning("Failed to save cache for %s: %s", cache_key, e)

    # =========================================================================
    # Partial Fetch Resume Support
    # =========================================================================

    def _save_partial_fetch_state(
        self,
        cache_key: str,
        state: dict[str, Any],
    ) -> None:
        """Save partial fetch state for resume on interruption.

        Args:
            cache_key: Cache key for this fetch operation
            state: State to save (last timestamp, data collected, etc.)
        """
        self._partial_fetch_state[cache_key] = state

        if self.enable_persistent_cache and self.cache_directory:
            state_file = (
                Path(self.cache_directory) / f"partial_{hashlib.sha256(cache_key.encode()).hexdigest()[:16]}.json"
            )
            try:
                # Convert datetime keys to strings for JSON
                serializable_state: dict[str, Any] = {}
                for k, v in state.items():
                    if isinstance(v, datetime):
                        serializable_state[k] = v.isoformat()
                    elif isinstance(v, dict):
                        serializable_state[k] = {
                            (ts.isoformat() if isinstance(ts, datetime) else str(ts)): (
                                val if isinstance(val, int | float | str) else str(val)
                            )
                            for ts, val in v.items()
                        }
                    else:
                        serializable_state[k] = v

                with open(state_file, "w") as f:
                    json.dump(serializable_state, f)
            except Exception as e:
                logger.debug("Failed to save partial fetch state: %s", e)

    def _load_partial_fetch_state(
        self,
        cache_key: str,
    ) -> dict[str, Any] | None:
        """Load partial fetch state for resume.

        Args:
            cache_key: Cache key for the fetch operation

        Returns:
            Saved state or None if not available
        """
        # Check in-memory first
        if cache_key in self._partial_fetch_state:
            return self._partial_fetch_state[cache_key]

        # Check disk
        if self.enable_persistent_cache and self.cache_directory:
            state_file = (
                Path(self.cache_directory) / f"partial_{hashlib.sha256(cache_key.encode()).hexdigest()[:16]}.json"
            )
            if state_file.exists():
                try:
                    with open(state_file) as f:
                        state = json.load(f)

                    # Convert ISO strings back to datetime where needed
                    if "last_timestamp" in state and isinstance(state["last_timestamp"], str):
                        state["last_timestamp"] = datetime.fromisoformat(state["last_timestamp"])

                    return state
                except Exception as e:
                    logger.debug("Failed to load partial fetch state: %s", e)

        return None

    def _clear_partial_fetch_state(
        self,
        cache_key: str,
    ) -> None:
        """Clear partial fetch state after successful completion.

        Args:
            cache_key: Cache key to clear state for
        """
        self._partial_fetch_state.pop(cache_key, None)

        if self.enable_persistent_cache and self.cache_directory:
            state_file = (
                Path(self.cache_directory) / f"partial_{hashlib.sha256(cache_key.encode()).hexdigest()[:16]}.json"
            )
            if state_file.exists():
                try:
                    state_file.unlink()
                except Exception:
                    pass

    # =========================================================================
    # Data Coverage Validation
    # =========================================================================

    def validate_data_coverage(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        expected_interval_hours: int = 24,
    ) -> DataCoverageReport:
        """Validate data coverage and report gaps.

        Analyzes all cached data to determine coverage percentage and
        identify any gaps in the historical data.

        Args:
            start_time: Start of the period to validate
            end_time: End of the period to validate
            expected_interval_hours: Expected interval between data points (default 24h)

        Returns:
            DataCoverageReport with coverage statistics and gaps

        Example:
            report = loader.validate_data_coverage(
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 6, 1, tzinfo=UTC),
            )
            print(f"Overall coverage: {report.coverage_pct:.1f}%")
            for gap in report.gaps:
                print(f"  Gap: {gap.start} to {gap.end} ({gap.gap_duration})")
        """
        # Ensure timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        # Calculate expected data points
        total_hours = (end_time - start_time).total_seconds() / 3600
        expected_points = int(total_hours / expected_interval_hours) + 1

        gaps: list[DataGap] = []
        by_source: dict[str, float] = {}
        total_available = 0

        # Analyze each cache entry
        for cache_key, data in self.cache.items():
            source = cache_key.split(":")[0]
            identifier = cache_key.split(":")[-1]

            # Filter data within time range
            in_range = {ts: v for ts, v in data.items() if start_time <= ts <= end_time}
            available = len(in_range)
            total_available += available

            # Calculate source-specific coverage
            if available > 0:
                source_coverage = (available / expected_points) * 100
                if source in by_source:
                    by_source[source] = max(by_source[source], source_coverage)
                else:
                    by_source[source] = source_coverage

            # Find gaps
            source_gaps = self._find_gaps_in_data(
                sorted(in_range.keys()),
                start_time,
                end_time,
                expected_interval_hours,
                f"{source}:{identifier}",
            )
            gaps.extend(source_gaps)

        # Also analyze APY cache
        for cache_key, apy_data in self._apy_cache.items():
            source = cache_key.split(":")[0]
            identifier = cache_key.split(":")[-1]

            # Filter data within time range
            apy_in_range = {ts: v for ts, v in apy_data.items() if start_time <= ts <= end_time}
            available = len(apy_in_range)
            total_available += available

            # Calculate source-specific coverage
            if available > 0:
                source_coverage = (available / expected_points) * 100
                key = f"{source}:{identifier}"
                if key in by_source:
                    by_source[key] = max(by_source[key], source_coverage)
                else:
                    by_source[key] = source_coverage

        # Calculate overall coverage
        # Use the number of unique sources to avoid double-counting
        num_sources = max(1, len(self.cache) + len(self._apy_cache))
        coverage_pct = (total_available / (expected_points * num_sources)) * 100 if num_sources > 0 else 0

        return DataCoverageReport(
            total_expected_points=expected_points * num_sources,
            total_available_points=total_available,
            coverage_pct=min(100.0, coverage_pct),
            gaps=gaps,
            by_source=by_source,
        )

    def _find_gaps_in_data(
        self,
        timestamps: list[datetime],
        start_time: datetime,
        end_time: datetime,
        expected_interval_hours: int,
        identifier: str,
    ) -> list[DataGap]:
        """Find gaps in a series of timestamps.

        Args:
            timestamps: Sorted list of timestamps
            start_time: Expected start time
            end_time: Expected end time
            expected_interval_hours: Expected interval between points
            identifier: Identifier for gap reporting

        Returns:
            List of detected gaps
        """
        gaps: list[DataGap] = []
        expected_interval = timedelta(hours=expected_interval_hours)
        gap_threshold = expected_interval * 2  # Consider a gap if > 2x expected interval

        if not timestamps:
            # Entire range is a gap
            gaps.append(
                DataGap(
                    start=start_time,
                    end=end_time,
                    gap_duration=end_time - start_time,
                    data_type=identifier.split(":")[0],
                    identifier=identifier,
                )
            )
            return gaps

        # Check gap at start
        if timestamps[0] - start_time > gap_threshold:
            gaps.append(
                DataGap(
                    start=start_time,
                    end=timestamps[0],
                    gap_duration=timestamps[0] - start_time,
                    data_type=identifier.split(":")[0],
                    identifier=identifier,
                )
            )

        # Check gaps between points
        for i in range(1, len(timestamps)):
            gap = timestamps[i] - timestamps[i - 1]
            if gap > gap_threshold:
                gaps.append(
                    DataGap(
                        start=timestamps[i - 1],
                        end=timestamps[i],
                        gap_duration=gap,
                        data_type=identifier.split(":")[0],
                        identifier=identifier,
                    )
                )

        # Check gap at end
        if end_time - timestamps[-1] > gap_threshold:
            gaps.append(
                DataGap(
                    start=timestamps[-1],
                    end=end_time,
                    gap_duration=end_time - timestamps[-1],
                    data_type=identifier.split(":")[0],
                    identifier=identifier,
                )
            )

        return gaps

    def get_cached_apy(
        self,
        protocol: str,
        market: str,
        chain: str,
        timestamp: datetime,
    ) -> APYSnapshot | None:
        """Get a cached APY from previously fetched data.

        Finds the closest APY at or before the requested timestamp.

        Args:
            protocol: Lending protocol identifier
            market: Market symbol
            chain: Chain identifier
            timestamp: The target timestamp to look up

        Returns:
            The APY snapshot at or before timestamp, or None if not found.
        """
        cache_key = f"apy:{protocol.lower()}:{chain}:{market.upper()}"
        if cache_key not in self._apy_cache:
            return None

        snapshots = self._apy_cache[cache_key]
        # Find the closest timestamp <= target
        valid_timestamps = [ts for ts in snapshots if ts <= timestamp]
        if not valid_timestamps:
            return None

        closest = max(valid_timestamps)
        return snapshots[closest]
