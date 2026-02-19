"""Historical liquidity depth provider for accurate slippage modeling.

This module provides a liquidity depth provider that fetches historical liquidity
data from various DEX subgraphs. It supports both concentrated liquidity pools
(V3-style with tick data) and constant product pools (V2-style with reserves).

Key Features:
    - Query tick liquidity from V3 subgraphs (Uniswap V3, SushiSwap V3, PancakeSwap V3)
    - Query reserves from V2 subgraphs (Aerodrome, Uniswap V2-style)
    - Support time-weighted average depth calculation
    - Return LiquidityResult with appropriate confidence based on data source

Example:
    from almanak.framework.backtesting.pnl.providers.liquidity_depth import (
        LiquidityDepthProvider,
    )
    from almanak.core.enums import Chain, Protocol
    from datetime import datetime, UTC

    provider = LiquidityDepthProvider()
    async with provider:
        # Query liquidity at a specific timestamp
        liquidity = await provider.get_liquidity_depth(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain=Chain.ARBITRUM,
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            protocol=Protocol.UNISWAP_V3,
        )
        print(f"Liquidity depth: ${liquidity.depth}")
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.core.enums import Chain, Protocol

from ..types import DataConfidence, DataSourceInfo, LiquidityResult
from .base import HistoricalLiquidityProvider
from .dex import (
    AERODROME_SUBGRAPH_IDS,
    BALANCER_SUBGRAPH_IDS,
    CURVE_SUBGRAPH_IDS,
    PANCAKESWAP_V3_SUBGRAPH_IDS,
    SUSHISWAP_V3_SUBGRAPH_IDS,
    TRADERJOE_V2_SUBGRAPH_IDS,
    UNISWAP_V3_SUBGRAPH_IDS,
)
from .subgraph_client import (
    SubgraphClient,
    SubgraphClientConfig,
    SubgraphQueryError,
    SubgraphRateLimitError,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Data source identifiers
DATA_SOURCE_UNISWAP_V3 = "uniswap_v3_subgraph"
DATA_SOURCE_SUSHISWAP_V3 = "sushiswap_v3_subgraph"
DATA_SOURCE_PANCAKESWAP_V3 = "pancakeswap_v3_subgraph"
DATA_SOURCE_AERODROME = "aerodrome_subgraph"
DATA_SOURCE_TRADERJOE_V2 = "traderjoe_v2_subgraph"
DATA_SOURCE_CURVE = "curve_subgraph"
DATA_SOURCE_BALANCER = "balancer_subgraph"
DATA_SOURCE_FALLBACK = "liquidity_fallback"

# Protocol types for routing
V3_PROTOCOLS = ["uniswap_v3", "sushiswap_v3", "pancakeswap_v3"]
V2_PROTOCOLS = ["aerodrome"]  # Solidly-style AMMs
LIQUIDITY_BOOK_PROTOCOLS = ["traderjoe_v2"]  # Bin-based liquidity
WEIGHTED_POOL_PROTOCOLS = ["balancer"]
STABLESWAP_PROTOCOLS = ["curve"]

# Default window for time-weighted average (in hours)
DEFAULT_TWAP_WINDOW_HOURS = 24

# Protocol to subgraph IDs mapping
PROTOCOL_SUBGRAPH_IDS: dict[str, dict[Chain, str]] = {
    "uniswap_v3": UNISWAP_V3_SUBGRAPH_IDS,
    "sushiswap_v3": SUSHISWAP_V3_SUBGRAPH_IDS,
    "pancakeswap_v3": PANCAKESWAP_V3_SUBGRAPH_IDS,
    "aerodrome": AERODROME_SUBGRAPH_IDS,
    "traderjoe_v2": TRADERJOE_V2_SUBGRAPH_IDS,
    "curve": CURVE_SUBGRAPH_IDS,
    "balancer": BALANCER_SUBGRAPH_IDS,
}

# Protocol to data source mapping
PROTOCOL_DATA_SOURCE: dict[str, str] = {
    "uniswap_v3": DATA_SOURCE_UNISWAP_V3,
    "sushiswap_v3": DATA_SOURCE_SUSHISWAP_V3,
    "pancakeswap_v3": DATA_SOURCE_PANCAKESWAP_V3,
    "aerodrome": DATA_SOURCE_AERODROME,
    "traderjoe_v2": DATA_SOURCE_TRADERJOE_V2,
    "curve": DATA_SOURCE_CURVE,
    "balancer": DATA_SOURCE_BALANCER,
}

# Supported chains overall
SUPPORTED_CHAINS: list[Chain] = [
    Chain.ETHEREUM,
    Chain.ARBITRUM,
    Chain.BASE,
    Chain.OPTIMISM,
    Chain.POLYGON,
    Chain.AVALANCHE,
    Chain.BSC,
]


# =============================================================================
# GraphQL Queries
# =============================================================================

# V3-style pools: Query pool daily snapshots for TVL/liquidity
# poolDayDatas gives us tvlUSD which is the total value locked
V3_POOL_DAY_DATA_QUERY = """
query GetPoolLiquidity($poolAddress: String!, $startDate: Int!, $endDate: Int!) {
    poolDayDatas(
        first: 1000
        where: {
            pool: $poolAddress
            date_gte: $startDate
            date_lte: $endDate
        }
        orderBy: date
        orderDirection: desc
    ) {
        id
        date
        tvlUSD
        liquidity
    }
}
"""

# V3-style pools: Query current pool state for point-in-time liquidity
V3_POOL_QUERY = """
query GetPool($poolAddress: ID!) {
    pool(id: $poolAddress) {
        id
        totalValueLockedUSD
        liquidity
        sqrtPrice
        tick
    }
}
"""

# V2/Solidly-style pools: Query pair daily data for reserves
V2_PAIR_DAY_DATA_QUERY = """
query GetPairLiquidity($pairAddress: String!, $startDate: Int!, $endDate: Int!) {
    pairDayDatas(
        first: 1000
        where: {
            pairAddress: $pairAddress
            date_gte: $startDate
            date_lte: $endDate
        }
        orderBy: date
        orderDirection: desc
    ) {
        id
        date
        reserveUSD
        dailyVolumeUSD
    }
}
"""

# Liquidity Book (TraderJoe V2): Query lb pair daily data
LB_PAIR_DAY_DATA_QUERY = """
query GetLBPairLiquidity($lbPairAddress: String!, $startDate: Int!, $endDate: Int!) {
    lbPairDayDatas(
        first: 1000
        where: {
            lbPair: $lbPairAddress
            date_gte: $startDate
            date_lte: $endDate
        }
        orderBy: date
        orderDirection: desc
    ) {
        id
        date
        totalValueLockedUSD
        volumeUSD
    }
}
"""

# Balancer: Query pool snapshots
BALANCER_POOL_SNAPSHOT_QUERY = """
query GetPoolSnapshots($poolAddress: String!, $startTimestamp: Int!, $endTimestamp: Int!) {
    poolSnapshots(
        first: 1000
        where: {
            pool: $poolAddress
            timestamp_gte: $startTimestamp
            timestamp_lte: $endTimestamp
        }
        orderBy: timestamp
        orderDirection: desc
    ) {
        id
        timestamp
        liquidity
        swapVolume
        swapFees
    }
}
"""

# Curve (Messari schema): Query pool daily snapshots
CURVE_POOL_DAILY_QUERY = """
query GetPoolLiquidity($poolAddress: String!, $startDay: Int!, $endDay: Int!) {
    liquidityPoolDailySnapshots(
        first: 1000
        where: {
            pool: $poolAddress
            day_gte: $startDay
            day_lte: $endDay
        }
        orderBy: day
        orderDirection: desc
    ) {
        id
        day
        totalValueLockedUSD
        dailyVolumeUSD
    }
}
"""


# =============================================================================
# LiquidityDepthProvider
# =============================================================================


class LiquidityDepthProvider(HistoricalLiquidityProvider):
    """Historical liquidity depth provider for multiple DEX protocols.

    Fetches historical liquidity depth data from various DEX subgraphs for
    accurate slippage modeling in backtesting. Supports both concentrated
    liquidity pools (V3-style) and constant product pools (V2-style).

    Attributes:
        client: SubgraphClient for querying The Graph
        fallback_depth: Depth to return when subgraph data unavailable
        use_twap: Whether to use time-weighted average depth
        twap_window_hours: Window size for TWAP calculation

    Example:
        provider = LiquidityDepthProvider()

        # Use as async context manager
        async with provider:
            liquidity = await provider.get_liquidity_depth(
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain=Chain.ARBITRUM,
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                protocol=Protocol.UNISWAP_V3,
            )

        # Or manually close
        provider = LiquidityDepthProvider()
        try:
            liquidity = await provider.get_liquidity_depth(...)
        finally:
            await provider.close()
    """

    def __init__(
        self,
        client: SubgraphClient | None = None,
        fallback_depth: Decimal = Decimal("0"),
        use_twap: bool = False,
        twap_window_hours: int = DEFAULT_TWAP_WINDOW_HOURS,
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the liquidity depth provider.

        Args:
            client: Optional SubgraphClient instance. If None, creates one
                    using THEGRAPH_API_KEY from environment.
            fallback_depth: Depth to return when subgraph data unavailable.
                           Default is 0, indicating no data.
            use_twap: Whether to use time-weighted average depth.
                     Default is False (returns point-in-time depth).
            twap_window_hours: Window size in hours for TWAP calculation.
                              Default is 24 hours.
            requests_per_minute: Rate limit for subgraph requests. Default 100.
        """
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            config = SubgraphClientConfig(requests_per_minute=requests_per_minute)
            self._client = SubgraphClient(config=config)
            self._owns_client = True

        self._fallback_depth = fallback_depth
        self._use_twap = use_twap
        self._twap_window_hours = twap_window_hours

        logger.debug(
            "Initialized LiquidityDepthProvider: fallback_depth=%s, use_twap=%s, twap_window_hours=%s",
            fallback_depth,
            use_twap,
            twap_window_hours,
        )

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def close(self) -> None:
        """Close the subgraph client and release resources."""
        if self._owns_client:
            await self._client.close()
        logger.debug("LiquidityDepthProvider closed")

    async def __aenter__(self) -> "LiquidityDepthProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the client."""
        await self.close()

    def _get_protocol_id(self, protocol: Protocol | str | None) -> str | None:
        """Normalize protocol to string identifier.

        Args:
            protocol: Protocol enum, string identifier, or None

        Returns:
            Lowercase string protocol identifier or None
        """
        if protocol is None:
            return None
        if isinstance(protocol, Protocol):
            return protocol.value.lower()
        return protocol.lower()

    def _get_subgraph_id(self, protocol_id: str, chain: Chain) -> str | None:
        """Get the subgraph ID for a protocol and chain.

        Args:
            protocol_id: Lowercase protocol identifier
            chain: The blockchain chain

        Returns:
            Subgraph deployment ID or None if not supported
        """
        subgraph_ids = PROTOCOL_SUBGRAPH_IDS.get(protocol_id)
        if subgraph_ids is None:
            return None
        return subgraph_ids.get(chain)

    def _detect_protocol_from_chain(self, chain: Chain) -> str | None:
        """Attempt to detect protocol based on chain.

        Uses heuristics based on chain-specific DEXs.

        Args:
            chain: The blockchain chain

        Returns:
            Best-guess protocol identifier or None
        """
        # Chain-specific DEX defaults
        chain_defaults: dict[Chain, str] = {
            Chain.BASE: "aerodrome",
            Chain.AVALANCHE: "traderjoe_v2",
        }

        if chain in chain_defaults:
            return chain_defaults[chain]

        # Default to Uniswap V3 for other chains
        if chain in UNISWAP_V3_SUBGRAPH_IDS:
            return "uniswap_v3"

        return None

    def _datetime_to_timestamp(self, dt: datetime) -> int:
        """Convert datetime to Unix timestamp.

        Args:
            dt: Datetime to convert

        Returns:
            Unix timestamp
        """
        return int(dt.timestamp())

    def _date_to_day_number(self, dt: datetime) -> int:
        """Convert datetime to Messari day number (days since epoch).

        Args:
            dt: Datetime to convert

        Returns:
            Day number (days since 1970-01-01)
        """
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return (dt.date() - epoch.date()).days

    def _create_fallback_result(self, timestamp: datetime) -> LiquidityResult:
        """Create a fallback LiquidityResult with LOW confidence.

        Args:
            timestamp: Timestamp for the result

        Returns:
            LiquidityResult with fallback depth and LOW confidence
        """
        return LiquidityResult(
            depth=self._fallback_depth,
            source_info=DataSourceInfo(
                source=DATA_SOURCE_FALLBACK,
                confidence=DataConfidence.LOW,
                timestamp=timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC),
            ),
        )

    def _calculate_twap_depth(
        self,
        data_points: list[tuple[datetime, Decimal]],
        target_timestamp: datetime,
        window_hours: int,
    ) -> Decimal:
        """Calculate time-weighted average depth.

        Args:
            data_points: List of (timestamp, depth) tuples, sorted by timestamp desc
            target_timestamp: The target timestamp
            window_hours: Window size in hours

        Returns:
            Time-weighted average depth
        """
        if not data_points:
            return Decimal("0")

        # Filter to window
        window_start = target_timestamp - timedelta(hours=window_hours)
        points_in_window = [(ts, depth) for ts, depth in data_points if window_start <= ts <= target_timestamp]

        if not points_in_window:
            # Use the most recent point if no points in window
            return data_points[0][1]

        if len(points_in_window) == 1:
            return points_in_window[0][1]

        # Sort by timestamp ascending for TWAP calculation
        points_in_window.sort(key=lambda x: x[0])

        # Calculate time-weighted average
        total_weight = Decimal("0")
        weighted_sum = Decimal("0")

        for i in range(len(points_in_window) - 1):
            ts1, depth1 = points_in_window[i]
            ts2, depth2 = points_in_window[i + 1]

            # Weight is the time duration this depth was active
            weight = Decimal(str((ts2 - ts1).total_seconds()))
            total_weight += weight
            # Use average of the two depths for this period
            avg_depth = (depth1 + depth2) / 2
            weighted_sum += weight * avg_depth

        if total_weight == 0:
            return points_in_window[-1][1]

        return weighted_sum / total_weight

    async def _query_v3_liquidity(
        self,
        pool_address: str,
        chain: Chain,
        timestamp: datetime,
        protocol_id: str,
    ) -> LiquidityResult | None:
        """Query liquidity from V3-style subgraph.

        Args:
            pool_address: Pool contract address
            chain: Blockchain chain
            timestamp: Target timestamp
            protocol_id: Protocol identifier

        Returns:
            LiquidityResult or None if query fails
        """
        subgraph_id = self._get_subgraph_id(protocol_id, chain)
        if subgraph_id is None:
            return None

        pool_address_lower = pool_address.lower()
        data_source = PROTOCOL_DATA_SOURCE.get(protocol_id, DATA_SOURCE_FALLBACK)

        # Calculate date range for query
        if self._use_twap:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(hours=self._twap_window_hours))
        else:
            # Query a small window around the target timestamp
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(days=1))

        end_timestamp = self._datetime_to_timestamp(timestamp)

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=V3_POOL_DAY_DATA_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startDate": start_timestamp,
                    "endDate": end_timestamp,
                },
            )

            pool_day_datas = data.get("poolDayDatas", [])

            if not pool_day_datas:
                logger.warning(
                    "No liquidity data from V3 subgraph: protocol=%s, chain=%s, pool=%s...",
                    protocol_id,
                    chain.value,
                    pool_address_lower[:10],
                )
                return None

            # Parse results
            data_points: list[tuple[datetime, Decimal]] = []
            for day_data in pool_day_datas:
                day_timestamp = int(day_data.get("date", 0))
                day_dt = datetime.fromtimestamp(day_timestamp, tz=UTC)
                tvl_usd = Decimal(str(day_data.get("tvlUSD", "0")))
                data_points.append((day_dt, tvl_usd))

            # Calculate depth (TWAP or point-in-time)
            if self._use_twap and len(data_points) > 1:
                depth = self._calculate_twap_depth(data_points, timestamp, self._twap_window_hours)
            else:
                # Use most recent data point
                depth = data_points[0][1] if data_points else Decimal("0")

            logger.info(
                "Fetched V3 liquidity: protocol=%s, chain=%s, pool=%s..., depth=$%s",
                protocol_id,
                chain.value,
                pool_address_lower[:10],
                depth,
            )

            return LiquidityResult(
                depth=depth,
                source_info=DataSourceInfo(
                    source=data_source,
                    confidence=DataConfidence.HIGH,
                    timestamp=timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC),
                ),
            )

        except (SubgraphRateLimitError, SubgraphQueryError) as e:
            logger.warning(
                "V3 subgraph error: protocol=%s, chain=%s, pool=%s...: %s",
                protocol_id,
                chain.value,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_v2_liquidity(
        self,
        pool_address: str,
        chain: Chain,
        timestamp: datetime,
        protocol_id: str,
    ) -> LiquidityResult | None:
        """Query liquidity from V2/Solidly-style subgraph.

        Args:
            pool_address: Pool contract address
            chain: Blockchain chain
            timestamp: Target timestamp
            protocol_id: Protocol identifier

        Returns:
            LiquidityResult or None if query fails
        """
        subgraph_id = self._get_subgraph_id(protocol_id, chain)
        if subgraph_id is None:
            return None

        pool_address_lower = pool_address.lower()
        data_source = PROTOCOL_DATA_SOURCE.get(protocol_id, DATA_SOURCE_FALLBACK)

        # Calculate date range for query
        if self._use_twap:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(hours=self._twap_window_hours))
        else:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(days=1))

        end_timestamp = self._datetime_to_timestamp(timestamp)

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=V2_PAIR_DAY_DATA_QUERY,
                variables={
                    "pairAddress": pool_address_lower,
                    "startDate": start_timestamp,
                    "endDate": end_timestamp,
                },
            )

            pair_day_datas = data.get("pairDayDatas", [])

            if not pair_day_datas:
                logger.warning(
                    "No liquidity data from V2 subgraph: protocol=%s, chain=%s, pool=%s...",
                    protocol_id,
                    chain.value,
                    pool_address_lower[:10],
                )
                return None

            # Parse results
            data_points: list[tuple[datetime, Decimal]] = []
            for day_data in pair_day_datas:
                day_timestamp = int(day_data.get("date", 0))
                day_dt = datetime.fromtimestamp(day_timestamp, tz=UTC)
                reserve_usd = Decimal(str(day_data.get("reserveUSD", "0")))
                data_points.append((day_dt, reserve_usd))

            # Calculate depth (TWAP or point-in-time)
            if self._use_twap and len(data_points) > 1:
                depth = self._calculate_twap_depth(data_points, timestamp, self._twap_window_hours)
            else:
                depth = data_points[0][1] if data_points else Decimal("0")

            logger.info(
                "Fetched V2 liquidity: protocol=%s, chain=%s, pool=%s..., depth=$%s",
                protocol_id,
                chain.value,
                pool_address_lower[:10],
                depth,
            )

            return LiquidityResult(
                depth=depth,
                source_info=DataSourceInfo(
                    source=data_source,
                    confidence=DataConfidence.HIGH,
                    timestamp=timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC),
                ),
            )

        except (SubgraphRateLimitError, SubgraphQueryError) as e:
            logger.warning(
                "V2 subgraph error: protocol=%s, chain=%s, pool=%s...: %s",
                protocol_id,
                chain.value,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_liquidity_book(
        self,
        pool_address: str,
        chain: Chain,
        timestamp: datetime,
        protocol_id: str,
    ) -> LiquidityResult | None:
        """Query liquidity from TraderJoe V2 Liquidity Book subgraph.

        Args:
            pool_address: Pool contract address
            chain: Blockchain chain
            timestamp: Target timestamp
            protocol_id: Protocol identifier

        Returns:
            LiquidityResult or None if query fails
        """
        subgraph_id = self._get_subgraph_id(protocol_id, chain)
        if subgraph_id is None:
            return None

        pool_address_lower = pool_address.lower()
        data_source = PROTOCOL_DATA_SOURCE.get(protocol_id, DATA_SOURCE_FALLBACK)

        # Calculate date range for query
        if self._use_twap:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(hours=self._twap_window_hours))
        else:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(days=1))

        end_timestamp = self._datetime_to_timestamp(timestamp)

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=LB_PAIR_DAY_DATA_QUERY,
                variables={
                    "lbPairAddress": pool_address_lower,
                    "startDate": start_timestamp,
                    "endDate": end_timestamp,
                },
            )

            lb_pair_day_datas = data.get("lbPairDayDatas", [])

            if not lb_pair_day_datas:
                logger.warning(
                    "No liquidity data from LB subgraph: protocol=%s, chain=%s, pool=%s...",
                    protocol_id,
                    chain.value,
                    pool_address_lower[:10],
                )
                return None

            # Parse results
            data_points: list[tuple[datetime, Decimal]] = []
            for day_data in lb_pair_day_datas:
                day_timestamp = int(day_data.get("date", 0))
                day_dt = datetime.fromtimestamp(day_timestamp, tz=UTC)
                tvl_usd = Decimal(str(day_data.get("totalValueLockedUSD", "0")))
                data_points.append((day_dt, tvl_usd))

            # Calculate depth (TWAP or point-in-time)
            if self._use_twap and len(data_points) > 1:
                depth = self._calculate_twap_depth(data_points, timestamp, self._twap_window_hours)
            else:
                depth = data_points[0][1] if data_points else Decimal("0")

            logger.info(
                "Fetched LB liquidity: protocol=%s, chain=%s, pool=%s..., depth=$%s",
                protocol_id,
                chain.value,
                pool_address_lower[:10],
                depth,
            )

            return LiquidityResult(
                depth=depth,
                source_info=DataSourceInfo(
                    source=data_source,
                    confidence=DataConfidence.HIGH,
                    timestamp=timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC),
                ),
            )

        except (SubgraphRateLimitError, SubgraphQueryError) as e:
            logger.warning(
                "LB subgraph error: protocol=%s, chain=%s, pool=%s...: %s",
                protocol_id,
                chain.value,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_balancer_liquidity(
        self,
        pool_address: str,
        chain: Chain,
        timestamp: datetime,
        protocol_id: str,
    ) -> LiquidityResult | None:
        """Query liquidity from Balancer subgraph.

        Args:
            pool_address: Pool contract address
            chain: Blockchain chain
            timestamp: Target timestamp
            protocol_id: Protocol identifier

        Returns:
            LiquidityResult or None if query fails
        """
        subgraph_id = self._get_subgraph_id(protocol_id, chain)
        if subgraph_id is None:
            return None

        pool_address_lower = pool_address.lower()
        data_source = PROTOCOL_DATA_SOURCE.get(protocol_id, DATA_SOURCE_FALLBACK)

        # Balancer V2 subgraph expects full pool ID (64 hex chars = address + type)
        # Warn if bare address is provided
        if len(pool_address_lower) == 42:
            logger.warning(
                "Balancer pool address appears to be a bare address (42 chars). "
                "Balancer V2 subgraph typically requires full pool ID (64 hex chars). "
                "Query may return no data. pool=%s...",
                pool_address_lower[:10],
            )

        # Calculate timestamp range for query
        if self._use_twap:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(hours=self._twap_window_hours))
        else:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(days=1))

        end_timestamp = self._datetime_to_timestamp(timestamp)

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=BALANCER_POOL_SNAPSHOT_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startTimestamp": start_timestamp,
                    "endTimestamp": end_timestamp,
                },
            )

            pool_snapshots = data.get("poolSnapshots", [])

            if not pool_snapshots:
                logger.warning(
                    "No liquidity data from Balancer subgraph: chain=%s, pool=%s...",
                    chain.value,
                    pool_address_lower[:10],
                )
                return None

            # Parse results
            data_points: list[tuple[datetime, Decimal]] = []
            for snapshot in pool_snapshots:
                snapshot_timestamp = int(snapshot.get("timestamp", 0))
                snapshot_dt = datetime.fromtimestamp(snapshot_timestamp, tz=UTC)
                # Balancer uses 'liquidity' field for TVL
                liquidity = Decimal(str(snapshot.get("liquidity", "0")))
                data_points.append((snapshot_dt, liquidity))

            # Calculate depth (TWAP or point-in-time)
            if self._use_twap and len(data_points) > 1:
                depth = self._calculate_twap_depth(data_points, timestamp, self._twap_window_hours)
            else:
                depth = data_points[0][1] if data_points else Decimal("0")

            logger.info(
                "Fetched Balancer liquidity: chain=%s, pool=%s..., depth=$%s",
                chain.value,
                pool_address_lower[:10],
                depth,
            )

            return LiquidityResult(
                depth=depth,
                source_info=DataSourceInfo(
                    source=data_source,
                    confidence=DataConfidence.HIGH,
                    timestamp=timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC),
                ),
            )

        except (SubgraphRateLimitError, SubgraphQueryError) as e:
            logger.warning(
                "Balancer subgraph error: chain=%s, pool=%s...: %s",
                chain.value,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_curve_liquidity(
        self,
        pool_address: str,
        chain: Chain,
        timestamp: datetime,
        protocol_id: str,
    ) -> LiquidityResult | None:
        """Query liquidity from Curve (Messari) subgraph.

        Args:
            pool_address: Pool contract address
            chain: Blockchain chain
            timestamp: Target timestamp
            protocol_id: Protocol identifier

        Returns:
            LiquidityResult or None if query fails
        """
        subgraph_id = self._get_subgraph_id(protocol_id, chain)
        if subgraph_id is None:
            return None

        pool_address_lower = pool_address.lower()
        data_source = PROTOCOL_DATA_SOURCE.get(protocol_id, DATA_SOURCE_FALLBACK)

        # Calculate day number range for Messari schema
        if self._use_twap:
            start_dt = timestamp - timedelta(hours=self._twap_window_hours)
        else:
            start_dt = timestamp - timedelta(days=1)

        start_day = self._date_to_day_number(start_dt)
        end_day = self._date_to_day_number(timestamp)

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=CURVE_POOL_DAILY_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startDay": start_day,
                    "endDay": end_day,
                },
            )

            pool_snapshots = data.get("liquidityPoolDailySnapshots", [])

            if not pool_snapshots:
                logger.warning(
                    "No liquidity data from Curve subgraph: chain=%s, pool=%s...",
                    chain.value,
                    pool_address_lower[:10],
                )
                return None

            # Parse results
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            data_points: list[tuple[datetime, Decimal]] = []
            for snapshot in pool_snapshots:
                day_num = int(snapshot.get("day", 0))
                snapshot_dt = epoch + timedelta(days=day_num)
                tvl_usd = Decimal(str(snapshot.get("totalValueLockedUSD", "0")))
                data_points.append((snapshot_dt, tvl_usd))

            # Calculate depth (TWAP or point-in-time)
            if self._use_twap and len(data_points) > 1:
                depth = self._calculate_twap_depth(data_points, timestamp, self._twap_window_hours)
            else:
                depth = data_points[0][1] if data_points else Decimal("0")

            logger.info(
                "Fetched Curve liquidity: chain=%s, pool=%s..., depth=$%s",
                chain.value,
                pool_address_lower[:10],
                depth,
            )

            return LiquidityResult(
                depth=depth,
                source_info=DataSourceInfo(
                    source=data_source,
                    confidence=DataConfidence.HIGH,
                    timestamp=timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC),
                ),
            )

        except (SubgraphRateLimitError, SubgraphQueryError) as e:
            logger.warning(
                "Curve subgraph error: chain=%s, pool=%s...: %s",
                chain.value,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def get_liquidity_depth(
        self,
        pool_address: str,
        chain: Chain,
        timestamp: datetime,
        protocol: Protocol | str | None = None,
    ) -> LiquidityResult:
        """Fetch historical liquidity depth for a pool at a specific timestamp.

        Routes the query to the appropriate DEX-specific subgraph based on
        the protocol parameter. If no protocol is specified, attempts to
        detect based on chain.

        For concentrated liquidity pools (V3-style), depth represents the
        total value locked (TVL) in USD. For constant product pools (V2-style),
        depth represents total reserves in USD.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on.
            timestamp: The point in time to get liquidity depth for.
            protocol: Protocol enum, string identifier (e.g., "curve"), or None.
                     If None, attempts to detect based on chain.

        Returns:
            LiquidityResult containing the liquidity depth in USD and source
            info with confidence level. Returns HIGH confidence for subgraph
            data, LOW confidence for fallback.

        Example:
            # With explicit protocol
            liquidity = await provider.get_liquidity_depth(
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain=Chain.ARBITRUM,
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                protocol=Protocol.UNISWAP_V3,
            )

            # Auto-detect protocol
            liquidity = await provider.get_liquidity_depth(
                pool_address="0x...",
                chain=Chain.BASE,
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )  # Will use Aerodrome for Base chain
        """
        # Normalize protocol identifier
        protocol_id = self._get_protocol_id(protocol)

        # If no protocol specified, try to detect from chain
        if protocol_id is None:
            protocol_id = self._detect_protocol_from_chain(chain)
            if protocol_id:
                logger.info(
                    "Auto-detected protocol %s for chain %s",
                    protocol_id,
                    chain.value,
                )

        # If still no protocol, return fallback
        if protocol_id is None:
            logger.warning(
                "Could not determine protocol for chain=%s, pool=%s..., returning fallback",
                chain.value,
                pool_address[:10],
            )
            return self._create_fallback_result(timestamp)

        # Ensure timestamp has timezone
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        # Check if chain is supported by this protocol
        subgraph_ids = PROTOCOL_SUBGRAPH_IDS.get(protocol_id)
        if subgraph_ids is None or chain not in subgraph_ids:
            logger.warning(
                "Chain %s not supported by protocol %s, returning fallback",
                chain.value,
                protocol_id,
            )
            return self._create_fallback_result(timestamp)

        # Route to the appropriate query method based on protocol type
        logger.info(
            "Querying liquidity depth: protocol=%s, chain=%s, pool=%s...",
            protocol_id,
            chain.value,
            pool_address[:10],
        )

        result: LiquidityResult | None = None

        try:
            if protocol_id in V3_PROTOCOLS:
                result = await self._query_v3_liquidity(pool_address, chain, timestamp, protocol_id)
            elif protocol_id in V2_PROTOCOLS:
                result = await self._query_v2_liquidity(pool_address, chain, timestamp, protocol_id)
            elif protocol_id in LIQUIDITY_BOOK_PROTOCOLS:
                result = await self._query_liquidity_book(pool_address, chain, timestamp, protocol_id)
            elif protocol_id in WEIGHTED_POOL_PROTOCOLS:
                result = await self._query_balancer_liquidity(pool_address, chain, timestamp, protocol_id)
            elif protocol_id in STABLESWAP_PROTOCOLS:
                result = await self._query_curve_liquidity(pool_address, chain, timestamp, protocol_id)
            else:
                logger.warning(
                    "Unknown protocol type for %s, returning fallback",
                    protocol_id,
                )

        except Exception as e:
            logger.error(
                "Unexpected error querying liquidity: protocol=%s, chain=%s, pool=%s...: %s",
                protocol_id,
                chain.value,
                pool_address[:10],
                str(e),
            )

        # Return result or fallback
        if result is not None:
            return result

        return self._create_fallback_result(timestamp)

    async def get_liquidity_depth_range(
        self,
        pool_address: str,
        chain: Chain,
        start_time: datetime,
        end_time: datetime,
        protocol: Protocol | str | None = None,
    ) -> list[LiquidityResult]:
        """Fetch historical liquidity depth for a date range.

        Convenience method that fetches liquidity depth for each day in the
        specified range. Useful for backtesting that requires daily liquidity
        snapshots.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on.
            start_time: Start of date range (inclusive).
            end_time: End of date range (inclusive).
            protocol: Protocol enum, string identifier, or None.

        Returns:
            List of LiquidityResult objects, one per day in the range.
        """
        results: list[LiquidityResult] = []
        current = start_time

        while current <= end_time:
            result = await self.get_liquidity_depth(
                pool_address=pool_address,
                chain=chain,
                timestamp=current,
                protocol=protocol,
            )
            results.append(result)
            current += timedelta(days=1)

        return results


__all__ = [
    "LiquidityDepthProvider",
    "DATA_SOURCE_UNISWAP_V3",
    "DATA_SOURCE_SUSHISWAP_V3",
    "DATA_SOURCE_PANCAKESWAP_V3",
    "DATA_SOURCE_AERODROME",
    "DATA_SOURCE_TRADERJOE_V2",
    "DATA_SOURCE_CURVE",
    "DATA_SOURCE_BALANCER",
    "DATA_SOURCE_FALLBACK",
    "V3_PROTOCOLS",
    "V2_PROTOCOLS",
    "LIQUIDITY_BOOK_PROTOCOLS",
    "WEIGHTED_POOL_PROTOCOLS",
    "STABLESWAP_PROTOCOLS",
    "SUPPORTED_CHAINS",
    "DEFAULT_TWAP_WINDOW_HOURS",
]
