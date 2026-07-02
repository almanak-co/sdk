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
    from datetime import datetime, UTC

    provider = LiquidityDepthProvider()
    async with provider:
        # Query liquidity at a specific timestamp
        liquidity = await provider.get_liquidity_depth(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain="arbitrum",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            protocol="uniswap_v3",
        )
        print(f"Liquidity depth: ${liquidity.depth}")
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry
from almanak.core.chains import ChainRegistry

from ...exceptions import DataSourceUnavailableError
from ..types import DataConfidence, DataSourceInfo, LiquidityResult
from .base import HistoricalLiquidityProvider
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

# Fallback data-source identifier. Per-protocol provenance derives as
# "<protocol>_subgraph" from the connector-declared dispatch key (the legacy
# DATA_SOURCE_* constants were exactly that pattern); AMM-family routing and
# chain defaults derive from each connector's DexVolumeDecl via
# DexVolumeRegistry (VIB-4851 Phase D).
DATA_SOURCE_FALLBACK = "liquidity_fallback"

# Default window for time-weighted average (in hours)
DEFAULT_TWAP_WINDOW_HOURS = 24

LIQUIDITY_QUERY_METHOD_BY_FAMILY = {
    "v3_concentrated": "_query_v3_liquidity",
    "solidly_v2": "_query_v2_liquidity",
    "liquidity_book": "_query_liquidity_book",
    "weighted": "_query_balancer_liquidity",
    "stableswap": "_query_curve_liquidity",
}


# Safety valve for cursor pagination (VIB-5089). Daily snapshots: 100 pages
# of 1000 covers ~270 years, so real windows never hit the valve.
MAX_PAGINATION_PAGES = 100


# =============================================================================
# GraphQL Queries
# =============================================================================
#
# All range queries are cursor-paginated (VIB-5089): ordered ascending by
# their time field, with the _gte lower bound advancing page by page. The
# provider sorts parsed data points by timestamp before use, so "most
# recent" selection does not depend on response order.

# V3-style pools: Query pool daily snapshots for TVL/liquidity
# poolDayDatas gives us tvlUSD which is the total value locked
V3_POOL_DAY_DATA_QUERY = """
query GetPoolLiquidity($first: Int!, $poolAddress: String!, $startDate: Int!, $endDate: Int!) {
    poolDayDatas(
        first: $first
        where: {
            pool: $poolAddress
            date_gte: $startDate
            date_lte: $endDate
        }
        orderBy: date
        orderDirection: asc
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
query GetPairLiquidity($first: Int!, $pairAddress: String!, $startDate: Int!, $endDate: Int!) {
    pairDayDatas(
        first: $first
        where: {
            pairAddress: $pairAddress
            date_gte: $startDate
            date_lte: $endDate
        }
        orderBy: date
        orderDirection: asc
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
query GetLBPairLiquidity($first: Int!, $lbPairAddress: String!, $startDate: Int!, $endDate: Int!) {
    lbPairDayDatas(
        first: $first
        where: {
            lbPair: $lbPairAddress
            date_gte: $startDate
            date_lte: $endDate
        }
        orderBy: date
        orderDirection: asc
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
query GetPoolSnapshots($first: Int!, $poolAddress: String!, $startTimestamp: Int!, $endTimestamp: Int!) {
    poolSnapshots(
        first: $first
        where: {
            pool: $poolAddress
            timestamp_gte: $startTimestamp
            timestamp_lte: $endTimestamp
        }
        orderBy: timestamp
        orderDirection: asc
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
query GetPoolLiquidity($first: Int!, $poolAddress: String!, $startDay: Int!, $endDay: Int!) {
    liquidityPoolDailySnapshots(
        first: $first
        where: {
            pool: $poolAddress
            day_gte: $startDay
            day_lte: $endDay
        }
        orderBy: day
        orderDirection: asc
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
                chain="arbitrum",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                protocol="uniswap_v3",
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
    def supported_chains(self) -> list[str]:
        """Chains any declared DEX serves liquidity data for (sorted)."""
        declared = DexVolumeRegistry.all_supported_chains()
        return [name for name in ChainRegistry.names() if name in declared]

    async def close(self) -> None:
        """Close the subgraph client and release resources."""
        if self._owns_client:
            await self._client.close()
        logger.debug("LiquidityDepthProvider closed")

    async def __aenter__(self) -> LiquidityDepthProvider:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the client."""
        await self.close()

    def _get_protocol_id(self, protocol: str | None) -> str | None:
        """Normalize protocol to its canonical declared identifier.

        Args:
            protocol: String protocol identifier, or None

        Returns:
            The canonical declared protocol key when the identifier (or one
            of its declared aliases, e.g. ``"uni_v3"``) is known; the raw
            lowercase string for unknown identifiers; None for None.
        """
        if protocol is None:
            return None
        proto_str = str(protocol).lower()
        # Unknown identifiers keep the raw string rather than None: in this
        # provider None means "auto-detect from chain", and an explicit but
        # unknown protocol must hit the warning + fallback path instead of
        # silently detecting a different DEX.
        return DexVolumeRegistry.canonical(proto_str) or proto_str

    def _data_source_for(self, protocol_id: str) -> str:
        """Provenance string for a protocol's liquidity rows.

        Derives "<protocol>_subgraph" for declared DEXes (byte-identical to
        the legacy per-protocol DATA_SOURCE_* constants) and falls back to
        :data:`DATA_SOURCE_FALLBACK` for unknown identifiers.
        """
        if DexVolumeRegistry.has(protocol_id):
            return f"{DexVolumeRegistry.canonical(protocol_id)}_subgraph"
        return DATA_SOURCE_FALLBACK

    def _get_subgraph_id(self, protocol_id: str, chain: str) -> str | None:
        """Get the subgraph ID for a protocol and chain.

        Args:
            protocol_id: Lowercase protocol identifier
            chain: The blockchain chain

        Returns:
            Subgraph deployment ID or None if not supported
        """
        subgraph_ids = DexVolumeRegistry.liquidity_subgraph_ids_for(protocol_id)
        if subgraph_ids is None:
            return None
        return subgraph_ids.get(chain)

    def _detect_protocol_from_chain(self, chain: str) -> str | None:
        """Attempt to detect protocol based on chain.

        Uses heuristics based on chain-specific DEXs.

        Args:
            chain: The blockchain chain

        Returns:
            Best-guess protocol identifier or None
        """
        # Connector-declared defaults: chain_default declarations win
        # (aerodrome on base, traderjoe_v2 on avalanche), then the
        # generic_default DEX (uniswap_v3) for any chain it supports.
        return DexVolumeRegistry.chain_default(chain)

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
                timestamp=self._ensure_utc_timestamp(timestamp),
            ),
        )

    def _select_depth(
        self,
        data_points: list[tuple[datetime, Decimal]],
        timestamp: datetime,
    ) -> Decimal:
        """Select the depth from parsed (timestamp, depth) data points.

        Sorts the points by timestamp ascending first, so the selection does
        not depend on subgraph response order (the cursor-paginated queries
        return ascending pages; VIB-5089).

        Args:
            data_points: List of (timestamp, depth) tuples
            timestamp: The target timestamp

        Returns:
            TWAP depth when enabled and multiple points exist, otherwise the
            most recent point's depth, or Decimal('0') if empty
        """
        if not data_points:
            return Decimal("0")
        data_points.sort(key=lambda point: point[0])
        if self._use_twap and len(data_points) > 1:
            return self._calculate_twap_depth(data_points, timestamp, self._twap_window_hours)
        return data_points[-1][1]

    def _calculate_twap_depth(
        self,
        data_points: list[tuple[datetime, Decimal]],
        target_timestamp: datetime,
        window_hours: int,
    ) -> Decimal:
        """Calculate time-weighted average depth.

        Args:
            data_points: List of (timestamp, depth) tuples, sorted by timestamp asc
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
            return data_points[-1][1]

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
        chain: str,
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
        data_source = self._data_source_for(protocol_id)

        # Calculate date range for query
        if self._use_twap:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(hours=self._twap_window_hours))
        else:
            # Query a small window around the target timestamp
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(days=1))

        end_timestamp = self._datetime_to_timestamp(timestamp)

        try:
            pool_day_datas = await self._client.query_with_pagination(
                subgraph_id=subgraph_id,
                query=V3_POOL_DAY_DATA_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startDate": start_timestamp,
                    "endDate": end_timestamp,
                },
                data_path="poolDayDatas",
                max_pages=MAX_PAGINATION_PAGES,
                cursor_field="date",
                cursor_variable="startDate",
            )

            if not pool_day_datas:
                logger.warning(
                    "No liquidity data from V3 subgraph: protocol=%s, chain=%s, pool=%s...",
                    protocol_id,
                    chain,
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
            depth = self._select_depth(data_points, timestamp)

            logger.info(
                "Fetched V3 liquidity: protocol=%s, chain=%s, pool=%s..., depth=$%s",
                protocol_id,
                chain,
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
                chain,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_v2_liquidity(
        self,
        pool_address: str,
        chain: str,
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
        data_source = self._data_source_for(protocol_id)

        # Calculate date range for query
        if self._use_twap:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(hours=self._twap_window_hours))
        else:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(days=1))

        end_timestamp = self._datetime_to_timestamp(timestamp)

        try:
            pair_day_datas = await self._client.query_with_pagination(
                subgraph_id=subgraph_id,
                query=V2_PAIR_DAY_DATA_QUERY,
                variables={
                    "pairAddress": pool_address_lower,
                    "startDate": start_timestamp,
                    "endDate": end_timestamp,
                },
                data_path="pairDayDatas",
                max_pages=MAX_PAGINATION_PAGES,
                cursor_field="date",
                cursor_variable="startDate",
            )

            if not pair_day_datas:
                logger.warning(
                    "No liquidity data from V2 subgraph: protocol=%s, chain=%s, pool=%s...",
                    protocol_id,
                    chain,
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
            depth = self._select_depth(data_points, timestamp)

            logger.info(
                "Fetched V2 liquidity: protocol=%s, chain=%s, pool=%s..., depth=$%s",
                protocol_id,
                chain,
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
                chain,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_liquidity_book(
        self,
        pool_address: str,
        chain: str,
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
        data_source = self._data_source_for(protocol_id)

        # Calculate date range for query
        if self._use_twap:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(hours=self._twap_window_hours))
        else:
            start_timestamp = self._datetime_to_timestamp(timestamp - timedelta(days=1))

        end_timestamp = self._datetime_to_timestamp(timestamp)

        try:
            lb_pair_day_datas = await self._client.query_with_pagination(
                subgraph_id=subgraph_id,
                query=LB_PAIR_DAY_DATA_QUERY,
                variables={
                    "lbPairAddress": pool_address_lower,
                    "startDate": start_timestamp,
                    "endDate": end_timestamp,
                },
                data_path="lbPairDayDatas",
                max_pages=MAX_PAGINATION_PAGES,
                cursor_field="date",
                cursor_variable="startDate",
            )

            if not lb_pair_day_datas:
                logger.warning(
                    "No liquidity data from LB subgraph: protocol=%s, chain=%s, pool=%s...",
                    protocol_id,
                    chain,
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
            depth = self._select_depth(data_points, timestamp)

            logger.info(
                "Fetched LB liquidity: protocol=%s, chain=%s, pool=%s..., depth=$%s",
                protocol_id,
                chain,
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
                chain,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_balancer_liquidity(
        self,
        pool_address: str,
        chain: str,
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
        data_source = self._data_source_for(protocol_id)

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
            pool_snapshots = await self._client.query_with_pagination(
                subgraph_id=subgraph_id,
                query=BALANCER_POOL_SNAPSHOT_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startTimestamp": start_timestamp,
                    "endTimestamp": end_timestamp,
                },
                data_path="poolSnapshots",
                max_pages=MAX_PAGINATION_PAGES,
                cursor_field="timestamp",
                cursor_variable="startTimestamp",
            )

            if not pool_snapshots:
                logger.warning(
                    "No liquidity data from Balancer subgraph: chain=%s, pool=%s...",
                    chain,
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
            depth = self._select_depth(data_points, timestamp)

            logger.info(
                "Fetched Balancer liquidity: chain=%s, pool=%s..., depth=$%s",
                chain,
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
                chain,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def _query_curve_liquidity(
        self,
        pool_address: str,
        chain: str,
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
        data_source = self._data_source_for(protocol_id)

        # Calculate day number range for Messari schema
        if self._use_twap:
            start_dt = timestamp - timedelta(hours=self._twap_window_hours)
        else:
            start_dt = timestamp - timedelta(days=1)

        start_day = self._date_to_day_number(start_dt)
        end_day = self._date_to_day_number(timestamp)

        try:
            pool_snapshots = await self._client.query_with_pagination(
                subgraph_id=subgraph_id,
                query=CURVE_POOL_DAILY_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startDay": start_day,
                    "endDay": end_day,
                },
                data_path="liquidityPoolDailySnapshots",
                max_pages=MAX_PAGINATION_PAGES,
                cursor_field="day",
                cursor_variable="startDay",
            )

            if not pool_snapshots:
                logger.warning(
                    "No liquidity data from Curve subgraph: chain=%s, pool=%s...",
                    chain,
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
            depth = self._select_depth(data_points, timestamp)

            logger.info(
                "Fetched Curve liquidity: chain=%s, pool=%s..., depth=$%s",
                chain,
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
                chain,
                pool_address_lower[:10],
                str(e),
            )
            return None

    async def get_liquidity_depth(
        self,
        pool_address: str,
        chain: str,
        timestamp: datetime,
        protocol: str | None = None,
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
            protocol: String protocol identifier (e.g., "curve"), or None.
                     If None, attempts to detect based on chain.

        Returns:
            LiquidityResult containing the liquidity depth in USD and source
            info with confidence level. Returns HIGH confidence for subgraph
            data, LOW confidence for fallback.

        Example:
            # With explicit protocol
            liquidity = await provider.get_liquidity_depth(
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain="arbitrum",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                protocol="uniswap_v3",
            )

            # Auto-detect protocol
            liquidity = await provider.get_liquidity_depth(
                pool_address="0x...",
                chain="base",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )  # Will use Aerodrome for Base chain
        """
        timestamp = self._ensure_utc_timestamp(timestamp)
        protocol_id = self._resolve_protocol_id(protocol, chain, pool_address)
        if protocol_id is None:
            return self._create_fallback_result(timestamp)

        if not self._protocol_supports_chain(protocol_id, chain):
            return self._create_fallback_result(timestamp)

        result: LiquidityResult | None = None
        try:
            result = await self._query_liquidity_by_family(
                pool_address=pool_address,
                chain=chain,
                timestamp=timestamp,
                protocol_id=protocol_id,
            )
        except DataSourceUnavailableError:
            # Pagination overflow must stay loud (VIB-5089): a partial series
            # silently swapped for fallback would be silent truncation.
            raise
        except Exception as e:
            logger.error(
                "Unexpected error querying liquidity: protocol=%s, chain=%s, pool=%s...: %s",
                protocol_id,
                chain,
                pool_address[:10],
                str(e),
            )

        # Return result or fallback
        if result is not None:
            return result

        return self._create_fallback_result(timestamp)

    @staticmethod
    def _ensure_utc_timestamp(timestamp: datetime) -> datetime:
        """Return timezone-aware UTC timestamp for result metadata."""
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=UTC)
        return timestamp.astimezone(UTC)

    def _resolve_protocol_id(
        self,
        protocol: str | None,
        chain: str,
        pool_address: str,
    ) -> str | None:
        """Resolve explicit or chain-default protocol id for a liquidity lookup."""
        protocol_id = self._get_protocol_id(protocol)
        if protocol_id is not None:
            return protocol_id

        detected_protocol_id = self._detect_protocol_from_chain(chain)
        if detected_protocol_id is not None:
            logger.info(
                "Auto-detected protocol %s for chain %s",
                detected_protocol_id,
                chain,
            )
            return detected_protocol_id

        logger.warning(
            "Could not determine protocol for chain=%s, pool=%s..., returning fallback",
            chain,
            pool_address[:10],
        )
        return None

    @staticmethod
    def _protocol_supports_chain(protocol_id: str, chain: str) -> bool:
        """Return whether this protocol has a configured subgraph for chain."""
        subgraph_ids = DexVolumeRegistry.liquidity_subgraph_ids_for(protocol_id)
        if subgraph_ids is not None and chain in subgraph_ids:
            return True

        logger.warning(
            "Chain %s not supported by protocol %s, returning fallback",
            chain,
            protocol_id,
        )
        return False

    async def _query_liquidity_by_family(
        self,
        *,
        pool_address: str,
        chain: str,
        timestamp: datetime,
        protocol_id: str,
    ) -> LiquidityResult | None:
        """Dispatch a supported protocol to its AMM-family query method."""
        logger.info(
            "Querying liquidity depth: protocol=%s, chain=%s, pool=%s...",
            protocol_id,
            chain,
            pool_address[:10],
        )

        entry = DexVolumeRegistry.entry_for(protocol_id)
        method_name = LIQUIDITY_QUERY_METHOD_BY_FAMILY.get(entry.amm_family) if entry is not None else None
        if method_name is None:
            logger.warning(
                "Unknown protocol type for %s, returning fallback",
                protocol_id,
            )
            return None

        query_method = getattr(self, method_name)
        return await query_method(pool_address, chain, timestamp, protocol_id)

    async def get_liquidity_depth_range(
        self,
        pool_address: str,
        chain: str,
        start_time: datetime,
        end_time: datetime,
        protocol: str | None = None,
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
            protocol: String protocol identifier, or None.

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
    "DATA_SOURCE_FALLBACK",
    "DEFAULT_TWAP_WINDOW_HOURS",
    "MAX_PAGINATION_PAGES",
]
