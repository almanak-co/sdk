"""Balancer V2 historical volume provider.

This module provides a historical volume data provider for Balancer V2 pools
across multiple chains. It implements the HistoricalVolumeProvider interface
and fetches data from The Graph's Balancer V2 subgraphs.

Balancer is a multi-token automated market maker (AMM) that supports weighted
pools, stable pools, and various other pool types. Pools can have 2-8 tokens
with customizable weights.

Key Features:
    - Supports Ethereum, Arbitrum, Polygon chains
    - Fetches daily volume data from PoolSnapshot entity
    - Integrates with SubgraphClient for rate limiting and retry logic
    - Returns VolumeResult with HIGH confidence for subgraph data
    - Falls back to LOW confidence results when data unavailable

Example:
    from almanak.framework.backtesting.pnl.providers.dex import (
        BalancerVolumeProvider,
    )
    from almanak.core.enums import Chain
    from datetime import date

    provider = BalancerVolumeProvider()

    # Fetch volume for a Balancer weighted pool on Ethereum
    async with provider:
        volumes = await provider.get_volume(
            pool_address="0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )
        for vol in volumes:
            print(f"{vol.source_info.timestamp}: ${vol.value}")
"""

import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.core.enums import Chain

from ...types import DataConfidence, DataSourceInfo, VolumeResult
from ..base import HistoricalVolumeProvider
from ..subgraph_client import (
    SubgraphClient,
    SubgraphClientConfig,
    SubgraphQueryError,
    SubgraphRateLimitError,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Balancer V2 Subgraph IDs (from The Graph Explorer)
# =============================================================================

# Subgraph deployment IDs for Balancer V2 on various chains
# These are from The Graph's decentralized network
# Source: https://docs-v2.balancer.fi/reference/subgraph/
BALANCER_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "C4ayEZP2yTXRAB8vSaTrgN4m9anTe9Mdm2ViyiAuV9TV",
    Chain.ARBITRUM: "98cQDy6tufTJtshDCuhh9z2kWXsQWBHVh2bqnLHsGAeS",
    Chain.POLYGON: "H9oPAbXnobBRq1cB3HDmbZ1E8MWQyJYQjT1QDJMrdbNp",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(BALANCER_SUBGRAPH_IDS.keys())

# Data source identifier
DATA_SOURCE = "balancer_v2_subgraph"

# GraphQL query for fetching pool snapshots
# Balancer uses PoolSnapshot entity with timestamp (start of day UTC)
POOL_SNAPSHOTS_QUERY = """
query GetPoolSnapshots($poolId: String!, $startTimestamp: Int!, $endTimestamp: Int!) {
    poolSnapshots(
        first: 1000
        where: {
            pool: $poolId
            timestamp_gte: $startTimestamp
            timestamp_lte: $endTimestamp
        }
        orderBy: timestamp
        orderDirection: asc
    ) {
        id
        timestamp
        swapVolume
        swapFees
        liquidity
        totalShares
    }
}
"""


# =============================================================================
# BalancerVolumeProvider
# =============================================================================


class BalancerVolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for Balancer V2 pools.

    Fetches daily volume data from The Graph's Balancer V2 subgraphs for
    Ethereum, Arbitrum, and Polygon chains. Uses the PoolSnapshot entity
    which contains daily snapshots of pool metrics.

    Balancer supports various pool types:
        - Weighted Pools: Multiple tokens with custom weights
        - Stable Pools: 2-5 tokens with similar prices
        - Composable Stable Pools: Nested stable pools
        - Linear Pools: Wrapping pools for yield-bearing tokens

    Attributes:
        client: SubgraphClient for querying The Graph
        fallback_volume: Volume to return when subgraph data unavailable

    Example:
        provider = BalancerVolumeProvider()

        # Use as async context manager
        async with provider:
            volumes = await provider.get_volume(
                pool_address="0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )

        # Or manually close
        provider = BalancerVolumeProvider()
        try:
            volumes = await provider.get_volume(...)
        finally:
            await provider.close()
    """

    def __init__(
        self,
        client: SubgraphClient | None = None,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the Balancer volume provider.

        Args:
            client: Optional SubgraphClient instance. If None, creates one
                    using THEGRAPH_API_KEY from environment.
            fallback_volume: Volume to return when subgraph data unavailable.
                            Default is 0, indicating no data.
            requests_per_minute: Rate limit for subgraph requests. Default 100.
        """
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            config = SubgraphClientConfig(requests_per_minute=requests_per_minute)
            self._client = SubgraphClient(config=config)
            self._owns_client = True

        self._fallback_volume = fallback_volume

        logger.debug(
            "Initialized BalancerVolumeProvider: supported_chains=%s, fallback_volume=%s",
            [c.value for c in SUPPORTED_CHAINS],
            fallback_volume,
        )

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def close(self) -> None:
        """Close the subgraph client and release resources."""
        if self._owns_client:
            await self._client.close()
        logger.debug("BalancerVolumeProvider closed")

    async def __aenter__(self) -> "BalancerVolumeProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the client."""
        await self.close()

    def _get_subgraph_id(self, chain: Chain) -> str | None:
        """Get the subgraph ID for a chain.

        Args:
            chain: The blockchain to get subgraph ID for

        Returns:
            Subgraph deployment ID or None if chain not supported
        """
        return BALANCER_SUBGRAPH_IDS.get(chain)

    def _date_to_timestamp(self, d: date) -> int:
        """Convert a date to Unix timestamp (start of day UTC).

        Args:
            d: Date to convert

        Returns:
            Unix timestamp for start of day UTC
        """
        return int(datetime.combine(d, datetime.min.time(), tzinfo=UTC).timestamp())

    def _timestamp_to_datetime(self, ts: int) -> datetime:
        """Convert Unix timestamp to datetime.

        Args:
            ts: Unix timestamp

        Returns:
            Datetime corresponding to the timestamp
        """
        return datetime.fromtimestamp(ts, tz=UTC)

    def _create_fallback_result(self, d: date) -> VolumeResult:
        """Create a fallback VolumeResult with LOW confidence.

        Args:
            d: Date for the result

        Returns:
            VolumeResult with fallback volume and LOW confidence
        """
        return VolumeResult(
            value=self._fallback_volume,
            source_info=DataSourceInfo(
                source="fallback",
                confidence=DataConfidence.LOW,
                timestamp=datetime.combine(d, datetime.min.time(), tzinfo=UTC),
            ),
        )

    def _parse_volume_data(self, snapshot: dict[str, Any]) -> VolumeResult:
        """Parse subgraph response into VolumeResult.

        Args:
            snapshot: Raw data from subgraph PoolSnapshot query

        Returns:
            VolumeResult with HIGH confidence
        """
        timestamp = int(snapshot.get("timestamp", 0))
        snapshot_dt = self._timestamp_to_datetime(timestamp)
        # Balancer uses swapVolume for volume in the pool's accounting token
        volume = Decimal(str(snapshot.get("swapVolume", "0")))

        return VolumeResult(
            value=volume,
            source_info=DataSourceInfo(
                source=DATA_SOURCE,
                confidence=DataConfidence.HIGH,
                timestamp=snapshot_dt,
            ),
        )

    async def get_volume(
        self,
        pool_address: str,
        chain: Chain,
        start_date: date,
        end_date: date,
    ) -> list[VolumeResult]:
        """Fetch historical volume data for a Balancer V2 pool.

        Queries The Graph's Balancer V2 subgraph for daily volume data
        (PoolSnapshot) within the specified date range.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on. Must be one of:
                   ETHEREUM, ARBITRUM, POLYGON.
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of VolumeResult objects, one per day with available data.
            Returns HIGH confidence results from subgraph data.
            Returns LOW confidence fallback results if subgraph unavailable.

        Raises:
            ValueError: If chain is not supported.

        Example:
            volumes = await provider.get_volume(
                pool_address="0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )
            for vol in volumes:
                if vol.source_info.confidence == DataConfidence.HIGH:
                    print(f"Real volume: ${vol.value}")
        """
        # Validate chain
        subgraph_id = self._get_subgraph_id(chain)
        if subgraph_id is None:
            raise ValueError(f"Unsupported chain: {chain}. Supported chains: {[c.value for c in SUPPORTED_CHAINS]}")

        # Normalize pool ID - Balancer uses pool ID format
        # Pool IDs are the pool address + pool type index (64 hex chars total)
        # If a bare address (42 chars) is provided, warn that it may not return data
        pool_id_lower = pool_address.lower()
        if len(pool_id_lower) == 42:
            logger.warning(
                "Balancer pool address appears to be a bare address (42 chars). "
                "Balancer V2 subgraph typically requires full pool ID (64 hex chars = address + type). "
                "Query may return no data. pool=%s...",
                pool_id_lower[:10],
            )

        # Convert dates to timestamps
        start_timestamp = self._date_to_timestamp(start_date)
        # End timestamp should be end of day (start of next day - 1 second)
        end_timestamp = self._date_to_timestamp(end_date + timedelta(days=1)) - 1

        logger.info(
            "Fetching Balancer V2 volume: chain=%s, pool=%s..., start=%s, end=%s",
            chain.value,
            pool_id_lower[:10],
            start_date,
            end_date,
        )

        try:
            # Query subgraph
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=POOL_SNAPSHOTS_QUERY,
                variables={
                    "poolId": pool_id_lower,
                    "startTimestamp": start_timestamp,
                    "endTimestamp": end_timestamp,
                },
            )

            snapshots = data.get("poolSnapshots", [])

            if not snapshots:
                logger.warning(
                    "No volume data from Balancer subgraph: chain=%s, pool=%s..., range=%s to %s",
                    chain.value,
                    pool_id_lower[:10],
                    start_date,
                    end_date,
                )
                # Return fallback results for the date range
                return self._generate_fallback_results(start_date, end_date)

            # Parse results
            results = [self._parse_volume_data(snapshot) for snapshot in snapshots]

            logger.info(
                "Fetched %d days of Balancer V2 volume: chain=%s, pool=%s...",
                len(results),
                chain.value,
                pool_id_lower[:10],
            )

            return results

        except SubgraphRateLimitError as e:
            logger.warning(
                "Subgraph rate limit exceeded: chain=%s, pool=%s...: %s",
                chain.value,
                pool_id_lower[:10],
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except SubgraphQueryError as e:
            logger.error(
                "Subgraph query error: chain=%s, pool=%s...: %s",
                chain.value,
                pool_id_lower[:10],
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except Exception as e:
            logger.error(
                "Unexpected error fetching Balancer volume: chain=%s, pool=%s...: %s",
                chain.value,
                pool_id_lower[:10],
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

    def _generate_fallback_results(
        self,
        start_date: date,
        end_date: date,
    ) -> list[VolumeResult]:
        """Generate fallback results for a date range.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            List of VolumeResult with LOW confidence fallback values
        """
        results = []
        current = start_date
        while current <= end_date:
            results.append(self._create_fallback_result(current))
            current += timedelta(days=1)
        return results


__all__ = [
    "BalancerVolumeProvider",
    "BALANCER_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
