"""TraderJoe V2 (Liquidity Book) historical volume provider.

This module provides a historical volume data provider for TraderJoe V2 Liquidity Book
pools across multiple chains. It implements the HistoricalVolumeProvider interface
and fetches data from The Graph's TraderJoe V2 subgraphs.

TraderJoe V2 uses a unique bin-based liquidity system (Liquidity Book) that differs
from traditional constant product AMMs. The subgraph uses `lbPairDayDatas` entity
to track daily volume data.

Key Features:
    - Supports Avalanche chain (Arbitrum support pending subgraph deployment)
    - Fetches daily volume data from lbPairDayDatas
    - Integrates with SubgraphClient for rate limiting and retry logic
    - Returns VolumeResult with HIGH confidence for subgraph data
    - Falls back to LOW confidence results when data unavailable

Example:
    from almanak.framework.backtesting.pnl.providers.dex import (
        TraderJoeV2VolumeProvider,
    )
    from almanak.core.enums import Chain
    from datetime import date

    provider = TraderJoeV2VolumeProvider()

    # Fetch volume for a date range
    async with provider:
        volumes = await provider.get_volume(
            pool_address="0x7eC3717f70894F6d9BA0F8ff67a0115e4c919Cc2",
            chain=Chain.AVALANCHE,
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
# TraderJoe V2 Subgraph IDs (from The Graph Explorer)
# =============================================================================

# Subgraph deployment IDs for TraderJoe V2 (Liquidity Book) on various chains
# These are from The Graph's decentralized network
# Source: https://thegraph.com/explorer/subgraphs/6KD9JYCg2qa3TxNK3tLdhj5zuZTABoLLNcnUZXKG9vuH
TRADERJOE_V2_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.AVALANCHE: "6KD9JYCg2qa3TxNK3tLdhj5zuZTABoLLNcnUZXKG9vuH",
    # Arbitrum support pending - hosted service available but not on decentralized network
    # https://thegraph.com/hosted-service/subgraph/traderjoe-xyz/joe-v2-arbitrum
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(TRADERJOE_V2_SUBGRAPH_IDS.keys())

# Data source identifier
DATA_SOURCE = "traderjoe_v2_subgraph"

# GraphQL query for fetching LB pair day data
# TraderJoe V2 uses `lbPairDayDatas` entity with `lbPair` relationship
LB_PAIR_DAY_DATA_QUERY = """
query GetLBPairDayDatas($lbPairAddress: String!, $startDate: Int!, $endDate: Int!) {
    lbPairDayDatas(
        first: 1000
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
        volumeUSD
        volumeTokenX
        volumeTokenY
        feesUSD
        totalValueLockedUSD
        txCount
    }
}
"""


# =============================================================================
# TraderJoeV2VolumeProvider
# =============================================================================


class TraderJoeV2VolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for TraderJoe V2 (Liquidity Book) pools.

    Fetches daily volume data from The Graph's TraderJoe V2 subgraphs.
    Currently supports Avalanche chain. Arbitrum support is pending
    subgraph deployment to The Graph's decentralized network.

    TraderJoe V2 uses a bin-based Liquidity Book AMM that provides:
    - Zero slippage within bins
    - More capital efficient liquidity positions
    - Fungible LP tokens (ERC-1155 style)

    Attributes:
        client: SubgraphClient for querying The Graph
        fallback_volume: Volume to return when subgraph data unavailable

    Example:
        provider = TraderJoeV2VolumeProvider()

        # Use as async context manager
        async with provider:
            volumes = await provider.get_volume(
                pool_address="0x7eC3717f70894F6d9BA0F8ff67a0115e4c919Cc2",
                chain=Chain.AVALANCHE,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )

        # Or manually close
        provider = TraderJoeV2VolumeProvider()
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
        """Initialize the TraderJoe V2 volume provider.

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
            "Initialized TraderJoeV2VolumeProvider: supported_chains=%s, fallback_volume=%s",
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
        logger.debug("TraderJoeV2VolumeProvider closed")

    async def __aenter__(self) -> "TraderJoeV2VolumeProvider":
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
        return TRADERJOE_V2_SUBGRAPH_IDS.get(chain)

    def _date_to_timestamp(self, d: date) -> int:
        """Convert a date to Unix timestamp (start of day UTC).

        Args:
            d: Date to convert

        Returns:
            Unix timestamp for start of day UTC
        """
        return int(datetime.combine(d, datetime.min.time(), tzinfo=UTC).timestamp())

    def _timestamp_to_date(self, ts: int) -> date:
        """Convert Unix timestamp to date.

        Args:
            ts: Unix timestamp

        Returns:
            Date corresponding to the timestamp
        """
        return datetime.fromtimestamp(ts, tz=UTC).date()

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

    def _parse_volume_data(self, day_data: dict[str, Any]) -> VolumeResult:
        """Parse subgraph response into VolumeResult.

        Args:
            day_data: Raw data from subgraph lbPairDayDatas query

        Returns:
            VolumeResult with HIGH confidence
        """
        day_timestamp = int(day_data.get("date", 0))
        day_dt = datetime.fromtimestamp(day_timestamp, tz=UTC)
        volume_usd = Decimal(str(day_data.get("volumeUSD", "0")))

        return VolumeResult(
            value=volume_usd,
            source_info=DataSourceInfo(
                source=DATA_SOURCE,
                confidence=DataConfidence.HIGH,
                timestamp=day_dt,
            ),
        )

    async def get_volume(
        self,
        pool_address: str,
        chain: Chain,
        start_date: date,
        end_date: date,
    ) -> list[VolumeResult]:
        """Fetch historical volume data for a TraderJoe V2 Liquidity Book pool.

        Queries The Graph's TraderJoe V2 subgraph for daily volume data
        (lbPairDayDatas) within the specified date range.

        Args:
            pool_address: The LB pair contract address (checksummed or lowercase).
            chain: The blockchain the pool is on. Must be AVALANCHE.
                   (Arbitrum support pending subgraph deployment)
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
                pool_address="0x7eC3717f70894F6d9BA0F8ff67a0115e4c919Cc2",
                chain=Chain.AVALANCHE,
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

        # Normalize pool address
        pool_address_lower = pool_address.lower()

        # Convert dates to timestamps
        start_timestamp = self._date_to_timestamp(start_date)
        end_timestamp = self._date_to_timestamp(end_date)

        logger.info(
            "Fetching TraderJoe V2 volume: chain=%s, pool=%s..., start=%s, end=%s",
            chain.value,
            pool_address_lower[:10],
            start_date,
            end_date,
        )

        try:
            # Query subgraph
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
                    "No volume data from subgraph: chain=%s, pool=%s..., range=%s to %s",
                    chain.value,
                    pool_address_lower[:10],
                    start_date,
                    end_date,
                )
                # Return fallback results for the date range
                return self._generate_fallback_results(start_date, end_date)

            # Parse results
            results = [self._parse_volume_data(day_data) for day_data in lb_pair_day_datas]

            logger.info(
                "Fetched %d days of TraderJoe V2 volume: chain=%s, pool=%s...",
                len(results),
                chain.value,
                pool_address_lower[:10],
            )

            return results

        except SubgraphRateLimitError as e:
            logger.warning(
                "Subgraph rate limit exceeded: chain=%s, pool=%s...: %s",
                chain.value,
                pool_address_lower[:10],
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except SubgraphQueryError as e:
            logger.error(
                "Subgraph query error: chain=%s, pool=%s...: %s",
                chain.value,
                pool_address_lower[:10],
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except Exception as e:
            logger.error(
                "Unexpected error fetching volume: chain=%s, pool=%s...: %s",
                chain.value,
                pool_address_lower[:10],
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
    "TraderJoeV2VolumeProvider",
    "TRADERJOE_V2_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
