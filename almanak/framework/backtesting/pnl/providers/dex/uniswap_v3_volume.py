"""Uniswap V3 historical volume provider.

This module provides a historical volume data provider for Uniswap V3 pools
across multiple chains. It implements the HistoricalVolumeProvider interface
and fetches data from The Graph's Uniswap V3 subgraphs.

Key Features:
    - Supports Ethereum, Arbitrum, Base, Optimism, Polygon chains
    - Fetches daily volume data from poolDayDatas
    - Integrates with SubgraphClient for rate limiting and retry logic
    - Returns VolumeResult with HIGH confidence for subgraph data
    - Falls back to LOW confidence results when data unavailable

Example:
    from almanak.framework.backtesting.pnl.providers.dex import (
        UniswapV3VolumeProvider,
    )
    from almanak.core.enums import Chain
    from datetime import date

    provider = UniswapV3VolumeProvider()

    # Fetch volume for a date range
    async with provider:
        volumes = await provider.get_volume(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain=Chain.ARBITRUM,
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
# Uniswap V3 Subgraph IDs (from The Graph Explorer)
# =============================================================================

# Subgraph deployment IDs for Uniswap V3 on various chains
# These are from The Graph's decentralized network
UNISWAP_V3_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    Chain.ARBITRUM: "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    Chain.BASE: "43Hwfi3dJSoGpyas9VwNoDAv28rqtbnqUk3EYCRr3j6i",
    Chain.OPTIMISM: "Gc2DPCVq5UkBfyHjZDMbKTc7ynrjoSKxc6sHLKY9Pmjc",
    Chain.POLYGON: "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(UNISWAP_V3_SUBGRAPH_IDS.keys())

# Data source identifier
DATA_SOURCE = "uniswap_v3_subgraph"

# GraphQL query for fetching pool day data
POOL_DAY_DATA_QUERY = """
query GetPoolDayDatas($poolAddress: String!, $startDate: Int!, $endDate: Int!) {
    poolDayDatas(
        first: 1000
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
        volumeUSD
        feesUSD
        tvlUSD
        liquidity
    }
}
"""


# =============================================================================
# UniswapV3VolumeProvider
# =============================================================================


class UniswapV3VolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for Uniswap V3 pools.

    Fetches daily volume data from The Graph's Uniswap V3 subgraphs for
    Ethereum, Arbitrum, Base, Optimism, and Polygon chains.

    Attributes:
        client: SubgraphClient for querying The Graph
        fallback_volume: Volume to return when subgraph data unavailable

    Example:
        provider = UniswapV3VolumeProvider()

        # Use as async context manager
        async with provider:
            volumes = await provider.get_volume(
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )

        # Or manually close
        provider = UniswapV3VolumeProvider()
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
        """Initialize the Uniswap V3 volume provider.

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
            "Initialized UniswapV3VolumeProvider: supported_chains=%s, fallback_volume=%s",
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
        logger.debug("UniswapV3VolumeProvider closed")

    async def __aenter__(self) -> "UniswapV3VolumeProvider":
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
        return UNISWAP_V3_SUBGRAPH_IDS.get(chain)

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
            day_data: Raw data from subgraph poolDayDatas query

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
        """Fetch historical volume data for a Uniswap V3 pool.

        Queries The Graph's Uniswap V3 subgraph for daily volume data
        (poolDayDatas) within the specified date range.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on. Must be one of:
                   ETHEREUM, ARBITRUM, BASE, OPTIMISM, POLYGON.
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
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain=Chain.ARBITRUM,
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
            "Fetching Uniswap V3 volume: chain=%s, pool=%s..., start=%s, end=%s",
            chain.value,
            pool_address_lower[:10],
            start_date,
            end_date,
        )

        try:
            # Query subgraph
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=POOL_DAY_DATA_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startDate": start_timestamp,
                    "endDate": end_timestamp,
                },
            )

            pool_day_datas = data.get("poolDayDatas", [])

            if not pool_day_datas:
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
            results = [self._parse_volume_data(day_data) for day_data in pool_day_datas]

            logger.info(
                "Fetched %d days of Uniswap V3 volume: chain=%s, pool=%s...",
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
    "UniswapV3VolumeProvider",
    "UNISWAP_V3_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
