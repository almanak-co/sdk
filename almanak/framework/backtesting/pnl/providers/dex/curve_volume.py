"""Curve Finance historical volume provider.

This module provides a historical volume data provider for Curve Finance pools
across multiple chains. It implements the HistoricalVolumeProvider interface
and fetches data from The Graph's Messari Curve Finance subgraphs.

Curve Finance is a multi-token AMM optimized for stablecoin and similar asset
swaps. The subgraphs use Messari's standardized DEX schema with
`liquidityPoolDailySnapshots` for daily volume data.

Key Features:
    - Supports Ethereum and Optimism chains (decentralized network)
    - Arbitrum and Polygon pending subgraph deployment on decentralized network
    - Uses Messari's standardized DEX schema (liquidityPoolDailySnapshots)
    - Handles multi-token pools (3pool, tricrypto, etc.)
    - Integrates with SubgraphClient for rate limiting and retry logic
    - Returns VolumeResult with HIGH confidence for subgraph data
    - Falls back to LOW confidence results when data unavailable

Example:
    from almanak.framework.backtesting.pnl.providers.dex import (
        CurveVolumeProvider,
    )
    from almanak.core.enums import Chain
    from datetime import date

    provider = CurveVolumeProvider()

    # Fetch volume for a Curve 3pool on Ethereum
    async with provider:
        volumes = await provider.get_volume(
            pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
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
# Curve Finance Subgraph IDs (Messari standardized schema)
# =============================================================================

# Subgraph deployment IDs for Curve Finance on various chains
# These are Messari's standardized DEX subgraphs on The Graph's decentralized network
# Source: https://thegraph.com/explorer - search "Curve Finance"
CURVE_SUBGRAPH_IDS: dict[Chain, str] = {
    # Ethereum Mainnet - Messari Curve Finance subgraph
    Chain.ETHEREUM: "3fy93eAT56UJsRCEht8iFhfi6wjHWXtZ9dnnbQmvFopF",
    # Optimism - Messari Curve Finance Optimism subgraph
    Chain.OPTIMISM: "CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX",
    # Note: Arbitrum and Polygon subgraphs are on hosted service (deprecated),
    # not yet migrated to decentralized network. Will be added when available.
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(CURVE_SUBGRAPH_IDS.keys())

# Data source identifier
DATA_SOURCE = "curve_messari_subgraph"

# GraphQL query for fetching liquidity pool daily snapshots (Messari schema)
# Curve uses Messari's standardized DEX schema which differs from Uniswap's native schema
LIQUIDITY_POOL_DAILY_SNAPSHOTS_QUERY = """
query GetLiquidityPoolDailySnapshots($poolAddress: String!, $startDay: Int!, $endDay: Int!) {
    liquidityPoolDailySnapshots(
        first: 1000
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
        dailyVolumeUSD
        totalValueLockedUSD
        cumulativeVolumeUSD
    }
}
"""


# =============================================================================
# CurveVolumeProvider
# =============================================================================


class CurveVolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for Curve Finance pools.

    Fetches daily volume data from The Graph's Messari Curve Finance subgraphs
    for Ethereum and Optimism chains. Uses the standardized Messari DEX schema
    with `liquidityPoolDailySnapshots` entity.

    Curve Finance is a multi-token AMM specialized for efficient stablecoin and
    similar asset swaps with low slippage. Pools can have 2-5+ tokens.

    Attributes:
        client: SubgraphClient for querying The Graph
        fallback_volume: Volume to return when subgraph data unavailable

    Example:
        provider = CurveVolumeProvider()

        # Use as async context manager
        async with provider:
            volumes = await provider.get_volume(
                pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )

        # Or manually close
        provider = CurveVolumeProvider()
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
        """Initialize the Curve volume provider.

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
            "Initialized CurveVolumeProvider: supported_chains=%s, fallback_volume=%s",
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
        logger.debug("CurveVolumeProvider closed")

    async def __aenter__(self) -> "CurveVolumeProvider":
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
        return CURVE_SUBGRAPH_IDS.get(chain)

    def _date_to_day_number(self, d: date) -> int:
        """Convert a date to days since Unix epoch.

        Messari schema uses `day` field as days since Unix epoch (not timestamp).

        Args:
            d: Date to convert

        Returns:
            Days since Unix epoch (January 1, 1970)
        """
        epoch = date(1970, 1, 1)
        return (d - epoch).days

    def _day_number_to_datetime(self, day: int) -> datetime:
        """Convert day number (days since epoch) to datetime.

        Args:
            day: Days since Unix epoch

        Returns:
            Datetime at start of that day (UTC)
        """
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        return epoch + timedelta(days=day)

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
            snapshot: Raw data from subgraph liquidityPoolDailySnapshots query

        Returns:
            VolumeResult with HIGH confidence
        """
        day_number = int(snapshot.get("day", 0))
        day_dt = self._day_number_to_datetime(day_number)
        volume_usd = Decimal(str(snapshot.get("dailyVolumeUSD", "0")))

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
        """Fetch historical volume data for a Curve pool.

        Queries The Graph's Messari Curve Finance subgraph for daily volume data
        (liquidityPoolDailySnapshots) within the specified date range.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on. Must be one of:
                   ETHEREUM, OPTIMISM (more chains pending).
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
                pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
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
            raise ValueError(
                f"Unsupported chain: {chain}. Supported chains: {[c.value for c in SUPPORTED_CHAINS]}. "
                f"Note: Arbitrum and Polygon support pending subgraph migration to decentralized network."
            )

        # Normalize pool address
        pool_address_lower = pool_address.lower()

        # Convert dates to day numbers (Messari schema uses days since epoch)
        start_day = self._date_to_day_number(start_date)
        end_day = self._date_to_day_number(end_date)

        logger.info(
            "Fetching Curve volume: chain=%s, pool=%s..., start=%s, end=%s",
            chain.value,
            pool_address_lower[:10],
            start_date,
            end_date,
        )

        try:
            # Query subgraph
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=LIQUIDITY_POOL_DAILY_SNAPSHOTS_QUERY,
                variables={
                    "poolAddress": pool_address_lower,
                    "startDay": start_day,
                    "endDay": end_day,
                },
            )

            snapshots = data.get("liquidityPoolDailySnapshots", [])

            if not snapshots:
                logger.warning(
                    "No volume data from Curve subgraph: chain=%s, pool=%s..., range=%s to %s",
                    chain.value,
                    pool_address_lower[:10],
                    start_date,
                    end_date,
                )
                # Return fallback results for the date range
                return self._generate_fallback_results(start_date, end_date)

            # Parse results
            results = [self._parse_volume_data(snapshot) for snapshot in snapshots]

            logger.info(
                "Fetched %d days of Curve volume: chain=%s, pool=%s...",
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
                "Unexpected error fetching Curve volume: chain=%s, pool=%s...: %s",
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
    "CurveVolumeProvider",
    "CURVE_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
