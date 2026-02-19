"""Aave V3 historical APY provider.

This module provides a historical APY data provider for Aave V3 lending protocol
across multiple chains. It implements the HistoricalAPYProvider interface
and fetches data from The Graph's Aave V3 subgraphs.

Key Features:
    - Supports Ethereum, Arbitrum, Optimism, Polygon, Base, Avalanche chains
    - Fetches historical supply and borrow APY from reserveParamsHistoryItems
    - Integrates with SubgraphClient for rate limiting and retry logic
    - Converts RAY units (1e27) to APY percentages
    - Returns APYResult with HIGH confidence for subgraph data
    - Falls back to LOW confidence results when data unavailable

Aave V3 Rate Units:
    - All rates (liquidityRate, variableBorrowRate) are stored in RAY units (1e27)
    - liquidityRate = supply APY (annualized)
    - variableBorrowRate = borrow APY (annualized)
    - Conversion: APY = rate / 1e27 (already annualized in the subgraph)

Example:
    from almanak.framework.backtesting.pnl.providers.lending import (
        AaveV3APYProvider,
    )
    from almanak.core.enums import Chain
    from datetime import datetime, UTC

    provider = AaveV3APYProvider()

    # Fetch APY for a date range
    async with provider:
        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )
        for apy in apys:
            print(f"{apy.source_info.timestamp}: supply={apy.supply_apy:.4f}, borrow={apy.borrow_apy:.4f}")
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.core.enums import Chain

from ...types import APYResult, DataConfidence, DataSourceInfo
from ..base import HistoricalAPYProvider
from ..subgraph_client import (
    SubgraphClient,
    SubgraphClientConfig,
    SubgraphQueryError,
    SubgraphRateLimitError,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Aave V3 Subgraph IDs (from The Graph Explorer)
# =============================================================================

# Subgraph deployment IDs for Aave V3 on various chains
# These are from The Graph's decentralized network
AAVE_V3_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g",
    Chain.ARBITRUM: "DLuE98kEb5pQNXAcKFQGQgfSQ57Xdou4jnVbAEqMfy3B",
    Chain.OPTIMISM: "DSfLz8oQBUeU5atALgUFQKMTSYV9mZAVYp4noLSXAfvb",
    Chain.POLYGON: "Co2URyXjnxaw8WqxKyVHdirq9Ahhm5vcTs4dMedAq211",
    Chain.BASE: "GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF",
    Chain.AVALANCHE: "2h9woxy8RTjHu1HJsCEnmzpPHFArU33avmUh4f71JpVn",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(AAVE_V3_SUBGRAPH_IDS.keys())

# Data source identifier
DATA_SOURCE = "aave_v3_subgraph"

# RAY units (1e27) - Aave stores rates with this precision
RAY = Decimal("1e27")

# Default fallback APY values
DEFAULT_SUPPLY_APY_FALLBACK = Decimal("0.03")  # 3% APY
DEFAULT_BORROW_APY_FALLBACK = Decimal("0.05")  # 5% APY

# GraphQL query for fetching reserve params history
RESERVE_PARAMS_HISTORY_QUERY = """
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
        id
        timestamp
        liquidityRate
        variableBorrowRate
        stableBorrowRate
        utilizationRate
    }
}
"""

# GraphQL query to find reserves by symbol
RESERVES_BY_SYMBOL_QUERY = """
query GetReservesBySymbol($symbol: String!) {
    reserves(where: { symbol: $symbol }) {
        id
        symbol
        name
        decimals
        underlyingAsset
    }
}
"""


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class AaveV3ClientConfig:
    """Configuration for Aave V3 APY provider.

    Attributes:
        chain: Default chain for requests (default: ETHEREUM)
        requests_per_minute: Rate limit for subgraph requests (default: 100)
        supply_apy_fallback: Fallback supply APY when data unavailable
        borrow_apy_fallback: Fallback borrow APY when data unavailable
    """

    chain: Chain = Chain.ETHEREUM
    requests_per_minute: int = 100
    supply_apy_fallback: Decimal = DEFAULT_SUPPLY_APY_FALLBACK
    borrow_apy_fallback: Decimal = DEFAULT_BORROW_APY_FALLBACK


# =============================================================================
# AaveV3APYProvider
# =============================================================================


class AaveV3APYProvider(HistoricalAPYProvider):
    """Historical APY provider for Aave V3 lending protocol.

    Fetches historical supply and borrow APY data from The Graph's Aave V3
    subgraphs for Ethereum, Arbitrum, Optimism, Polygon, Base, and Avalanche.

    The provider queries reserveParamsHistoryItems which contains historical
    rate snapshots. Rates are stored in RAY units (1e27) and converted to
    decimal APY values.

    Attributes:
        config: Client configuration
        client: SubgraphClient for querying The Graph

    Example:
        provider = AaveV3APYProvider()

        # Use as async context manager
        async with provider:
            apys = await provider.get_apy(
                protocol="aave_v3",
                market="USDC",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )

        # Or manually close
        provider = AaveV3APYProvider()
        try:
            apys = await provider.get_apy(...)
        finally:
            await provider.close()
    """

    def __init__(
        self,
        config: AaveV3ClientConfig | None = None,
        client: SubgraphClient | None = None,
    ) -> None:
        """Initialize the Aave V3 APY provider.

        Args:
            config: Client configuration. If None, uses defaults.
            client: Optional SubgraphClient instance. If None, creates one
                    using THEGRAPH_API_KEY from environment.
        """
        self._config = config or AaveV3ClientConfig()

        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            subgraph_config = SubgraphClientConfig(requests_per_minute=self._config.requests_per_minute)
            self._client = SubgraphClient(config=subgraph_config)
            self._owns_client = True

        # Cache for reserve IDs to avoid repeated lookups
        self._reserve_cache: dict[str, dict[str, str]] = {}

        logger.debug(
            "Initialized AaveV3APYProvider: chain=%s, supported_chains=%s",
            self._config.chain.value,
            [c.value for c in SUPPORTED_CHAINS],
        )

    @property
    def config(self) -> AaveV3ClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def close(self) -> None:
        """Close the subgraph client and release resources."""
        if self._owns_client:
            await self._client.close()
        logger.debug("AaveV3APYProvider closed")

    async def __aenter__(self) -> "AaveV3APYProvider":
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
        return AAVE_V3_SUBGRAPH_IDS.get(chain)

    def _ray_to_decimal(self, ray_value: str | int) -> Decimal:
        """Convert RAY units (1e27) to decimal.

        Aave stores rates in RAY precision. The rates are already annualized,
        so we just need to convert from RAY units to a decimal.

        Args:
            ray_value: Rate value in RAY units (1e27)

        Returns:
            Decimal value (e.g., 0.03 for 3% APY)
        """
        try:
            return Decimal(str(ray_value)) / RAY
        except (ValueError, TypeError, InvalidOperation):
            return Decimal("0")

    def _normalize_market_symbol(self, market: str) -> str:
        """Normalize market symbol for querying.

        Args:
            market: Market symbol (e.g., "USDC", "usdc", "WETH", "ETH")

        Returns:
            Normalized symbol in uppercase
        """
        symbol = market.upper().strip()
        # Handle common aliases
        if symbol == "ETH":
            symbol = "WETH"
        return symbol

    def _create_fallback_result(self, timestamp: datetime) -> APYResult:
        """Create a fallback APYResult with LOW confidence.

        Args:
            timestamp: Timestamp for the result

        Returns:
            APYResult with fallback APY values and LOW confidence
        """
        return APYResult(
            supply_apy=self._config.supply_apy_fallback,
            borrow_apy=self._config.borrow_apy_fallback,
            source_info=DataSourceInfo(
                source="fallback",
                confidence=DataConfidence.LOW,
                timestamp=timestamp,
            ),
        )

    def _parse_apy_data(self, history_item: dict[str, Any]) -> APYResult:
        """Parse subgraph response into APYResult.

        Args:
            history_item: Raw data from subgraph reserveParamsHistoryItems query

        Returns:
            APYResult with HIGH confidence
        """
        timestamp = int(history_item.get("timestamp", 0))
        dt = datetime.fromtimestamp(timestamp, tz=UTC)

        # Convert RAY rates to decimal APY
        supply_apy = self._ray_to_decimal(history_item.get("liquidityRate", "0"))
        borrow_apy = self._ray_to_decimal(history_item.get("variableBorrowRate", "0"))

        return APYResult(
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            source_info=DataSourceInfo(
                source=DATA_SOURCE,
                confidence=DataConfidence.HIGH,
                timestamp=dt,
            ),
        )

    async def _find_reserve_id(
        self,
        chain: Chain,
        symbol: str,
    ) -> str | None:
        """Find the reserve ID for a given symbol on a chain.

        The reserve ID in Aave V3 subgraph is typically in the format:
        {underlyingAsset}-{poolAddress}-{poolId}

        Args:
            chain: The blockchain
            symbol: Asset symbol (e.g., "USDC", "WETH")

        Returns:
            Reserve ID or None if not found
        """
        # Check cache first
        cache_key = f"{chain.value}:{symbol}"
        if cache_key in self._reserve_cache:
            return self._reserve_cache.get(cache_key, {}).get("id")

        subgraph_id = self._get_subgraph_id(chain)
        if subgraph_id is None:
            return None

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=RESERVES_BY_SYMBOL_QUERY,
                variables={"symbol": symbol},
            )

            reserves = data.get("reserves", [])
            if not reserves:
                logger.warning(
                    "No reserve found for symbol=%s on chain=%s",
                    symbol,
                    chain.value,
                )
                return None

            # Use the first matching reserve
            reserve_id = reserves[0].get("id")
            if reserve_id:
                self._reserve_cache[cache_key] = {"id": reserve_id}
                logger.debug(
                    "Found reserve: symbol=%s, chain=%s, id=%s",
                    symbol,
                    chain.value,
                    reserve_id[:30] + "..." if len(reserve_id) > 30 else reserve_id,
                )
            return reserve_id

        except (SubgraphQueryError, SubgraphRateLimitError) as e:
            logger.error(
                "Error finding reserve: symbol=%s, chain=%s, error=%s",
                symbol,
                chain.value,
                str(e),
            )
            return None

    async def get_apy(
        self,
        protocol: str,
        market: str,
        start_date: datetime,
        end_date: datetime,
        *,
        _chain_override: Chain | None = None,
    ) -> list[APYResult]:
        """Fetch historical APY data for an Aave V3 market.

        Queries The Graph's Aave V3 subgraph for historical rate snapshots
        (reserveParamsHistoryItems) within the specified date range.

        Args:
            protocol: The protocol identifier. Must be "aave_v3" or similar.
            market: The asset symbol (e.g., "USDC", "WETH", "DAI").
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
            _chain_override: Internal parameter for thread-safe chain override.
                Do not use directly; call get_apy_for_chain instead.

        Returns:
            List of APYResult objects containing supply and borrow APYs.
            Returns HIGH confidence results from subgraph data.
            Returns LOW confidence fallback results if subgraph unavailable.

        Example:
            apys = await provider.get_apy(
                protocol="aave_v3",
                market="USDC",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )
            for apy in apys:
                print(f"Supply: {apy.supply_apy:.4f}, Borrow: {apy.borrow_apy:.4f}")
        """
        # Normalize inputs
        symbol = self._normalize_market_symbol(market)
        chain = _chain_override if _chain_override is not None else self._config.chain

        # Ensure timestamps are in UTC (convert if needed to avoid day off-by-one)
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        else:
            start_date = start_date.astimezone(UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)
        else:
            end_date = end_date.astimezone(UTC)

        # Validate chain
        subgraph_id = self._get_subgraph_id(chain)
        if subgraph_id is None:
            logger.warning(
                "Unsupported chain for Aave V3: chain=%s. Returning fallback.",
                chain.value,
            )
            return self._generate_fallback_results(start_date, end_date)

        logger.info(
            "Fetching Aave V3 APY: chain=%s, market=%s, start=%s, end=%s",
            chain.value,
            symbol,
            start_date,
            end_date,
        )

        try:
            # Find the reserve ID for the symbol
            reserve_id = await self._find_reserve_id(chain, symbol)
            if reserve_id is None:
                logger.warning(
                    "Reserve not found: chain=%s, symbol=%s. Returning fallback.",
                    chain.value,
                    symbol,
                )
                return self._generate_fallback_results(start_date, end_date)

            # Convert to timestamps
            start_timestamp = int(start_date.timestamp())
            end_timestamp = int(end_date.timestamp())

            # Query subgraph
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=RESERVE_PARAMS_HISTORY_QUERY,
                variables={
                    "reserveId": reserve_id,
                    "startTimestamp": start_timestamp,
                    "endTimestamp": end_timestamp,
                },
            )

            history_items = data.get("reserveParamsHistoryItems", [])

            if not history_items:
                logger.warning(
                    "No APY history from subgraph: chain=%s, market=%s, range=%s to %s",
                    chain.value,
                    symbol,
                    start_date,
                    end_date,
                )
                return self._generate_fallback_results(start_date, end_date)

            # Parse results
            results = [self._parse_apy_data(item) for item in history_items]

            logger.info(
                "Fetched %d APY data points: chain=%s, market=%s",
                len(results),
                chain.value,
                symbol,
            )

            return results

        except SubgraphRateLimitError as e:
            logger.warning(
                "Subgraph rate limit exceeded: chain=%s, market=%s: %s",
                chain.value,
                symbol,
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except SubgraphQueryError as e:
            logger.error(
                "Subgraph query error: chain=%s, market=%s: %s",
                chain.value,
                symbol,
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except Exception as e:
            logger.error(
                "Unexpected error fetching APY: chain=%s, market=%s: %s",
                chain.value,
                symbol,
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

    def _generate_fallback_results(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[APYResult]:
        """Generate fallback results for a date range.

        Generates daily fallback results for the specified range.

        Args:
            start_date: Start datetime
            end_date: End datetime

        Returns:
            List of APYResult with LOW confidence fallback values
        """
        results = []
        current = start_date
        while current <= end_date:
            results.append(self._create_fallback_result(current))
            current += timedelta(days=1)
        return results

    async def get_apy_for_chain(
        self,
        chain: Chain,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[APYResult]:
        """Fetch historical APY data for a specific chain.

        Convenience method that temporarily overrides the config chain
        for a single query.

        Args:
            chain: The blockchain to query
            market: The asset symbol (e.g., "USDC", "WETH")
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            List of APYResult objects

        Example:
            apys = await provider.get_apy_for_chain(
                chain=Chain.ARBITRUM,
                market="USDC",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )
        """
        # Use chain override parameter for thread-safe chain switching
        return await self.get_apy(
            protocol="aave_v3",
            market=market,
            start_date=start_date,
            end_date=end_date,
            _chain_override=chain,
        )

    async def get_current_apy(
        self,
        market: str,
        chain: Chain | None = None,
    ) -> APYResult:
        """Fetch the current APY for a market.

        Convenience method to get just the most recent APY without
        fetching historical data.

        Args:
            market: The asset symbol (e.g., "USDC", "WETH")
            chain: Optional chain override (default: uses config.chain)

        Returns:
            APYResult with current rates

        Example:
            apy = await provider.get_current_apy("USDC")
            print(f"Current USDC supply APY: {apy.supply_apy:.4f}")
        """
        chain = chain or self._config.chain
        now = datetime.now(UTC)

        # Query for recent data (last 24 hours)
        start = now - timedelta(hours=24)

        results = await self.get_apy_for_chain(
            chain=chain,
            market=market,
            start_date=start,
            end_date=now,
        )

        if results:
            # Return the most recent result
            return results[-1]

        return self._create_fallback_result(now)


__all__ = [
    "AaveV3APYProvider",
    "AaveV3ClientConfig",
    "AAVE_V3_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
    "DEFAULT_SUPPLY_APY_FALLBACK",
    "DEFAULT_BORROW_APY_FALLBACK",
]
