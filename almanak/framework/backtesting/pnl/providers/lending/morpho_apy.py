"""Morpho Blue historical APY provider.

This module provides a historical APY data provider for Morpho Blue lending protocol
across multiple chains. It implements the HistoricalAPYProvider interface
and fetches data from The Graph's Morpho Blue subgraphs (using Messari schema).

Key Features:
    - Supports Ethereum and Base chains
    - Fetches historical supply and borrow APY from MarketDailySnapshot
    - Integrates with SubgraphClient for rate limiting and retry logic
    - Uses Messari standardized schema for lending protocols
    - Returns APYResult with HIGH confidence for subgraph data
    - Falls back to LOW confidence results when data unavailable

Morpho Blue Subgraph Schema (Messari):
    - MarketDailySnapshot entity contains daily rate snapshots
    - `days` field: days since Unix epoch
    - `timestamp` field: seconds since Unix epoch
    - `rates` array: InterestRate objects with rate, side (LENDER/BORROWER), type

Subgraph Source:
    Official Morpho Blue subgraph implementing Messari schema
    https://github.com/morpho-org/morpho-blue-subgraph

Example:
    from almanak.framework.backtesting.pnl.providers.lending import (
        MorphoBlueAPYProvider,
    )
    from almanak.core.enums import Chain
    from datetime import datetime, UTC

    provider = MorphoBlueAPYProvider()

    # Fetch APY for a date range
    async with provider:
        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x...",  # market ID (unique key)
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )
        for apy in apys:
            print(f"{apy.source_info.timestamp}: supply={apy.supply_apy:.4f}, borrow={apy.borrow_apy:.4f}")
"""

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
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
# Morpho Blue Subgraph IDs (from The Graph Explorer)
# =============================================================================

# Subgraph deployment IDs for Morpho Blue on various chains
# Source: https://docs.morpho.org/tools/offchain/subgraphs/
MORPHO_BLUE_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "8Lz789DP5VKLXumTMTgygjU2xtuzx8AhbaacgN5PYCAs",
    Chain.BASE: "71ZTy1veF9twER9CLMnPWeLQ7GZcwKsjmygejrgKirqs",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(MORPHO_BLUE_SUBGRAPH_IDS.keys())

# Data source identifier
DATA_SOURCE = "morpho_blue_subgraph"

# Default fallback APY values
DEFAULT_SUPPLY_APY_FALLBACK = Decimal("0.03")  # 3% APY
DEFAULT_BORROW_APY_FALLBACK = Decimal("0.05")  # 5% APY

# Interest rate side constants (Messari schema)
LENDER_SIDE = "LENDER"
BORROWER_SIDE = "BORROWER"

# GraphQL query for fetching market daily snapshots
# Uses Messari schema: days field is days since Unix epoch
MARKET_DAILY_SNAPSHOTS_QUERY = """
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
        id
        days
        timestamp
        rates {
            id
            rate
            side
            type
        }
    }
}
"""

# GraphQL query to list available markets
MARKETS_QUERY = """
query GetMarkets($first: Int!) {
    markets(first: $first, orderBy: totalBorrowBalanceUSD, orderDirection: desc) {
        id
        name
        inputToken {
            id
            symbol
            name
        }
    }
}
"""

# GraphQL query to find a market by input token symbol
MARKET_BY_TOKEN_QUERY = """
query GetMarketByToken($symbol: String!) {
    markets(first: 10, where: { inputToken_: { symbol: $symbol } }) {
        id
        name
        inputToken {
            id
            symbol
            name
        }
    }
}
"""


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MorphoBlueClientConfig:
    """Configuration for Morpho Blue APY provider.

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
# MorphoBlueAPYProvider
# =============================================================================


class MorphoBlueAPYProvider(HistoricalAPYProvider):
    """Historical APY provider for Morpho Blue lending protocol.

    Fetches historical supply and borrow APY data from The Graph's Morpho Blue
    subgraphs for Ethereum and Base chains.

    The provider queries MarketDailySnapshot which contains daily rate snapshots
    using the Messari standardized schema. Rates are stored as percentages
    in the rates array with LENDER/BORROWER side indicators.

    Attributes:
        config: Client configuration
        client: SubgraphClient for querying The Graph

    Example:
        provider = MorphoBlueAPYProvider()

        # Use as async context manager
        async with provider:
            apys = await provider.get_apy(
                protocol="morpho_blue",
                market="0x...",  # market unique key
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )

        # Or manually close
        provider = MorphoBlueAPYProvider()
        try:
            apys = await provider.get_apy(...)
        finally:
            await provider.close()
    """

    def __init__(
        self,
        config: MorphoBlueClientConfig | None = None,
        client: SubgraphClient | None = None,
    ) -> None:
        """Initialize the Morpho Blue APY provider.

        Args:
            config: Client configuration. If None, uses defaults.
            client: Optional SubgraphClient instance. If None, creates one
                    using THEGRAPH_API_KEY from environment.
        """
        self._config = config or MorphoBlueClientConfig()

        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            subgraph_config = SubgraphClientConfig(requests_per_minute=self._config.requests_per_minute)
            self._client = SubgraphClient(config=subgraph_config)
            self._owns_client = True

        # Cache for market IDs to avoid repeated lookups
        self._market_cache: dict[str, str] = {}

        logger.debug(
            "Initialized MorphoBlueAPYProvider: chain=%s, supported_chains=%s",
            self._config.chain.value,
            [c.value for c in SUPPORTED_CHAINS],
        )

    @property
    def config(self) -> MorphoBlueClientConfig:
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
        logger.debug("MorphoBlueAPYProvider closed")

    async def __aenter__(self) -> "MorphoBlueAPYProvider":
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
        return MORPHO_BLUE_SUBGRAPH_IDS.get(chain)

    def _date_to_day_number(self, dt: datetime | date) -> int:
        """Convert a date/datetime to day number (days since Unix epoch).

        Args:
            dt: The date or datetime to convert

        Returns:
            Number of days since January 1, 1970
        """
        if isinstance(dt, datetime):
            return (dt.date() - date(1970, 1, 1)).days
        return (dt - date(1970, 1, 1)).days

    def _parse_decimal(self, value: str | float | None) -> Decimal:
        """Parse a decimal value from subgraph response.

        The Messari schema stores rates as percentages (e.g., 5.21 for 5.21%).
        We convert to decimal form (0.0521).

        Args:
            value: Value from subgraph (string or number)

        Returns:
            Decimal value as fraction (not percentage), or 0 if parsing fails
        """
        if value is None:
            return Decimal("0")
        try:
            # Convert percentage to decimal (5.21% -> 0.0521)
            return Decimal(str(value)) / Decimal("100")
        except (ValueError, TypeError, InvalidOperation):
            return Decimal("0")

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

    def _extract_rates_from_snapshot(self, rates: list[dict[str, Any]]) -> tuple[Decimal, Decimal]:
        """Extract supply and borrow rates from rates array.

        The Messari schema stores rates in an array with side indicators.

        Args:
            rates: List of rate objects from snapshot

        Returns:
            Tuple of (supply_apy, borrow_apy) as Decimal
        """
        supply_apy = Decimal("0")
        borrow_apy = Decimal("0")

        for rate_info in rates:
            side = rate_info.get("side", "")
            rate_value = rate_info.get("rate")

            if side == LENDER_SIDE:
                supply_apy = self._parse_decimal(rate_value)
            elif side == BORROWER_SIDE:
                borrow_apy = self._parse_decimal(rate_value)

        return supply_apy, borrow_apy

    def _parse_apy_data(self, daily_snapshot: dict[str, Any]) -> APYResult:
        """Parse subgraph response into APYResult.

        Args:
            daily_snapshot: Raw data from subgraph MarketDailySnapshot query

        Returns:
            APYResult with HIGH confidence
        """
        timestamp = int(daily_snapshot.get("timestamp", 0))
        dt = datetime.fromtimestamp(timestamp, tz=UTC)

        rates = daily_snapshot.get("rates", [])
        supply_apy, borrow_apy = self._extract_rates_from_snapshot(rates)

        return APYResult(
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            source_info=DataSourceInfo(
                source=DATA_SOURCE,
                confidence=DataConfidence.HIGH,
                timestamp=dt,
            ),
        )

    def _normalize_market_id(self, market: str) -> str:
        """Normalize market identifier.

        Morpho Blue uses market IDs (unique keys) that are hex strings.

        Args:
            market: Market ID or symbol

        Returns:
            Normalized market ID in lowercase
        """
        return market.lower().strip()

    async def _find_market_by_token(
        self,
        chain: Chain,
        symbol: str,
    ) -> str | None:
        """Find a market ID by input token symbol.

        Args:
            chain: The blockchain
            symbol: Token symbol (e.g., "USDC", "WETH")

        Returns:
            Market ID or None if not found
        """
        # Check cache first
        cache_key = f"{chain.value}:{symbol}"
        if cache_key in self._market_cache:
            return self._market_cache[cache_key]

        subgraph_id = self._get_subgraph_id(chain)
        if subgraph_id is None:
            return None

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=MARKET_BY_TOKEN_QUERY,
                variables={"symbol": symbol.upper()},
            )

            markets = data.get("markets", [])
            if not markets:
                logger.warning(
                    "No market found for symbol=%s on chain=%s",
                    symbol,
                    chain.value,
                )
                return None

            # Use the first matching market
            market_id = markets[0].get("id")
            if market_id:
                self._market_cache[cache_key] = market_id
                logger.debug(
                    "Found market: symbol=%s, chain=%s, id=%s",
                    symbol,
                    chain.value,
                    market_id[:20] + "..." if len(market_id) > 20 else market_id,
                )
            return market_id

        except (SubgraphQueryError, SubgraphRateLimitError) as e:
            logger.error(
                "Error finding market: symbol=%s, chain=%s, error=%s",
                symbol,
                chain.value,
                str(e),
            )
            return None

    async def _resolve_market_id(self, chain: Chain, market: str) -> str | None:
        """Resolve market identifier to a market ID.

        Args:
            chain: The blockchain
            market: Market ID (hex string) or token symbol

        Returns:
            Market ID in lowercase or None if not found
        """
        # Check if market is already an ID (0x prefix or long hex string)
        if market.startswith("0x") or len(market) > 20:
            return self._normalize_market_id(market)

        # Try to find by token symbol
        return await self._find_market_by_token(chain, market)

    async def get_apy(
        self,
        protocol: str,
        market: str,
        start_date: datetime,
        end_date: datetime,
        *,
        _chain_override: Chain | None = None,
    ) -> list[APYResult]:
        """Fetch historical APY data for a Morpho Blue market.

        Queries The Graph's Morpho Blue subgraph for historical rate snapshots
        (MarketDailySnapshot) within the specified date range.

        Args:
            protocol: The protocol identifier. Must be "morpho_blue" or similar.
            market: The market ID (unique key) or token symbol (e.g., "USDC").
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
            _chain_override: Optional chain override for thread-safe multi-chain queries.

        Returns:
            List of APYResult objects containing supply and borrow APYs.
            Returns HIGH confidence results from subgraph data.
            Returns LOW confidence fallback results if subgraph unavailable.

        Example:
            apys = await provider.get_apy(
                protocol="morpho_blue",
                market="0x...",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )
            for apy in apys:
                print(f"Supply: {apy.supply_apy:.4f}, Borrow: {apy.borrow_apy:.4f}")
        """
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
                "Unsupported chain for Morpho Blue: chain=%s. Returning fallback.",
                chain.value,
            )
            return self._generate_fallback_results(start_date, end_date)

        # Resolve market ID
        market_id = await self._resolve_market_id(chain, market)
        if market_id is None:
            logger.warning(
                "Unknown market for Morpho Blue: chain=%s, market=%s. Returning fallback.",
                chain.value,
                market,
            )
            return self._generate_fallback_results(start_date, end_date)

        logger.info(
            "Fetching Morpho Blue APY: chain=%s, market=%s, start=%s, end=%s",
            chain.value,
            market_id[:20] + "..." if len(market_id) > 20 else market_id,
            start_date,
            end_date,
        )

        try:
            # Convert dates to day numbers
            start_day = self._date_to_day_number(start_date)
            end_day = self._date_to_day_number(end_date)

            # Query subgraph
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=MARKET_DAILY_SNAPSHOTS_QUERY,
                variables={
                    "marketId": market_id,
                    "startDay": start_day,
                    "endDay": end_day,
                },
            )

            daily_snapshots = data.get("marketDailySnapshots", [])

            if not daily_snapshots:
                logger.warning(
                    "No APY history from subgraph: chain=%s, market=%s, range=%s to %s",
                    chain.value,
                    market,
                    start_date,
                    end_date,
                )
                return self._generate_fallback_results(start_date, end_date)

            # Parse results
            results = [self._parse_apy_data(snapshot) for snapshot in daily_snapshots]

            logger.info(
                "Fetched %d APY data points: chain=%s, market=%s",
                len(results),
                chain.value,
                market,
            )

            return results

        except SubgraphRateLimitError as e:
            logger.warning(
                "Subgraph rate limit exceeded: chain=%s, market=%s: %s",
                chain.value,
                market,
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except SubgraphQueryError as e:
            logger.error(
                "Subgraph query error: chain=%s, market=%s: %s",
                chain.value,
                market,
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

        except Exception as e:
            logger.error(
                "Unexpected error fetching APY: chain=%s, market=%s: %s",
                chain.value,
                market,
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

        Thread-safe convenience method that uses chain override instead of
        mutating shared config state.

        Args:
            chain: The blockchain to query
            market: The market ID or token symbol
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            List of APYResult objects

        Example:
            apys = await provider.get_apy_for_chain(
                chain=Chain.BASE,
                market="0x...",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )
        """
        return await self.get_apy(
            protocol="morpho_blue",
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
            market: The market ID or token symbol
            chain: Optional chain override (default: uses config.chain)

        Returns:
            APYResult with current rates

        Example:
            apy = await provider.get_current_apy("0x...")
            print(f"Current supply APY: {apy.supply_apy:.4f}")
        """
        chain = chain or self._config.chain
        now = datetime.now(UTC)

        # Query for recent data (last 7 days to ensure we get data)
        start = now - timedelta(days=7)

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

    async def list_markets(self, chain: Chain | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """List available markets on a chain.

        Args:
            chain: The blockchain to query (default: uses config.chain)
            limit: Maximum number of markets to return (default: 100)

        Returns:
            List of market info dicts with 'id', 'name', and 'inputToken' keys
        """
        chain = chain or self._config.chain
        subgraph_id = self._get_subgraph_id(chain)

        if subgraph_id is None:
            logger.warning("Unsupported chain for Morpho Blue: chain=%s", chain.value)
            return []

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=MARKETS_QUERY,
                variables={"first": limit},
            )

            markets = data.get("markets", [])
            logger.info("Found %d markets on chain=%s", len(markets), chain.value)
            return markets

        except (SubgraphQueryError, SubgraphRateLimitError) as e:
            logger.error("Error listing markets: chain=%s, error=%s", chain.value, str(e))
            return []


__all__ = [
    "MorphoBlueAPYProvider",
    "MorphoBlueClientConfig",
    "MORPHO_BLUE_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
    "DEFAULT_SUPPLY_APY_FALLBACK",
    "DEFAULT_BORROW_APY_FALLBACK",
]
