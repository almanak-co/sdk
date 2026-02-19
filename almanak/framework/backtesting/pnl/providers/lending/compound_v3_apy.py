"""Compound V3 historical APY provider.

This module provides a historical APY data provider for Compound V3 (Comet) lending protocol
across multiple chains. It implements the HistoricalAPYProvider interface
and fetches data from The Graph's Compound V3 community subgraphs.

Key Features:
    - Supports Ethereum, Arbitrum, Polygon, Base chains
    - Fetches historical supply and borrow APR from DailyMarketAccounting
    - Integrates with SubgraphClient for rate limiting and retry logic
    - APR values are already in decimal format [0.0, 1.0]
    - Returns APYResult with HIGH confidence for subgraph data
    - Falls back to LOW confidence results when data unavailable

Compound V3 Subgraph Schema:
    - DailyMarketAccounting entity contains daily rate snapshots
    - `day` field: days since Unix epoch
    - `timestamp` field: seconds since Unix epoch
    - `accounting.supplyApr`: Base supply APR (decimal)
    - `accounting.borrowApr`: Base borrow APR (decimal)
    - `accounting.netSupplyApr`: Net supply APR (base + rewards)
    - `accounting.netBorrowApr`: Net borrow APR (base - rewards)

Subgraph Source:
    Community subgraph by Paperclip Labs
    https://github.com/papercliplabs/compound-v3-subgraph

Example:
    from almanak.framework.backtesting.pnl.providers.lending import (
        CompoundV3APYProvider,
    )
    from almanak.core.enums import Chain
    from datetime import datetime, UTC

    provider = CompoundV3APYProvider()

    # Fetch APY for a date range
    async with provider:
        apys = await provider.get_apy(
            protocol="compound_v3",
            market="USDC",  # or comet address
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
# Compound V3 Subgraph IDs (from Paperclip Labs community subgraph)
# =============================================================================

# Subgraph deployment IDs for Compound V3 on various chains
# Source: https://github.com/papercliplabs/compound-v3-subgraph
COMPOUND_V3_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp",
    Chain.ARBITRUM: "Ff7ha9ELmpmg81D6nYxy4t8aGP26dPztqD1LDJNPqjLS",
    Chain.POLYGON: "AaFtUWKfFdj2x8nnE3RxTSJkHwGHvawH3VWFBykCGzLs",
    Chain.BASE: "2hcXhs36pTBDVUmk5K2Zkr6N4UYGwaHuco2a6jyTsijo",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(COMPOUND_V3_SUBGRAPH_IDS.keys())

# Data source identifier
DATA_SOURCE = "compound_v3_subgraph"

# Default fallback APY values
DEFAULT_SUPPLY_APY_FALLBACK = Decimal("0.03")  # 3% APY
DEFAULT_BORROW_APY_FALLBACK = Decimal("0.05")  # 5% APY

# Known Comet (market) addresses by chain and base asset
# These are the main Compound V3 market addresses
KNOWN_COMET_ADDRESSES: dict[Chain, dict[str, str]] = {
    Chain.ETHEREUM: {
        "USDC": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "WETH": "0xA17581A9E3356d9A858b789D68B4d866e593aE94",
        "USDT": "0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840",
    },
    Chain.ARBITRUM: {
        "USDC": "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA",
        "USDC.e": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
        "USDT": "0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07",
        "WETH": "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486",
    },
    Chain.POLYGON: {
        "USDC": "0xF25212E676D1F7F89Cd72fFEe66158f541246445",
        "USDT": "0xaeB318360f27748Acb200CE616E389A6C9409a07",
    },
    Chain.BASE: {
        "USDC": "0xb125E6687d4313864e53df431d5425969c15Eb2F",
        "USDbC": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
        "WETH": "0x46e6b214b524310239732D51387075E0e70970bf",
        "AERO": "0x784efeB622244d2348d4F2522f8860B96fbEcE89",
    },
}

# GraphQL query for fetching daily market accounting data
# Uses day number for filtering (days since epoch)
DAILY_MARKET_ACCOUNTING_QUERY = """
query GetDailyMarketAccounting($marketId: String!, $startDay: BigInt!, $endDay: BigInt!) {
    dailyMarketAccountings(
        first: 1000
        where: {
            market_: { id: $marketId }
            day_gte: $startDay
            day_lte: $endDay
        }
        orderBy: day
        orderDirection: asc
    ) {
        id
        day
        timestamp
        accounting {
            supplyApr
            borrowApr
            rewardSupplyApr
            rewardBorrowApr
            netSupplyApr
            netBorrowApr
            utilization
        }
    }
}
"""

# GraphQL query to list available markets
MARKETS_QUERY = """
query GetMarkets {
    markets {
        id
        cometProxy
        protocol {
            id
        }
    }
}
"""


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CompoundV3ClientConfig:
    """Configuration for Compound V3 APY provider.

    Attributes:
        chain: Default chain for requests (default: ETHEREUM)
        requests_per_minute: Rate limit for subgraph requests (default: 100)
        supply_apy_fallback: Fallback supply APY when data unavailable
        borrow_apy_fallback: Fallback borrow APY when data unavailable
        use_net_rates: If True, use net rates (includes rewards); else use base rates
    """

    chain: Chain = Chain.ETHEREUM
    requests_per_minute: int = 100
    supply_apy_fallback: Decimal = DEFAULT_SUPPLY_APY_FALLBACK
    borrow_apy_fallback: Decimal = DEFAULT_BORROW_APY_FALLBACK
    use_net_rates: bool = False  # Use base rates by default for consistency


# =============================================================================
# CompoundV3APYProvider
# =============================================================================


class CompoundV3APYProvider(HistoricalAPYProvider):
    """Historical APY provider for Compound V3 (Comet) lending protocol.

    Fetches historical supply and borrow APY data from The Graph's Compound V3
    community subgraphs for Ethereum, Arbitrum, Polygon, and Base.

    The provider queries DailyMarketAccounting which contains daily rate snapshots.
    Rates are already in decimal format [0.0, 1.0], so no conversion is needed.

    Attributes:
        config: Client configuration
        client: SubgraphClient for querying The Graph

    Example:
        provider = CompoundV3APYProvider()

        # Use as async context manager
        async with provider:
            apys = await provider.get_apy(
                protocol="compound_v3",
                market="USDC",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )

        # Or manually close
        provider = CompoundV3APYProvider()
        try:
            apys = await provider.get_apy(...)
        finally:
            await provider.close()
    """

    def __init__(
        self,
        config: CompoundV3ClientConfig | None = None,
        client: SubgraphClient | None = None,
    ) -> None:
        """Initialize the Compound V3 APY provider.

        Args:
            config: Client configuration. If None, uses defaults.
            client: Optional SubgraphClient instance. If None, creates one
                    using THEGRAPH_API_KEY from environment.
        """
        self._config = config or CompoundV3ClientConfig()

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
            "Initialized CompoundV3APYProvider: chain=%s, supported_chains=%s",
            self._config.chain.value,
            [c.value for c in SUPPORTED_CHAINS],
        )

    @property
    def config(self) -> CompoundV3ClientConfig:
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
        logger.debug("CompoundV3APYProvider closed")

    async def __aenter__(self) -> "CompoundV3APYProvider":
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
        return COMPOUND_V3_SUBGRAPH_IDS.get(chain)

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

        Args:
            value: Value from subgraph (string or number)

        Returns:
            Decimal value, or 0 if parsing fails
        """
        if value is None:
            return Decimal("0")
        try:
            return Decimal(str(value))
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

    def _get_comet_address(self, chain: Chain, symbol: str) -> str | None:
        """Get the Comet (market) address for a symbol on a chain.

        Args:
            chain: The blockchain
            symbol: Asset symbol (e.g., "USDC", "WETH")

        Returns:
            Comet address in lowercase or None if not found
        """
        chain_markets = KNOWN_COMET_ADDRESSES.get(chain, {})
        # Case-insensitive lookup to handle mixed-case symbols like USDC.e, USDbC
        chain_markets_upper = {k.upper(): v for k, v in chain_markets.items()}
        address = chain_markets_upper.get(symbol.upper())
        if address:
            return address.lower()
        return None

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

    def _parse_apy_data(self, daily_accounting: dict[str, Any]) -> APYResult:
        """Parse subgraph response into APYResult.

        Args:
            daily_accounting: Raw data from subgraph DailyMarketAccounting query

        Returns:
            APYResult with HIGH confidence
        """
        timestamp = int(daily_accounting.get("timestamp", 0))
        dt = datetime.fromtimestamp(timestamp, tz=UTC)

        accounting = daily_accounting.get("accounting", {})

        # Use net rates or base rates based on config
        if self._config.use_net_rates:
            supply_apy = self._parse_decimal(accounting.get("netSupplyApr"))
            borrow_apy = self._parse_decimal(accounting.get("netBorrowApr"))
        else:
            supply_apy = self._parse_decimal(accounting.get("supplyApr"))
            borrow_apy = self._parse_decimal(accounting.get("borrowApr"))

        return APYResult(
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            source_info=DataSourceInfo(
                source=DATA_SOURCE,
                confidence=DataConfidence.HIGH,
                timestamp=dt,
            ),
        )

    def _resolve_market_id(self, chain: Chain, market: str) -> str | None:
        """Resolve market identifier to a comet address or market ID.

        Args:
            chain: The blockchain
            market: Market symbol (e.g., "USDC") or comet address

        Returns:
            Market ID (comet address in lowercase) or None if not found
        """
        # Check if market is already an address (0x prefix)
        if market.startswith("0x") and len(market) == 42:
            return market.lower()

        # Normalize and look up in known addresses
        symbol = self._normalize_market_symbol(market)
        return self._get_comet_address(chain, symbol)

    async def get_apy(
        self,
        protocol: str,
        market: str,
        start_date: datetime,
        end_date: datetime,
        *,
        _chain_override: Chain | None = None,
    ) -> list[APYResult]:
        """Fetch historical APY data for a Compound V3 market.

        Queries The Graph's Compound V3 subgraph for historical rate snapshots
        (DailyMarketAccounting) within the specified date range.

        Args:
            protocol: The protocol identifier. Must be "compound_v3" or similar.
            market: The asset symbol (e.g., "USDC", "WETH") or comet address.
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
                protocol="compound_v3",
                market="USDC",
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
                "Unsupported chain for Compound V3: chain=%s. Returning fallback.",
                chain.value,
            )
            return self._generate_fallback_results(start_date, end_date)

        # Resolve market to comet address
        market_id = self._resolve_market_id(chain, market)
        if market_id is None:
            logger.warning(
                "Unknown market for Compound V3: chain=%s, market=%s. Returning fallback.",
                chain.value,
                market,
            )
            return self._generate_fallback_results(start_date, end_date)

        logger.info(
            "Fetching Compound V3 APY: chain=%s, market=%s (%s), start=%s, end=%s",
            chain.value,
            market,
            market_id[:10] + "...",
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
                query=DAILY_MARKET_ACCOUNTING_QUERY,
                variables={
                    "marketId": market_id,
                    "startDay": str(start_day),
                    "endDay": str(end_day),
                },
            )

            daily_accountings = data.get("dailyMarketAccountings", [])

            if not daily_accountings:
                logger.warning(
                    "No APY history from subgraph: chain=%s, market=%s, range=%s to %s",
                    chain.value,
                    market,
                    start_date,
                    end_date,
                )
                return self._generate_fallback_results(start_date, end_date)

            # Parse results
            results = [self._parse_apy_data(item) for item in daily_accountings]

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

        Convenience method that temporarily overrides the config chain
        for a single query.

        Args:
            chain: The blockchain to query
            market: The asset symbol (e.g., "USDC", "WETH") or comet address
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
            protocol="compound_v3",
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
            market: The asset symbol (e.g., "USDC", "WETH") or comet address
            chain: Optional chain override (default: uses config.chain)

        Returns:
            APYResult with current rates

        Example:
            apy = await provider.get_current_apy("USDC")
            print(f"Current USDC supply APY: {apy.supply_apy:.4f}")
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

    async def list_markets(self, chain: Chain | None = None) -> list[dict[str, str]]:
        """List available markets on a chain.

        Args:
            chain: The blockchain to query (default: uses config.chain)

        Returns:
            List of market info dicts with 'id' and 'cometProxy' keys
        """
        chain = chain or self._config.chain
        subgraph_id = self._get_subgraph_id(chain)

        if subgraph_id is None:
            logger.warning("Unsupported chain for Compound V3: chain=%s", chain.value)
            return []

        try:
            data = await self._client.query(
                subgraph_id=subgraph_id,
                query=MARKETS_QUERY,
                variables={},
            )

            markets = data.get("markets", [])
            logger.info("Found %d markets on chain=%s", len(markets), chain.value)
            return markets

        except (SubgraphQueryError, SubgraphRateLimitError) as e:
            logger.error("Error listing markets: chain=%s, error=%s", chain.value, str(e))
            return []


__all__ = [
    "CompoundV3APYProvider",
    "CompoundV3ClientConfig",
    "COMPOUND_V3_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
    "DEFAULT_SUPPLY_APY_FALLBACK",
    "DEFAULT_BORROW_APY_FALLBACK",
    "KNOWN_COMET_ADDRESSES",
]
