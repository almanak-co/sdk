"""GMX V2 historical funding rate provider.

This module provides a historical funding rate data provider for GMX V2 perpetuals
on Arbitrum and Avalanche. It implements the HistoricalFundingProvider interface
and fetches data from the GMX Stats API.

Key Features:
    - Supports Arbitrum and Avalanche chains
    - Fetches funding rates from GMX Stats API (/markets/info endpoint)
    - Rate limiting (~30 req/min) to avoid API throttling
    - Returns FundingResult with MEDIUM confidence for API data (current rate as historical approximation)
    - Falls back to LOW confidence results when API unavailable

GMX V2 Funding Rate Notes:
    - GMX V2 uses adaptive funding rates based on open interest imbalance
    - Rates are expressed per hour as decimals (e.g., 0.0001 = 0.01%/hr)
    - Long and short positions can have different funding rates
    - When Long OI > Short OI, longs pay shorts (positive rate for longs)

API Information:
    - Arbitrum: https://arbitrum-api.gmxinfra.io/markets/info
    - Avalanche: https://avalanche-api.gmxinfra.io/markets/info
    - Rate limit: ~30 requests per minute (conservative estimate)

Example:
    from almanak.framework.backtesting.pnl.providers.perp import GMXFundingProvider
    from datetime import datetime, UTC

    provider = GMXFundingProvider()

    # Fetch funding rates for a date range
    async with provider:
        rates = await provider.get_funding_rates(
            market="ETH-USD",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )
        for rate in rates:
            print(f"{rate.source_info.timestamp}: {rate.rate}")
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import aiohttp

from almanak.core.enums import Chain

from ...types import DataConfidence, DataSourceInfo, FundingResult
from ..base import HistoricalFundingProvider
from ..rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# GMX Stats API URLs by chain
GMX_API_URLS: dict[Chain, str] = {
    Chain.ARBITRUM: "https://arbitrum-api.gmxinfra.io",
    Chain.AVALANCHE: "https://avalanche-api.gmxinfra.io",
}

# Fallback API URLs for redundancy
GMX_API_FALLBACK_URLS: dict[Chain, list[str]] = {
    Chain.ARBITRUM: [
        "https://arbitrum-api-fallback.gmxinfra.io",
        "https://arbitrum-api-fallback.gmxinfra2.io",
    ],
    Chain.AVALANCHE: [
        "https://avalanche-api-fallback.gmxinfra.io",
    ],
}

# GMX V2 market token addresses (Arbitrum)
# These map market symbols to market token addresses for lookup
GMX_MARKET_TOKENS: dict[str, dict[str, str]] = {
    "ETH-USD": {
        "arbitrum": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
    },
    "BTC-USD": {
        "arbitrum": "0x47c031236e19d024b42f8AE6780E44A573170703",
    },
    "SOL-USD": {
        "arbitrum": "0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
    },
    "ARB-USD": {
        "arbitrum": "0xC25cEf6061Cf5De5eb761b50E4743c1F5D7E5407",
    },
    "LINK-USD": {
        "arbitrum": "0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
    },
    "DOGE-USD": {
        "arbitrum": "0x6853EA96FF216fAb11D2d930CE3C508556A4bdc4",
    },
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = [Chain.ARBITRUM, Chain.AVALANCHE]

# Data source identifier
DATA_SOURCE = "gmx_api"

# Default rate limit: ~30 requests per minute (conservative for GMX API)
DEFAULT_REQUESTS_PER_MINUTE = 30

# Default HTTP timeout in seconds
DEFAULT_TIMEOUT_SECONDS = 30

# Funding rate interval (GMX funding is calculated continuously, sampled hourly)
FUNDING_INTERVAL_HOURS = 1


# =============================================================================
# Exceptions
# =============================================================================


class GMXAPIError(Exception):
    """Raised when GMX API request fails."""


class GMXRateLimitError(GMXAPIError):
    """Raised when GMX API rate limit is exceeded."""


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class GMXClientConfig:
    """Configuration for GMX API client.

    Attributes:
        requests_per_minute: Rate limit for requests (default: 30)
        timeout_seconds: HTTP request timeout (default: 30)
        chain: Default chain for requests (default: ARBITRUM)
        fallback_rate: Fallback funding rate when API unavailable
    """

    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    chain: Chain = Chain.ARBITRUM
    fallback_rate: Decimal = Decimal("0.0001")  # 0.01% per hour


@dataclass
class GMXMarketInfo:
    """Market information from GMX API.

    Attributes:
        name: Market name (e.g., "ETH/USD [WETH-USDC]")
        market_token: Market token address
        index_token: Index token address
        long_token: Long collateral token address
        short_token: Short collateral token address
        funding_rate_long: Current funding rate for long positions
        funding_rate_short: Current funding rate for short positions
        net_rate_long: Net rate (funding + borrowing) for longs
        net_rate_short: Net rate (funding + borrowing) for shorts
    """

    name: str
    market_token: str
    index_token: str
    long_token: str
    short_token: str
    funding_rate_long: Decimal
    funding_rate_short: Decimal
    net_rate_long: Decimal
    net_rate_short: Decimal


# =============================================================================
# GMXFundingProvider
# =============================================================================


class GMXFundingProvider(HistoricalFundingProvider):
    """Historical funding rate provider for GMX V2 perpetuals.

    Fetches funding rate data from the GMX Stats API for Arbitrum and Avalanche
    chains. The API provides current funding rates; for historical backtesting,
    rates are interpolated based on the assumption that rates change gradually.

    Important: GMX V2 does not provide a direct historical funding rate API.
    This provider fetches current rates and generates interpolated historical
    data for backtesting purposes. For accurate historical data, consider
    using on-chain event data from the GMX subgraph.

    Attributes:
        config: Client configuration
        rate_limiter: Rate limiter for API requests

    Example:
        provider = GMXFundingProvider()

        async with provider:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )

        # Or manually close
        provider = GMXFundingProvider()
        try:
            rates = await provider.get_funding_rates(...)
        finally:
            await provider.close()
    """

    def __init__(
        self,
        config: GMXClientConfig | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        """Initialize the GMX funding rate provider.

        Args:
            config: Client configuration. If None, uses defaults.
            rate_limiter: Optional rate limiter. If None, creates one
                          based on config.requests_per_minute.
        """
        self._config = config or GMXClientConfig()

        # Create or use provided rate limiter
        if rate_limiter is not None:
            self._rate_limiter = rate_limiter
            self._owns_rate_limiter = False
        else:
            self._rate_limiter = TokenBucketRateLimiter(
                requests_per_minute=self._config.requests_per_minute,
            )
            self._owns_rate_limiter = True

        # HTTP session (lazy initialized)
        self._session: aiohttp.ClientSession | None = None

        # Cache for market info to avoid repeated API calls
        # Uses per-chain timestamps to properly invalidate each chain's cache independently
        self._market_cache: dict[str, list[GMXMarketInfo]] = {}
        self._cache_timestamps: dict[str, datetime] = {}
        self._cache_ttl_seconds = 60  # Cache for 1 minute

        logger.debug(
            "Initialized GMXFundingProvider: chain=%s, rate_limit=%d req/min",
            self._config.chain.value,
            self._config.requests_per_minute,
        )

    @property
    def config(self) -> GMXClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        """Get the rate limiter."""
        return self._rate_limiter

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session.

        Returns:
            aiohttp.ClientSession with configured timeout
        """
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._config.timeout_seconds)
            connector = aiohttp.TCPConnector(
                limit=10,
                limit_per_host=5,
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session and release resources."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
        logger.debug("GMXFundingProvider session closed")

    async def __aenter__(self) -> "GMXFundingProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the session."""
        await self.close()

    def _get_api_url(self, chain: Chain) -> str:
        """Get the API URL for a chain.

        Args:
            chain: The blockchain to get API URL for

        Returns:
            API base URL

        Raises:
            ValueError: If chain is not supported
        """
        if chain not in GMX_API_URLS:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {[c.value for c in SUPPORTED_CHAINS]}")
        return GMX_API_URLS[chain]

    def _parse_market_info(self, data: dict[str, Any]) -> GMXMarketInfo:
        """Parse market info from API response.

        Args:
            data: Raw market data from API

        Returns:
            Parsed GMXMarketInfo object
        """

        # Parse funding rates (stored as raw numbers, need to scale)
        # GMX rates are typically in 1e30 scale, need to normalize
        def parse_rate(value: Any) -> Decimal:
            """Parse rate value to Decimal."""
            if value is None:
                return Decimal("0")
            try:
                # GMX rates are in high precision, normalize to percentage per hour
                # Rate is per-second, multiply by 3600 to get hourly rate
                raw_rate = Decimal(str(value))
                # Normalize from 1e30 scale to human-readable
                hourly_rate = raw_rate * 3600 / Decimal("1e30")
                return hourly_rate
            except (ValueError, TypeError):
                return Decimal("0")

        return GMXMarketInfo(
            name=data.get("name", ""),
            market_token=data.get("marketToken", ""),
            index_token=data.get("indexToken", ""),
            long_token=data.get("longToken", ""),
            short_token=data.get("shortToken", ""),
            funding_rate_long=parse_rate(data.get("fundingRateLong")),
            funding_rate_short=parse_rate(data.get("fundingRateShort")),
            net_rate_long=parse_rate(data.get("netRateLong")),
            net_rate_short=parse_rate(data.get("netRateShort")),
        )

    async def _fetch_markets_info(self, chain: Chain) -> list[GMXMarketInfo]:
        """Fetch market information from GMX API.

        Args:
            chain: The blockchain to fetch markets for

        Returns:
            List of GMXMarketInfo objects

        Raises:
            GMXAPIError: If API request fails
            GMXRateLimitError: If rate limit exceeded
        """
        # Check cache with per-chain timestamp
        cache_key = chain.value
        now = datetime.now(UTC)
        if (
            cache_key in self._market_cache
            and cache_key in self._cache_timestamps
            and (now - self._cache_timestamps[cache_key]).total_seconds() < self._cache_ttl_seconds
        ):
            logger.debug("Using cached market info for chain=%s", chain.value)
            return self._market_cache[cache_key]

        # Acquire rate limit token
        await self._rate_limiter.acquire()

        session = await self._get_session()
        base_url = self._get_api_url(chain)
        url = f"{base_url}/markets/info"

        logger.info("Fetching GMX markets info: chain=%s, url=%s", chain.value, url)

        try:
            async with session.get(url) as response:
                if response.status == 429:
                    await self._rate_limiter.on_rate_limit_response()
                    raise GMXRateLimitError("GMX API rate limit exceeded")

                if response.status != 200:
                    error_text = await response.text()
                    raise GMXAPIError(f"GMX API error: HTTP {response.status}: {error_text[:500]}")

                data = await response.json()

                # Parse markets
                markets = [self._parse_market_info(m) for m in data if isinstance(m, dict)]

                # Update cache with per-chain timestamp
                self._market_cache[cache_key] = markets
                self._cache_timestamps[cache_key] = now

                logger.info("Fetched %d markets from GMX API: chain=%s", len(markets), chain.value)
                return markets

        except aiohttp.ClientError as e:
            logger.error("GMX API connection error: chain=%s, error=%s", chain.value, str(e))
            raise GMXAPIError(f"Connection failed: {e}") from e

    def _find_market(
        self,
        markets: list[GMXMarketInfo],
        market_symbol: str,
    ) -> GMXMarketInfo | None:
        """Find a market by symbol.

        Args:
            markets: List of markets to search
            market_symbol: Market symbol to find (e.g., "ETH-USD", "BTC-USD")

        Returns:
            Matching market or None if not found
        """
        # Normalize the search symbol
        symbol_parts = market_symbol.upper().replace("-", "/").split("/")
        if len(symbol_parts) < 2:
            return None

        base_token = symbol_parts[0]  # e.g., "ETH"
        quote_token = symbol_parts[1] if len(symbol_parts) > 1 else "USD"  # e.g., "USD"
        target_pair = f"{base_token}/{quote_token}"

        # Search by exact pair match in the name prefix (e.g., "ETH/USD [ETH-USDC]")
        # Use exact matching to avoid USD matching USDC
        for market in markets:
            # Extract pair from market name - typically "ETH/USD [ETH-USDC]" format
            # Take the part before space or bracket
            market_pair = market.name.split(" ")[0].split("[")[0].replace("-", "/").upper().strip()
            if market_pair == target_pair:
                return market

        # Also try market token address lookup
        if market_symbol in GMX_MARKET_TOKENS:
            market_tokens = GMX_MARKET_TOKENS[market_symbol]
            for market in markets:
                if market.market_token.lower() in [addr.lower() for addr in market_tokens.values()]:
                    return market

        return None

    def _create_fallback_result(self, timestamp: datetime) -> FundingResult:
        """Create a fallback FundingResult with LOW confidence.

        Args:
            timestamp: Timestamp for the result

        Returns:
            FundingResult with fallback rate and LOW confidence
        """
        return FundingResult(
            rate=self._config.fallback_rate,
            source_info=DataSourceInfo(
                source="fallback",
                confidence=DataConfidence.LOW,
                timestamp=timestamp,
            ),
        )

    def _create_result(
        self,
        rate: Decimal,
        timestamp: datetime,
        confidence: DataConfidence = DataConfidence.HIGH,
    ) -> FundingResult:
        """Create a FundingResult.

        Args:
            rate: Funding rate value
            timestamp: Timestamp for the result
            confidence: Confidence level (default HIGH)

        Returns:
            FundingResult with the specified values
        """
        return FundingResult(
            rate=rate,
            source_info=DataSourceInfo(
                source=DATA_SOURCE,
                confidence=confidence,
                timestamp=timestamp,
            ),
        )

    async def get_funding_rates(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult]:
        """Fetch historical funding rates for a GMX V2 market.

        This method fetches the current funding rate from the GMX API and
        generates hourly data points for the requested date range. Since GMX
        does not provide a historical funding rate API, the current rate is
        used as an approximation.

        For more accurate historical data, consider using on-chain event data
        from the GMX synthetics subgraph.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD").
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of FundingResult objects, one per hour in the date range.
            Returns HIGH confidence results from API data.
            Returns LOW confidence fallback results if API unavailable.

        Example:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
            )
            for rate in rates:
                print(f"{rate.source_info.timestamp}: {rate.rate:.6f}")
        """
        logger.info(
            "Fetching GMX funding rates: market=%s, start=%s, end=%s",
            market,
            start_date,
            end_date,
        )

        # Ensure timestamps have timezone info
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)

        try:
            # Fetch current market info
            markets = await self._fetch_markets_info(self._config.chain)

            # Find the requested market
            market_info = self._find_market(markets, market)
            if market_info is None:
                logger.warning(
                    "Market not found: market=%s, available markets: %s",
                    market,
                    [m.name for m in markets[:5]],
                )
                return self._generate_fallback_results(start_date, end_date)

            # Use the long funding rate as the primary rate
            # (positive rate means longs pay shorts)
            current_rate = market_info.funding_rate_long

            logger.info(
                "Found market %s: funding_rate_long=%s, funding_rate_short=%s",
                market_info.name,
                market_info.funding_rate_long,
                market_info.funding_rate_short,
            )

            # Generate hourly data points
            # Note: This uses current rate as approximation for historical data
            # For accurate historical data, use the GMX subgraph
            results = []
            current = start_date
            while current <= end_date:
                # Use MEDIUM confidence since this is current rate used for historical
                # (it's better than fallback but not true historical data)
                results.append(
                    self._create_result(
                        rate=current_rate,
                        timestamp=current,
                        confidence=DataConfidence.MEDIUM,
                    )
                )
                current += timedelta(hours=FUNDING_INTERVAL_HOURS)

            logger.info(
                "Generated %d funding rate data points for market=%s",
                len(results),
                market,
            )
            return results

        except GMXRateLimitError as e:
            logger.warning("GMX API rate limit exceeded: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

        except GMXAPIError as e:
            logger.error("GMX API error: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

        except Exception as e:
            logger.error("Unexpected error fetching funding rates: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

    def _generate_fallback_results(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult]:
        """Generate fallback results for a date range.

        Args:
            start_date: Start datetime
            end_date: End datetime

        Returns:
            List of FundingResult with LOW confidence fallback values
        """
        results = []
        current = start_date
        while current <= end_date:
            results.append(self._create_fallback_result(current))
            current += timedelta(hours=FUNDING_INTERVAL_HOURS)
        return results

    async def get_current_funding_rate(
        self,
        market: str,
        chain: Chain | None = None,
    ) -> FundingResult:
        """Fetch the current funding rate for a market.

        This is a convenience method to get just the current rate without
        generating historical data points.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD")
            chain: Optional chain override (default: uses config.chain)

        Returns:
            FundingResult with current rate

        Example:
            rate = await provider.get_current_funding_rate("ETH-USD")
            print(f"Current ETH-USD funding rate: {rate.rate:.6f}")
        """
        chain = chain or self._config.chain

        try:
            markets = await self._fetch_markets_info(chain)
            market_info = self._find_market(markets, market)

            if market_info is None:
                return self._create_fallback_result(datetime.now(UTC))

            return self._create_result(
                rate=market_info.funding_rate_long,
                timestamp=datetime.now(UTC),
                confidence=DataConfidence.HIGH,
            )

        except (GMXAPIError, GMXRateLimitError) as e:
            logger.error("Error fetching current funding rate: %s", str(e))
            return self._create_fallback_result(datetime.now(UTC))


__all__ = [
    # Constants (SCREAMING_SNAKE_CASE) first
    "DATA_SOURCE",
    "DEFAULT_REQUESTS_PER_MINUTE",
    "GMX_API_FALLBACK_URLS",
    "GMX_API_URLS",
    "GMX_MARKET_TOKENS",
    "SUPPORTED_CHAINS",
    # Classes (CamelCase) second
    "GMXAPIError",
    "GMXClientConfig",
    "GMXFundingProvider",
    "GMXMarketInfo",
    "GMXRateLimitError",
]
