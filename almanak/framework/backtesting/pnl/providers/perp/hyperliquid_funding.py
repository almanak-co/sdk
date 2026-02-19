"""Hyperliquid historical funding rate provider.

This module provides a historical funding rate data provider for Hyperliquid
perpetual futures. It implements the HistoricalFundingProvider interface
and fetches data from the Hyperliquid Info API.

Key Features:
    - Fetches true historical funding rates (not just current rates)
    - Supports hourly funding rate data
    - Rate limiting (~30 req/min conservative estimate)
    - Returns FundingResult with HIGH confidence for API data
    - Falls back to LOW confidence results when API unavailable

Hyperliquid Funding Rate Notes:
    - Funding is hourly and capped at 4%/hour
    - Formula: F = Average Premium Index (P) + clamp(interest rate - P, -0.0005, 0.0005)
    - Premium is sampled every 5 seconds and averaged over the hour
    - Positive rate means longs pay shorts

API Information:
    - Endpoint: POST https://api.hyperliquid.xyz/info
    - Request type: "fundingHistory"
    - Max 500 hours of history per request
    - Rate limit: ~30 requests per minute (conservative estimate)

Example:
    from almanak.framework.backtesting.pnl.providers.perp import HyperliquidFundingProvider
    from datetime import datetime, UTC

    provider = HyperliquidFundingProvider()

    # Fetch funding rates for a date range
    async with provider:
        rates = await provider.get_funding_rates(
            market="ETH-USD",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
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

from ...types import DataConfidence, DataSourceInfo, FundingResult
from ..base import HistoricalFundingProvider
from ..rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Hyperliquid API endpoint
HYPERLIQUID_API_URL = "https://api.hyperliquid.xyz/info"

# Data source identifier
DATA_SOURCE = "hyperliquid_api"

# Default rate limit: ~30 requests per minute (conservative estimate)
DEFAULT_REQUESTS_PER_MINUTE = 30

# Default HTTP timeout in seconds
DEFAULT_TIMEOUT_SECONDS = 30

# Funding rate interval (Hyperliquid funding is hourly)
FUNDING_INTERVAL_HOURS = 1

# Max hours per request (Hyperliquid limit)
MAX_HOURS_PER_REQUEST = 500


# =============================================================================
# Exceptions
# =============================================================================


class HyperliquidAPIError(Exception):
    """Raised when Hyperliquid API request fails."""


class HyperliquidRateLimitError(HyperliquidAPIError):
    """Raised when Hyperliquid API rate limit is exceeded."""


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class HyperliquidClientConfig:
    """Configuration for Hyperliquid API client.

    Attributes:
        requests_per_minute: Rate limit for requests (default: 30)
        timeout_seconds: HTTP request timeout (default: 30)
        fallback_rate: Fallback funding rate when API unavailable
    """

    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    fallback_rate: Decimal = Decimal("0.0001")  # 0.01% per hour


@dataclass
class HyperliquidFundingEntry:
    """Single funding rate entry from Hyperliquid API.

    Attributes:
        coin: Asset symbol (e.g., "ETH", "BTC")
        funding_rate: Funding rate for the hour
        premium: Premium value
        timestamp: UTC timestamp for this funding rate
    """

    coin: str
    funding_rate: Decimal
    premium: Decimal
    timestamp: datetime


# =============================================================================
# HyperliquidFundingProvider
# =============================================================================


class HyperliquidFundingProvider(HistoricalFundingProvider):
    """Historical funding rate provider for Hyperliquid perpetual futures.

    Fetches true historical funding rate data from the Hyperliquid Info API.
    Unlike GMX, Hyperliquid provides actual historical funding rates through
    the fundingHistory endpoint.

    Attributes:
        config: Client configuration
        rate_limiter: Rate limiter for API requests

    Example:
        provider = HyperliquidFundingProvider()

        async with provider:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
            )

        # Or manually close
        provider = HyperliquidFundingProvider()
        try:
            rates = await provider.get_funding_rates(...)
        finally:
            await provider.close()
    """

    def __init__(
        self,
        config: HyperliquidClientConfig | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        """Initialize the Hyperliquid funding rate provider.

        Args:
            config: Client configuration. If None, uses defaults.
            rate_limiter: Optional rate limiter. If None, creates one
                          based on config.requests_per_minute.
        """
        self._config = config or HyperliquidClientConfig()

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

        logger.debug(
            "Initialized HyperliquidFundingProvider: rate_limit=%d req/min",
            self._config.requests_per_minute,
        )

    @property
    def config(self) -> HyperliquidClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        """Get the rate limiter."""
        return self._rate_limiter

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
        logger.debug("HyperliquidFundingProvider session closed")

    async def __aenter__(self) -> "HyperliquidFundingProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the session."""
        await self.close()

    def _normalize_market_symbol(self, market: str) -> str:
        """Normalize market symbol to Hyperliquid coin format.

        Hyperliquid uses simple coin symbols like "ETH", "BTC".
        Accepts formats like "ETH-USD", "ETH/USD", "ETH-PERP", or just "ETH".

        Args:
            market: Market identifier in various formats

        Returns:
            Normalized coin symbol (e.g., "ETH")
        """
        # Remove common suffixes and separators
        symbol = market.upper()

        # Handle common formats
        for separator in ["-", "/", "_"]:
            if separator in symbol:
                symbol = symbol.split(separator)[0]
                break

        # Remove common suffixes
        for suffix in ["PERP", "USD", "USDT", "USDC"]:
            if symbol.endswith(suffix) and len(symbol) > len(suffix):
                symbol = symbol[: -len(suffix)]

        return symbol.strip()

    def _parse_funding_entry(self, data: dict[str, Any]) -> HyperliquidFundingEntry:
        """Parse funding rate entry from API response.

        Args:
            data: Raw funding entry from API

        Returns:
            Parsed HyperliquidFundingEntry object
        """
        # Hyperliquid returns funding rate as string decimal
        funding_rate_str = str(data.get("fundingRate", "0"))
        premium_str = str(data.get("premium", "0"))

        # Time is in milliseconds
        time_ms = data.get("time", 0)
        timestamp = datetime.fromtimestamp(time_ms / 1000, tz=UTC)

        return HyperliquidFundingEntry(
            coin=data.get("coin", ""),
            funding_rate=Decimal(funding_rate_str),
            premium=Decimal(premium_str),
            timestamp=timestamp,
        )

    async def _fetch_funding_history(
        self,
        coin: str,
        start_time_ms: int,
        end_time_ms: int | None = None,
    ) -> list[HyperliquidFundingEntry]:
        """Fetch funding rate history from Hyperliquid API.

        Args:
            coin: Coin symbol (e.g., "ETH", "BTC")
            start_time_ms: Start timestamp in milliseconds
            end_time_ms: End timestamp in milliseconds (optional)

        Returns:
            List of HyperliquidFundingEntry objects

        Raises:
            HyperliquidAPIError: If API request fails
            HyperliquidRateLimitError: If rate limit exceeded
        """
        # Acquire rate limit token
        await self._rate_limiter.acquire()

        session = await self._get_session()

        # Build request payload
        payload: dict[str, Any] = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_time_ms,
        }
        if end_time_ms is not None:
            payload["endTime"] = end_time_ms

        logger.debug(
            "Fetching Hyperliquid funding history: coin=%s, start=%s, end=%s",
            coin,
            start_time_ms,
            end_time_ms,
        )

        try:
            async with session.post(
                HYPERLIQUID_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status == 429:
                    await self._rate_limiter.on_rate_limit_response()
                    raise HyperliquidRateLimitError("Hyperliquid API rate limit exceeded")

                if response.status != 200:
                    error_text = await response.text()
                    raise HyperliquidAPIError(f"Hyperliquid API error: HTTP {response.status}: {error_text[:500]}")

                data = await response.json()

                # Response is a list of funding entries
                if not isinstance(data, list):
                    logger.warning("Unexpected response format: %s", type(data))
                    return []

                entries = [self._parse_funding_entry(entry) for entry in data if isinstance(entry, dict)]

                logger.debug(
                    "Fetched %d funding entries for coin=%s",
                    len(entries),
                    coin,
                )
                return entries

        except aiohttp.ClientError as e:
            logger.error("Hyperliquid API connection error: %s", str(e))
            raise HyperliquidAPIError(f"Connection failed: {e}") from e

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

    async def get_funding_rates(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult]:
        """Fetch historical funding rates for a Hyperliquid market.

        This method fetches true historical funding rates from the Hyperliquid API.
        Data is returned at hourly intervals.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD", "ETH").
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
            "Fetching Hyperliquid funding rates: market=%s, start=%s, end=%s",
            market,
            start_date,
            end_date,
        )

        # Ensure timestamps have timezone info
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)

        # Normalize market symbol to coin
        coin = self._normalize_market_symbol(market)

        # Convert to milliseconds
        start_time_ms = int(start_date.timestamp() * 1000)
        end_time_ms = int(end_date.timestamp() * 1000)

        try:
            # Hyperliquid limits to 500 hours per request
            # Chunk requests if needed
            all_entries: list[HyperliquidFundingEntry] = []
            current_start_ms = start_time_ms

            while current_start_ms < end_time_ms:
                # Calculate chunk end (max 500 hours from start)
                chunk_end_ms = min(
                    current_start_ms + (MAX_HOURS_PER_REQUEST * 3600 * 1000),
                    end_time_ms,
                )

                entries = await self._fetch_funding_history(
                    coin=coin,
                    start_time_ms=current_start_ms,
                    end_time_ms=chunk_end_ms,
                )

                all_entries.extend(entries)

                # Move to next chunk
                current_start_ms = chunk_end_ms

            if not all_entries:
                logger.warning(
                    "No funding data returned for coin=%s, returning fallback",
                    coin,
                )
                return self._generate_fallback_results(start_date, end_date)

            # Sort entries by timestamp and convert to FundingResult
            all_entries.sort(key=lambda x: x.timestamp)

            results = [
                self._create_result(
                    rate=entry.funding_rate,
                    timestamp=entry.timestamp,
                    confidence=DataConfidence.HIGH,
                )
                for entry in all_entries
            ]

            logger.info(
                "Fetched %d funding rate data points for market=%s",
                len(results),
                market,
            )
            return results

        except HyperliquidRateLimitError as e:
            logger.warning("Hyperliquid API rate limit exceeded: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

        except HyperliquidAPIError as e:
            logger.error("Hyperliquid API error: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

        except Exception as e:
            logger.error("Unexpected error fetching funding rates: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

    async def get_current_funding_rate(
        self,
        market: str,
    ) -> FundingResult:
        """Fetch the current funding rate for a market.

        This is a convenience method to get just the current rate.
        Fetches the most recent funding rate from history.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD", "ETH")

        Returns:
            FundingResult with current rate

        Example:
            rate = await provider.get_current_funding_rate("ETH-USD")
            print(f"Current ETH funding rate: {rate.rate:.6f}")
        """
        now = datetime.now(UTC)
        # Fetch last 2 hours to ensure we get at least one data point
        start_time = now - timedelta(hours=2)

        try:
            rates = await self.get_funding_rates(
                market=market,
                start_date=start_time,
                end_date=now,
            )

            if rates:
                # Return the most recent rate
                return rates[-1]

            return self._create_fallback_result(now)

        except (HyperliquidAPIError, HyperliquidRateLimitError) as e:
            logger.error("Error fetching current funding rate: %s", str(e))
            return self._create_fallback_result(now)


__all__ = [
    "HyperliquidFundingProvider",
    "HyperliquidClientConfig",
    "HyperliquidFundingEntry",
    "HyperliquidAPIError",
    "HyperliquidRateLimitError",
    "HYPERLIQUID_API_URL",
    "DATA_SOURCE",
    "DEFAULT_REQUESTS_PER_MINUTE",
    "MAX_HOURS_PER_REQUEST",
]
