"""Hyperliquid historical funding rate provider.

Thin client of the gateway's ``RateHistoryService`` (``GetFundingRateHistory``,
venue ``hyperliquid``). All HTTP egress lives on the gateway side in the
Hyperliquid connector's ``GatewayFundingHistoryCapability`` implementation —
this module holds no API URL and no HTTP session (VIB-4851 Phase D; previously
it posted ``fundingHistory`` requests to the Hyperliquid Info API directly).

Hyperliquid provides true historical funding rates at hourly intervals;
results carry HIGH confidence per measured entry, with LOW-confidence fallback
fills only when no data comes back at all (preserving the pre-cutover
graceful-degradation contract).

Example:
    from almanak.connectors.hyperliquid.backtest_funding import HyperliquidFundingProvider
    from datetime import datetime, UTC

    provider = HyperliquidFundingProvider()

    async with provider:
        rates = await provider.get_funding_rates(
            market="ETH-USD",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
        )
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.framework.backtesting.pnl.providers.base import BacktestProviderConfig, HistoricalFundingProvider
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
    fetch_funding_points,
    run_sync_gateway_call,
)
from almanak.framework.backtesting.pnl.providers.rate_limiter import TokenBucketRateLimiter
from almanak.framework.backtesting.pnl.types import DataConfidence, DataSourceInfo, FundingResult
from almanak.framework.data.interfaces import DataSourceUnavailable

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Data source identifier stamped on measured results.
DATA_SOURCE = "gateway"

# Default client-side request throttle (the gateway owns the real upstream
# rate-limit budget; this only smooths RPC bursts from tight backtest loops).
DEFAULT_REQUESTS_PER_MINUTE = 30

# Default HTTP timeout in seconds (legacy config echo; transport timeouts are
# gateway-owned since the Phase D cutover).
DEFAULT_TIMEOUT_SECONDS = 30

# Maximum hours of data per upstream request (Hyperliquid API limit; the
# shared gateway helper chunks wider windows client-side).
MAX_HOURS_PER_REQUEST = 500

# Funding rate interval for fallback grids (Hyperliquid funding is hourly).
FUNDING_INTERVAL_HOURS = 1

# The manifest key this provider resolves its venue through.
_PROTOCOL_KEY = "hyperliquid"


# =============================================================================
# Exceptions
# =============================================================================


class HyperliquidAPIError(Exception):
    """Raised when the funding-history fetch fails."""


class HyperliquidRateLimitError(HyperliquidAPIError):
    """Raised when the upstream rate limit is exceeded.

    Retained for API compatibility: the gateway owns rate limiting since the
    Phase D cutover, so this provider no longer raises it on its own.
    """


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class HyperliquidClientConfig:
    """Configuration for the Hyperliquid funding provider.

    Attributes:
        requests_per_minute: Client-side RPC throttle (default: 30)
        timeout_seconds: Legacy config echo (default: 30)
        fallback_rate: Fallback funding rate when data unavailable
    """

    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    fallback_rate: Decimal = Decimal("0.0001")  # 0.01% per hour


@dataclass
class HyperliquidFundingEntry:
    """Single funding rate entry.

    Attributes:
        coin: Asset symbol (e.g., "ETH", "BTC")
        funding_rate: Funding rate for the hour
        premium: Premium value (unmeasured through the history lane — the
            gateway point shape carries timestamp + hourly rate only)
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

    Fetches true historical funding rates through the gateway, one
    HIGH-confidence result per measured hourly entry. When no data comes back
    (or the gateway round-trip fails) the requested range is filled with
    LOW-confidence fallback results — :meth:`get_funding_rates` never raises.

    Attributes:
        config: Client configuration
        rate_limiter: Client-side RPC throttle

    Example:
        provider = HyperliquidFundingProvider()

        async with provider:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
            )
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
        # Sticky per-run memo: after two CONSECUTIVE gateway TRANSPORT
        # failures, stop re-dialing a dead gateway every tick (~2s per dial
        # adds minutes to an hourly-tick backtest). One failure alone is not
        # memoized -- a single DEADLINE on a slow response must not disable
        # the lane for the run. Data-level errors are NOT memoized.
        self._gateway_unavailable = False
        self._transport_failure_streak = 0

        # Create or use provided rate limiter
        if rate_limiter is not None:
            self._rate_limiter = rate_limiter
            self._owns_rate_limiter = False
        else:
            self._rate_limiter = TokenBucketRateLimiter(
                requests_per_minute=self._config.requests_per_minute,
            )
            self._owns_rate_limiter = True

        logger.debug(
            "Initialized HyperliquidFundingProvider: rate_limit=%d req/min",
            self._config.requests_per_minute,
        )

    @classmethod
    def for_backtest(cls, config: BacktestProviderConfig) -> "HyperliquidFundingProvider":
        """Construct from the adapter's protocol-neutral backtest config."""
        return cls(
            config=HyperliquidClientConfig(
                fallback_rate=(
                    config.funding_fallback_rate if config.funding_fallback_rate is not None else Decimal("0.0001")
                ),
            )
        )

    @property
    def config(self) -> HyperliquidClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        """Get the rate limiter."""
        return self._rate_limiter

    async def close(self) -> None:
        """Release resources (no-op; retained for API compatibility)."""
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

    async def _fetch_points(
        self,
        market: str,
        start_ts: int,
        end_ts: int,
    ) -> list[FundingHistoryPoint]:
        """Fetch measured funding points for ``[start_ts, end_ts]``.

        The gateway connector resolves canonical ``"<COIN>-USD"`` markets
        fail-closed, so the legacy input formats are normalized client-side
        first.

        Raises:
            HyperliquidAPIError: When the gateway round-trip fails.
        """
        venue = FundingHistoryRegistry.venue_for(_PROTOCOL_KEY)
        if venue is None:  # pragma: no cover - manifest declares the venue
            raise HyperliquidAPIError("No funding-history venue declared for Hyperliquid")

        coin = self._normalize_market_symbol(market)

        await self._rate_limiter.acquire()
        try:
            return await run_sync_gateway_call(
                fetch_funding_points,
                venue=venue,
                market=f"{coin}-USD",
                chain="",
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except DataSourceUnavailable as e:
            raise HyperliquidAPIError(f"Gateway funding history unavailable: {e}") from e

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

    def _normalize_datetime(self, value: datetime) -> datetime:
        """Normalize a request timestamp to UTC."""
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    async def get_funding_rates(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult]:
        """Fetch historical funding rates for a Hyperliquid market.

        This method fetches true historical funding rates. Data is returned at
        hourly intervals, one result per measured entry.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD", "ETH").
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of FundingResult objects, one per measured hourly entry, with
            HIGH confidence. Returns LOW confidence fallback results covering
            the range if no data is available. Never raises.

        Example:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
            )
        """
        logger.info(
            "Fetching Hyperliquid funding rates: market=%s, start=%s, end=%s",
            market,
            start_date,
            end_date,
        )

        start_date = self._normalize_datetime(start_date)
        end_date = self._normalize_datetime(end_date)

        if self._gateway_unavailable:
            return self._generate_fallback_results(start_date, end_date)

        try:
            results = await self._fetch_funding_results(market, start_date, end_date)
            self._transport_failure_streak = 0

        except HyperliquidAPIError as e:
            if getattr(e.__cause__, "transport", False):
                self._transport_failure_streak += 1
                if self._transport_failure_streak >= 2:
                    self._gateway_unavailable = True
                    logger.error(
                        "Hyperliquid funding gateway lane unavailable (%d consecutive transport "
                        "failures); using fallback rate for the remainder of this provider's "
                        "lifetime (logged once): %s",
                        self._transport_failure_streak,
                        e,
                    )
                else:
                    logger.warning("Hyperliquid funding gateway transport failure (will retry next fetch): %s", e)
            else:
                self._transport_failure_streak = 0
                logger.error("Hyperliquid funding history error: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

        except Exception as e:
            # A non-transport failure breaks the CONSECUTIVE-transport streak:
            # transport -> generic -> transport must not memoize.
            self._transport_failure_streak = 0
            logger.error("Unexpected error fetching funding rates: %s", str(e))
            return self._generate_fallback_results(start_date, end_date)

        if results is None:
            return self._generate_fallback_results(start_date, end_date)
        return results

    async def _fetch_funding_results(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult] | None:
        """Fetch measured funding rows, or None when the gateway has no data."""
        points = await self._fetch_points(
            market=market,
            start_ts=int(start_date.timestamp()),
            end_ts=int(end_date.timestamp()),
        )

        if not points:
            logger.warning(
                "No funding data returned for market=%s, returning fallback",
                market,
            )
            return None

        results = [
            self._create_result(
                rate=point.rate_hourly,
                timestamp=datetime.fromtimestamp(point.timestamp, tz=UTC),
                confidence=DataConfidence.HIGH,
            )
            for point in points
        ]
        logger.info(
            "Fetched %d funding rate data points for market=%s",
            len(results),
            market,
        )
        return results

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
    "DATA_SOURCE",
    "DEFAULT_REQUESTS_PER_MINUTE",
    "MAX_HOURS_PER_REQUEST",
]
