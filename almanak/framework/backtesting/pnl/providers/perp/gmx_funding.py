"""GMX V2 historical funding rate provider.

Thin client of the gateway's ``RateHistoryService`` (``GetFundingRateHistory``,
venue ``gmx_v2``). All HTTP egress lives on the gateway side in the GMX V2
connector's ``GatewayFundingHistoryCapability`` implementation — this module
holds no API URLs, no market-token tables, and no HTTP session (VIB-4851
Phase D; previously it called the GMX Stats API directly and extrapolated the
*current* rate backwards over the whole requested range).

GMX V2 has no native historical funding endpoint; the connector serves the
history lane through the documented Hyperliquid cross-venue fallback (both
venues quote the same reference markets). Real per-hour history replaces the
old current-rate extrapolation, so results carry HIGH confidence where a
measured point covers the hour and LOW-confidence fallback fills elsewhere.

Example:
    from almanak.framework.backtesting.pnl.providers.perp import GMXFundingProvider
    from datetime import datetime, UTC

    provider = GMXFundingProvider()

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

from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.core.enums import Chain
from almanak.framework.data.interfaces import DataSourceUnavailable

from ...types import DataConfidence, DataSourceInfo, FundingResult
from ..base import HistoricalFundingProvider
from ..rate_limiter import TokenBucketRateLimiter
from ._gateway_history import FundingHistoryPoint, fetch_funding_points, run_sync_gateway_call

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

# Funding rate interval (GMX funding is calculated continuously, sampled hourly)
FUNDING_INTERVAL_HOURS = 1

# The manifest key this provider resolves its venue/chains through.
_PROTOCOL_KEY = "gmx_v2"


# =============================================================================
# Exceptions
# =============================================================================


class GMXAPIError(Exception):
    """Raised when the funding-history fetch fails."""


class GMXRateLimitError(GMXAPIError):
    """Raised when the upstream rate limit is exceeded.

    Retained for API compatibility: the gateway owns rate limiting since the
    Phase D cutover, so this provider no longer raises it on its own.
    """


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class GMXClientConfig:
    """Configuration for the GMX funding provider.

    Attributes:
        requests_per_minute: Client-side RPC throttle (default: 30)
        timeout_seconds: Legacy config echo (default: 30)
        chain: Default chain for requests (default: ARBITRUM)
        fallback_rate: Fallback funding rate when data unavailable
    """

    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    chain: Chain = Chain.ARBITRUM
    fallback_rate: Decimal = Decimal("0.0001")  # 0.01% per hour


# =============================================================================
# GMXFundingProvider
# =============================================================================


class GMXFundingProvider(HistoricalFundingProvider):
    """Historical funding rate provider for GMX V2 perpetuals.

    Fetches measured funding-rate history through the gateway and fills the
    requested hourly grid with carry-forward semantics (the rate in effect at
    hour ``t`` is the latest measured point at or before ``t``). Hours before
    the first measured point fall back to ``config.fallback_rate`` at LOW
    confidence — the provider never raises from :meth:`get_funding_rates`,
    preserving the pre-cutover graceful-degradation contract.

    Attributes:
        config: Client configuration
        rate_limiter: Client-side RPC throttle

    Example:
        provider = GMXFundingProvider()

        async with provider:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )
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
        """Chains the GMX V2 connector declares funding data for."""
        # Decl chains are lowercase identifiers; Chain enum values are UPPERCASE.
        return [Chain(c.upper()) for c in FundingHistoryRegistry.declared_chains(_PROTOCOL_KEY)]

    async def close(self) -> None:
        """Release resources (no-op; retained for API compatibility)."""
        logger.debug("GMXFundingProvider session closed")

    async def __aenter__(self) -> "GMXFundingProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the session."""
        await self.close()

    def _validate_chain(self, chain: Chain) -> str:
        """Validate ``chain`` against the connector-declared funding chains.

        Args:
            chain: The blockchain to query

        Returns:
            The lowercase chain identifier for the RPC

        Raises:
            ValueError: If the connector declares no funding data for it
        """
        declared = FundingHistoryRegistry.declared_chains(_PROTOCOL_KEY)
        chain_lower = chain.value.lower()
        if chain_lower not in declared:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(declared)}")
        return chain_lower

    async def _fetch_points(
        self,
        market: str,
        chain: Chain,
        start_ts: int,
        end_ts: int,
    ) -> list[FundingHistoryPoint]:
        """Fetch measured funding points for ``[start_ts, end_ts]``.

        Raises:
            GMXAPIError: When the gateway round-trip fails.
        """
        venue = FundingHistoryRegistry.venue_for(_PROTOCOL_KEY)
        if venue is None:  # pragma: no cover - manifest declares the venue
            raise GMXAPIError("No funding-history venue declared for GMX V2")

        await self._rate_limiter.acquire()
        try:
            return await run_sync_gateway_call(
                fetch_funding_points,
                venue=venue,
                market=market.upper(),
                chain=self._validate_chain(chain),
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except DataSourceUnavailable as e:
            raise GMXAPIError(f"Gateway funding history unavailable: {e}") from e

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

    def _grid_results(
        self,
        points: list[FundingHistoryPoint],
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult]:
        """Fill the hourly grid with carry-forward rates from ``points``.

        The rate in effect at grid hour ``t`` is the latest measured point at
        or before ``t``; hours before the first point fall back at LOW
        confidence so the grid is always fully covered (one result per hour,
        ``start_date`` to ``end_date`` inclusive).
        """
        results: list[FundingResult] = []
        index = -1  # latest point applied so far
        current = start_date
        while current <= end_date:
            current_ts = int(current.timestamp())
            while index + 1 < len(points) and points[index + 1].timestamp <= current_ts:
                index += 1
            if index >= 0:
                results.append(self._create_result(rate=points[index].rate_hourly, timestamp=current))
            else:
                results.append(self._create_fallback_result(current))
            current += timedelta(hours=FUNDING_INTERVAL_HOURS)
        return results

    async def get_funding_rates(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult]:
        """Fetch historical funding rates for a GMX V2 market.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD").
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of FundingResult objects, one per hour in the date range.
            Measured history yields HIGH confidence results; hours without
            measured coverage (and any gateway failure) yield LOW confidence
            fallback results. Never raises.

        Example:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
            )
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
            points = await self._fetch_points(
                market=market,
                chain=self._config.chain,
                start_ts=int(start_date.timestamp()),
                end_ts=int(end_date.timestamp()),
            )
            results = self._grid_results(points, start_date, end_date)
            logger.info(
                "Generated %d funding rate data points for market=%s (%d measured)",
                len(results),
                market,
                len(points),
            )
            return results

        except GMXAPIError as e:
            logger.error("GMX funding history error: %s", str(e))
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

        Resolves the latest measured point in the trailing 24 hours.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD")
            chain: Optional chain override (default: uses config.chain)

        Returns:
            FundingResult with current rate (fallback at LOW confidence when
            no measured point is available)
        """
        chain = chain or self._config.chain
        now = datetime.now(UTC)

        try:
            points = await self._fetch_points(
                market=market,
                chain=chain,
                start_ts=int(now.timestamp()) - 86_400,
                end_ts=int(now.timestamp()),
            )
            if not points:
                return self._create_fallback_result(now)
            return self._create_result(
                rate=points[-1].rate_hourly,
                timestamp=now,
                confidence=DataConfidence.HIGH,
            )

        except (GMXAPIError, ValueError) as e:
            logger.error("Error fetching current funding rate: %s", str(e))
            return self._create_fallback_result(now)


__all__ = [
    # Constants (SCREAMING_SNAKE_CASE) first
    "DATA_SOURCE",
    "DEFAULT_REQUESTS_PER_MINUTE",
    "FUNDING_INTERVAL_HOURS",
    # Classes (CamelCase) second
    "GMXAPIError",
    "GMXClientConfig",
    "GMXFundingProvider",
    "GMXRateLimitError",
]
