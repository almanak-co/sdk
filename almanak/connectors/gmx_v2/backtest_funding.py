"""GMX V2 historical funding rate provider.

Thin client of the gateway's ``RateHistoryService`` (``GetFundingRateHistory``,
venue ``gmx_v2``). All HTTP egress lives on the gateway side in the GMX V2
connector's ``GatewayFundingHistoryCapability`` implementation — this module
holds no API URLs, no market-token tables, and no HTTP session (VIB-4851
Phase D; previously it called the GMX Stats API directly and extrapolated the
*current* rate backwards over the whole requested range).

The gateway reads GMX's official Synthetics indexer by verified market-token
address.  There is no symbol table, cross-venue substitution, current-rate
extrapolation, or implicit fallback.  Every returned hour is measured GMX
history; missing coverage fails closed unless a caller explicitly opts into a
fallback rate for legacy simulations.

Example:
    from almanak.connectors.gmx_v2.backtest_funding import GMXFundingProvider
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
from almanak.core.chains import ChainRegistry
from almanak.framework.backtesting.pnl.data_provider import is_address_like
from almanak.framework.backtesting.pnl.providers.base import BacktestProviderConfig, HistoricalFundingProvider
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
    fetch_funding_points,
    run_sync_gateway_call,
)
from almanak.framework.backtesting.pnl.providers.rate_limiter import TokenBucketRateLimiter
from almanak.framework.backtesting.pnl.types import DataConfidence, DataSourceInfo, FundingResult
from almanak.framework.data.interfaces import DataSourceError

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
        chain: Default chain for requests (default: "arbitrum")
        fallback_rate: Explicit opt-in fallback; ``None`` fails closed
    """

    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    chain: str = "arbitrum"
    fallback_rate: Decimal | None = None


# =============================================================================
# GMXFundingProvider
# =============================================================================


class GMXFundingProvider(HistoricalFundingProvider):
    """Historical funding rate provider for GMX V2 perpetuals.

    Fetches measured funding-rate history through the gateway and fills the
    requested hourly grid. Hours before the first measured point fail closed
    by default. An explicit ``config.fallback_rate`` retains the legacy
    low-confidence fill for callers that deliberately request simulation.

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
        # Public chain scope (HistoricalFundingProvider contract): the perp
        # adapter keys injected providers by (protocol, chain) via this.
        self.chain = self._config.chain
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
            "Initialized GMXFundingProvider: chain=%s, rate_limit=%d req/min",
            self._config.chain,
            self._config.requests_per_minute,
        )

    @classmethod
    def for_backtest(cls, config: BacktestProviderConfig) -> "GMXFundingProvider":
        """Construct from the adapter's protocol-neutral backtest config."""
        chain = "arbitrum"
        if config.chain is not None:
            requested = config.chain.strip().lower()
            declared = FundingHistoryRegistry.declared_chains(_PROTOCOL_KEY)
            if requested in declared:
                descriptor = ChainRegistry.try_resolve(requested)
                if descriptor is not None:
                    chain = descriptor.name
                else:
                    logger.warning(
                        "GMX V2 backtest requested declared chain %r, but it is not a registered chain; falling back to %s",
                        config.chain,
                        chain,
                    )
            else:
                logger.warning(
                    "GMX V2 backtest requested unsupported chain %r; declared chains are %s; falling back to %s",
                    config.chain,
                    list(declared),
                    chain,
                )
        return cls(
            config=GMXClientConfig(
                chain=chain,
                fallback_rate=config.funding_fallback_rate,
            )
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
    def supported_chains(self) -> list[str]:
        """Chains the GMX V2 connector declares funding data for."""
        return [ChainRegistry.resolve(c).name for c in FundingHistoryRegistry.declared_chains(_PROTOCOL_KEY)]

    async def close(self) -> None:
        """Release resources (no-op; retained for API compatibility)."""
        logger.debug("GMXFundingProvider session closed")

    async def __aenter__(self) -> "GMXFundingProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the session."""
        await self.close()

    def _validate_chain(self, chain: str) -> str:
        """Validate ``chain`` against the connector-declared funding chains.

        Args:
            chain: The blockchain to query

        Returns:
            The lowercase chain identifier for the RPC

        Raises:
            ValueError: If the connector declares no funding data for it
        """
        declared = FundingHistoryRegistry.declared_chains(_PROTOCOL_KEY)
        if chain not in declared:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(declared)}")
        return chain

    async def _fetch_points(
        self,
        market: str,
        chain: str,
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
        raw_market = market.strip()
        market_address = raw_market if is_address_like(raw_market) else ""
        try:
            return await run_sync_gateway_call(
                fetch_funding_points,
                venue=venue,
                market=raw_market if market_address else raw_market.upper(),
                market_address=market_address,
                chain=self._validate_chain(chain),
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except DataSourceError as e:
            raise GMXAPIError(f"Gateway funding history unavailable: {e}") from e

    def _create_fallback_result(self, timestamp: datetime) -> FundingResult:
        """Create a fallback FundingResult with LOW confidence.

        Args:
            timestamp: Timestamp for the result

        Returns:
            FundingResult with fallback rate and LOW confidence
        """
        if self._config.fallback_rate is None:
            raise GMXAPIError("GMX funding history unavailable and no explicit fallback_rate was configured")
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
        *,
        long_rate_hourly: Decimal | None = None,
        short_rate_hourly: Decimal | None = None,
        source: str = DATA_SOURCE,
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
            long_rate_hourly=long_rate_hourly,
            short_rate_hourly=short_rate_hourly,
            source_info=DataSourceInfo(
                source=source,
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
        """Require one exact observation for every requested grid hour.

        The gateway's GMX-native contract is a complete hourly series. This
        second boundary check prevents a partial or duplicated response from
        being disguised by carry-forward interpolation. Explicit fallback is
        still honored for legacy callers that deliberately configured it.
        """
        by_timestamp: dict[int, FundingHistoryPoint] = {}
        for history_point in points:
            if history_point.timestamp in by_timestamp:
                raise GMXAPIError(f"Duplicate GMX funding observation at {history_point.timestamp}")
            by_timestamp[history_point.timestamp] = history_point

        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        grid_start_ts = ((start_ts + 3599) // 3600) * 3600
        grid_end_ts = (end_ts // 3600) * 3600
        if grid_start_ts > grid_end_ts:
            return []

        results: list[FundingResult] = []
        current = datetime.fromtimestamp(grid_start_ts, tz=UTC)
        grid_end = datetime.fromtimestamp(grid_end_ts, tz=UTC)
        while current <= grid_end:
            current_ts = int(current.timestamp())
            point = by_timestamp.get(current_ts)
            if point is not None:
                results.append(
                    self._create_result(
                        rate=point.rate_hourly,
                        timestamp=current,
                        long_rate_hourly=point.long_rate_hourly,
                        short_rate_hourly=point.short_rate_hourly,
                        source=point.source,
                    )
                )
            elif self._config.fallback_rate is not None:
                results.append(self._create_fallback_result(current))
            else:
                raise GMXAPIError(f"Missing GMX funding observation at {current_ts}")
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
            Measured history yields HIGH confidence results. Missing coverage
            and gateway failures raise unless an explicit fallback was
            configured.

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

        if self._gateway_unavailable:
            return self._generate_fallback_results(start_date, end_date)

        try:
            points = await self._fetch_points(
                market=market,
                chain=self._config.chain,
                start_ts=int(start_date.timestamp()),
                end_ts=int(end_date.timestamp()),
            )
            results = self._grid_results(points, start_date, end_date)
            self._transport_failure_streak = 0
            logger.info(
                "Generated %d funding rate data points for market=%s (%d measured)",
                len(results),
                market,
                len(points),
            )
            return results

        except GMXAPIError as e:
            if getattr(e.__cause__, "transport", False):
                self._transport_failure_streak += 1
                if self._transport_failure_streak >= 2:
                    self._gateway_unavailable = True
                    logger.error(
                        "GMX funding gateway lane unavailable (%d consecutive transport failures); "
                        "using fallback rate for the remainder of this provider's lifetime "
                        "(logged once): %s",
                        self._transport_failure_streak,
                        e,
                    )
                else:
                    logger.warning("GMX funding gateway transport failure (will retry next fetch): %s", e)
            else:
                self._transport_failure_streak = 0
                logger.error("GMX funding history error: %s", str(e))
            if self._config.fallback_rate is None:
                raise
            return self._generate_fallback_results(start_date, end_date)

        except Exception:
            # Programming errors are not market-data fallbacks. A non-
            # transport failure also breaks the consecutive-transport streak.
            self._transport_failure_streak = 0
            raise

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
        chain: str | None = None,
    ) -> FundingResult:
        """Fetch the current funding rate for a market.

        Resolves the latest measured point in the trailing 24 hours.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD")
            chain: Optional chain override (default: uses config.chain)

        Returns:
            FundingResult with current rate. Missing data raises unless an
            explicit fallback was configured.
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
                long_rate_hourly=points[-1].long_rate_hourly,
                short_rate_hourly=points[-1].short_rate_hourly,
                source=points[-1].source,
            )

        except (GMXAPIError, ValueError) as e:
            logger.error("Error fetching current funding rate: %s", str(e))
            if self._config.fallback_rate is None:
                raise
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
