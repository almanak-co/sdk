"""Funding rate data provider for perpetual futures protocols.

This module is a thin client of the gateway's ``RateHistoryService``
(``GetFundingRateHistory``). All HTTP egress for funding data happens on the
gateway side via each connector's ``GatewayFundingHistoryCapability``
implementation — this provider holds no HTTP client, no API URLs, and no
venue-specific market tables (VIB-4851 Phase D; previously this module opened
its own ``aiohttp`` sessions against GMX / Hyperliquid endpoints).

Protocol identifiers, aliases, and chain support derive from connector
manifests through
:class:`~almanak.connectors._strategy_base.funding_history_registry.FundingHistoryRegistry`
— adding a funding venue is one connector folder, no edit here.

Key Features:
    - Historical funding rates by protocol, market, and timestamp
    - Caching with 1-hour TTL
    - Falls back to a default rate when data is unavailable

Example:
    from almanak.framework.backtesting.pnl.providers.funding_rates import (
        FundingRateProvider,
        FundingRateData,
    )
    from datetime import datetime, timezone

    provider = FundingRateProvider()

    # Get historical funding rate for GMX ETH-USD
    rate = await provider.get_historical_funding_rate(
        protocol="gmx",
        market="ETH-USD",
        timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    )
    print(f"Funding rate: {rate.rate} ({rate.annualized_rate_pct}% APR)")
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.core.chains import DEFAULT_CHAIN
from almanak.framework.data.interfaces import DataSourceUnavailable

from .perp._gateway_history import fetch_funding_points

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Default cache TTL: 1 hour for historical data
DEFAULT_CACHE_TTL_SECONDS = 3600

# Rate limit settings (legacy ctor-signature compat; HTTP rate limiting is
# gateway-owned since the Phase D cutover, so these only echo into to_dict()).
DEFAULT_REQUESTS_PER_MINUTE = 30
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30

# Default hourly funding rate used when the gateway has no data for a query.
# One scalar, not a per-protocol table: every supported venue used the same
# fallback value, and the protocol set now lives on the connector manifests.
DEFAULT_FUNDING_RATE = Decimal("0.0001")  # 0.01% per hour (~8.76% APR)

# Lookback window for resolving "the rate at timestamp T" from the history
# series: take the latest point in [T - lookback, T]. One day comfortably
# covers hourly (Hyperliquid) and continuous (GMX) funding cadences.
_POINT_LOOKBACK_SECONDS = 86_400


def supported_protocols() -> list[str]:
    """Accepted protocol identifiers (manifest-derived, includes aliases).

    Lazy by design: deriving at module import would trigger connector
    discovery on package import (the VIB-4928 hazard).
    """
    return list(FundingHistoryRegistry.supported_protocols())


# =============================================================================
# Exceptions
# =============================================================================


class FundingRateError(Exception):
    """Base exception for funding rate provider errors."""


class FundingRateNotFoundError(FundingRateError):
    """Raised when funding rate data is not found for a market."""

    def __init__(self, protocol: str, market: str, timestamp: datetime) -> None:
        self.protocol = protocol
        self.market = market
        self.timestamp = timestamp
        super().__init__(f"Funding rate not found for {protocol} {market} at {timestamp.isoformat()}")


class FundingRateRateLimitError(FundingRateError):
    """Raised when API rate limit is exceeded.

    Retained for API compatibility: since the gateway cutover the gateway owns
    rate limiting, so this provider no longer raises it on its own.
    """

    def __init__(self, retry_after_seconds: float | None = None) -> None:
        self.retry_after_seconds = retry_after_seconds
        msg = "Funding rate API rate limit exceeded"
        if retry_after_seconds:
            msg += f", retry after {retry_after_seconds}s"
        super().__init__(msg)


class UnsupportedProtocolError(FundingRateError):
    """Raised when protocol is not supported."""

    def __init__(self, protocol: str) -> None:
        self.protocol = protocol
        super().__init__(f"Unsupported protocol: {protocol}. Supported: {supported_protocols()}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FundingRateData:
    """Funding rate data for a market at a specific time.

    Attributes:
        protocol: The perpetual protocol (gmx, hyperliquid)
        market: The market identifier (e.g., "ETH-USD", "BTC-USD")
        timestamp: The timestamp this rate applies to
        rate: The hourly funding rate (positive = longs pay, negative = shorts pay)
        annualized_rate_pct: Annualized funding rate as percentage
        next_funding_time: When the next funding payment occurs (if available)
        open_interest_long: Long open interest in USD (if available)
        open_interest_short: Short open interest in USD (if available)
        source: Data source (gateway, fallback)
    """

    protocol: str
    market: str
    timestamp: datetime
    rate: Decimal  # Hourly funding rate
    annualized_rate_pct: Decimal = Decimal("0")
    next_funding_time: datetime | None = None
    open_interest_long: Decimal | None = None
    open_interest_short: Decimal | None = None
    source: str = "gateway"

    def __post_init__(self) -> None:
        """Calculate annualized rate if not provided."""
        if self.annualized_rate_pct == Decimal("0") and self.rate != Decimal("0"):
            # Annualize: hourly_rate * 24 * 365 * 100 for percentage
            self.annualized_rate_pct = self.rate * Decimal("8760") * Decimal("100")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "protocol": self.protocol,
            "market": self.market,
            "timestamp": self.timestamp.isoformat(),
            "rate": str(self.rate),
            "annualized_rate_pct": str(self.annualized_rate_pct),
            "next_funding_time": self.next_funding_time.isoformat() if self.next_funding_time else None,
            "open_interest_long": str(self.open_interest_long) if self.open_interest_long else None,
            "open_interest_short": str(self.open_interest_short) if self.open_interest_short else None,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FundingRateData":
        """Deserialize from dictionary."""
        return cls(
            protocol=data["protocol"],
            market=data["market"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            rate=Decimal(data["rate"]),
            annualized_rate_pct=Decimal(data.get("annualized_rate_pct", "0")),
            next_funding_time=datetime.fromisoformat(data["next_funding_time"])
            if data.get("next_funding_time")
            else None,
            open_interest_long=Decimal(data["open_interest_long"]) if data.get("open_interest_long") else None,
            open_interest_short=Decimal(data["open_interest_short"]) if data.get("open_interest_short") else None,
            source=data.get("source", "gateway"),
        )


@dataclass
class CachedFundingRate:
    """Cached funding rate data with expiration."""

    data: FundingRateData
    fetched_at: float
    ttl_seconds: float

    @property
    def is_expired(self) -> bool:
        """Check if the cached data has expired."""
        return time.time() - self.fetched_at > self.ttl_seconds


@dataclass
class RateLimitState:
    """Tracks rate limit state for exponential backoff.

    Retained as exported API for callers that imported it; the provider itself
    no longer rate-limits client-side (the gateway owns the budget).
    """

    last_limit_time: float | None = None
    backoff_seconds: float = 1.0
    consecutive_limits: int = 0
    requests_this_minute: int = 0
    minute_start: float = field(default_factory=time.time)

    def record_rate_limit(self) -> None:
        """Record a rate limit hit and increase backoff."""
        self.last_limit_time = time.time()
        self.consecutive_limits += 1
        # Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 32s
        self.backoff_seconds = min(32.0, 2 ** (self.consecutive_limits - 1))

    def record_success(self) -> None:
        """Record successful request, reset backoff."""
        self.consecutive_limits = 0
        self.backoff_seconds = 1.0

    def get_wait_time(self) -> float:
        """Get time to wait before next request."""
        if self.last_limit_time is None:
            return 0.0
        elapsed = time.time() - self.last_limit_time
        remaining = self.backoff_seconds - elapsed
        return max(0.0, remaining)

    def record_request(self) -> None:
        """Record a request for rate limiting."""
        current_time = time.time()
        if current_time - self.minute_start >= 60:
            # Reset counter for new minute
            self.minute_start = current_time
            self.requests_this_minute = 0
        self.requests_this_minute += 1


# =============================================================================
# Funding Rate Provider
# =============================================================================


class FundingRateProvider:
    """Provider for fetching historical funding rates from perp protocols.

    Thin gateway client: each query is one ``GetFundingRateHistory`` RPC
    resolved to the venue the protocol's connector manifest declares. Results
    are cached with a 1-hour TTL; when the gateway has no data for a query the
    provider falls back to :data:`DEFAULT_FUNDING_RATE` (source ``fallback``),
    preserving the pre-cutover graceful-degradation contract.

    Attributes:
        chain: The chain forwarded to on-chain venues (arbitrum, avalanche)
        cache_ttl_seconds: Cache TTL in seconds (default: 1 hour)
        request_timeout: Legacy config echo (transport timeouts are
            gateway-owned)
        requests_per_minute: Legacy config echo (rate limiting is
            gateway-owned)

    Example:
        provider = FundingRateProvider()

        # Get historical funding rate
        rate = await provider.get_historical_funding_rate(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
        )

        # Get current funding rate
        rate = await provider.get_current_funding_rate(
            protocol="hyperliquid",
            market="BTC-USD",
        )
    """

    def __init__(
        self,
        chain: str = DEFAULT_CHAIN,
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    ) -> None:
        """Initialize the funding rate provider.

        Args:
            chain: Chain forwarded to on-chain venues (must be declared by at
                least one funding-history connector manifest)
            cache_ttl_seconds: Cache TTL in seconds (default: 3600 = 1 hour)
            request_timeout: Legacy config echo (gateway owns transport)
            requests_per_minute: Legacy config echo (gateway owns rate limits)

        Raises:
            ValueError: If no funding-history connector declares the chain
        """
        chain_lower = chain.lower()
        declared_chains = FundingHistoryRegistry.all_declared_chains()
        if chain_lower not in declared_chains:
            raise ValueError(f"Unsupported chain for GMX: {chain}. Supported: {sorted(declared_chains)}")

        self._chain = chain_lower
        self._cache_ttl_seconds = cache_ttl_seconds
        self._request_timeout = request_timeout
        self._requests_per_minute = requests_per_minute

        # Cache: (protocol, market, timestamp_hour) -> CachedFundingRate
        self._cache: dict[tuple[str, str, datetime], CachedFundingRate] = {}

    @property
    def chain(self) -> str:
        """Get the chain this provider forwards to on-chain venues."""
        return self._chain

    @property
    def provider_name(self) -> str:
        """Get the provider name."""
        return f"funding_rates_{self._chain}"

    async def close(self) -> None:
        """Release resources (no-op; retained for API compatibility)."""
        return None

    def _normalize_timestamp(self, timestamp: datetime) -> datetime:
        """Normalize timestamp to hourly boundary for caching."""
        # Round down to the hour
        return timestamp.replace(minute=0, second=0, microsecond=0)

    def _get_cache_key(self, protocol: str, market: str, timestamp: datetime) -> tuple[str, str, datetime]:
        """Get cache key for a funding rate query."""
        return (protocol.lower(), market.upper(), self._normalize_timestamp(timestamp))

    def _get_from_cache(self, protocol: str, market: str, timestamp: datetime) -> FundingRateData | None:
        """Try to get funding rate from cache."""
        key = self._get_cache_key(protocol, market, timestamp)
        cached = self._cache.get(key)

        if cached is None:
            return None

        if cached.is_expired:
            # Remove expired entry
            del self._cache[key]
            return None

        logger.debug(f"Cache hit for {protocol} {market} at {timestamp.isoformat()}")
        return cached.data

    def _add_to_cache(self, data: FundingRateData) -> None:
        """Add funding rate to cache."""
        key = self._get_cache_key(data.protocol, data.market, data.timestamp)
        self._cache[key] = CachedFundingRate(
            data=data,
            fetched_at=time.time(),
            ttl_seconds=self._cache_ttl_seconds,
        )

    def _fetch_point_via_gateway(
        self,
        protocol_lower: str,
        market: str,
        timestamp: datetime,
    ) -> FundingRateData:
        """Resolve "the rate at ``timestamp``" through the gateway history RPC.

        Takes the latest point in ``[timestamp - lookback, timestamp]`` so a
        backtest never reads a rate from its future (no look-ahead bias).

        Raises:
            FundingRateNotFoundError: When the gateway is unavailable or the
                window holds no measured point — callers fall back to the
                default rate, preserving the pre-cutover contract.
        """
        venue = FundingHistoryRegistry.venue_for(protocol_lower)
        if venue is None:  # pragma: no cover - guarded by caller validation
            raise UnsupportedProtocolError(protocol_lower)

        # On-chain venues get the configured chain; chain-agnostic venues
        # (empty declared chains) get "" per the RPC contract.
        chain = self._chain if FundingHistoryRegistry.declared_chains(protocol_lower) else ""

        # Naive timestamps are UTC by contract (same normalization as the
        # venue providers' get_funding_rates) — bare .timestamp() would read
        # them in the host timezone and shift the query window.
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        end_ts = int(timestamp.timestamp())
        try:
            points = fetch_funding_points(
                venue=venue,
                market=market.upper(),
                chain=chain,
                start_ts=end_ts - _POINT_LOOKBACK_SECONDS,
                end_ts=end_ts,
            )
        except DataSourceUnavailable as exc:
            logger.warning(
                "Gateway funding history unavailable for %s %s: %s",
                protocol_lower,
                market,
                exc,
            )
            raise FundingRateNotFoundError(protocol_lower, market, timestamp) from exc

        if not points:
            raise FundingRateNotFoundError(protocol_lower, market, timestamp)

        latest = points[-1]
        return FundingRateData(
            protocol=protocol_lower,
            market=market.upper(),
            timestamp=self._normalize_timestamp(timestamp),
            rate=latest.rate_hourly,
            source="gateway",
        )

    async def get_historical_funding_rate(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> FundingRateData:
        """Get historical funding rate for a market at a specific timestamp.

        Args:
            protocol: The perpetual protocol (gmx, gmx_v2, hyperliquid)
            market: The market identifier (e.g., "ETH-USD")
            timestamp: The timestamp to query funding rate for

        Returns:
            FundingRateData with the funding rate information

        Raises:
            UnsupportedProtocolError: If protocol is not supported
        """
        protocol_lower = protocol.lower()

        # Validate protocol against the manifest-derived identifier set
        if not FundingHistoryRegistry.has(protocol_lower):
            raise UnsupportedProtocolError(protocol)

        # Check cache first
        cached = self._get_from_cache(protocol_lower, market, timestamp)
        if cached is not None:
            return cached

        try:
            # The RPC stub is synchronous; keep the event loop responsive.
            data = await asyncio.to_thread(
                self._fetch_point_via_gateway,
                protocol_lower,
                market,
                timestamp,
            )
            self._add_to_cache(data)
            return data
        except FundingRateNotFoundError:
            # Fall back to default rate
            logger.warning(f"Funding rate not found for {protocol} {market}, using default")
            return self._get_default_funding_rate(protocol_lower, market, timestamp)

    async def get_current_funding_rate(
        self,
        protocol: str,
        market: str,
    ) -> FundingRateData:
        """Get current funding rate for a market.

        Convenience method that queries the current timestamp. For
        live-quality current rates with next-funding metadata, prefer
        ``almanak.framework.data.funding.GatewayFundingRateProvider`` (the
        ``FundingRateService.GetFundingRate`` client).

        Args:
            protocol: The perpetual protocol
            market: The market identifier

        Returns:
            FundingRateData with the current funding rate
        """
        return await self.get_historical_funding_rate(
            protocol=protocol,
            market=market,
            timestamp=datetime.now(UTC),
        )

    def _get_default_funding_rate(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> FundingRateData:
        """Get default funding rate when gateway data is unavailable.

        Args:
            protocol: The protocol
            market: The market
            timestamp: The timestamp

        Returns:
            FundingRateData with default rate
        """
        return FundingRateData(
            protocol=protocol,
            market=market.upper(),
            timestamp=self._normalize_timestamp(timestamp),
            rate=DEFAULT_FUNDING_RATE,
            source="fallback",
        )

    def get_default_rate(self, protocol: str) -> Decimal:
        """Get the default funding rate for a protocol.

        Args:
            protocol: Protocol name (unused since the per-protocol fallback
                table collapsed to one scalar; kept for signature stability)

        Returns:
            Default hourly funding rate
        """
        del protocol  # one fallback policy for every venue
        return DEFAULT_FUNDING_RATE

    def clear_cache(self) -> None:
        """Clear all cached funding rates."""
        self._cache.clear()

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        total = len(self._cache)
        expired = sum(1 for c in self._cache.values() if c.is_expired)
        return {
            "total_entries": total,
            "expired_entries": expired,
            "valid_entries": total - expired,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize provider config to dictionary."""
        return {
            "provider_name": self.provider_name,
            "chain": self._chain,
            "cache_ttl_seconds": self._cache_ttl_seconds,
            "request_timeout": self._request_timeout,
            "requests_per_minute": self._requests_per_minute,
            "supported_protocols": supported_protocols(),
        }


__all__ = [
    # Main Provider
    "FundingRateProvider",
    # Data Classes
    "FundingRateData",
    "CachedFundingRate",
    "RateLimitState",
    # Exceptions
    "FundingRateError",
    "FundingRateNotFoundError",
    "FundingRateRateLimitError",
    "UnsupportedProtocolError",
    # Constants / accessors
    "DEFAULT_FUNDING_RATE",
    "supported_protocols",
]
