"""Funding rate data models.

Pure data classes, exceptions, and constants shared by the gateway-backed
funding rate provider. No network egress lives in this module — the only
provider implementation is :class:`GatewayFundingRateProvider`, which routes
all venue calls through the gateway sidecar.

Venue registry contract
-----------------------
``Venue`` is the authoritative public type used wherever a funding venue is
accepted or returned.  ``SUPPORTED_VENUES`` is derived from it and kept as a
plain ``list[str]`` for backward compatibility with callers that do set/membership
tests.  When a new venue connector ships it must touch three places:

1. Add a member to the ``Venue`` enum below.
2. Declare ``funding_history=FundingHistoryDecl(...)`` on the connector manifest
   (this is the live source of ``SUPPORTED_MARKETS`` and the perp-adapter routing).
3. The existing ``test_provider.py`` ``len(SUPPORTED_VENUES) == 2`` pin and the
   parity test in ``test_models_derivation.py`` will fail loudly — fix them both.

``SUPPORTED_MARKETS`` is derived lazily via PEP 562 ``__getattr__`` from
``FundingHistoryRegistry.all_markets()``.  It is NOT a module-level literal.

``VENUE_CHAINS``
----------------
DEPRECATED — kept for backward compatibility.  The literal below is intentionally
frozen and may be stale (gmx_v2 also supports avalanche; the "hyperliquid" key is
a venue name masquerading as a chain).  Use
``FundingHistoryRegistry.declared_chains()`` / ``FundingHistoryRegistry.all_declared_chains()``
for the live, registry-derived source.  Zero functional consumers of this table
exist in the framework; it is re-exported only so existing import sites do not
break.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class Venue(StrEnum):
    """Supported perpetual venues."""

    GMX_V2 = "gmx_v2"
    HYPERLIQUID = "hyperliquid"


SUPPORTED_VENUES: list[str] = [v.value for v in Venue]

# DEPRECATED: frozen legacy literal.  Use FundingHistoryRegistry.declared_chains()
# / all_declared_chains() for the live registry-derived source.  This literal is
# stale (missing avalanche for gmx_v2; "hyperliquid" key is a venue not a chain).
VENUE_CHAINS: dict[str, list[str]] = {
    "arbitrum": ["gmx_v2"],
    "hyperliquid": ["hyperliquid"],
}

DEFAULT_CACHE_TTL_SECONDS = 10.0
HOURS_PER_YEAR = 8760


def _supported_markets() -> dict[str, list[str]]:
    """Derive the supported-markets table from connector manifests (plan 023).

    Calls ``FundingHistoryRegistry.all_markets()`` lazily so connector discovery
    is deferred until first use — the import of this module stays side-effect free.
    """
    from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

    return FundingHistoryRegistry.all_markets()


class FundingRateError(Exception):
    """Base exception for funding rate errors."""


class FundingRateUnavailableError(FundingRateError):
    """Raised when a funding rate cannot be fetched."""

    def __init__(self, venue: str, market: str, reason: str) -> None:
        self.venue = venue
        self.market = market
        self.reason = reason
        super().__init__(f"Funding rate unavailable for {venue}/{market}: {reason}")


class VenueNotSupportedError(FundingRateError):
    """Raised when venue is not supported."""

    def __init__(self, venue: str) -> None:
        self.venue = venue
        super().__init__(f"Venue '{venue}' not supported. Supported venues: {SUPPORTED_VENUES}")


class MarketNotSupportedError(FundingRateError):
    """Raised when market is not supported by venue."""

    def __init__(self, market: str, venue: str) -> None:
        self.market = market
        self.venue = venue
        supported = _supported_markets().get(venue, [])
        super().__init__(f"Market '{market}' not supported by {venue}. Supported markets: {supported}")


@dataclass
class FundingRate:
    """Funding rate data for a specific venue/market.

    Funding rates indicate the cost of holding a perpetual position.
    - Positive rate: longs pay shorts (bullish market)
    - Negative rate: shorts pay longs (bearish market)
    """

    venue: str
    market: str
    rate_hourly: Decimal
    rate_8h: Decimal
    rate_annualized: Decimal
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    next_funding_time: datetime | None = None
    open_interest_long: Decimal | None = None
    open_interest_short: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    is_live_data: bool = True

    @property
    def rate_percent_8h(self) -> Decimal:
        return self.rate_8h * Decimal("100")

    @property
    def rate_percent_annualized(self) -> Decimal:
        return self.rate_annualized * Decimal("100")

    @property
    def is_positive(self) -> bool:
        return self.rate_hourly > Decimal("0")

    @property
    def is_negative(self) -> bool:
        return self.rate_hourly < Decimal("0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "market": self.market,
            "rate_hourly": str(self.rate_hourly),
            "rate_8h": str(self.rate_8h),
            "rate_annualized": str(self.rate_annualized),
            "rate_percent_8h": float(self.rate_percent_8h),
            "rate_percent_annualized": float(self.rate_percent_annualized),
            "timestamp": self.timestamp.isoformat(),
            "next_funding_time": self.next_funding_time.isoformat() if self.next_funding_time else None,
            "open_interest_long": float(self.open_interest_long) if self.open_interest_long is not None else None,
            "open_interest_short": float(self.open_interest_short) if self.open_interest_short is not None else None,
            "mark_price": float(self.mark_price) if self.mark_price is not None else None,
            "index_price": float(self.index_price) if self.index_price is not None else None,
            "is_live_data": self.is_live_data,
        }


@dataclass
class FundingRateSpread:
    """Funding rate spread between two venues.

    A positive ``spread_8h`` means ``venue_a`` has higher funding than
    ``venue_b``, creating an arbitrage opportunity (short ``venue_a``,
    long ``venue_b``).
    """

    market: str
    venue_a: str
    venue_b: str
    rate_a: FundingRate
    rate_b: FundingRate
    spread_8h: Decimal
    spread_annualized: Decimal
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def spread_percent_8h(self) -> Decimal:
        return self.spread_8h * Decimal("100")

    @property
    def spread_percent_annualized(self) -> Decimal:
        return self.spread_annualized * Decimal("100")

    @property
    def is_profitable(self) -> bool:
        # Minimum 0.01% 8h spread to consider profitable.
        return abs(self.spread_8h) > Decimal("0.0001")

    @property
    def recommended_direction(self) -> str | None:
        if not self.is_profitable:
            return None
        return "short_a_long_b" if self.spread_8h > Decimal("0") else "short_b_long_a"

    def to_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "venue_a": self.venue_a,
            "venue_b": self.venue_b,
            "rate_a": self.rate_a.to_dict(),
            "rate_b": self.rate_b.to_dict(),
            "spread_8h": str(self.spread_8h),
            "spread_annualized": str(self.spread_annualized),
            "spread_percent_8h": float(self.spread_percent_8h),
            "spread_percent_annualized": float(self.spread_percent_annualized),
            "is_profitable": self.is_profitable,
            "recommended_direction": self.recommended_direction,
            "timestamp": self.timestamp.isoformat(),
        }


def __getattr__(name: str) -> Any:  # noqa: ANN401 - PEP 562 lazy module attribute
    """Serve ``SUPPORTED_MARKETS`` lazily without import-time connector discovery."""
    if name == "SUPPORTED_MARKETS":
        return _supported_markets()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DEFAULT_CACHE_TTL_SECONDS",
    "FundingRate",
    "FundingRateError",
    "FundingRateSpread",
    "FundingRateUnavailableError",
    "HOURS_PER_YEAR",
    "MarketNotSupportedError",
    "SUPPORTED_MARKETS",  # noqa: F822 - served via PEP 562 __getattr__
    "SUPPORTED_VENUES",
    "VENUE_CHAINS",
    "Venue",
    "VenueNotSupportedError",
]
