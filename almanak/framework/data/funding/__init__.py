"""Funding rate provider module.

Provides funding rate data for perpetual positions across DeFi venues
(GMX V2, Hyperliquid). All venue access is mediated by the gateway
sidecar — there is no direct HTTP egress from the strategy container.

Example:
    from almanak.framework.data.funding import GatewayFundingRateProvider, Venue
    from almanak.framework.gateway_client import GatewayClient

    with GatewayClient() as gateway:
        provider = GatewayFundingRateProvider(gateway_client=gateway)
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
"""

from .gateway_provider import GatewayFundingRateProvider
from .models import (
    DEFAULT_CACHE_TTL_SECONDS,
    HOURS_PER_YEAR,
    SUPPORTED_MARKETS,
    SUPPORTED_VENUES,
    VENUE_CHAINS,
    FundingRate,
    FundingRateError,
    FundingRateSpread,
    FundingRateUnavailableError,
    MarketNotSupportedError,
    Venue,
    VenueNotSupportedError,
)

__all__ = [
    "DEFAULT_CACHE_TTL_SECONDS",
    "FundingRate",
    "FundingRateError",
    "FundingRateSpread",
    "FundingRateUnavailableError",
    "GatewayFundingRateProvider",
    "HOURS_PER_YEAR",
    "MarketNotSupportedError",
    "SUPPORTED_MARKETS",
    "SUPPORTED_VENUES",
    "VENUE_CHAINS",
    "Venue",
    "VenueNotSupportedError",
]
