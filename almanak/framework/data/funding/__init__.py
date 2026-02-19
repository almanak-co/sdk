"""Funding Rate Provider Module.

This module provides funding rate data for perpetual positions from multiple
DeFi venues including GMX V2 and Hyperliquid.

Key Features:
    - Fetch current funding rates from multiple venues
    - Cross-venue funding rate spread comparison
    - Historical funding rate data (last 24h minimum)
    - Caching to minimize API calls

Example (Gateway-backed - recommended):
    from almanak.framework.data.funding import GatewayFundingRateProvider, Venue
    from almanak.framework.gateway_client import GatewayClient

    with GatewayClient() as gateway:
        provider = GatewayFundingRateProvider(gateway_client=gateway)

        # Get funding rate for specific venue/market
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        print(f"GMX V2 ETH Funding: {rate.rate_8h}")

Example (Direct API - deprecated for production):
    from almanak.framework.data.funding import FundingRateProvider, Venue

    provider = FundingRateProvider()
    rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
"""

from .gateway_provider import GatewayFundingRateProvider
from .provider import (
    DEFAULT_CACHE_TTL_SECONDS,
    HOURS_PER_YEAR,
    SUPPORTED_MARKETS,
    # Constants
    SUPPORTED_VENUES,
    VENUE_CHAINS,
    # Data classes
    FundingRate,
    # Exceptions
    FundingRateError,
    # Main service
    FundingRateProvider,
    FundingRateSpread,
    FundingRateUnavailableError,
    HistoricalFundingData,
    HistoricalFundingRate,
    MarketNotSupportedError,
    # Enums
    Venue,
    VenueNotSupportedError,
)

__all__ = [
    # Gateway-backed provider (recommended)
    "GatewayFundingRateProvider",
    # Direct API provider (deprecated for production)
    "FundingRateProvider",
    # Data classes
    "FundingRate",
    "HistoricalFundingRate",
    "FundingRateSpread",
    "HistoricalFundingData",
    # Enums
    "Venue",
    # Exceptions
    "FundingRateError",
    "FundingRateUnavailableError",
    "VenueNotSupportedError",
    "MarketNotSupportedError",
    # Constants
    "SUPPORTED_VENUES",
    "VENUE_CHAINS",
    "SUPPORTED_MARKETS",
    "DEFAULT_CACHE_TTL_SECONDS",
    "HOURS_PER_YEAR",
]
