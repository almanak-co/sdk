"""Perpetual protocol historical data providers.

This module provides historical data providers for perpetual futures protocols
including funding rate data for accurate P&L calculations in backtesting.

Available Providers:
    - GMXFundingProvider: GMX V2 historical funding rate provider (Arbitrum, Avalanche)
    - HyperliquidFundingProvider: Hyperliquid historical funding rate provider

Example:
    from almanak.framework.backtesting.pnl.providers.perp import (
        GMXFundingProvider,
        HyperliquidFundingProvider,
    )

    # GMX V2 funding rates
    async with GMXFundingProvider() as provider:
        rates = await provider.get_funding_rates(
            market="ETH-USD",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )

    # Hyperliquid funding rates
    async with HyperliquidFundingProvider() as provider:
        rates = await provider.get_funding_rates(
            market="ETH-USD",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
        )
"""

from .gmx_funding import (
    DATA_SOURCE as GMX_DATA_SOURCE,
)
from .gmx_funding import (
    GMX_API_URLS,
    GMX_MARKET_TOKENS,
    GMXFundingProvider,
)
from .gmx_funding import (
    SUPPORTED_CHAINS as GMX_SUPPORTED_CHAINS,
)
from .hyperliquid_funding import (
    DATA_SOURCE as HYPERLIQUID_DATA_SOURCE,
)
from .hyperliquid_funding import (
    HYPERLIQUID_API_URL,
    HyperliquidFundingProvider,
)
from .hyperliquid_funding import (
    MAX_HOURS_PER_REQUEST as HYPERLIQUID_MAX_HOURS_PER_REQUEST,
)

__all__ = [
    # GMX V2 Provider
    "GMXFundingProvider",
    "GMX_API_URLS",
    "GMX_MARKET_TOKENS",
    "GMX_SUPPORTED_CHAINS",
    "GMX_DATA_SOURCE",
    # Hyperliquid Provider
    "HyperliquidFundingProvider",
    "HYPERLIQUID_API_URL",
    "HYPERLIQUID_DATA_SOURCE",
    "HYPERLIQUID_MAX_HOURS_PER_REQUEST",
]
