"""Gateway integrations package.

This package provides third-party data source integrations for the gateway:
- BinanceIntegration: Binance market data (ticker, klines, order book)
- CoinGeckoIntegration: CoinGecko price and market data
- TheGraphIntegration: TheGraph subgraph queries
- ZerionIntegration: Wallet portfolio and DeFi position data
- OkxIntegration: OKX OnchainOS wallet portfolio and token balances

All integrations inherit from BaseIntegration and provide:
- Rate limiting (per-integration configurable)
- Response caching with TTL
- Health checks
- Structured error handling
"""

from almanak.integrations._base.gateway.base import (
    BaseIntegration,
    IntegrationError,
    IntegrationRateLimitError,
    IntegrationRegistry,
    RateLimiter,
)
from almanak.integrations._base.gateway.models import WalletPortfolioSnapshot, WalletPosition
from almanak.integrations.okx.gateway.client import OkxIntegration
from almanak.integrations.zerion.gateway.client import ZerionIntegration

__all__ = [
    "BaseIntegration",
    "IntegrationError",
    "IntegrationRateLimitError",
    "IntegrationRegistry",
    "OkxIntegration",
    "RateLimiter",
    "WalletPortfolioSnapshot",
    "WalletPosition",
    "ZerionIntegration",
]
