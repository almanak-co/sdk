"""OHLCV Module - Candlestick data with caching.

This module provides OHLCV data providers and modules for efficient
historical candlestick data access.

Providers:
    - GatewayOHLCVProvider: Gateway-backed provider (recommended for production)
    - BinanceOHLCVProvider: Direct Binance access (deprecated for production)
    - GeckoTerminalOHLCVProvider: DEX-native OHLCV from GeckoTerminal (DeFi primary)
    - CoinGeckoOHLCVProvider: Available in indicators.rsi module

Modules:
    - OHLCVModule: Combines providers with persistent SQLite caching
"""

from almanak.framework.data.ohlcv.binance_provider import (
    BINANCE_SYMBOL_MAP,
    BinanceOHLCVProvider,
)
from almanak.framework.data.ohlcv.gateway_data_adapter import (
    GatewayOHLCVDataProvider,
)
from almanak.framework.data.ohlcv.gateway_provider import (
    TOKEN_TO_BINANCE_SYMBOL,
    GatewayOHLCVProvider,
)
from almanak.framework.data.ohlcv.geckoterminal_provider import (
    GeckoTerminalOHLCVProvider,
)
from almanak.framework.data.ohlcv.module import GapStrategy, OHLCVModule
from almanak.framework.data.ohlcv.ohlcv_router import (
    OHLCVRouter,
    classify_instrument,
)
from almanak.framework.data.ohlcv.routing_provider import (
    RoutingOHLCVProvider,
)

__all__ = [
    "GatewayOHLCVProvider",
    "GatewayOHLCVDataProvider",
    "TOKEN_TO_BINANCE_SYMBOL",
    "BinanceOHLCVProvider",
    "BINANCE_SYMBOL_MAP",
    "GeckoTerminalOHLCVProvider",
    "GapStrategy",
    "OHLCVModule",
    "OHLCVRouter",
    "RoutingOHLCVProvider",
    "classify_instrument",
]
