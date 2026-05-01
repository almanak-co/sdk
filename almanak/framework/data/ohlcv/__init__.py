"""OHLCV Module - Candlestick data with caching.

This module provides strategy-side OHLCV data providers and modules for
efficient historical candlestick data access.

Providers:
    - GatewayOHLCVProvider: Gateway-backed provider (recommended for production)
    - GatewayGeckoTerminalOHLCVProvider: gRPC client for GeckoTerminal data
    - DedupingOHLCVProvider: Deduplication wrapper

Modules:
    - OHLCVModule: Combines providers with persistent SQLite caching
    - OHLCVRouter: Multi-source provider routing
    - RoutingOHLCVProvider: Routing-aware provider wrapper

Note:
    Raw HTTP providers (Binance, GeckoTerminal direct) live under
    ``almanak.gateway.data.ohlcv`` because they perform outbound network
    egress and are gateway-side only. Strategy-container code must not
    import them directly (VIB-3799).
"""

from almanak.framework.data.ohlcv.dedup_provider import DedupingOHLCVProvider
from almanak.framework.data.ohlcv.gateway_data_adapter import (
    GatewayOHLCVDataProvider,
    GeckoTerminalGatewayDataProvider,
)
from almanak.framework.data.ohlcv.gateway_provider import (
    TOKEN_TO_BINANCE_SYMBOL,
    GatewayGeckoTerminalOHLCVProvider,
    GatewayOHLCVProvider,
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
    "GatewayGeckoTerminalOHLCVProvider",
    "GatewayOHLCVDataProvider",
    "GeckoTerminalGatewayDataProvider",
    "TOKEN_TO_BINANCE_SYMBOL",
    "DedupingOHLCVProvider",
    "GapStrategy",
    "OHLCVModule",
    "OHLCVRouter",
    "RoutingOHLCVProvider",
    "classify_instrument",
]
