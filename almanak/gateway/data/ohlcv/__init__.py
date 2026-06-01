"""OHLCV providers that perform raw HTTP egress.

These providers belong to the gateway egress layer because they make
direct outbound HTTP calls to external data APIs (Binance, GeckoTerminal).
The strategy container has no outbound network access except the gateway
gRPC channel, so these classes must NOT be imported from
``almanak/framework/`` or ``strategies/`` code paths.

Strategy-side OHLCV access goes through ``GatewayOHLCVProvider`` (gRPC)
or ``MarketSnapshot``.
"""

from almanak.gateway.data.ohlcv.binance_provider import (
    BINANCE_SYMBOL_MAP,
    BinanceOHLCVProvider,
)
from almanak.gateway.data.ohlcv.coingecko_provider import (
    CoinGeckoOHLCVProvider,
)
from almanak.gateway.data.ohlcv.geckoterminal_provider import (
    GeckoTerminalOHLCVProvider,
)

__all__ = [
    "BINANCE_SYMBOL_MAP",
    "BinanceOHLCVProvider",
    "CoinGeckoOHLCVProvider",
    "GeckoTerminalOHLCVProvider",
]
