"""Price providers for gateway.

Contains the actual price source implementations that make external API calls.
These are only available in the gateway, not in the framework.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .aggregator import PriceAggregator
from .multi_dex import (
    DEX_CHAINS,
    SUPPORTED_DEXS,
    BestDexResult,
    Dex,
    DexNotSupportedError,
    DexQuote,
    MultiDexPriceError,
    MultiDexPriceResult,
    MultiDexPriceService,
    QuoteUnavailableError,
)
from .multi_dex import (
    TokenNotSupportedError as DexTokenNotSupportedError,
)

if TYPE_CHECKING:
    from almanak.integrations.binance.gateway.price_source import BinancePriceSource as BinancePriceSource
    from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource as ChainlinkPriceSource
    from almanak.integrations.chainlink.gateway.live import OnChainPriceSource as OnChainPriceSource
    from almanak.integrations.coingecko.gateway.price_source import CoinGeckoPriceSource as CoinGeckoPriceSource
    from almanak.integrations.dexscreener.gateway.price_source import (
        CHAIN_TO_DEXSCREENER_PLATFORM as CHAIN_TO_DEXSCREENER_PLATFORM,
    )
    from almanak.integrations.dexscreener.gateway.price_source import (
        DexScreenerPriceSource as DexScreenerPriceSource,
    )
    from almanak.integrations.pyth.gateway.price_source import PythPriceSource as PythPriceSource


_PROVIDER_EXPORTS = {
    "BinancePriceSource": ("almanak.integrations.binance.gateway.price_source", "BinancePriceSource"),
    "CHAIN_TO_DEXSCREENER_PLATFORM": (
        "almanak.integrations.dexscreener.gateway.price_source",
        "CHAIN_TO_DEXSCREENER_PLATFORM",
    ),
    "CoinGeckoPriceSource": ("almanak.integrations.coingecko.gateway.price_source", "CoinGeckoPriceSource"),
    "ChainlinkPriceSource": ("almanak.integrations.chainlink.gateway.live", "ChainlinkPriceSource"),
    "DexScreenerPriceSource": (
        "almanak.integrations.dexscreener.gateway.price_source",
        "DexScreenerPriceSource",
    ),
    "OnChainPriceSource": ("almanak.integrations.chainlink.gateway.live", "OnChainPriceSource"),
    "PythPriceSource": ("almanak.integrations.pyth.gateway.price_source", "PythPriceSource"),
}


def __getattr__(name: str) -> Any:
    """Lazily expose provider classes without importing every egress client.

    In particular, the canonical Chainlink implementation imports the shared
    aggregator helper. Eagerly importing Chainlink from this package's
    ``__init__`` would therefore create a package-initialization cycle.
    """
    target = _PROVIDER_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


__all__ = [
    "BinancePriceSource",
    "CHAIN_TO_DEXSCREENER_PLATFORM",
    "CoinGeckoPriceSource",
    "ChainlinkPriceSource",
    "DexScreenerPriceSource",
    "OnChainPriceSource",
    "PriceAggregator",
    "PythPriceSource",
    # Multi-DEX exports
    "MultiDexPriceService",
    "DexQuote",
    "MultiDexPriceResult",
    "BestDexResult",
    "Dex",
    "MultiDexPriceError",
    "QuoteUnavailableError",
    "DexNotSupportedError",
    "DexTokenNotSupportedError",
    "SUPPORTED_DEXS",
    "DEX_CHAINS",
]
