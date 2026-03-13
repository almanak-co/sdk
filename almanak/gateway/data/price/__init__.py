"""Price providers for gateway.

Contains the actual price source implementations that make external API calls.
These are only available in the gateway, not in the framework.
"""

from .aggregator import PriceAggregator
from .binance import BinancePriceSource
from .coingecko import CoinGeckoPriceSource
from .dexscreener import CHAIN_TO_DEXSCREENER_PLATFORM, DexScreenerPriceSource
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
    SUPPORTED_TOKENS as DEX_SUPPORTED_TOKENS,
)
from .multi_dex import (
    TokenNotSupportedError as DexTokenNotSupportedError,
)
from .onchain import OnChainPriceSource
from .pyth import PythPriceSource

__all__ = [
    "BinancePriceSource",
    "CHAIN_TO_DEXSCREENER_PLATFORM",
    "CoinGeckoPriceSource",
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
    "DEX_SUPPORTED_TOKENS",
]
