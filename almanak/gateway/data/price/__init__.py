"""Price providers for gateway.

Contains the actual price source implementations that make external API calls.
These are only available in the gateway, not in the framework.
"""

from .aggregator import PriceAggregator
from .coingecko import CoinGeckoPriceSource
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

__all__ = [
    "CoinGeckoPriceSource",
    "OnChainPriceSource",
    "PriceAggregator",
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
