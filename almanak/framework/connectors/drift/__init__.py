"""Drift Protocol Connector.

Provides perpetual futures trading on Drift (Solana's #1 perps DEX).
Supports market orders for opening and closing perp positions.

Key classes:
- DriftAdapter: Compiles PerpOpen/PerpClose intents to ActionBundles
- DriftSDK: Low-level instruction building (PDA derivation, Borsh encoding)
- DriftDataClient: REST client for Drift Data API (market info, funding rates)
- DriftReceiptParser: Parses transaction receipts for fill data
- DriftConfig: Adapter configuration
"""

from .adapter import DriftAdapter
from .client import DriftDataClient
from .constants import (
    DRIFT_DATA_API_BASE_URL,
    DRIFT_PROGRAM_ID,
    PERP_MARKET_SYMBOL_TO_INDEX,
    PERP_MARKETS,
)
from .exceptions import (
    DriftAccountNotFoundError,
    DriftAPIError,
    DriftConfigError,
    DriftError,
    DriftMarketError,
    DriftValidationError,
)
from .models import (
    DriftConfig,
    DriftMarket,
    DriftPerpPosition,
    DriftSpotPosition,
    DriftUserAccount,
    FundingRate,
    OrderParams,
)
from .receipt_parser import DriftReceiptParser
from .sdk import DriftSDK

__all__ = [
    # Adapter
    "DriftAdapter",
    "DriftConfig",
    # SDK
    "DriftSDK",
    # Client
    "DriftDataClient",
    # Receipt Parser
    "DriftReceiptParser",
    # Models
    "DriftMarket",
    "DriftPerpPosition",
    "DriftSpotPosition",
    "DriftUserAccount",
    "FundingRate",
    "OrderParams",
    # Constants
    "DRIFT_PROGRAM_ID",
    "DRIFT_DATA_API_BASE_URL",
    "PERP_MARKETS",
    "PERP_MARKET_SYMBOL_TO_INDEX",
    # Exceptions
    "DriftError",
    "DriftAPIError",
    "DriftConfigError",
    "DriftValidationError",
    "DriftAccountNotFoundError",
    "DriftMarketError",
]
