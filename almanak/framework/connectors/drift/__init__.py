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
    DRIFT_PROGRAM_ID,
    PERP_MARKET_SYMBOL_TO_INDEX,
    PERP_MARKETS,
    SPOT_MARKET_SYMBOL_TO_INDEX,
    SPOT_MARKETS,
    get_drift_data_api_base_url,
)
from .exceptions import (
    DriftAccountNotFoundError,
    DriftAPIError,
    DriftConfigError,
    DriftError,
    DriftMarketError,
    DriftValidationError,
)
from .market_rules import (
    ALLOWED_COLLATERAL_MINTS,
    is_supported_collateral,
    validate_drift_collateral,
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
    "DRIFT_DATA_API_BASE_URL",
    "DRIFT_PROGRAM_ID",
    "PERP_MARKETS",
    "PERP_MARKET_SYMBOL_TO_INDEX",
    "SPOT_MARKETS",
    "SPOT_MARKET_SYMBOL_TO_INDEX",
    "get_drift_data_api_base_url",
    # Market rules
    "ALLOWED_COLLATERAL_MINTS",
    "is_supported_collateral",
    "validate_drift_collateral",
    # Exceptions
    "DriftError",
    "DriftAPIError",
    "DriftConfigError",
    "DriftValidationError",
    "DriftAccountNotFoundError",
    "DriftMarketError",
]


def __getattr__(name: str) -> str:
    """Package-level shim for the legacy ``DRIFT_DATA_API_BASE_URL`` import.

    Internal code now reads via :func:`get_drift_data_api_base_url`, but
    external consumers still use ``from almanak.framework.connectors.drift
    import DRIFT_DATA_API_BASE_URL``. Mirrors the same shim in
    ``constants.py`` so package-level imports keep working.
    """
    if name == "DRIFT_DATA_API_BASE_URL":
        return get_drift_data_api_base_url()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
