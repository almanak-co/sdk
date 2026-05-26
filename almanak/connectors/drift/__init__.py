"""Drift Protocol Connector.

Provides perpetual futures trading on Drift (Solana's #1 perps DEX).
Supports market orders for opening and closing perp positions.

Key classes:
- DriftAdapter: Compiles PerpOpen/PerpClose intents to ActionBundles
- DriftSDK: Low-level instruction building (PDA derivation, Borsh encoding)
- DriftDataClient: REST client for Drift Data API (market info, funding rates)
- DriftReceiptParser: Parses transaction receipts for fill data
- DriftConfig: Adapter configuration

Lazy attribute access (VIB-4835): the strategy-side public surface is
exposed via PEP 562 ``__getattr__`` so importing
``almanak.connectors.drift.gateway.provider`` at gateway boot does not
eagerly pull adapter / SDK / client modules. The pre-existing
``DRIFT_DATA_API_BASE_URL`` shim is preserved in the same ``__getattr__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    "ALLOWED_COLLATERAL_MINTS",
    "DRIFT_PROGRAM_ID",
    "DriftAPIError",
    "DriftAccountNotFoundError",
    "DriftAdapter",
    "DriftConfig",
    "DriftConfigError",
    "DriftDataClient",
    "DriftError",
    "DriftMarket",
    "DriftMarketError",
    "DriftPerpPosition",
    "DriftReceiptParser",
    "DriftSDK",
    "DriftSpotPosition",
    "DriftUserAccount",
    "DriftValidationError",
    "FundingRate",
    "OrderParams",
    "PERP_MARKET_SYMBOL_TO_INDEX",
    "PERP_MARKETS",
    "SPOT_MARKET_SYMBOL_TO_INDEX",
    "SPOT_MARKETS",
    "get_drift_data_api_base_url",
    "is_supported_collateral",
    "validate_drift_collateral",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DriftAdapter": (".adapter", "DriftAdapter"),
    "DriftDataClient": (".client", "DriftDataClient"),
    "DRIFT_PROGRAM_ID": (".constants", "DRIFT_PROGRAM_ID"),
    "PERP_MARKET_SYMBOL_TO_INDEX": (".constants", "PERP_MARKET_SYMBOL_TO_INDEX"),
    "PERP_MARKETS": (".constants", "PERP_MARKETS"),
    "SPOT_MARKET_SYMBOL_TO_INDEX": (".constants", "SPOT_MARKET_SYMBOL_TO_INDEX"),
    "SPOT_MARKETS": (".constants", "SPOT_MARKETS"),
    "get_drift_data_api_base_url": (".constants", "get_drift_data_api_base_url"),
    "DriftAccountNotFoundError": (".exceptions", "DriftAccountNotFoundError"),
    "DriftAPIError": (".exceptions", "DriftAPIError"),
    "DriftConfigError": (".exceptions", "DriftConfigError"),
    "DriftError": (".exceptions", "DriftError"),
    "DriftMarketError": (".exceptions", "DriftMarketError"),
    "DriftValidationError": (".exceptions", "DriftValidationError"),
    "ALLOWED_COLLATERAL_MINTS": (".market_rules", "ALLOWED_COLLATERAL_MINTS"),
    "is_supported_collateral": (".market_rules", "is_supported_collateral"),
    "validate_drift_collateral": (".market_rules", "validate_drift_collateral"),
    "DriftConfig": (".models", "DriftConfig"),
    "DriftMarket": (".models", "DriftMarket"),
    "DriftPerpPosition": (".models", "DriftPerpPosition"),
    "DriftSpotPosition": (".models", "DriftSpotPosition"),
    "DriftUserAccount": (".models", "DriftUserAccount"),
    "FundingRate": (".models", "FundingRate"),
    "OrderParams": (".models", "OrderParams"),
    "DriftReceiptParser": (".receipt_parser", "DriftReceiptParser"),
    "DriftSDK": (".sdk", "DriftSDK"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    _registered = True
    try:
        from almanak.connectors._strategy_base.registry import register_connector
        from almanak.framework.intents.vocabulary import IntentType

        register_connector(
            name="drift",
            intents=(
                IntentType.PERP_OPEN,
                IntentType.PERP_CLOSE,
            ),
            chains=("solana",),
        )
    except Exception:
        _registered = False
        raise


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access.

    Resolves ``DRIFT_DATA_API_BASE_URL`` to the runtime accessor (legacy
    shim from ``constants.py``) and everything else via ``_LAZY``.
    """
    if name == "DRIFT_DATA_API_BASE_URL":
        # Preserve the legacy package-level shim — internal code reads via
        # ``get_drift_data_api_base_url`` but external consumers still
        # ``from almanak.connectors.drift import DRIFT_DATA_API_BASE_URL``.
        from .constants import get_drift_data_api_base_url

        return get_drift_data_api_base_url()
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
