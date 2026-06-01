"""LiFi Cross-Chain Aggregator Connector.

LiFi is a cross-chain liquidity meta-aggregator that routes through bridges
(Across, Stargate, Hop, Wormhole, etc.) and DEXs (1inch, 0x, Paraswap, etc.)
to find optimal routes for cross-chain and same-chain swaps.

This connector provides:
- LiFiClient: SDK for interacting with the LiFi API
- LiFiAdapter: Adapter for converting intents to LiFi transactions
- LiFiReceiptParser: Parser for extracting results from transaction receipts

Supports both cross-chain bridges and same-chain swaps. Uses standard ERC-20
approvals (no Permit2 needed).

Example:
    from almanak.connectors.lifi import LiFiClient, LiFiAdapter, LiFiConfig

    config = LiFiConfig(chain_id=42161, wallet_address="0x...")
    client = LiFiClient(config)

    # Cross-chain swap: Arbitrum USDC -> Base USDC
    quote = client.get_quote(
        from_chain_id=42161,
        to_chain_id=8453,
        from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        to_token="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        from_amount="1000000000",
        from_address="0x...",
    )

    # Same-chain swap: Arbitrum USDC -> WETH
    same_chain_quote = client.get_quote(
        from_chain_id=42161,
        to_chain_id=42161,
        from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        from_amount="1000000000",
        from_address="0x...",
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import LiFiAdapter
    from .client import (
        CHAIN_ID_TO_NAME,
        CHAIN_MAPPING,
        LIFI_DIAMOND_ADDRESS,
        LiFiClient,
        LiFiConfig,
    )
    from .exceptions import (
        LiFiAPIError,
        LiFiConfigError,
        LiFiError,
        LiFiRouteNotFoundError,
        LiFiTransferFailedError,
        LiFiValidationError,
    )
    from .models import (
        LiFiAction,
        LiFiEstimate,
        LiFiFeeCost,
        LiFiGasCost,
        LiFiOrderStrategy,
        LiFiStatusResponse,
        LiFiStep,
        LiFiStepType,
        LiFiToken,
        LiFiTransactionRequest,
        LiFiTransferStatus,
        LiFiTransferSubstatus,
    )
    from .receipt_parser import LiFiReceiptParser

__all__ = [
    "CHAIN_ID_TO_NAME",
    "CHAIN_MAPPING",
    "LIFI_DIAMOND_ADDRESS",
    "LiFiAPIError",
    "LiFiAction",
    "LiFiAdapter",
    "LiFiClient",
    "LiFiConfig",
    "LiFiConfigError",
    "LiFiError",
    "LiFiEstimate",
    "LiFiFeeCost",
    "LiFiGasCost",
    "LiFiOrderStrategy",
    "LiFiReceiptParser",
    "LiFiRouteNotFoundError",
    "LiFiStatusResponse",
    "LiFiStep",
    "LiFiStepType",
    "LiFiToken",
    "LiFiTransactionRequest",
    "LiFiTransferFailedError",
    "LiFiTransferStatus",
    "LiFiTransferSubstatus",
    "LiFiValidationError",
]

_LAZY: dict[str, tuple[str, str]] = {
    "CHAIN_ID_TO_NAME": (".client", "CHAIN_ID_TO_NAME"),
    "CHAIN_MAPPING": (".client", "CHAIN_MAPPING"),
    "LIFI_DIAMOND_ADDRESS": (".client", "LIFI_DIAMOND_ADDRESS"),
    "LiFiAPIError": (".exceptions", "LiFiAPIError"),
    "LiFiAction": (".models", "LiFiAction"),
    "LiFiAdapter": (".adapter", "LiFiAdapter"),
    "LiFiClient": (".client", "LiFiClient"),
    "LiFiConfig": (".client", "LiFiConfig"),
    "LiFiConfigError": (".exceptions", "LiFiConfigError"),
    "LiFiError": (".exceptions", "LiFiError"),
    "LiFiEstimate": (".models", "LiFiEstimate"),
    "LiFiFeeCost": (".models", "LiFiFeeCost"),
    "LiFiGasCost": (".models", "LiFiGasCost"),
    "LiFiOrderStrategy": (".models", "LiFiOrderStrategy"),
    "LiFiReceiptParser": (".receipt_parser", "LiFiReceiptParser"),
    "LiFiRouteNotFoundError": (".exceptions", "LiFiRouteNotFoundError"),
    "LiFiStatusResponse": (".models", "LiFiStatusResponse"),
    "LiFiStep": (".models", "LiFiStep"),
    "LiFiStepType": (".models", "LiFiStepType"),
    "LiFiToken": (".models", "LiFiToken"),
    "LiFiTransactionRequest": (".models", "LiFiTransactionRequest"),
    "LiFiTransferFailedError": (".exceptions", "LiFiTransferFailedError"),
    "LiFiTransferStatus": (".models", "LiFiTransferStatus"),
    "LiFiTransferSubstatus": (".models", "LiFiTransferSubstatus"),
    "LiFiValidationError": (".exceptions", "LiFiValidationError"),
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
        from almanak.connectors._strategy_base.registry import (
            MatrixEntry,
            register_connector,
        )

        # Import CHAIN_MAPPING locally so the matrix coverage tracks the
        # connector's own data without exposing the literal chain list
        # in the manifest. ``CHAIN_MAPPING`` holds 10 LiFi-supported
        # chains (bsc encoded as ``"bsc"``, no further normalisation
        # needed).
        from almanak.connectors.lifi.client import CHAIN_MAPPING as _LIFI_CHAINS
        from almanak.framework.intents.vocabulary import IntentType

        register_connector(
            name="lifi",
            intents=(IntentType.SWAP, IntentType.BRIDGE),
            chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb"),
            # Matrix output — LiFi is an aggregator (cross-chain quote
            # router with bridge fallback). Surfaces under the
            # ``aggregator`` category, NOT ``swap``/``bridge``, so Edge /
            # agent classifiers don't pick it as a primary venue for
            # either. Chains come from the connector's own
            # ``CHAIN_MAPPING`` (10), not this manifest's 7. VIB-4856.
            matrix_entries=(
                MatrixEntry(
                    matrix_name="lifi",
                    category="aggregator",
                    chains=frozenset(_LIFI_CHAINS.keys()),
                ),
            ),
        )
    except Exception:
        _registered = False
        raise


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
