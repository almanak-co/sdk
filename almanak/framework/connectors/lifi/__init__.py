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
    from almanak.framework.connectors.lifi import LiFiClient, LiFiAdapter, LiFiConfig

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

from .adapter import LiFiAdapter
from .client import CHAIN_ID_TO_NAME, CHAIN_MAPPING, LIFI_DIAMOND_ADDRESS, LiFiClient, LiFiConfig
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
    # Client
    "LiFiClient",
    "LiFiConfig",
    "CHAIN_MAPPING",
    "CHAIN_ID_TO_NAME",
    "LIFI_DIAMOND_ADDRESS",
    # Adapter
    "LiFiAdapter",
    # Receipt Parser
    "LiFiReceiptParser",
    # Models
    "LiFiStep",
    "LiFiAction",
    "LiFiEstimate",
    "LiFiToken",
    "LiFiTransactionRequest",
    "LiFiStatusResponse",
    "LiFiGasCost",
    "LiFiFeeCost",
    "LiFiOrderStrategy",
    "LiFiStepType",
    "LiFiTransferStatus",
    "LiFiTransferSubstatus",
    # Exceptions
    "LiFiError",
    "LiFiAPIError",
    "LiFiConfigError",
    "LiFiValidationError",
    "LiFiRouteNotFoundError",
    "LiFiTransferFailedError",
]
