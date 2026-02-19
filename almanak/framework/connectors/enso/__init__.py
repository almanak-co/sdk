"""Enso Finance Protocol Connector.

Enso is a routing and composable transaction protocol that aggregates
liquidity across multiple DEXs and lending protocols.

This connector provides:
- EnsoClient: SDK for interacting with the Enso Finance API
- EnsoAdapter: Adapter for converting intents to Enso transactions
- EnsoReceiptParser: Parser for extracting results from transaction receipts

Supports both same-chain and cross-chain swaps via Enso's bridge aggregation
(Stargate, LayerZero).

Example:
    from almanak.framework.connectors.enso import EnsoClient, EnsoAdapter, EnsoConfig

    # Create client
    config = EnsoConfig(
        api_key="your-api-key",
        chain="base",
        wallet_address="0x...",
    )
    client = EnsoClient(config)

    # Same-chain swap
    route = client.get_route(
        token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC on Base
        token_out="0x4200000000000000000000000000000000000006",  # WETH on Base
        amount_in=1000000000,  # 1000 USDC
        slippage_bps=50,  # 0.5%
    )

    # Cross-chain swap: Base -> Arbitrum
    cross_chain_route = client.get_cross_chain_route(
        token_in="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC on Base
        token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH on Arbitrum
        amount_in=1000000000,
        destination_chain="arbitrum",
    )
"""

from .adapter import EnsoAdapter
from .client import CHAIN_MAPPING, EnsoClient, EnsoConfig
from .exceptions import (
    EnsoAPIError,
    EnsoConfigError,
    EnsoError,
    EnsoValidationError,
    PriceImpactExceedsThresholdError,
)
from .models import (
    Hop,
    Quote,
    RouteParams,
    RouteTransaction,
    RoutingStrategy,
    Transaction,
)
from .receipt_parser import EnsoReceiptParser

__all__ = [
    # Client
    "EnsoClient",
    "EnsoConfig",
    "CHAIN_MAPPING",
    # Adapter
    "EnsoAdapter",
    # Receipt Parser
    "EnsoReceiptParser",
    # Models
    "RouteParams",
    "RouteTransaction",
    "Transaction",
    "Quote",
    "Hop",
    "RoutingStrategy",
    # Exceptions
    "EnsoError",
    "EnsoAPIError",
    "EnsoValidationError",
    "EnsoConfigError",
    "PriceImpactExceedsThresholdError",
]
