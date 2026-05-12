"""Jupiter DEX Aggregator Protocol Connector.

Jupiter is the primary DEX aggregator on Solana, routing across
Raydium, Orca, Meteora, and other Solana AMMs.

This connector provides:
- JupiterClient: HTTP client for the Jupiter API v6
- JupiterAdapter: Adapter for converting SwapIntents to Solana transactions
- JupiterReceiptParser: Balance-delta parser for extracting swap results

Example:
    from almanak.framework.connectors.jupiter import JupiterClient, JupiterAdapter, JupiterConfig

    config = JupiterConfig(wallet_address="your-solana-pubkey")
    client = JupiterClient(config)

    # Get a swap quote
    quote = client.get_quote(
        input_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        output_mint="So11111111111111111111111111111111111111112",     # WSOL
        amount=1000000000,
        slippage_bps=50,
    )
"""

from .adapter import JupiterAdapter
from .client import JupiterClient, JupiterConfig
from .exceptions import (
    JupiterAPIError,
    JupiterConfigError,
    JupiterError,
    JupiterPriceImpactError,
    JupiterValidationError,
)
from .models import JupiterQuote, JupiterRoutePlan, JupiterSwapTransaction
from .receipt_parser import JupiterReceiptParser

__all__ = [
    # Client
    "JupiterClient",
    "JupiterConfig",
    # Adapter
    "JupiterAdapter",
    # Receipt Parser
    "JupiterReceiptParser",
    # Models
    "JupiterQuote",
    "JupiterSwapTransaction",
    "JupiterRoutePlan",
    # Exceptions
    "JupiterError",
    "JupiterAPIError",
    "JupiterValidationError",
    "JupiterConfigError",
    "JupiterPriceImpactError",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="jupiter",
    intents=(IntentType.SWAP,),
    chains=("solana",),
)
