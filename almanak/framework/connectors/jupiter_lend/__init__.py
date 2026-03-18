"""Jupiter Lend Protocol Connector.

Jupiter Lend is the #2 Solana money market (~$1.65B TVL),
featuring isolated vaults, rehypothecation, and aggressive LTV ratios.

This connector provides:
- JupiterLendClient: HTTP client for the Jupiter Lend API
- JupiterLendAdapter: Adapter for converting lending intents to Solana transactions
- JupiterLendReceiptParser: Balance-delta parser for extracting lending results

Example:
    from almanak.framework.connectors.jupiter_lend import JupiterLendClient, JupiterLendConfig

    config = JupiterLendConfig(wallet_address="your-solana-pubkey")
    client = JupiterLendClient(config)

    # Get available vaults
    vaults = client.get_vaults()

    # Build a deposit transaction
    tx = client.deposit(vault=vaults[0].address, amount="100.0")
"""

from .adapter import JupiterLendAdapter
from .client import U64_MAX, JupiterLendClient, JupiterLendConfig
from .exceptions import (
    JupiterLendAPIError,
    JupiterLendConfigError,
    JupiterLendError,
    JupiterLendValidationError,
)
from .models import JupiterLendTransactionResponse, JupiterLendVault
from .receipt_parser import JupiterLendReceiptParser

__all__ = [
    # Client
    "JupiterLendClient",
    "JupiterLendConfig",
    "U64_MAX",
    # Adapter
    "JupiterLendAdapter",
    # Receipt Parser
    "JupiterLendReceiptParser",
    # Models
    "JupiterLendVault",
    "JupiterLendTransactionResponse",
    # Exceptions
    "JupiterLendError",
    "JupiterLendAPIError",
    "JupiterLendValidationError",
    "JupiterLendConfigError",
]
