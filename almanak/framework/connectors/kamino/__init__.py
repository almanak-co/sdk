"""Kamino Finance Lending Protocol Connector.

Kamino is the primary lending protocol on Solana (~$2.8B TVL),
providing Aave-style lending/borrowing with a REST API.

This connector provides:
- KaminoClient: HTTP client for the Kamino Finance API
- KaminoAdapter: Adapter for converting lending intents to Solana transactions
- KaminoReceiptParser: Balance-delta parser for extracting lending results

Example:
    from almanak.framework.connectors.kamino import KaminoClient, KaminoConfig

    config = KaminoConfig(wallet_address="your-solana-pubkey")
    client = KaminoClient(config)

    # Get reserves for the main market
    reserves = client.get_reserves()

    # Build a deposit transaction
    tx = client.deposit(reserve=reserves[0].address, amount="100.0")
"""

from .adapter import KaminoAdapter
from .client import KAMINO_MAIN_MARKET, U64_MAX, KaminoClient, KaminoConfig
from .exceptions import (
    KaminoAPIError,
    KaminoConfigError,
    KaminoError,
    KaminoValidationError,
)
from .models import KaminoMarket, KaminoReserve, KaminoTransactionResponse
from .receipt_parser import KaminoReceiptParser

__all__ = [
    # Client
    "KaminoClient",
    "KaminoConfig",
    "KAMINO_MAIN_MARKET",
    "U64_MAX",
    # Adapter
    "KaminoAdapter",
    # Receipt Parser
    "KaminoReceiptParser",
    # Models
    "KaminoMarket",
    "KaminoReserve",
    "KaminoTransactionResponse",
    # Exceptions
    "KaminoError",
    "KaminoAPIError",
    "KaminoValidationError",
    "KaminoConfigError",
]
