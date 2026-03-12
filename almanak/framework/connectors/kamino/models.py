"""Kamino Finance Lending Data Models.

Dataclasses for Kamino API requests and responses.
Kamino is the primary lending protocol on Solana (~$2.8B TVL),
providing Aave-style lending with a REST API.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class KaminoMarket:
    """A Kamino lending market.

    Attributes:
        address: Market account public key (Base58)
        name: Human-readable market name (e.g., "Main Market")
        description: Market description
        is_primary: Whether this is the primary market
        lookup_table: Address lookup table for the market
    """

    address: str
    name: str = ""
    description: str = ""
    is_primary: bool = False
    lookup_table: str = ""

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "KaminoMarket":
        """Create from Kamino API market response."""
        return cls(
            address=data.get("lendingMarket", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            is_primary=data.get("isPrimary", False),
            lookup_table=data.get("lookupTable", ""),
        )


@dataclass
class KaminoReserve:
    """A reserve (token pool) within a Kamino lending market.

    Attributes:
        address: Reserve account public key (Base58)
        token_symbol: Token symbol (e.g., "USDC", "SOL")
        token_mint: SPL token mint address (Base58)
        max_ltv: Maximum loan-to-value ratio (e.g., "0.8" = 80%)
        borrow_apy: Current borrow APY as decimal string
        supply_apy: Current supply APY as decimal string
        total_supply: Total supplied amount (in token units)
        total_borrow: Total borrowed amount (in token units)
        total_supply_usd: Total supplied in USD
        total_borrow_usd: Total borrowed in USD
    """

    address: str
    token_symbol: str = ""
    token_mint: str = ""
    max_ltv: str = "0"
    borrow_apy: str = "0"
    supply_apy: str = "0"
    total_supply: str = "0"
    total_borrow: str = "0"
    total_supply_usd: str = "0"
    total_borrow_usd: str = "0"

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "KaminoReserve":
        """Create from Kamino API reserve metrics response."""
        return cls(
            address=data.get("reserve", ""),
            token_symbol=data.get("liquidityToken", ""),
            token_mint=data.get("liquidityTokenMint", ""),
            max_ltv=data.get("maxLtv", "0"),
            borrow_apy=data.get("borrowApy", "0"),
            supply_apy=data.get("supplyApy", "0"),
            total_supply=data.get("totalSupply", "0"),
            total_borrow=data.get("totalBorrow", "0"),
            total_supply_usd=data.get("totalSupplyUsd", "0"),
            total_borrow_usd=data.get("totalBorrowUsd", "0"),
        )


@dataclass
class KaminoTransactionResponse:
    """Response from a Kamino transaction endpoint.

    The Kamino API returns a base64-encoded unsigned VersionedTransaction
    ready for signing and submission.

    Attributes:
        transaction: Base64-encoded unsigned Solana VersionedTransaction
        action: The lending action (deposit, borrow, repay, withdraw)
        raw_response: Full API response for debugging
    """

    transaction: str  # base64-encoded unsigned transaction
    action: str = ""
    raw_response: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any], action: str = "") -> "KaminoTransactionResponse":
        """Create from Kamino API transaction response.

        Args:
            data: Kamino /ktx/klend/* API response dict
            action: The lending action (deposit, borrow, repay, withdraw)

        Returns:
            Parsed KaminoTransactionResponse
        """
        return cls(
            transaction=data.get("transaction", ""),
            action=action,
            raw_response=data,
        )
