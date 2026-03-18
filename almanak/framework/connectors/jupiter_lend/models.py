"""Jupiter Lend Protocol Data Models.

Dataclasses for Jupiter Lend API requests and responses.
Jupiter Lend provides isolated lending vaults on Solana with
rehypothecation and aggressive LTV ratios.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class JupiterLendVault:
    """A Jupiter Lend isolated vault.

    Attributes:
        address: Vault account public key (Base58)
        name: Human-readable vault name
        token_symbol: Underlying token symbol (e.g., "USDC", "SOL")
        token_mint: SPL token mint address (Base58)
        max_ltv: Maximum loan-to-value ratio (e.g., "0.85" = 85%)
        borrow_apy: Current borrow APY as decimal string
        supply_apy: Current supply APY as decimal string
        total_supply: Total supplied amount (in token units)
        total_borrow: Total borrowed amount (in token units)
        total_supply_usd: Total supplied in USD
        total_borrow_usd: Total borrowed in USD
        utilization: Current utilization ratio as decimal string
    """

    address: str
    name: str = ""
    token_symbol: str = ""
    token_mint: str = ""
    max_ltv: str = "0"
    borrow_apy: str = "0"
    supply_apy: str = "0"
    total_supply: str = "0"
    total_borrow: str = "0"
    total_supply_usd: str = "0"
    total_borrow_usd: str = "0"
    utilization: str = "0"

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "JupiterLendVault":
        """Create from Jupiter Lend API vault response."""
        return cls(
            address=data.get("vaultAddress", data.get("address", "")),
            name=data.get("name", ""),
            token_symbol=data.get("tokenSymbol", data.get("symbol", "")),
            token_mint=data.get("tokenMint", data.get("mint", "")),
            max_ltv=data.get("maxLtv", "0"),
            borrow_apy=data.get("borrowApy", "0"),
            supply_apy=data.get("supplyApy", data.get("depositApy", "0")),
            total_supply=data.get("totalSupply", data.get("totalDeposits", "0")),
            total_borrow=data.get("totalBorrow", data.get("totalBorrows", "0")),
            total_supply_usd=data.get("totalSupplyUsd", data.get("totalDepositsUsd", "0")),
            total_borrow_usd=data.get("totalBorrowUsd", data.get("totalBorrowsUsd", "0")),
            utilization=data.get("utilization", "0"),
        )


@dataclass
class JupiterLendTransactionResponse:
    """Response from a Jupiter Lend transaction endpoint.

    The Jupiter Lend API returns a base64-encoded unsigned VersionedTransaction
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
    def from_api_response(cls, data: dict[str, Any], action: str = "") -> "JupiterLendTransactionResponse":
        """Create from Jupiter Lend API transaction response.

        Args:
            data: Jupiter Lend API response dict
            action: The lending action (deposit, borrow, repay, withdraw)

        Returns:
            Parsed JupiterLendTransactionResponse
        """
        transaction = data.get("transaction", "")
        if not transaction:
            from .exceptions import JupiterLendAPIError

            raise JupiterLendAPIError(
                message=f"API response missing 'transaction' field for {action or 'unknown'} action",
                status_code=0,
                endpoint=f"/v1/{action}" if action else None,
                error_data=data,
            )
        return cls(
            transaction=transaction,
            action=action,
            raw_response=data,
        )
