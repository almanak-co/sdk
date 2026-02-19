"""Balancer Flash Loan Adapter.

This module provides the BalancerFlashLoanAdapter for executing flash loans
via Balancer's Vault contract.

Balancer Vault flash loan function:
flashLoan(
    IFlashLoanRecipient recipient,
    IERC20[] memory tokens,
    uint256[] memory amounts,
    bytes memory userData
)

Key differences from Aave:
- Zero fees (no premium to repay)
- All tokens and amounts in arrays (batch flash loans native)
- userData is arbitrary bytes passed to receiver
- Receiver must implement receiveFlashLoan() not executeOperation()

Contract addresses:
- Ethereum: 0xBA12222222228d8Ba445958a75a0704d566BF2C8
- Arbitrum: 0xBA12222222228d8Ba445958a75a0704d566BF2C8
- Optimism: 0xBA12222222228d8Ba445958a75a0704d566BF2C8
- Polygon: 0xBA12222222228d8Ba445958a75a0704d566BF2C8
- Base: 0xBA12222222228d8Ba445958a75a0704d566BF2C8
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Balancer Vault addresses (same on all chains)
BALANCER_VAULT_ADDRESSES: dict[str, str] = {
    "ethereum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "arbitrum": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "optimism": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "polygon": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    "base": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
}

# Function selector for flashLoan(address,address[],uint256[],bytes)
BALANCER_FLASH_LOAN_SELECTOR = "0x5c38449e"

# Gas estimates
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "flash_loan": 400000,  # Balancer flash loan base gas (slightly less than Aave multi)
    "flash_loan_simple": 250000,  # Single-token flash loan base gas
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class BalancerFlashLoanConfig:
    """Configuration for Balancer flash loan adapter.

    Attributes:
        chain: Target blockchain (ethereum, arbitrum, optimism, polygon, base)
        wallet_address: Address executing the flash loan
    """

    chain: str
    wallet_address: str


@dataclass
class BalancerFlashLoanParams:
    """Parameters for a Balancer flash loan.

    Attributes:
        recipient: Contract that will receive the flash loan and handle callbacks
        tokens: List of token addresses to borrow
        amounts: List of amounts to borrow (in wei)
        user_data: Arbitrary bytes to pass to the receiver's receiveFlashLoan()
    """

    recipient: str
    tokens: list[str]
    amounts: list[int]
    user_data: bytes = field(default_factory=bytes)


@dataclass
class TransactionResult:
    """Result of a transaction operation.

    Attributes:
        success: Whether the operation succeeded
        calldata: Generated calldata for the transaction
        to: Target contract address
        value: ETH value to send (always 0 for flash loans)
        gas_estimate: Estimated gas for the transaction
        error: Error message if failed
    """

    success: bool
    calldata: bytes = field(default_factory=bytes)
    to: str = ""
    value: int = 0
    gas_estimate: int = 0
    error: str | None = None


# =============================================================================
# Adapter
# =============================================================================


class BalancerFlashLoanAdapter:
    """Adapter for Balancer Vault flash loans.

    Balancer flash loans have zero fees, making them ideal for arbitrage strategies.
    The Vault contract holds all pool liquidity, enabling large flash loans.

    Example:
        config = BalancerFlashLoanConfig(
            chain="arbitrum",
            wallet_address="0x...",
        )
        adapter = BalancerFlashLoanAdapter(config)

        # Get flash loan calldata
        calldata = adapter.get_flash_loan_calldata(
            recipient="0x...",
            tokens=["0x...USDC", "0x...WETH"],
            amounts=[1000000000, 500000000000000000],
            user_data=b"",
        )
    """

    def __init__(self, chain: str, protocol: str = "balancer") -> None:
        """Initialize the adapter.

        Args:
            chain: Target blockchain
            protocol: Protocol name (always "balancer")
        """
        self.chain = chain
        self.protocol = protocol

        # Get vault address
        self.vault_address = BALANCER_VAULT_ADDRESSES.get(chain, "0x0000000000000000000000000000000000000000")

    def get_vault_address(self) -> str:
        """Get the Balancer Vault address."""
        return self.vault_address

    def get_flash_loan_calldata(
        self,
        recipient: str,
        tokens: list[str],
        amounts: list[int],
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a Balancer flash loan.

        Balancer flashLoan function:
        flashLoan(
            IFlashLoanRecipient recipient,
            IERC20[] memory tokens,
            uint256[] memory amounts,
            bytes memory userData
        )

        Args:
            recipient: Contract address that will receive and handle the flash loan
            tokens: List of token addresses to borrow
            amounts: List of amounts to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        n_tokens = len(tokens)
        if n_tokens != len(amounts):
            raise ValueError("tokens and amounts must have same length")

        # ABI encoding for flashLoan(address,address[],uint256[],bytes)
        # Layout:
        # - recipient (32 bytes, padded address)
        # - offset to tokens array (32 bytes)
        # - offset to amounts array (32 bytes)
        # - offset to userData (32 bytes)
        # - tokens array: length (32) + addresses (32 * n)
        # - amounts array: length (32) + amounts (32 * n)
        # - userData: length (32) + data (padded to 32)

        # Calculate offsets
        # Fixed header: recipient(32) + 3 offsets(32*3) = 128 bytes
        tokens_offset = 128
        amounts_offset = tokens_offset + 32 + n_tokens * 32
        user_data_offset = amounts_offset + 32 + n_tokens * 32

        # Build header
        encoded = self._pad_address(recipient)
        encoded += self._pad_uint256(tokens_offset)
        encoded += self._pad_uint256(amounts_offset)
        encoded += self._pad_uint256(user_data_offset)

        # Encode tokens array
        encoded += self._pad_uint256(n_tokens)
        for token in tokens:
            encoded += self._pad_address(token)

        # Encode amounts array
        encoded += self._pad_uint256(n_tokens)
        for amount in amounts:
            encoded += self._pad_uint256(amount)

        # Encode userData
        user_data_hex = user_data.hex() if user_data else ""
        user_data_len = len(user_data)
        encoded += self._pad_uint256(user_data_len)
        if user_data_len > 0:
            # Pad to 32-byte boundary
            padded_data = user_data_hex + "0" * ((64 - len(user_data_hex) % 64) % 64)
            encoded += padded_data

        return bytes.fromhex(BALANCER_FLASH_LOAN_SELECTOR[2:] + encoded)

    def get_flash_loan_simple_calldata(
        self,
        recipient: str,
        token: str,
        amount: int,
        user_data: bytes = b"",
    ) -> bytes:
        """Generate calldata for a single-token flash loan.

        This is a convenience method that wraps get_flash_loan_calldata
        for single-token flash loans.

        Args:
            recipient: Contract address that will receive the flash loan
            token: Token address to borrow
            amount: Amount to borrow (in token's smallest units)
            user_data: Extra data to pass to receiver's receiveFlashLoan

        Returns:
            Encoded calldata for the flashLoan transaction
        """
        return self.get_flash_loan_calldata(
            recipient=recipient,
            tokens=[token],
            amounts=[amount],
            user_data=user_data,
        )

    def estimate_flash_loan_gas(self) -> int:
        """Estimate gas for a multi-token flash loan (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["flash_loan"]

    def estimate_flash_loan_simple_gas(self) -> int:
        """Estimate gas for a single-token flash loan (base only, not including callbacks)."""
        return DEFAULT_GAS_ESTIMATES["flash_loan_simple"]

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad an address to 32 bytes (64 hex chars)."""
        clean_addr = addr.lower().replace("0x", "")
        return clean_addr.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad a uint256 to 32 bytes (64 hex chars)."""
        return hex(value)[2:].zfill(64)
