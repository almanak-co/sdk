"""Constants for Safe wallet operations.

This module contains ABIs, addresses, and enums for interacting with
Gnosis Safe contracts, Zodiac Roles modules, and MultiSend contracts.

Key Components:
    - SafeOperation: Enum for Safe transaction operation types
    - ABIs: Contract ABIs for Safe, Zodiac, and MultiSend interactions
    - MULTISEND_ADDRESSES: Chain-specific MultiSend contract addresses
    - ENSO_DELEGATE_ADDRESSES: Known Enso delegates requiring DELEGATECALL
"""

from enum import IntEnum
from typing import Final

# =============================================================================
# Enums
# =============================================================================


class SafeOperation(IntEnum):
    """Operation types for Safe transactions.

    CALL (0): Standard external call from Safe to target contract.
    DELEGATECALL (1): Delegatecall from Safe, target code runs in Safe's context.

    Note: DELEGATECALL should only be used for trusted contracts like MultiSend
    or Enso delegates, as it allows the target to modify Safe's state.
    """

    CALL = 0
    DELEGATE_CALL = 1


# =============================================================================
# ABIs
# =============================================================================

# Zodiac Roles Module - execTransactionWithRole function
ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "address", "name": "to", "type": "address"},
            {"internalType": "uint256", "name": "value", "type": "uint256"},
            {"internalType": "bytes", "name": "data", "type": "bytes"},
            {"internalType": "enum Enum.Operation", "name": "operation", "type": "uint8"},
            {"internalType": "bytes32", "name": "roleKey", "type": "bytes32"},
            {"internalType": "bool", "name": "shouldRevert", "type": "bool"},
        ],
        "name": "execTransactionWithRole",
        "outputs": [{"internalType": "bool", "name": "success", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Safe's execTransaction function
SAFE_EXEC_TRANSACTION_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"},
        ],
        "name": "execTransaction",
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "payable",
        "type": "function",
    }
]

# Safe's getTransactionHash function for computing tx hash to sign
SAFE_GET_TX_HASH_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "_nonce", "type": "uint256"},
        ],
        "name": "getTransactionHash",
        "outputs": [{"type": "bytes32"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Safe's nonce function
SAFE_NONCE_ABI: Final[list[dict]] = [
    {
        "inputs": [],
        "name": "nonce",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Safe's getOwners function (for ownership verification)
SAFE_GET_OWNERS_ABI: Final[list[dict]] = [
    {
        "inputs": [],
        "name": "getOwners",
        "outputs": [{"type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Safe's getThreshold function (for threshold verification)
SAFE_GET_THRESHOLD_ABI: Final[list[dict]] = [
    {
        "inputs": [],
        "name": "getThreshold",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]


# =============================================================================
# MultiSend Constants
# =============================================================================

# MultiSend function selector for multiSend(bytes transactions)
MULTISEND_SELECTOR: Final[str] = "0x8d80ff0a"

# MultiSend contract addresses (deployed via CREATE2, same address on all chains)
# Reference: https://github.com/safe-global/safe-deployments
MULTISEND_ADDRESSES: Final[dict[str, str]] = {
    "ethereum": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
    "arbitrum": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
    "optimism": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
    "polygon": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
    "base": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
    "avalanche": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
    "gnosis": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
    "bsc": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
}


# =============================================================================
# Enso Delegate Addresses
# =============================================================================

# Known Enso delegate addresses that require DELEGATECALL
# These are trusted contracts that execute swaps/actions in the Safe's context
ENSO_DELEGATE_ADDRESSES: Final[set[str]] = {
    "0x7663fd40081dccd47805c00e613b6beac3b87f08",  # Delegate 1 (multiple chains)
    "0xa2f4f9c6ec598ca8c633024f8851c79ca5f43e48",  # Delegate 2 (Ethereum mainnet)
}


# =============================================================================
# Default Configuration Values
# =============================================================================

# Default role key for Zodiac Roles module
DEFAULT_ROLE_KEY: Final[str] = "AlmanakAgentRole"

# Gas buffer multiplier for Safe transactions (accounts for Safe overhead)
DEFAULT_GAS_BUFFER_MULTIPLIER: Final[float] = 2.0

# Zero address (used for gasToken and refundReceiver in Safe transactions)
ZERO_ADDRESS: Final[str] = "0x0000000000000000000000000000000000000000"


# =============================================================================
# Helper Functions
# =============================================================================


def get_multisend_address(chain: str) -> str:
    """Get the MultiSend contract address for a chain.

    Args:
        chain: Chain name (e.g., "arbitrum", "ethereum")

    Returns:
        MultiSend contract address for the chain

    Raises:
        ValueError: If chain is not supported
    """
    chain_lower = chain.lower()
    if chain_lower not in MULTISEND_ADDRESSES:
        valid_chains = ", ".join(sorted(MULTISEND_ADDRESSES.keys()))
        raise ValueError(f"No MultiSend address for chain '{chain}'. Supported chains: {valid_chains}")
    return MULTISEND_ADDRESSES[chain_lower]


def is_enso_delegate(address: str) -> bool:
    """Check if an address is a known Enso delegate requiring DELEGATECALL.

    Args:
        address: Contract address to check

    Returns:
        True if the address is an Enso delegate, False otherwise
    """
    return address.lower() in ENSO_DELEGATE_ADDRESSES


def get_operation_type(target_address: str) -> SafeOperation:
    """Determine the operation type for a target address.

    Enso delegates require DELEGATECALL so they can execute swaps
    in the context of the Safe. All other contracts use CALL.

    Args:
        target_address: Target contract address

    Returns:
        SafeOperation.DELEGATE_CALL for Enso delegates, CALL otherwise
    """
    if is_enso_delegate(target_address):
        return SafeOperation.DELEGATE_CALL
    return SafeOperation.CALL


def role_key_to_bytes32(role_name: str) -> bytes:
    """Convert a role name string to bytes32 format.

    The role name is UTF-8 encoded and left-padded with null bytes
    to reach 32 bytes total length.

    Args:
        role_name: Role name string (max 32 characters)

    Returns:
        32-byte representation of the role name

    Raises:
        ValueError: If role name exceeds 32 characters
    """
    if len(role_name) > 32:
        raise ValueError(f"Role name too long ({len(role_name)} > 32 characters)")
    return role_name.encode("utf-8").ljust(32, b"\0")


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Enums
    "SafeOperation",
    # ABIs
    "ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI",
    "SAFE_EXEC_TRANSACTION_ABI",
    "SAFE_GET_TX_HASH_ABI",
    "SAFE_NONCE_ABI",
    # MultiSend
    "MULTISEND_SELECTOR",
    "MULTISEND_ADDRESSES",
    # Enso
    "ENSO_DELEGATE_ADDRESSES",
    # Defaults
    "DEFAULT_ROLE_KEY",
    "DEFAULT_GAS_BUFFER_MULTIPLIER",
    "ZERO_ADDRESS",
    # Helper functions
    "get_multisend_address",
    "is_enso_delegate",
    "get_operation_type",
    "role_key_to_bytes32",
]
