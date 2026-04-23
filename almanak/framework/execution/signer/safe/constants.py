"""Constants for Safe wallet operations.

This module contains ABIs, addresses, and enums for interacting with
Gnosis Safe contracts, Zodiac Roles modules, and MultiSend contracts.

Key Components:
    - SafeOperation: Enum for Safe transaction operation types
    - ABIs: Contract ABIs for Safe, Zodiac, and MultiSend interactions
        - Safe core: execTransaction, nonce, owners/threshold queries
        - Safe deployment: setup, enableModule, SafeProxyFactory
        - Zodiac module deployment: ModuleProxyFactory.deployModule
        - Zodiac Roles v2: setUp, assignRoles, setDefaultRole,
          allowTarget, scopeTarget, allowFunction, revokeTarget,
          execTransactionWithRole
    - Canonical addresses: Safe v1.4.1 factory + singleton,
      ModuleProxyFactory, Roles Modifier master copy — all CREATE2-
      deployed at identical addresses on every supported EVM chain
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

# Safe's setup initializer — called once on a fresh proxy to install owners/threshold.
# Source: safe-global/safe-smart-account v1.4.1 Safe.sol
SAFE_SETUP_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "address[]", "name": "_owners", "type": "address[]"},
            {"internalType": "uint256", "name": "_threshold", "type": "uint256"},
            {"internalType": "address", "name": "to", "type": "address"},
            {"internalType": "bytes", "name": "data", "type": "bytes"},
            {"internalType": "address", "name": "fallbackHandler", "type": "address"},
            {"internalType": "address", "name": "paymentToken", "type": "address"},
            {"internalType": "uint256", "name": "payment", "type": "uint256"},
            {"internalType": "address payable", "name": "paymentReceiver", "type": "address"},
        ],
        "name": "setup",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Safe's enableModule — authorized (must be called by the Safe itself via execTransaction).
# Source: safe-global/safe-smart-account v1.4.1 ModuleManager.sol
SAFE_ENABLE_MODULE_ABI: Final[list[dict]] = [
    {
        "inputs": [{"internalType": "address", "name": "module", "type": "address"}],
        "name": "enableModule",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# SafeProxyFactory.createProxyWithNonce — deploys a new Safe proxy for a singleton + initializer.
# Source: safe-global/safe-smart-account v1.4.1 SafeProxyFactory.sol
SAFE_PROXY_FACTORY_CREATE_PROXY_WITH_NONCE_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "address", "name": "_singleton", "type": "address"},
            {"internalType": "bytes", "name": "initializer", "type": "bytes"},
            {"internalType": "uint256", "name": "saltNonce", "type": "uint256"},
        ],
        "name": "createProxyWithNonce",
        "outputs": [{"internalType": "contract SafeProxy", "name": "proxy", "type": "address"}],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Gnosis Guild ModuleProxyFactory.deployModule — clones a Zodiac module master copy as an
# EIP-1167 minimal proxy. Public; anyone can call.
# Source: gnosisguild/zodiac master/contracts/factory/ModuleProxyFactory.sol
MODULE_PROXY_FACTORY_DEPLOY_MODULE_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "address", "name": "masterCopy", "type": "address"},
            {"internalType": "bytes", "name": "initializer", "type": "bytes"},
            {"internalType": "uint256", "name": "saltNonce", "type": "uint256"},
        ],
        "name": "deployModule",
        "outputs": [{"internalType": "address", "name": "proxy", "type": "address"}],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Roles v2 setUp — initializer on a cloned proxy. `initParams` wraps abi.encode(owner, avatar, target).
# Source: gnosisguild/zodiac-modifier-roles main/packages/evm/contracts/Roles.sol
ROLES_SET_UP_ABI: Final[list[dict]] = [
    {
        "inputs": [{"internalType": "bytes", "name": "initParams", "type": "bytes"}],
        "name": "setUp",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Roles v2 assignRoles — grant/revoke role membership. onlyOwner (must route via Safe.execTransaction).
# The first parameter is named `module` in Solidity but semantically is the member address.
ROLES_ASSIGN_ROLES_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "address", "name": "module", "type": "address"},
            {"internalType": "bytes32[]", "name": "roleKeys", "type": "bytes32[]"},
            {"internalType": "bool[]", "name": "memberOf", "type": "bool[]"},
        ],
        "name": "assignRoles",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Roles v2 setDefaultRole — picks which role applies when a member calls execTransactionFromModule
# (the non-WithRole variant). onlyOwner.
ROLES_SET_DEFAULT_ROLE_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "address", "name": "module", "type": "address"},
            {"internalType": "bytes32", "name": "roleKey", "type": "bytes32"},
        ],
        "name": "setDefaultRole",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Roles v2 allowTarget — wildcard a whole target under a role. onlyOwner.
# `options` is the ExecutionOptions enum: 0=None, 1=Send, 2=DelegateCall, 3=Both.
ROLES_ALLOW_TARGET_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "bytes32", "name": "roleKey", "type": "bytes32"},
            {"internalType": "address", "name": "targetAddress", "type": "address"},
            {"internalType": "enum ExecutionOptions", "name": "options", "type": "uint8"},
        ],
        "name": "allowTarget",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Roles v2 scopeTarget — put a target in "Function" clearance so selectors can be allowed individually.
# Pairs with allowFunction or scopeFunction. onlyOwner.
ROLES_SCOPE_TARGET_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "bytes32", "name": "roleKey", "type": "bytes32"},
            {"internalType": "address", "name": "targetAddress", "type": "address"},
        ],
        "name": "scopeTarget",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Roles v2 allowFunction — wildcard a selector under a scoped target (no argument constraints). onlyOwner.
ROLES_ALLOW_FUNCTION_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "bytes32", "name": "roleKey", "type": "bytes32"},
            {"internalType": "address", "name": "targetAddress", "type": "address"},
            {"internalType": "bytes4", "name": "selector", "type": "bytes4"},
            {"internalType": "enum ExecutionOptions", "name": "options", "type": "uint8"},
        ],
        "name": "allowFunction",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Roles v2 revokeTarget — remove all clearance for a target under a role. onlyOwner.
ROLES_REVOKE_TARGET_ABI: Final[list[dict]] = [
    {
        "inputs": [
            {"internalType": "bytes32", "name": "roleKey", "type": "bytes32"},
            {"internalType": "address", "name": "targetAddress", "type": "address"},
        ],
        "name": "revokeTarget",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


# =============================================================================
# Canonical Deployment Addresses (CREATE2 — same address on every EVM chain)
# =============================================================================

# Gnosis Safe v1.4.1 singletons + factory. Canonical (CREATE2) addresses — identical on all
# EVM chains where they're deployed. Source: safe-global/safe-deployments v1.4.1.
SAFE_PROXY_FACTORY_V1_4_1: Final[str] = "0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67"
SAFE_L2_SINGLETON_V1_4_1: Final[str] = "0x29fcB43b46531BcA003ddC8FCB67FFE91900C762"

# Gnosis Guild ModuleProxyFactory (v1.2.0) — deploys Zodiac module clones via CREATE2.
# Source: gnosisguild/zodiac master/sdk/contracts.ts (KnownContracts.FACTORY)
MODULE_PROXY_FACTORY: Final[str] = "0x000000000000aDdB49795b0f9bA5BC298cDda236"

# Zodiac Roles Modifier v2.1.0 master copy (the post-V1 rewrite with bytes32 roleKey).
# Source: gnosisguild/zodiac master/sdk/contracts.ts (KnownContracts.ROLES_V2)
ROLES_MODIFIER_SINGLETON: Final[str] = "0x9646fDAD06d3e24444381f44362a3B0eB343D337"


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
    # ABIs — Safe core
    "SAFE_EXEC_TRANSACTION_ABI",
    "SAFE_GET_TX_HASH_ABI",
    "SAFE_NONCE_ABI",
    "SAFE_GET_OWNERS_ABI",
    "SAFE_GET_THRESHOLD_ABI",
    # ABIs — Safe deployment + module management
    "SAFE_SETUP_ABI",
    "SAFE_ENABLE_MODULE_ABI",
    "SAFE_PROXY_FACTORY_CREATE_PROXY_WITH_NONCE_ABI",
    # ABIs — Zodiac module deployment
    "MODULE_PROXY_FACTORY_DEPLOY_MODULE_ABI",
    # ABIs — Zodiac Roles Modifier v2
    "ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI",
    "ROLES_SET_UP_ABI",
    "ROLES_ASSIGN_ROLES_ABI",
    "ROLES_SET_DEFAULT_ROLE_ABI",
    "ROLES_ALLOW_TARGET_ABI",
    "ROLES_SCOPE_TARGET_ABI",
    "ROLES_ALLOW_FUNCTION_ABI",
    "ROLES_REVOKE_TARGET_ABI",
    # Canonical addresses
    "SAFE_PROXY_FACTORY_V1_4_1",
    "SAFE_L2_SINGLETON_V1_4_1",
    "MODULE_PROXY_FACTORY",
    "ROLES_MODIFIER_SINGLETON",
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
