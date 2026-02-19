"""Safe wallet signer implementations.

This module provides signers for executing transactions through Gnosis Safe
multisig wallets. Two signing modes are supported:

1. Zodiac Mode (Production):
   - Uses Zodiac Roles module for role-based access control
   - Delegates signing to a remote signer service
   - Recommended for production deployments

2. Direct Mode (Testing):
   - Calls Safe.execTransaction() directly
   - EOA must be an owner of the Safe
   - Use only for local Anvil/Hardhat testing

Key Features:
    - Safe wallet transaction execution (instead of direct EOA)
    - Atomic transaction bundling via MultiSend
    - Support for both production (Zodiac) and testing (direct) modes
    - Proper gas buffering for Safe overhead

Example:
    from almanak.framework.execution.signer.safe import create_safe_signer, SafeSignerConfig

    # Create signer from config
    config = SafeSignerConfig(
        mode="direct",  # or "zodiac" for production
        wallet_config=wallet_config,
        private_key="0x...",
    )
    signer = create_safe_signer(config)

    # Sign single transaction
    signed = await signer.sign_with_web3(tx, web3, eoa_nonce)

    # Sign atomic bundle
    signed = await signer.sign_bundle_with_web3(txs, web3, eoa_nonce, chain)

Environment Variables:
    ALMANAK_PLATFORM_WALLETS: JSON array of Safe wallet configurations
    ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT: URL for signer service (Zodiac mode)
    ALMANAK_SIGNER_SERVICE_JWT: JWT token for signer service (Zodiac mode)
"""

from almanak.framework.execution.signer.safe.base import SafeSigner
from almanak.framework.execution.signer.safe.config import (
    SafeConfigError,
    SafeSignerConfig,
    SafeWalletConfig,
    SafeWalletMapping,
    create_signer_config_from_env,
    get_wallet_mapping,
)
from almanak.framework.execution.signer.safe.constants import (
    DEFAULT_GAS_BUFFER_MULTIPLIER,
    DEFAULT_ROLE_KEY,
    ENSO_DELEGATE_ADDRESSES,
    MULTISEND_ADDRESSES,
    MULTISEND_SELECTOR,
    SAFE_EXEC_TRANSACTION_ABI,
    SAFE_GET_TX_HASH_ABI,
    SAFE_NONCE_ABI,
    ZERO_ADDRESS,
    ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    SafeOperation,
    get_multisend_address,
    get_operation_type,
    is_enso_delegate,
    role_key_to_bytes32,
)
from almanak.framework.execution.signer.safe.direct import DirectSafeSigner
from almanak.framework.execution.signer.safe.multisend import MultiSendEncoder, MultiSendPayload
from almanak.framework.execution.signer.safe.zodiac import ZodiacRolesSigner


def create_safe_signer(config: SafeSignerConfig) -> SafeSigner:
    """Factory function to create a Safe signer based on mode.

    This is the recommended way to create Safe signers. It automatically
    selects the appropriate implementation based on the configured mode.

    Args:
        config: SafeSignerConfig with mode and wallet configuration

    Returns:
        SafeSigner implementation (DirectSafeSigner or ZodiacRolesSigner)

    Raises:
        ValueError: If mode is not recognized
        SigningError: If private key is invalid

    Example:
        config = SafeSignerConfig(
            mode="direct",
            wallet_config=wallet_config,
            private_key="0x...",
        )
        signer = create_safe_signer(config)
    """
    if config.mode == "direct":
        return DirectSafeSigner(config)
    elif config.mode == "zodiac":
        return ZodiacRolesSigner(config)
    else:
        raise ValueError(f"Unknown Safe signer mode: '{config.mode}'. Valid modes: 'direct', 'zodiac'")


def create_safe_signer_from_env(
    safe_address: str,
    private_key: str,
    mode: str = "direct",
) -> SafeSigner:
    """Create a Safe signer from environment variables.

    This is a convenience function that loads wallet configuration from
    ALMANAK_PLATFORM_WALLETS and creates the appropriate signer.

    Args:
        safe_address: The Safe address to use
        private_key: The EOA private key for signing
        mode: Signing mode - "zodiac" or "direct" (default: "direct")

    Returns:
        SafeSigner implementation

    Raises:
        SafeConfigError: If environment configuration is invalid

    Environment Variables:
        ALMANAK_PLATFORM_WALLETS: Required - wallet mappings
        ALMANAK_SIGNER_SERVICE_ENDPOINT_ROOT: Required for Zodiac mode
        ALMANAK_SIGNER_SERVICE_JWT: Required for Zodiac mode

    Example:
        signer = create_safe_signer_from_env(
            safe_address="0xSafe...",
            private_key="0x...",
            mode="direct",
        )
    """
    config = create_signer_config_from_env(
        safe_address=safe_address,
        private_key=private_key,
        mode=mode,
    )
    return create_safe_signer(config)


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Factory functions
    "create_safe_signer",
    "create_safe_signer_from_env",
    # Base class
    "SafeSigner",
    # Implementations
    "DirectSafeSigner",
    "ZodiacRolesSigner",
    # Configuration
    "SafeConfigError",
    "SafeSignerConfig",
    "SafeWalletConfig",
    "SafeWalletMapping",
    "create_signer_config_from_env",
    "get_wallet_mapping",
    # MultiSend
    "MultiSendEncoder",
    "MultiSendPayload",
    # Constants
    "SafeOperation",
    "ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI",
    "SAFE_EXEC_TRANSACTION_ABI",
    "SAFE_GET_TX_HASH_ABI",
    "SAFE_NONCE_ABI",
    "MULTISEND_SELECTOR",
    "MULTISEND_ADDRESSES",
    "ENSO_DELEGATE_ADDRESSES",
    "DEFAULT_ROLE_KEY",
    "DEFAULT_GAS_BUFFER_MULTIPLIER",
    "ZERO_ADDRESS",
    # Helper functions
    "get_multisend_address",
    "is_enso_delegate",
    "get_operation_type",
    "role_key_to_bytes32",
]
