"""Signer implementations for transaction signing.

This module provides implementations of the Signer ABC for various
signing backends.

Available Signers:
    - LocalKeySigner: Signs transactions using a local private key
    - SafeSigner: Base class for Safe wallet signers
    - DirectSafeSigner: Direct Safe signing for testing (Anvil)
    - ZodiacRolesSigner: Production Safe signing via Zodiac Roles

Example:
    # EOA signing
    from almanak.framework.execution.signer import LocalKeySigner

    signer = LocalKeySigner(private_key="0x...")
    signed_tx = await signer.sign(unsigned_tx, chain="arbitrum")

    # Safe wallet signing
    from almanak.framework.execution.signer import create_safe_signer, SafeSignerConfig

    config = SafeSignerConfig(
        mode="direct",
        wallet_config=wallet_config,
        private_key="0x...",
    )
    safe_signer = create_safe_signer(config)
    signed_tx = await safe_signer.sign_with_web3(tx, web3, eoa_nonce)
"""

from almanak.framework.execution.signer.local import LocalKeySigner
from almanak.framework.execution.signer.safe import (
    DirectSafeSigner,
    MultiSendEncoder,
    MultiSendPayload,
    SafeConfigError,
    SafeOperation,
    SafeSigner,
    SafeSignerConfig,
    SafeWalletConfig,
    SafeWalletMapping,
    ZodiacRolesSigner,
    create_safe_signer,
    create_safe_signer_from_env,
)

__all__ = [
    # EOA signer
    "LocalKeySigner",
    # Safe signers
    "SafeSigner",
    "DirectSafeSigner",
    "ZodiacRolesSigner",
    # Safe configuration
    "SafeConfigError",
    "SafeSignerConfig",
    "SafeWalletConfig",
    "SafeWalletMapping",
    # Safe factory functions
    "create_safe_signer",
    "create_safe_signer_from_env",
    # MultiSend
    "MultiSendEncoder",
    "MultiSendPayload",
    # Constants
    "SafeOperation",
]
