"""Solana Ed25519 transaction signer.

Uses solders for keypair management and VersionedTransaction signing.
Supports both base58 private keys and raw byte keypairs.
"""

from __future__ import annotations

import base64
import logging

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

logger = logging.getLogger(__name__)


class SolanaSignerError(Exception):
    """Error during Solana transaction signing."""


class SolanaSigner:
    """Ed25519 signer for Solana transactions.

    Handles deserialization and signing of Jupiter-style base64
    VersionedTransactions. Does NOT replace blockhashes — the caller
    must provide a fresh transaction (via Jupiter's deferred_swap pattern).

    Example:
        signer = SolanaSigner.from_base58(private_key_base58)
        signed_b64 = signer.sign_serialized_transaction(tx_base64)
    """

    def __init__(self, keypair: Keypair) -> None:
        self._keypair = keypair

    @classmethod
    def from_base58(cls, private_key: str) -> SolanaSigner:
        """Create a signer from a private key string.

        Auto-detects format:
        - 64-char hex string (32-byte seed) -> Keypair.from_seed()
        - Base58-encoded keypair or secret key -> Keypair.from_base58_string()

        Args:
            private_key: Private key in hex or base58 format.

        Returns:
            SolanaSigner instance.
        """
        # Auto-detect hex-encoded 32-byte seed (64 hex chars).
        # Only treat as hex if ALL characters are valid hex digits,
        # since a 64-char base58 string could be misinterpreted.
        if len(private_key) == 64 and all(c in "0123456789abcdefABCDEF" for c in private_key):
            seed_bytes = bytes.fromhex(private_key)
            keypair = Keypair.from_seed(seed_bytes)
            return cls(keypair)

        keypair = Keypair.from_base58_string(private_key)
        return cls(keypair)

    @classmethod
    def from_bytes(cls, key_bytes: bytes) -> SolanaSigner:
        """Create a signer from raw key bytes.

        Args:
            key_bytes: 64-byte Ed25519 keypair or 32-byte secret key.

        Returns:
            SolanaSigner instance.
        """
        keypair = Keypair.from_bytes(key_bytes)
        return cls(keypair)

    @property
    def pubkey(self) -> Pubkey:
        """Get the signer's public key."""
        return self._keypair.pubkey()

    @property
    def wallet_address(self) -> str:
        """Get the signer's wallet address as a base58 string."""
        return str(self._keypair.pubkey())

    def sign_serialized_transaction(
        self,
        serialized_tx_base64: str,
        additional_signers: list[str] | None = None,
    ) -> str:
        """Sign a base64-encoded serialized VersionedTransaction.

        Supports multi-signer transactions (e.g., Raydium LP open requires
        both the wallet keypair and the NFT mint keypair).

        Args:
            serialized_tx_base64: Base64-encoded unsigned VersionedTransaction.
            additional_signers: Optional list of base64-encoded keypair bytes
                for extra signers (e.g., NFT mint keypair for Raydium LP).

        Returns:
            Base64-encoded signed VersionedTransaction.

        Raises:
            SolanaSignerError: If signing fails.
        """
        try:
            tx_bytes = base64.b64decode(serialized_tx_base64)
            unsigned_tx = VersionedTransaction.from_bytes(tx_bytes)

            # Build signer list: wallet first, then additional signers
            signers = [self._keypair]
            if additional_signers:
                for signer_b64 in additional_signers:
                    signer_bytes = base64.b64decode(signer_b64)
                    signers.append(Keypair.from_bytes(signer_bytes))

            signed_tx = VersionedTransaction(unsigned_tx.message, signers)
            signed_bytes = bytes(signed_tx)
            return base64.b64encode(signed_bytes).decode("ascii")
        except Exception as e:
            raise SolanaSignerError(f"Failed to sign transaction: {e}") from e

    def sign_message(self, message_bytes: bytes) -> bytes:
        """Sign an arbitrary message with the Ed25519 keypair.

        Args:
            message_bytes: Message bytes to sign.

        Returns:
            64-byte Ed25519 signature.
        """
        sig = self._keypair.sign_message(message_bytes)
        return bytes(sig)


__all__ = [
    "SolanaSigner",
    "SolanaSignerError",
]
