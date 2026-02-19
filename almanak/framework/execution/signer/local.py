"""Local Private Key Signer for EOA Wallets.

This module implements the Signer interface for signing transactions
using a locally stored private key. This is suitable for development,
testing, and self-custody scenarios.

SECURITY NOTE:
    This signer stores private keys in memory. For production deployments
    with high-value wallets, consider using a KMS-based signer instead.

Example:
    from almanak.framework.execution.signer import LocalKeySigner

    # Create signer from private key
    signer = LocalKeySigner(private_key="0x...")

    # Sign a transaction
    signed_tx = await signer.sign(unsigned_tx, chain="arbitrum")

    # Get wallet address
    print(f"Wallet address: {signer.address}")
"""

import logging

from eth_account import Account
from eth_account.signers.local import LocalAccount

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    Signer,
    SigningError,
    TransactionType,
    UnsignedTransaction,
)

logger = logging.getLogger(__name__)


class LocalKeySigner(Signer):
    """Signs transactions using a local private key.

    This signer uses the eth-account library to sign transactions locally.
    The private key is stored in memory and derived to an Ethereum account
    at initialization time.

    SECURITY CONTRACT:
        - Private keys are NEVER logged, printed, or included in error messages
        - Private keys are NEVER exposed through any method or property
        - Transaction fields are validated before signing
        - The wallet address is derived from the key at initialization

    Supported transaction types:
        - EIP-1559 (Type 2): Modern fee market transactions
        - Legacy (Type 0): Pre-EIP-1559 transactions

    Attributes:
        address: The checksummed Ethereum address derived from the private key

    Example:
        # Initialize with private key
        signer = LocalKeySigner(private_key="0x...")

        # Create unsigned transaction
        tx = UnsignedTransaction(
            to="0x1234...",
            value=0,
            data="0xa9059cbb...",
            chain_id=42161,
            gas_limit=100000,
            nonce=5,
            max_fee_per_gas=100_000_000,
            max_priority_fee_per_gas=1_000_000,
        )

        # Sign transaction
        signed = await signer.sign(tx, chain="arbitrum")
        print(f"Signed tx hash: {signed.tx_hash}")
    """

    def __init__(self, private_key: str) -> None:
        """Initialize the signer with a private key.

        Args:
            private_key: Hex-encoded private key (with or without 0x prefix)

        Raises:
            SigningError: If the private key is invalid

        Example:
            signer = LocalKeySigner(private_key="0xabc123...")
        """
        try:
            self._account: LocalAccount = Account.from_key(private_key)
            self._address: str = self._account.address
            logger.debug("LocalKeySigner initialized for address %s", self._address)
        except Exception as e:
            # Never include the private key in error messages
            raise SigningError(reason=f"Invalid private key format: {type(e).__name__}") from None

    @property
    def address(self) -> str:
        """Return the wallet address associated with this signer.

        The address is derived from the private key at initialization
        and is cached for efficiency.

        Returns:
            Checksummed Ethereum address (0x-prefixed, 42 characters)

        Example:
            signer = LocalKeySigner(private_key="0x...")
            print(signer.address)  # 0x71C7656EC7ab88b098defB751B7401B5f6d8976F
        """
        return self._address

    async def sign(
        self,
        tx: UnsignedTransaction,
        chain: str,
    ) -> SignedTransaction:
        """Sign a transaction with the local private key.

        This method validates the transaction fields, builds the appropriate
        transaction dictionary based on transaction type (EIP-1559 or legacy),
        and signs it using the eth-account library.

        Args:
            tx: Unsigned transaction to sign
            chain: Chain name (e.g., "arbitrum", "ethereum") for logging/validation

        Returns:
            SignedTransaction containing the raw signed bytes and transaction hash

        Raises:
            SigningError: If signing fails due to invalid fields or key issues
            ValueError: If transaction fields are malformed

        Example:
            tx = UnsignedTransaction(
                to="0x1234...",
                value=1_000_000_000_000_000_000,  # 1 ETH
                data="0x",
                chain_id=1,
                gas_limit=21000,
                nonce=0,
                max_fee_per_gas=30_000_000_000,
                max_priority_fee_per_gas=1_000_000_000,
            )
            signed = await signer.sign(tx, chain="ethereum")
        """
        # Validate transaction fields
        self._validate_transaction(tx)

        try:
            # Build transaction dictionary based on type
            tx_dict = self._build_transaction_dict(tx)

            # Sign the transaction
            signed = self._account.sign_transaction(tx_dict)

            # Extract raw transaction and hash
            # eth-account returns different formats depending on version
            raw_tx = self._get_raw_tx_hex(signed)
            tx_hash = self._get_tx_hash_hex(signed)

            logger.debug(
                "Transaction signed: hash=%s, chain=%s, to=%s",
                tx_hash[:16] + "...",
                chain,
                tx.to[:16] + "..." if tx.to else "contract_creation",
            )

            return SignedTransaction(
                raw_tx=raw_tx,
                tx_hash=tx_hash,
                unsigned_tx=tx,
            )

        except SigningError:
            # Re-raise SigningError without modification
            raise
        except Exception as e:
            # Wrap any other exception in SigningError
            # Never include sensitive data in error messages
            raise SigningError(reason=f"Failed to sign transaction: {type(e).__name__}: {str(e)}") from e

    def _validate_transaction(self, tx: UnsignedTransaction) -> None:
        """Validate transaction fields before signing.

        This method checks that all required fields are present and valid
        for the transaction type.

        Args:
            tx: Transaction to validate

        Raises:
            SigningError: If validation fails
        """
        # Check nonce is set
        if tx.nonce is None:
            raise SigningError(reason="Transaction nonce must be set before signing")

        # Validate from_address matches signer if set
        if tx.from_address is not None:
            if tx.from_address.lower() != self._address.lower():
                raise SigningError(
                    reason=f"Transaction from_address ({tx.from_address}) does not match "
                    f"signer address ({self._address})"
                )

        # Validate gas fields based on transaction type
        if tx.tx_type == TransactionType.EIP_1559:
            if tx.max_fee_per_gas is None or tx.max_priority_fee_per_gas is None:
                raise SigningError(reason="EIP-1559 transaction requires max_fee_per_gas and max_priority_fee_per_gas")
            if tx.max_priority_fee_per_gas > tx.max_fee_per_gas:
                raise SigningError(
                    reason=f"max_priority_fee_per_gas ({tx.max_priority_fee_per_gas}) cannot exceed "
                    f"max_fee_per_gas ({tx.max_fee_per_gas})"
                )
        elif tx.tx_type == TransactionType.LEGACY:
            if tx.gas_price is None:
                raise SigningError(reason="Legacy transaction requires gas_price")

        # Validate data format
        if not tx.data.startswith("0x"):
            raise SigningError(reason=f"Transaction data must be hex-encoded with 0x prefix, got: {tx.data[:20]}...")

        # Validate to address format (if not contract creation)
        if tx.to is not None:
            if not tx.to.startswith("0x") or len(tx.to) != 42:
                raise SigningError(reason=f"Invalid 'to' address format: {tx.to}")

    def _build_transaction_dict(self, tx: UnsignedTransaction) -> dict:
        """Build transaction dictionary for signing.

        Args:
            tx: Unsigned transaction

        Returns:
            Dictionary suitable for eth-account sign_transaction
        """
        from web3 import Web3

        tx_dict: dict = {
            "to": Web3.to_checksum_address(tx.to) if tx.to else None,
            "value": tx.value,
            "data": tx.data,
            "chainId": tx.chain_id,
            "gas": tx.gas_limit,
            "nonce": tx.nonce,
        }

        if tx.tx_type == TransactionType.EIP_1559:
            tx_dict["maxFeePerGas"] = tx.max_fee_per_gas
            tx_dict["maxPriorityFeePerGas"] = tx.max_priority_fee_per_gas
            tx_dict["type"] = 2
        else:
            # Legacy transaction
            tx_dict["gasPrice"] = tx.gas_price
            # Type 0 is implicit when gasPrice is set

        return tx_dict

    def _get_raw_tx_hex(self, signed) -> str:
        """Extract raw transaction hex from signed result.

        Handles different eth-account versions and return types.

        Args:
            signed: Result from sign_transaction

        Returns:
            Hex-encoded raw transaction with 0x prefix
        """
        # Handle different return types from eth-account
        raw = signed.rawTransaction if hasattr(signed, "rawTransaction") else signed.raw_transaction

        if hasattr(raw, "hex"):
            hex_str = raw.hex()
        else:
            hex_str = raw.hex() if isinstance(raw, bytes) else str(raw)

        # Ensure 0x prefix
        if not hex_str.startswith("0x"):
            hex_str = "0x" + hex_str

        return hex_str

    def _get_tx_hash_hex(self, signed) -> str:
        """Extract transaction hash hex from signed result.

        Handles different eth-account versions and return types.

        Args:
            signed: Result from sign_transaction

        Returns:
            Hex-encoded transaction hash with 0x prefix
        """
        tx_hash = signed.hash

        if hasattr(tx_hash, "hex"):
            hex_str = tx_hash.hex()
        else:
            hex_str = tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash)

        # Ensure 0x prefix
        if not hex_str.startswith("0x"):
            hex_str = "0x" + hex_str

        return hex_str

    def __repr__(self) -> str:
        """Return string representation without exposing private key."""
        return f"LocalKeySigner(address={self._address})"

    def __str__(self) -> str:
        """Return string representation without exposing private key."""
        return f"LocalKeySigner({self._address})"
