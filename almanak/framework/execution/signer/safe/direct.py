"""Direct Safe signer for local testing.

This module implements the DirectSafeSigner for local Anvil/Hardhat testing.
It bypasses Zodiac Roles and calls Safe.execTransaction() directly, which
requires the EOA to be an owner of the Safe with threshold=1.

Use Cases:
    - Local development with Anvil fork
    - Integration tests with forked mainnet
    - Testing Safe transaction flows without Zodiac setup

Requirements:
    - EOA must be an owner of the Safe
    - Safe threshold must be 1 (or EOA has sufficient signing power)
    - NOT for production use - use ZodiacRolesSigner instead

Example:
    from almanak.framework.execution.signer.safe import DirectSafeSigner, SafeSignerConfig

    config = SafeSignerConfig(
        mode="direct",
        wallet_config=wallet_config,
        private_key="0x...",
    )
    signer = DirectSafeSigner(config)

    # Sign single transaction
    signed = await signer.sign_with_web3(tx, web3, eoa_nonce)

    # Sign atomic bundle
    signed = await signer.sign_bundle_with_web3(txs, web3, eoa_nonce, chain)
"""

import logging
from typing import Any, cast

from web3 import AsyncWeb3
from web3.types import TxParams

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    SigningError,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.signer.safe.base import SafeSigner
from almanak.framework.execution.signer.safe.config import SafeSignerConfig
from almanak.framework.execution.signer.safe.constants import (
    SAFE_EXEC_TRANSACTION_ABI,
    SAFE_GET_OWNERS_ABI,
    SAFE_GET_THRESHOLD_ABI,
    SAFE_GET_TX_HASH_ABI,
    SAFE_NONCE_ABI,
    ZERO_ADDRESS,
    SafeOperation,
)
from almanak.framework.execution.signer.safe.multisend import MultiSendEncoder

logger = logging.getLogger(__name__)


class DirectSafeSigner(SafeSigner):
    """Signer for local testing that bypasses Zodiac Roles.

    This signer calls Safe.execTransaction() directly, which requires:
    - The EOA to be an owner of the Safe
    - Safe threshold to be 1 (single signature sufficient)

    Use this ONLY for local Anvil/Hardhat testing, not production.

    The signing flow:
    1. Get Safe transaction hash via Safe.getTransactionHash()
    2. Generate EOA signature for the hash (raw signing, not EIP-191)
    3. Build Safe.execTransaction() with the signature
    4. Sign the wrapper transaction with EOA private key

    Attributes:
        address: Safe wallet address
        eoa_address: EOA that signs for the Safe
        gas_buffer_multiplier: Gas buffer for Safe overhead (default 2.0)

    Example:
        config = SafeSignerConfig(
            mode="direct",
            wallet_config=SafeWalletConfig(
                safe_address="0xSafe...",
                eoa_address="0xEOA...",
            ),
            private_key="0x...",
        )
        signer = DirectSafeSigner(config)

        # Sign a single transaction
        signed = await signer.sign_with_web3(
            tx, web3, eoa_nonce=5, pos_in_bundle=0
        )
    """

    def __init__(self, config: SafeSignerConfig) -> None:
        """Initialize the direct Safe signer.

        Args:
            config: SafeSignerConfig with mode="direct"

        Raises:
            SigningError: If private key is invalid
            ValueError: If mode is not "direct"
        """
        if config.mode != "direct":
            raise ValueError(f"DirectSafeSigner requires mode='direct', got '{config.mode}'")
        super().__init__(config)

        # Track ownership verification (done once per session)
        self._ownership_verified: bool = False

    # =========================================================================
    # Safe Nonce Management
    # =========================================================================

    async def _get_safe_nonce(
        self,
        web3: AsyncWeb3,
        pos_in_bundle: int = 0,
    ) -> int:
        """Get the Safe nonce for a transaction.

        For bundle execution:
        - pos=0: Reads nonce from chain and caches it
        - pos>0: Returns cached nonce + position

        Args:
            web3: AsyncWeb3 instance
            pos_in_bundle: Position in bundle (0 for single tx)

        Returns:
            Safe nonce to use for this transaction
        """
        safe_address = web3.to_checksum_address(self._safe_address)
        chain_id = await web3.eth.chain_id

        # Build chain-aware cache key for multi-chain support
        cache_key = f"{chain_id}:{safe_address.lower()}"

        if pos_in_bundle == 0 or cache_key not in self._safe_nonce_cache:
            # Read nonce from chain
            safe_nonce_contract = web3.eth.contract(
                address=safe_address,
                abi=SAFE_NONCE_ABI,
            )
            chain_nonce = await safe_nonce_contract.functions.nonce().call()
            logger.debug(f"Safe nonce from chain {chain_id}: {chain_nonce}")
        else:
            chain_nonce = self._safe_nonce_cache.get(cache_key, 0)

        return self._get_cached_safe_nonce(safe_address, chain_nonce, pos_in_bundle, chain_id=chain_id)

    async def _verify_safe_ownership(self, web3: AsyncWeb3) -> None:
        """Verify EOA is an owner of the Safe with sufficient signing power.

        This method checks:
        1. EOA is in the Safe's owner list
        2. Safe threshold is 1 (single-sig execution)

        Raises:
            SigningError: If EOA is not an owner or threshold > 1
        """
        safe_address = web3.to_checksum_address(self._safe_address)

        # Get owners
        owners_contract = web3.eth.contract(
            address=safe_address,
            abi=SAFE_GET_OWNERS_ABI,
        )
        owners = await owners_contract.functions.getOwners().call()
        owners_lower = [o.lower() for o in owners]

        if self._eoa_address.lower() not in owners_lower:
            raise SigningError(f"EOA {self._eoa_address} is not an owner of Safe {safe_address}. Owners: {owners}")

        # Get threshold
        threshold_contract = web3.eth.contract(
            address=safe_address,
            abi=SAFE_GET_THRESHOLD_ABI,
        )
        threshold = await threshold_contract.functions.getThreshold().call()

        if threshold > 1:
            raise SigningError(
                f"Safe {safe_address} has threshold={threshold}. "
                f"DirectSafeSigner requires threshold=1 for single-sig execution. "
                f"Use ZodiacRolesSigner for multi-sig Safes."
            )

        logger.debug(f"Safe ownership verified: {self._eoa_address[:10]}... is owner, threshold={threshold}")

    # =========================================================================
    # Signature Generation
    # =========================================================================

    def _generate_safe_signature(
        self,
        safe_tx_hash: bytes,
    ) -> bytes:
        """Generate a valid Safe signature for a transaction hash.

        Safe signature format: {bytes32 r}{bytes32 s}{uint8 v}
        For EOA signatures, v = 27 or 28

        IMPORTANT: Safe expects a raw signature of the transaction hash,
        NOT an Ethereum Signed Message (EIP-191). We sign the hash directly.

        Args:
            safe_tx_hash: The Safe transaction hash to sign

        Returns:
            65-byte signature (r + s + v)
        """
        from eth_keys import keys

        # Get the private key bytes from the account
        private_key_bytes = self._account.key

        # Create an eth_keys PrivateKey object for signing
        private_key = keys.PrivateKey(private_key_bytes)

        # Sign the raw hash directly (no EIP-191 prefix)
        # This is what Safe expects - raw signing of the transaction hash
        signature = private_key.sign_msg_hash(safe_tx_hash)

        # Extract r, s, v components
        r = signature.r.to_bytes(32, byteorder="big")
        s = signature.s.to_bytes(32, byteorder="big")

        # Safe expects v as 27/28 for EOA signatures
        v = signature.v
        if v < 27:
            v = v + 27
        v_byte = v.to_bytes(1, byteorder="big")

        sig_bytes = r + s + v_byte

        logger.debug(f"Generated Safe signature: len={len(sig_bytes)}, v={v}")

        return sig_bytes

    # =========================================================================
    # Transaction Signing
    # =========================================================================

    async def sign_with_web3(
        self,
        tx: UnsignedTransaction,
        web3: AsyncWeb3,
        eoa_nonce: int,
        pos_in_bundle: int = 0,
    ) -> SignedTransaction:
        """Sign a single transaction via Safe.execTransaction().

        This method:
        1. Gets the Safe transaction hash from the Safe contract
        2. Generates an EOA signature for the hash
        3. Builds Safe.execTransaction() with the signature
        4. Signs the wrapper transaction with the EOA key

        Args:
            tx: Unsigned transaction to execute through Safe
            web3: AsyncWeb3 instance
            eoa_nonce: EOA nonce for the wrapper transaction
            pos_in_bundle: Position in bundle (for Safe nonce tracking)

        Returns:
            SignedTransaction ready for submission

        Raises:
            SigningError: If signing fails
        """
        # Verify ownership on first signing (cached for session)
        if not self._ownership_verified:
            await self._verify_safe_ownership(web3)
            self._ownership_verified = True

        safe_address = web3.to_checksum_address(self._safe_address)
        eoa_address = web3.to_checksum_address(self._eoa_address)

        # Determine operation type
        if tx.to is None:
            raise SigningError(reason="Contract creation not supported via Safe")

        target_address = web3.to_checksum_address(tx.to)
        operation = self.get_operation_type(target_address)

        logger.debug(
            f"Signing tx via Safe: target={target_address[:10]}..., "
            f"operation={'DELEGATECALL' if operation == SafeOperation.DELEGATE_CALL else 'CALL'}"
        )

        # Get Safe nonce
        safe_nonce = await self._get_safe_nonce(web3, pos_in_bundle)
        logger.debug(f"Using Safe nonce: {safe_nonce} (pos={pos_in_bundle})")

        # Get Safe transaction hash
        safe_hash_contract = web3.eth.contract(
            address=safe_address,
            abi=SAFE_GET_TX_HASH_ABI,
        )

        safe_tx_hash = await safe_hash_contract.functions.getTransactionHash(
            target_address,
            tx.value,
            tx.data,
            operation,
            0,  # safeTxGas
            0,  # baseGas
            0,  # gasPrice
            ZERO_ADDRESS,  # gasToken
            ZERO_ADDRESS,  # refundReceiver
            safe_nonce,
        ).call()

        logger.debug(f"Safe tx hash: {web3.to_hex(safe_tx_hash)}")

        # Generate signature
        signature = self._generate_safe_signature(safe_tx_hash)

        # Build Safe.execTransaction parameters
        safe_tx_params = (
            target_address,
            tx.value,
            tx.data,
            operation,
            0,  # safeTxGas (0 = use all remaining gas)
            0,  # baseGas
            0,  # gasPrice (no refund)
            ZERO_ADDRESS,  # gasToken (ETH)
            ZERO_ADDRESS,  # refundReceiver
            signature,
        )

        # Create Safe contract instance
        safe_contract = web3.eth.contract(
            address=safe_address,
            abi=SAFE_EXEC_TRANSACTION_ABI,
        )

        # Get gas parameters from original transaction
        if tx.tx_type == TransactionType.EIP_1559:
            gas_params = {
                "maxPriorityFeePerGas": tx.max_priority_fee_per_gas,
                "maxFeePerGas": tx.max_fee_per_gas,
                "type": 2,
            }
        else:
            gas_params = {
                "gasPrice": tx.gas_price,
            }

        # Build the wrapper transaction
        wrapper_tx = await safe_contract.functions.execTransaction(*safe_tx_params).build_transaction(
            cast(
                TxParams,
                {
                    "from": eoa_address,
                    "nonce": eoa_nonce,
                    "value": 0,  # Value goes through Safe, not directly
                    "gas": 1,  # Placeholder, will be replaced
                    **gas_params,
                },
            )
        )

        # Estimate gas for the complete Safe wrapper transaction
        wrapper_tx["gas"] = await self._estimate_wrapper_gas(web3, wrapper_tx, tx.gas_limit)

        # Sign the wrapper transaction
        try:
            signed = self._account.sign_transaction(cast(dict[str, Any], wrapper_tx))

            # Extract raw tx and hash
            raw_tx = signed.raw_transaction.hex()
            if not raw_tx.startswith("0x"):
                raw_tx = "0x" + raw_tx

            tx_hash = signed.hash.hex()
            if not tx_hash.startswith("0x"):
                tx_hash = "0x" + tx_hash

            logger.debug(f"Signed Safe tx: hash={tx_hash[:16]}...")

            return SignedTransaction(
                raw_tx=raw_tx,
                tx_hash=tx_hash,
                unsigned_tx=tx,
            )

        except Exception as e:
            raise SigningError(reason=f"Failed to sign Safe transaction: {type(e).__name__}: {e}") from None

    async def sign_bundle_with_web3(
        self,
        txs: list[UnsignedTransaction],
        web3: AsyncWeb3,
        eoa_nonce: int,
        chain: str,
    ) -> SignedTransaction:
        """Sign multiple transactions as an atomic MultiSend bundle.

        This method:
        1. Clears the Safe nonce cache
        2. Encodes transactions via MultiSendEncoder
        3. Signs the MultiSend payload as a single Safe transaction

        All transactions succeed or fail atomically.

        Args:
            txs: List of transactions to bundle
            web3: AsyncWeb3 instance
            eoa_nonce: EOA nonce for the wrapper transaction
            chain: Chain name (for MultiSend address lookup)

        Returns:
            SignedTransaction containing the atomic bundle

        Raises:
            SigningError: If signing fails
            ValueError: If txs list is empty
        """
        if not txs:
            raise ValueError("Cannot sign empty transaction bundle")

        # Clear nonce cache for new bundle
        self.clear_nonce_cache()

        # Build MultiSend payload
        payload = MultiSendEncoder.build_payload(txs, chain, web3)

        logger.debug(f"Built MultiSend bundle: {len(txs)} txs, dataLen={len(payload.data)}")

        # Calculate total gas from all transactions
        total_gas = sum(tx.gas_limit for tx in txs)

        # Get gas parameters from first transaction
        first_tx = txs[0]
        if first_tx.tx_type == TransactionType.EIP_1559:
            max_fee = first_tx.max_fee_per_gas
            priority_fee = first_tx.max_priority_fee_per_gas
        else:
            max_fee = first_tx.gas_price
            priority_fee = None

        # Create an UnsignedTransaction for the MultiSend payload
        multisend_tx = UnsignedTransaction(
            to=payload.to,
            value=payload.value,
            data=payload.data,
            chain_id=first_tx.chain_id,
            gas_limit=total_gas,
            tx_type=first_tx.tx_type,
            from_address=self._safe_address,
            max_fee_per_gas=max_fee if first_tx.tx_type == TransactionType.EIP_1559 else None,
            max_priority_fee_per_gas=priority_fee,
            gas_price=max_fee if first_tx.tx_type == TransactionType.LEGACY else None,
        )

        # Sign via Safe.execTransaction with DELEGATECALL operation
        # We need to handle MultiSend specially since it requires DELEGATECALL
        return await self._sign_multisend_with_web3(
            multisend_tx,
            web3,
            eoa_nonce,
            payload.operation,
        )

    async def _sign_multisend_with_web3(
        self,
        tx: UnsignedTransaction,
        web3: AsyncWeb3,
        eoa_nonce: int,
        operation: SafeOperation,
    ) -> SignedTransaction:
        """Sign a MultiSend transaction with explicit operation type.

        This is similar to sign_with_web3 but uses DELEGATECALL for MultiSend.

        Args:
            tx: MultiSend transaction
            web3: AsyncWeb3 instance
            eoa_nonce: EOA nonce
            operation: Operation type (DELEGATECALL for MultiSend)

        Returns:
            SignedTransaction
        """
        safe_address = web3.to_checksum_address(self._safe_address)
        eoa_address = web3.to_checksum_address(self._eoa_address)

        if tx.to is None:
            raise SigningError(reason="MultiSend transaction requires 'to' address")

        target_address = web3.to_checksum_address(tx.to)

        # Get Safe nonce (pos=0 for bundle wrapper)
        safe_nonce = await self._get_safe_nonce(web3, pos_in_bundle=0)

        # Get Safe transaction hash
        safe_hash_contract = web3.eth.contract(
            address=safe_address,
            abi=SAFE_GET_TX_HASH_ABI,
        )

        safe_tx_hash = await safe_hash_contract.functions.getTransactionHash(
            target_address,
            tx.value,
            tx.data,
            operation,  # DELEGATECALL for MultiSend
            0,
            0,
            0,
            ZERO_ADDRESS,
            ZERO_ADDRESS,
            safe_nonce,
        ).call()

        # Generate signature
        signature = self._generate_safe_signature(safe_tx_hash)

        # Build Safe.execTransaction parameters
        safe_tx_params = (
            target_address,
            tx.value,
            tx.data,
            operation,
            0,
            0,
            0,
            ZERO_ADDRESS,
            ZERO_ADDRESS,
            signature,
        )

        safe_contract = web3.eth.contract(
            address=safe_address,
            abi=SAFE_EXEC_TRANSACTION_ABI,
        )

        # Get gas parameters
        if tx.tx_type == TransactionType.EIP_1559:
            gas_params = {
                "maxPriorityFeePerGas": tx.max_priority_fee_per_gas,
                "maxFeePerGas": tx.max_fee_per_gas,
                "type": 2,
            }
        else:
            gas_params = {
                "gasPrice": tx.gas_price,
            }

        # Build wrapper transaction
        wrapper_tx = await safe_contract.functions.execTransaction(*safe_tx_params).build_transaction(
            cast(
                TxParams,
                {
                    "from": eoa_address,
                    "nonce": eoa_nonce,
                    "value": 0,
                    "gas": 1,
                    **gas_params,
                },
            )
        )

        # Estimate gas for the complete Safe+MultiSend wrapper transaction
        buffered_gas = await self._estimate_wrapper_gas(web3, wrapper_tx, tx.gas_limit)
        wrapper_tx["gas"] = buffered_gas

        # Sign
        try:
            signed = self._account.sign_transaction(cast(dict[str, Any], wrapper_tx))

            raw_tx = signed.raw_transaction.hex()
            if not raw_tx.startswith("0x"):
                raw_tx = "0x" + raw_tx

            tx_hash = signed.hash.hex()
            if not tx_hash.startswith("0x"):
                tx_hash = "0x" + tx_hash

            logger.info(f"Signed MultiSend bundle: hash={tx_hash[:16]}..., gas={buffered_gas}")

            return SignedTransaction(
                raw_tx=raw_tx,
                tx_hash=tx_hash,
                unsigned_tx=tx,
            )

        except Exception as e:
            raise SigningError(reason=f"Failed to sign MultiSend bundle: {type(e).__name__}: {e}") from None


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "DirectSafeSigner",
]
