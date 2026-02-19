"""Zodiac Roles signer for production Safe operations.

This module implements the ZodiacRolesSigner for production deployments.
It wraps transactions with Zodiac.execTransactionWithRole() and uses
a remote signer service for signing (JWT authentication).

Use Cases:
    - Production deployments with role-based access control
    - Agent deployments where signing is delegated to a secure service
    - Multi-chain production infrastructure

Architecture:
    1. Transaction is wrapped in execTransactionWithRole()
    2. Wrapper transaction is sent to remote signer service
    3. Service signs with the EOA private key
    4. Signed transaction is returned for submission

Example:
    from almanak.framework.execution.signer.safe import ZodiacRolesSigner, SafeSignerConfig

    config = SafeSignerConfig(
        mode="zodiac",
        wallet_config=wallet_config,
        private_key="0x...",  # Local EOA key for gas estimation
        signer_service_url="https://signer.example.com",
        signer_service_jwt="eyJ...",
    )
    signer = ZodiacRolesSigner(config)

    signed = await signer.sign_with_web3(tx, web3, eoa_nonce)
"""

import logging
from typing import Any, cast

import aiohttp
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
    ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    SafeOperation,
    role_key_to_bytes32,
)
from almanak.framework.execution.signer.safe.multisend import MultiSendEncoder

logger = logging.getLogger(__name__)


class ZodiacRolesSigner(SafeSigner):
    """Production signer using Zodiac Roles module.

    This signer wraps transactions in Zodiac's execTransactionWithRole()
    and delegates signing to a remote signer service. This is the
    production deployment pattern for secure agent operations.

    The signing flow:
    1. Build execTransactionWithRole() wrapper with role key
    2. Send wrapper to remote signer service (POST with JWT)
    3. Service signs with EOA private key
    4. Return signed transaction for submission

    Attributes:
        address: Safe wallet address
        eoa_address: EOA that signs for the Safe
        zodiac_roles_address: Zodiac Roles module address
        role_key: Role key for authorization (default: "AlmanakAgentRole")

    Example:
        config = SafeSignerConfig(
            mode="zodiac",
            wallet_config=SafeWalletConfig(
                safe_address="0xSafe...",
                eoa_address="0xEOA...",
                zodiac_roles_address="0xZodiac...",
            ),
            private_key="0x...",
            signer_service_url="https://signer.example.com",
            signer_service_jwt="eyJ...",
        )
        signer = ZodiacRolesSigner(config)

        signed = await signer.sign_with_web3(tx, web3, eoa_nonce)
    """

    def __init__(self, config: SafeSignerConfig) -> None:
        """Initialize the Zodiac Roles signer.

        Args:
            config: SafeSignerConfig with mode="zodiac"

        Raises:
            SigningError: If private key is invalid
            ValueError: If mode is not "zodiac" or required fields missing
        """
        if config.mode != "zodiac":
            raise ValueError(f"ZodiacRolesSigner requires mode='zodiac', got '{config.mode}'")

        if not config.wallet_config.zodiac_roles_address:
            raise ValueError("ZodiacRolesSigner requires zodiac_roles_address")

        if not config.signer_service_url:
            raise ValueError("ZodiacRolesSigner requires signer_service_url")

        if not config.signer_service_jwt:
            raise ValueError("ZodiacRolesSigner requires signer_service_jwt")

        super().__init__(config)

        self._zodiac_roles_address = config.wallet_config.zodiac_roles_address
        self._role_key = config.wallet_config.role_key
        self._role_key_bytes = role_key_to_bytes32(self._role_key)
        self._signer_service_url = config.signer_service_url
        self._signer_service_jwt = config.signer_service_jwt

        logger.info(
            f"ZodiacRolesSigner initialized: zodiac={self._zodiac_roles_address[:10]}..., role={self._role_key}"
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def zodiac_roles_address(self) -> str:
        """Return the Zodiac Roles module address."""
        return self._zodiac_roles_address

    @property
    def role_key(self) -> str:
        """Return the role key for authorization."""
        return self._role_key

    # =========================================================================
    # Remote Signer Service
    # =========================================================================

    async def _sign_via_service(
        self,
        tx_dict: dict[str, Any],
    ) -> str:
        """Sign a transaction via the remote signer service.

        Args:
            tx_dict: Transaction dictionary to sign

        Returns:
            Signed transaction hex string

        Raises:
            SigningError: If signing fails
        """
        endpoint = f"{self._signer_service_url}/sign/transaction"

        request_payload = {
            "eoa_address": self._eoa_address,
            "transaction_payload": [tx_dict],
            "signing_type": "EVM",
        }

        headers = {
            "Authorization": f"Bearer {self._signer_service_jwt}",
            "Content-Type": "application/json",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    endpoint,
                    json=request_payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if not 200 <= response.status < 300:
                        error_text = await response.text()
                        raise SigningError(reason=f"Signer service error (HTTP {response.status}): {error_text}")

                    response_data = await response.json()
                    signed_transactions = response_data.get("signed_transactions", [])

                    if not signed_transactions:
                        raise SigningError(reason="Signer service returned no signed transactions")

                    return signed_transactions[0]

        except aiohttp.ClientError as e:
            raise SigningError(reason=f"Failed to connect to signer service: {type(e).__name__}: {e}") from None
        except SigningError:
            raise
        except Exception as e:
            raise SigningError(reason=f"Signer service error: {type(e).__name__}: {e}") from None

    def _compute_tx_hash(self, signed_tx_hex: str, web3: AsyncWeb3) -> str:
        """Compute transaction hash from signed transaction.

        Args:
            signed_tx_hex: Signed transaction hex string
            web3: AsyncWeb3 instance

        Returns:
            Transaction hash hex string
        """
        # Remove 0x prefix if present
        if signed_tx_hex.startswith("0x"):
            signed_tx_bytes = bytes.fromhex(signed_tx_hex[2:])
        else:
            signed_tx_bytes = bytes.fromhex(signed_tx_hex)

        tx_hash = web3.keccak(signed_tx_bytes)
        return web3.to_hex(tx_hash)

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
        """Sign a transaction via Zodiac execTransactionWithRole().

        This method:
        1. Builds execTransactionWithRole() wrapper
        2. Sends to remote signer service
        3. Returns signed transaction

        Args:
            tx: Unsigned transaction to execute through Safe
            web3: AsyncWeb3 instance
            eoa_nonce: EOA nonce for the wrapper transaction
            pos_in_bundle: Position in bundle (for nonce offset)

        Returns:
            SignedTransaction ready for submission

        Raises:
            SigningError: If signing fails
        """
        zodiac_address = web3.to_checksum_address(self._zodiac_roles_address)
        eoa_address = web3.to_checksum_address(self._eoa_address)

        # Determine operation type
        if tx.to is None:
            raise SigningError(reason="Contract creation not supported via Safe")

        target_address = web3.to_checksum_address(tx.to)
        operation = self.get_operation_type(target_address)

        logger.debug(
            f"Signing tx via Zodiac: target={target_address[:10]}..., "
            f"operation={'DELEGATECALL' if operation == SafeOperation.DELEGATE_CALL else 'CALL'}"
        )

        # Build execTransactionWithRole parameters
        exec_params = (
            target_address,
            tx.value,
            tx.data,
            operation,
            self._role_key_bytes,
            True,  # shouldRevert
        )

        # Create Zodiac contract instance
        zodiac_contract = web3.eth.contract(
            address=zodiac_address,
            abi=ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
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

        # Build the wrapper transaction
        wrapper_tx = await zodiac_contract.functions.execTransactionWithRole(*exec_params).build_transaction(
            cast(
                TxParams,
                {
                    "from": eoa_address,
                    "nonce": eoa_nonce + pos_in_bundle,
                    "value": 0,
                    "gas": 1,  # Placeholder
                    **gas_params,
                },
            )
        )

        # Estimate gas and set gasLimit (signer service expects gasLimit, not gas)
        estimated_gas = await self._estimate_wrapper_gas(web3, wrapper_tx, tx.gas_limit)
        wrapper_tx_dict = cast(dict[str, Any], wrapper_tx)
        del wrapper_tx_dict["gas"]
        wrapper_tx_dict["gasLimit"] = estimated_gas

        logger.debug(f"Wrapper tx: nonce={wrapper_tx_dict['nonce']}, gasLimit={estimated_gas}")

        # Sign via remote service
        signed_tx_hex = await self._sign_via_service(wrapper_tx_dict)

        # Ensure 0x prefix
        if not signed_tx_hex.startswith("0x"):
            signed_tx_hex = "0x" + signed_tx_hex

        # Compute transaction hash
        tx_hash = self._compute_tx_hash(signed_tx_hex, web3)

        logger.debug(f"Signed Zodiac tx: hash={tx_hash[:16]}...")

        return SignedTransaction(
            raw_tx=signed_tx_hex,
            tx_hash=tx_hash,
            unsigned_tx=tx,
        )

    async def sign_bundle_with_web3(
        self,
        txs: list[UnsignedTransaction],
        web3: AsyncWeb3,
        eoa_nonce: int,
        chain: str,
    ) -> SignedTransaction:
        """Sign multiple transactions as an atomic MultiSend bundle.

        For Zodiac mode, the MultiSend is wrapped in execTransactionWithRole
        and sent to the remote signer service.

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

        logger.debug(f"Built MultiSend bundle for Zodiac: {len(txs)} txs, dataLen={len(payload.data)}")

        # Calculate total gas from all transactions
        total_gas = sum(tx.gas_limit for tx in txs)

        # Get gas parameters from first transaction
        first_tx = txs[0]

        # Create an UnsignedTransaction for the MultiSend payload
        multisend_tx = UnsignedTransaction(
            to=payload.to,
            value=payload.value,
            data=payload.data,
            chain_id=first_tx.chain_id,
            gas_limit=total_gas,
            tx_type=first_tx.tx_type,
            from_address=self._safe_address,
            max_fee_per_gas=first_tx.max_fee_per_gas if first_tx.tx_type == TransactionType.EIP_1559 else None,
            max_priority_fee_per_gas=first_tx.max_priority_fee_per_gas
            if first_tx.tx_type == TransactionType.EIP_1559
            else None,
            gas_price=first_tx.gas_price if first_tx.tx_type == TransactionType.LEGACY else None,
        )

        # Sign via Zodiac with DELEGATECALL for MultiSend
        return await self._sign_multisend_with_zodiac(
            multisend_tx,
            web3,
            eoa_nonce,
            payload.operation,
        )

    async def _sign_multisend_with_zodiac(
        self,
        tx: UnsignedTransaction,
        web3: AsyncWeb3,
        eoa_nonce: int,
        operation: SafeOperation,
    ) -> SignedTransaction:
        """Sign a MultiSend transaction via Zodiac.

        Args:
            tx: MultiSend transaction
            web3: AsyncWeb3 instance
            eoa_nonce: EOA nonce
            operation: Operation type (DELEGATECALL for MultiSend)

        Returns:
            SignedTransaction
        """
        zodiac_address = web3.to_checksum_address(self._zodiac_roles_address)
        eoa_address = web3.to_checksum_address(self._eoa_address)

        if tx.to is None:
            raise SigningError(reason="MultiSend transaction requires 'to' address")

        target_address = web3.to_checksum_address(tx.to)

        # Build execTransactionWithRole parameters
        exec_params = (
            target_address,
            tx.value,
            tx.data,
            operation,  # DELEGATECALL for MultiSend
            self._role_key_bytes,
            True,  # shouldRevert
        )

        zodiac_contract = web3.eth.contract(
            address=zodiac_address,
            abi=ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
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
        wrapper_tx = await zodiac_contract.functions.execTransactionWithRole(*exec_params).build_transaction(
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

        # Estimate gas and set gasLimit (signer service expects gasLimit, not gas)
        estimated_gas = await self._estimate_wrapper_gas(web3, wrapper_tx, tx.gas_limit)
        wrapper_tx_dict = cast(dict[str, Any], wrapper_tx)
        del wrapper_tx_dict["gas"]
        wrapper_tx_dict["gasLimit"] = estimated_gas

        # Sign via remote service
        signed_tx_hex = await self._sign_via_service(wrapper_tx_dict)

        if not signed_tx_hex.startswith("0x"):
            signed_tx_hex = "0x" + signed_tx_hex

        tx_hash = self._compute_tx_hash(signed_tx_hex, web3)

        logger.info(f"Signed MultiSend bundle via Zodiac: hash={tx_hash[:16]}..., gasLimit={estimated_gas}")

        return SignedTransaction(
            raw_tx=signed_tx_hex,
            tx_hash=tx_hash,
            unsigned_tx=tx,
        )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "ZodiacRolesSigner",
]
