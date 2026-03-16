"""Base class for Safe signers.

This module provides the abstract base class for Safe wallet signers.
It extends the Signer ABC to add Safe-specific functionality like
bundle signing and operation type determination.

Key Features:
    - Extends Signer ABC for Safe wallet operations
    - Provides common functionality for Zodiac and Direct signers
    - Handles gas buffering for Safe overhead
    - Manages Safe nonce caching for bundles

Example:
    class MySafeSigner(SafeSigner):
        async def sign_with_web3(self, tx, web3, eoa_nonce):
            # Implementation specific signing logic
            ...

        async def sign_bundle_with_web3(self, txs, web3, eoa_nonce, chain):
            # Implementation specific bundle signing
            ...
"""

import logging
from abc import abstractmethod

from eth_account import Account
from eth_account.signers.local import LocalAccount
from web3 import AsyncWeb3
from web3.types import TxParams, Wei

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    Signer,
    SigningError,
    UnsignedTransaction,
)
from almanak.framework.execution.signer.safe.config import SafeSignerConfig
from almanak.framework.execution.signer.safe.constants import (
    SafeOperation,
    get_operation_type,
)

logger = logging.getLogger(__name__)


class SafeSigner(Signer):
    """Abstract base class for Safe signers.

    This class extends the Signer ABC to provide Safe-specific functionality.
    It serves as the base for both ZodiacSigner (production) and
    DirectSafeSigner (testing) implementations.

    SECURITY CONTRACT:
        - Private keys are NEVER logged, printed, or included in error messages
        - Private keys are NEVER exposed through any method or property
        - Wallet address is derived from key at initialization and cached

    The Safe address is the "from" address for transactions, but the EOA
    signs the wrapper transaction. This class manages:
        - Mapping between Safe and EOA addresses
        - Gas buffering for Safe execution overhead
        - Operation type determination (CALL vs DELEGATECALL)
        - Safe nonce caching for bundle execution

    Attributes:
        address: The Safe wallet address (execution source)
        eoa_address: The EOA that signs transactions for the Safe
        gas_buffer_multiplier: Multiplier for gas estimates (default 2.0)

    Example:
        class DirectSafeSigner(SafeSigner):
            async def sign_with_web3(self, tx, web3, eoa_nonce):
                # Sign via Safe.execTransaction()
                ...
    """

    def __init__(self, config: SafeSignerConfig) -> None:
        """Initialize the Safe signer.

        Args:
            config: SafeSignerConfig with wallet and signing settings

        Raises:
            SigningError: If the private key is invalid
        """
        self._config = config
        self._safe_address = config.wallet_config.safe_address
        self._eoa_address = config.wallet_config.eoa_address
        self._gas_buffer_multiplier = config.gas_buffer_multiplier

        # Initialize account from private key
        if config.mode == "zodiac":
            # Zodiac mode: key may be held by remote signer plugin or provided locally
            if config.private_key:
                try:
                    self._account: LocalAccount | None = Account.from_key(config.private_key)
                except Exception as e:
                    raise SigningError(reason=f"Invalid private key format: {type(e).__name__}") from None
            else:
                self._account = None
        else:
            try:
                self._account = Account.from_key(config.private_key)
                # Verify EOA address matches
                if self._account.address.lower() != self._eoa_address.lower():
                    raise SigningError(
                        reason=f"Private key does not match configured EOA address. "
                        f"Expected {self._eoa_address}, got {self._account.address}"
                    )
            except SigningError:
                raise
            except Exception as e:
                raise SigningError(reason=f"Invalid private key format: {type(e).__name__}") from None

        # Safe nonce cache for bundle execution
        # Cleared at the start of each bundle
        self._safe_nonce_cache: dict[str, int] = {}

        logger.info(
            f"SafeSigner initialized: mode={config.mode}, "
            f"safe={self._safe_address[:10]}..., eoa={self._eoa_address[:10]}..."
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def address(self) -> str:
        """Return the Safe wallet address.

        This is the address that appears as msg.sender to target contracts.
        Transactions are executed "from" the Safe, even though the EOA signs.

        Returns:
            Checksummed Safe wallet address
        """
        return self._safe_address

    @property
    def eoa_address(self) -> str:
        """Return the EOA address that signs for the Safe.

        This EOA:
        - Signs the wrapper transaction
        - Pays gas fees
        - Must be an owner of the Safe (for direct mode)
        - Must have a role configured (for Zodiac mode)

        Returns:
            Checksummed EOA address
        """
        return self._eoa_address

    @property
    def gas_buffer_multiplier(self) -> float:
        """Return the gas buffer multiplier.

        Safe transactions require additional gas overhead for the wrapper
        execution. This multiplier is applied to gas estimates.

        Returns:
            Gas buffer multiplier (default 2.0 = 200% buffer)
        """
        return self._gas_buffer_multiplier

    @property
    def mode(self) -> str:
        """Return the signing mode.

        Returns:
            "zodiac" for production, "direct" for testing
        """
        return self._config.mode

    # =========================================================================
    # Signer Interface Implementation
    # =========================================================================

    async def sign(
        self,
        tx: UnsignedTransaction,
        chain: str,
    ) -> SignedTransaction:
        """Sign a transaction for Safe execution.

        This method is part of the Signer interface. For Safe signers,
        it requires a Web3 instance to be set up separately, so this
        method raises an error directing callers to use sign_with_web3.

        For actual signing, use sign_with_web3() which accepts a Web3 instance.

        Args:
            tx: Unsigned transaction to sign
            chain: Chain name (e.g., "arbitrum")

        Raises:
            SigningError: Always - use sign_with_web3 instead
        """
        raise SigningError(
            reason="SafeSigner requires Web3 instance. Use sign_with_web3() or execute through ChainExecutor."
        )

    # =========================================================================
    # Safe-Specific Signing (Abstract)
    # =========================================================================

    @abstractmethod
    async def sign_with_web3(
        self,
        tx: UnsignedTransaction,
        web3: AsyncWeb3,
        eoa_nonce: int,
        pos_in_bundle: int = 0,
    ) -> SignedTransaction:
        """Sign a single transaction with Web3 instance.

        This is the main signing method for Safe signers. It wraps the
        transaction in the appropriate Safe/Zodiac structure and signs it.

        Args:
            tx: Unsigned transaction to sign
            web3: AsyncWeb3 instance for contract interactions
            eoa_nonce: EOA nonce for the wrapper transaction
            pos_in_bundle: Position in bundle (0 for single tx)

        Returns:
            SignedTransaction containing the raw signed bytes and hash

        Raises:
            SigningError: If signing fails
        """
        pass

    @abstractmethod
    async def sign_bundle_with_web3(
        self,
        txs: list[UnsignedTransaction],
        web3: AsyncWeb3,
        eoa_nonce: int,
        chain: str,
    ) -> SignedTransaction:
        """Sign multiple transactions as an atomic bundle.

        This method encodes multiple transactions into a MultiSend call
        and signs the resulting bundle for atomic execution.

        All transactions in the bundle will succeed or fail together.

        Args:
            txs: List of unsigned transactions to bundle
            web3: AsyncWeb3 instance for contract interactions
            eoa_nonce: EOA nonce for the wrapper transaction
            chain: Chain name (for MultiSend address lookup)

        Returns:
            SignedTransaction containing the atomic bundle

        Raises:
            SigningError: If signing fails
            ValueError: If txs list is empty
        """
        pass

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_operation_type(self, target: str) -> SafeOperation:
        """Determine the operation type for a target address.

        Enso delegates require DELEGATECALL to execute swaps in the
        Safe's context. All other contracts use CALL.

        Args:
            target: Target contract address

        Returns:
            SafeOperation.DELEGATE_CALL for Enso delegates, CALL otherwise
        """
        return get_operation_type(target)

    def calculate_gas_with_buffer(self, base_gas: int) -> int:
        """Apply the gas buffer to a base gas estimate.

        Safe transactions require additional gas overhead for:
        - execTransaction() signature validation
        - Storage slot reads/writes for nonce
        - Event emission

        Args:
            base_gas: Base gas estimate for the inner transaction

        Returns:
            Buffered gas estimate
        """
        # The buffer includes the base gas, so multiply by (1 + buffer)
        # For a 2.0 multiplier, this gives 3x the base gas
        return int(base_gas * (1 + self._gas_buffer_multiplier))

    # Safety margin applied to eth_estimateGas results.
    # 1.3x accounts for state changes between estimation and execution
    # (e.g., price movement affecting mint gas, MEV, front-running).
    _ESTIMATE_GAS_BUFFER = 1.3

    async def _estimate_wrapper_gas(
        self,
        web3: AsyncWeb3,
        wrapper_tx: TxParams,
        fallback_inner_gas: int,
    ) -> int:
        """Estimate gas for a Safe/Zodiac wrapper transaction.

        Uses eth_estimateGas on the fully-built wrapper transaction for an
        accurate gas limit that accounts for Safe/Zodiac overhead, MultiSend
        DELEGATECALL overhead, and the EIP-150 63/64 forwarding rule.

        Falls back to the buffer-based calculation only for transient RPC
        errors. If the estimation fails because the transaction would revert,
        the error is re-raised as SigningError so callers don't waste gas on
        guaranteed-to-fail transactions.

        Args:
            web3: AsyncWeb3 instance
            wrapper_tx: Built wrapper transaction dict (from build_transaction)
            fallback_inner_gas: Inner transaction gas limit for fallback

        Returns:
            Gas limit with safety buffer

        Raises:
            SigningError: If the transaction would revert (detected by estimateGas)
        """
        try:
            estimate_params: TxParams = {
                "from": wrapper_tx["from"],
                "to": wrapper_tx["to"],
                "data": wrapper_tx["data"],
                "value": Wei(wrapper_tx.get("value", 0)),
            }
            estimated = await web3.eth.estimate_gas(estimate_params)
            buffered = int(estimated * self._ESTIMATE_GAS_BUFFER)
            logger.debug(
                "Safe gas estimate: eth_estimateGas=%d, buffered=%d",
                estimated,
                buffered,
            )
            return buffered
        except Exception as e:
            err_str = str(e).lower()
            # If the error indicates the transaction itself would revert,
            # propagate rather than silently falling back to a heuristic.
            # This prevents wasting gas on guaranteed-to-fail transactions.
            if "revert" in err_str or "execution reverted" in err_str:
                raise SigningError(reason=f"Safe wrapper transaction would revert: {e}") from None

            # For transient errors (RPC timeout, connectivity), fall back
            # to the static buffer method.
            fallback = self.calculate_gas_with_buffer(fallback_inner_gas)
            logger.warning(
                "eth_estimateGas failed for Safe wrapper, using buffer fallback (%d): %s",
                fallback,
                e,
            )
            return fallback

    def clear_nonce_cache(self) -> None:
        """Clear the Safe nonce cache.

        Call this at the start of each bundle to ensure fresh nonce
        reads from the chain. The cache is used to track nonce increments
        within a bundle without making additional RPC calls.
        """
        self._safe_nonce_cache.clear()
        logger.debug("Cleared Safe nonce cache")

    def _get_cached_safe_nonce(
        self,
        safe_address: str,
        chain_nonce: int,
        pos_in_bundle: int,
        chain_id: int | None = None,
    ) -> int:
        """Get the Safe nonce for a transaction in a bundle.

        For the first transaction (pos=0), uses the chain nonce and caches it.
        For subsequent transactions, returns cached nonce + position.

        Args:
            safe_address: Safe address
            chain_nonce: Nonce read from chain (for pos=0)
            pos_in_bundle: Position of this transaction in bundle
            chain_id: Chain ID for multi-chain nonce isolation

        Returns:
            Safe nonce to use for this transaction
        """
        # Use chain-aware cache key to prevent nonce conflicts when same Safe
        # address exists on multiple chains (e.g., multi-chain strategies)
        if chain_id is not None:
            cache_key = f"{chain_id}:{safe_address.lower()}"
        else:
            cache_key = safe_address.lower()

        if pos_in_bundle == 0:
            # First transaction - cache the chain nonce
            self._safe_nonce_cache[cache_key] = chain_nonce
            return chain_nonce
        else:
            # Subsequent transaction - use cached base + offset
            base_nonce = self._safe_nonce_cache.get(cache_key)
            if base_nonce is None:
                # Cache miss - shouldn't happen if clear_nonce_cache was called
                logger.warning(f"Safe nonce cache miss for {safe_address} on chain {chain_id}, using chain nonce")
                self._safe_nonce_cache[cache_key] = chain_nonce
                base_nonce = chain_nonce

            return base_nonce + pos_in_bundle

    def __repr__(self) -> str:
        """Return string representation without exposing private key."""
        return (
            f"{self.__class__.__name__}(mode={self._config.mode!r}, safe={self._safe_address}, eoa={self._eoa_address})"
        )

    def __str__(self) -> str:
        """Return string representation."""
        return f"{self.__class__.__name__}({self._config.mode}, {self._safe_address[:10]}...)"


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "SafeSigner",
]
