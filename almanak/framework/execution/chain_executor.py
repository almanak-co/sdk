"""Chain Executor for per-chain transaction management.

This module provides the ChainExecutor class that manages Web3 connection,
nonce tracking, and transaction submission for a single blockchain network.
Each ChainExecutor is responsible for one chain in a multi-chain strategy.

Key Features:
    - Web3 connection management with chain-specific RPC URL
    - Isolated nonce tracking per chain
    - Transaction building, signing, and submission
    - Chain-specific gas estimation with configurable buffers
    - Async-first design for non-blocking execution

Example:
    from almanak.framework.execution.chain_executor import ChainExecutor

    # Create executor for Arbitrum
    executor = ChainExecutor(
        chain="arbitrum",
        rpc_url="https://arb1.arbitrum.io/rpc",
        private_key="0x...",
    )

    # Execute a transaction
    result = await executor.execute_transaction(unsigned_tx)

    # Get current nonce
    nonce = await executor.get_current_nonce()
"""

import asyncio
import logging
from dataclasses import dataclass, field
from decimal import Decimal

# Import SafeSigner conditionally to avoid circular imports
# Type hint only - actual import happens at runtime if needed
from typing import TYPE_CHECKING, Any, Optional

from eth_account import Account
from eth_account.signers.local import LocalAccount
from hexbytes import HexBytes
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.types import TxParams, Wei

from almanak.framework.execution.config import CHAIN_IDS, ConfigurationError
from almanak.framework.execution.interfaces import (
    ExecutionError,
    GasEstimationError,
    InsufficientFundsError,
    NonceError,
    SignedTransaction,
    SigningError,
    SubmissionError,
    SubmissionResult,
    TransactionReceipt,
    TransactionRevertedError,
    TransactionType,
    UnsignedTransaction,
)

if TYPE_CHECKING:
    from almanak.framework.execution.signer.safe import SafeSigner

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================


# Chain-specific gas buffer multipliers - imported from shared constants module
from almanak.framework.execution.gas.constants import CHAIN_GAS_BUFFERS, DEFAULT_GAS_BUFFER

GAS_BUFFER_MULTIPLIERS = CHAIN_GAS_BUFFERS

# Default gas limits for different transaction types
DEFAULT_GAS_LIMITS: dict[str, int] = {
    "transfer": 21000,
    "erc20_transfer": 65000,
    "swap": 300000,
    "supply": 350000,
    "borrow": 400000,
}


def _parse_insufficient_funds_error(error_msg: str) -> tuple[int, int]:
    """Parse 'have' and 'want' values from RPC insufficient funds error.

    Parses error messages like:
    'insufficient funds for gas * price + value: address 0x... have 123 want 456'

    Returns:
        Tuple of (available, required) in wei. Returns (0, 0) if parsing fails.
    """
    import re

    # Pattern: "have <number> want <number>"
    match = re.search(r"have\s+(\d+)\s+want\s+(\d+)", error_msg)
    if match:
        available = int(match.group(1))
        required = int(match.group(2))
        return available, required

    return 0, 0


def _format_wei_as_eth(wei: int) -> str:
    """Format wei amount as human-readable ETH string."""
    eth = Decimal(wei) / Decimal(10**18)
    return f"{eth:.6f}"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ChainExecutorConfig:
    """Configuration for a ChainExecutor.

    Attributes:
        chain: Chain name (e.g., "arbitrum")
        rpc_url: RPC endpoint URL
        private_key: Hex-encoded private key
        gas_buffer_multiplier: Multiplier for gas estimates (default chain-specific)
        max_gas_price_gwei: Maximum gas price in gwei (default 100)
        tx_timeout_seconds: Transaction confirmation timeout (default 120)
        max_retries: Maximum retry attempts for transient errors (default 3)
        base_retry_delay: Base delay for exponential backoff (default 1.0)
        max_retry_delay: Maximum delay for backoff (default 32.0)
    """

    chain: str
    rpc_url: str
    private_key: str = field(repr=False)  # Never include in repr

    gas_buffer_multiplier: float | None = None
    max_gas_price_gwei: int = 100
    tx_timeout_seconds: int = 120
    max_retries: int = 3
    base_retry_delay: float = 1.0
    max_retry_delay: float = 32.0

    def __post_init__(self) -> None:
        """Validate configuration."""
        # Validate chain
        if not self.chain:
            raise ConfigurationError(field="chain", reason="Chain cannot be empty")

        chain_lower = self.chain.lower()
        if chain_lower not in CHAIN_IDS:
            valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
            raise ConfigurationError(
                field="chain",
                reason=f"Unsupported chain '{self.chain}'. Valid chains: {valid_chains}",
            )
        self.chain = chain_lower

        # Validate RPC URL
        if not self.rpc_url:
            raise ConfigurationError(field="rpc_url", reason="RPC URL cannot be empty")

        # Validate private key
        if not self.private_key:
            raise ConfigurationError(field="private_key", reason="Private key cannot be empty")

        # Set default gas buffer multiplier based on chain
        if self.gas_buffer_multiplier is None:
            self.gas_buffer_multiplier = GAS_BUFFER_MULTIPLIERS.get(self.chain, DEFAULT_GAS_BUFFER)


@dataclass
class TransactionExecutionResult:
    """Result of executing a single transaction.

    Attributes:
        success: Whether the transaction succeeded
        tx_hash: Transaction hash
        receipt: Transaction receipt (if confirmed)
        gas_used: Actual gas consumed
        gas_cost_wei: Total gas cost in wei
        error: Error message if failed
        nonce_used: Nonce used for the transaction
    """

    success: bool
    tx_hash: str
    receipt: TransactionReceipt | None = None
    gas_used: int = 0
    gas_cost_wei: int = 0
    error: str | None = None
    nonce_used: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "tx_hash": self.tx_hash,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "gas_used": self.gas_used,
            "gas_cost_wei": str(self.gas_cost_wei),
            "error": self.error,
            "nonce_used": self.nonce_used,
        }


# =============================================================================
# Chain Executor
# =============================================================================


class ChainExecutor:
    """Manages Web3 connection, nonce tracking, and transactions for a single chain.

    The ChainExecutor provides a unified interface for executing transactions on
    a specific blockchain network. It handles:
    - Web3 connection lifecycle
    - Nonce management (with pending transaction tracking)
    - Transaction signing with local private key
    - Transaction submission and receipt polling
    - Chain-specific gas estimation with configurable buffers

    SECURITY CONTRACT:
    - Private keys are NEVER logged, printed, or included in error messages
    - Private keys are stored in memory only (consider KMS for production)
    - Wallet address is derived once at initialization and cached

    Example:
        # Create executor
        executor = ChainExecutor(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key="0x...",
        )

        # Build a transaction
        gas_params = await executor.get_gas_params()
        unsigned_tx = UnsignedTransaction(
            to="0x1234...",
            value=0,
            data="0x...",
            chain_id=executor.chain_id,
            gas_limit=100000,
            **gas_params,
        )

        # Execute (sign, submit, wait for receipt)
        result = await executor.execute_transaction(unsigned_tx)
        if result.success:
            print(f"Transaction confirmed: {result.tx_hash}")
    """

    def __init__(
        self,
        chain: str,
        rpc_url: str,
        private_key: str,
        gas_buffer_multiplier: float | None = None,
        max_gas_price_gwei: int = 100,
        tx_timeout_seconds: int = 120,
        max_retries: int = 3,
        base_retry_delay: float = 1.0,
        max_retry_delay: float = 32.0,
        safe_signer: Optional["SafeSigner"] = None,
    ) -> None:
        """Initialize the ChainExecutor.

        Args:
            chain: Chain name (e.g., "arbitrum", "optimism")
            rpc_url: RPC endpoint URL for the chain
            private_key: Hex-encoded private key for signing
            gas_buffer_multiplier: Multiplier for gas estimates (default chain-specific)
            max_gas_price_gwei: Maximum gas price in gwei (default 100)
            tx_timeout_seconds: Transaction confirmation timeout (default 120)
            max_retries: Maximum retry attempts for transient errors (default 3)
            base_retry_delay: Base delay for exponential backoff (default 1.0)
            max_retry_delay: Maximum delay for backoff (default 32.0)
            safe_signer: Optional SafeSigner for Safe wallet execution

        Raises:
            ConfigurationError: If chain or RPC URL is invalid
            SigningError: If private key is invalid
        """
        # Validate and normalize chain
        if not chain:
            raise ConfigurationError(field="chain", reason="Chain cannot be empty")

        chain_lower = chain.lower()
        if chain_lower not in CHAIN_IDS:
            valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
            raise ConfigurationError(
                field="chain",
                reason=f"Unsupported chain '{chain}'. Valid chains: {valid_chains}",
            )

        self._chain = chain_lower
        self._chain_id = CHAIN_IDS[chain_lower]
        self._rpc_url = rpc_url
        self._max_gas_price_gwei = max_gas_price_gwei
        self._tx_timeout_seconds = tx_timeout_seconds
        self._max_retries = max_retries
        self._base_retry_delay = base_retry_delay
        self._max_retry_delay = max_retry_delay

        # Set gas buffer multiplier
        if gas_buffer_multiplier is not None:
            self._gas_buffer_multiplier = gas_buffer_multiplier
        else:
            self._gas_buffer_multiplier = GAS_BUFFER_MULTIPLIERS.get(chain_lower, DEFAULT_GAS_BUFFER)

        # Initialize account from private key
        try:
            self._account: LocalAccount = Account.from_key(private_key)
            self._wallet_address: str = self._account.address
        except Exception as e:
            raise SigningError(reason=f"Invalid private key format: {type(e).__name__}") from None

        # Web3 instance (lazy initialization)
        self._web3: AsyncWeb3 | None = None

        # Nonce management
        self._local_nonce: int | None = None
        self._nonce_lock = asyncio.Lock()

        # Safe signer (optional)
        self._safe_signer: SafeSigner | None = safe_signer

        logger.info(
            f"ChainExecutor initialized: chain={self._chain}, "
            f"chain_id={self._chain_id}, wallet={self._wallet_address[:10]}..., "
            f"gas_buffer={self._gas_buffer_multiplier}x"
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def chain(self) -> str:
        """Get the chain name."""
        return self._chain

    @property
    def chain_id(self) -> int:
        """Get the numeric chain ID."""
        return self._chain_id

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def gas_buffer_multiplier(self) -> float:
        """Get the gas buffer multiplier."""
        return self._gas_buffer_multiplier

    @property
    def max_gas_price_wei(self) -> int:
        """Get maximum gas price in wei."""
        return self._max_gas_price_gwei * 10**9

    @property
    def is_safe_mode(self) -> bool:
        """Check if using Safe wallet mode."""
        return self._safe_signer is not None

    @property
    def execution_address(self) -> str:
        """Get the execution address (Safe if Safe mode, else EOA).

        This is the address that appears as msg.sender to target contracts.
        In Safe mode, this is the Safe address; in EOA mode, it's the wallet.

        Returns:
            Address that will execute transactions
        """
        if self._safe_signer is not None:
            return self._safe_signer.address
        return self._wallet_address

    @property
    def safe_signer(self) -> Optional["SafeSigner"]:
        """Get the Safe signer if configured."""
        return self._safe_signer

    # =========================================================================
    # Web3 Connection
    # =========================================================================

    async def _get_web3(self) -> AsyncWeb3:
        """Get or create the AsyncWeb3 instance.

        Returns:
            AsyncWeb3 instance connected to the RPC endpoint
        """
        if self._web3 is None:
            self._web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))
        return self._web3

    async def check_connection(self) -> bool:
        """Check if the RPC connection is working.

        Returns:
            True if connected, False otherwise
        """
        try:
            web3 = await self._get_web3()
            await web3.eth.block_number
            return True
        except Exception as e:
            logger.warning(f"Connection check failed for {self._chain}: {e}")
            return False

    async def get_block_number(self) -> int:
        """Get the current block number.

        Returns:
            Current block number
        """
        web3 = await self._get_web3()
        return await web3.eth.block_number

    # =========================================================================
    # Nonce Management
    # =========================================================================

    async def get_current_nonce(self) -> int:
        """Get the current nonce for the wallet from the network.

        This fetches the 'pending' nonce to account for unconfirmed transactions.

        Returns:
            Current nonce for the wallet
        """
        web3 = await self._get_web3()
        return await web3.eth.get_transaction_count(web3.to_checksum_address(self._wallet_address), "pending")

    async def get_next_nonce(self) -> int:
        """Get the next nonce for a new transaction.

        This method maintains a local nonce counter to handle multiple transactions
        submitted in quick succession before they're mined.

        Returns:
            Next nonce to use
        """
        async with self._nonce_lock:
            network_nonce = await self.get_current_nonce()

            if self._local_nonce is None:
                self._local_nonce = network_nonce
            else:
                # Use the higher of local and network nonce
                self._local_nonce = max(self._local_nonce, network_nonce)

            nonce_to_use = self._local_nonce
            self._local_nonce += 1

            logger.debug(
                f"Assigned nonce {nonce_to_use} on {self._chain} (network={network_nonce}, local={self._local_nonce})"
            )

            return nonce_to_use

    async def reset_nonce(self) -> int:
        """Reset the local nonce to match the network.

        Use this after a failed transaction or when nonce gets out of sync.

        Returns:
            The reset nonce value
        """
        async with self._nonce_lock:
            self._local_nonce = await self.get_current_nonce()
            logger.info(f"Reset nonce on {self._chain} to {self._local_nonce}")
            return self._local_nonce

    # =========================================================================
    # Gas Estimation
    # =========================================================================

    async def get_gas_params(self) -> dict[str, int]:
        """Get current gas parameters for EIP-1559 transactions.

        Returns:
            Dict with max_fee_per_gas, max_priority_fee_per_gas, and base_fee_per_gas
        """
        web3 = await self._get_web3()

        # Get latest block for base fee
        latest_block = await web3.eth.get_block("latest")
        base_fee = latest_block.get("baseFeePerGas", 0)
        base_fee_int = int(base_fee) if base_fee else 0

        # Get max priority fee suggestion
        try:
            max_priority_fee = int(await web3.eth.max_priority_fee)
        except Exception:
            # Fallback to 1 gwei if RPC doesn't support this
            max_priority_fee = 1_000_000_000

        # Calculate max fee (2x base fee + priority fee is common heuristic)
        max_fee = base_fee_int * 2 + max_priority_fee

        # Cap at configured maximum
        if max_fee > self.max_gas_price_wei:
            max_fee = self.max_gas_price_wei
            logger.warning(f"Capped max_fee_per_gas at {self._max_gas_price_gwei} gwei on {self._chain}")

        return {
            "max_fee_per_gas": max_fee,
            "max_priority_fee_per_gas": max_priority_fee,
            "base_fee_per_gas": base_fee_int,
        }

    async def estimate_gas(
        self,
        to: str,
        value: int = 0,
        data: str = "0x",
    ) -> int:
        """Estimate gas for a transaction.

        Args:
            to: Destination address
            value: Value to send in wei
            data: Transaction data

        Returns:
            Estimated gas with buffer applied

        Raises:
            GasEstimationError: If estimation fails
        """
        web3 = await self._get_web3()

        try:
            tx_params: TxParams = {
                "from": web3.to_checksum_address(self._wallet_address),
                "to": web3.to_checksum_address(to),
                "value": Wei(value),
                "data": HexBytes(data),
            }

            gas_estimate = await web3.eth.estimate_gas(tx_params)

            # Apply gas buffer
            buffered_gas = int(gas_estimate * self._gas_buffer_multiplier)

            logger.debug(
                f"Gas estimate on {self._chain}: {gas_estimate} -> {buffered_gas} "
                f"(buffer={self._gas_buffer_multiplier}x)"
            )

            return buffered_gas

        except Exception as e:
            error_message = str(e).lower()
            raise GasEstimationError(reason=f"Gas estimation failed on {self._chain}: {error_message}") from None

    # =========================================================================
    # Transaction Building
    # =========================================================================

    async def build_transaction(
        self,
        to: str,
        value: int = 0,
        data: str = "0x",
        gas_limit: int | None = None,
        nonce: int | None = None,
    ) -> UnsignedTransaction:
        """Build an unsigned transaction with current gas parameters.

        Args:
            to: Destination address
            value: Value to send in wei
            data: Transaction data (hex with 0x prefix)
            gas_limit: Gas limit (estimated if not provided)
            nonce: Transaction nonce (fetched if not provided)

        Returns:
            UnsignedTransaction ready for signing
        """
        # Get gas parameters
        gas_params = await self.get_gas_params()

        # Estimate gas if not provided
        if gas_limit is None:
            gas_limit = await self.estimate_gas(to, value, data)

        # Get nonce if not provided
        if nonce is None:
            nonce = await self.get_next_nonce()

        return UnsignedTransaction(
            to=to,
            value=value,
            data=data,
            chain_id=self._chain_id,
            gas_limit=gas_limit,
            nonce=nonce,
            tx_type=TransactionType.EIP_1559,
            from_address=self._wallet_address,
            max_fee_per_gas=gas_params["max_fee_per_gas"],
            max_priority_fee_per_gas=gas_params["max_priority_fee_per_gas"],
        )

    # =========================================================================
    # Transaction Signing
    # =========================================================================

    async def sign_transaction(
        self,
        tx: UnsignedTransaction,
    ) -> SignedTransaction:
        """Sign an unsigned transaction.

        Args:
            tx: Unsigned transaction to sign

        Returns:
            Signed transaction

        Raises:
            SigningError: If signing fails
        """
        # Validate transaction has nonce
        if tx.nonce is None:
            raise SigningError(reason="Transaction nonce must be set before signing")

        try:
            # Build transaction dict for signing
            tx_dict: dict[str, Any] = {
                "to": tx.to,
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
                tx_dict["gasPrice"] = tx.gas_price

            # Sign the transaction
            signed = self._account.sign_transaction(tx_dict)

            # Extract raw tx and hash - handle different eth-account versions
            # signed.rawTransaction or signed.raw_transaction depending on version
            raw_tx_attr = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
            if raw_tx_attr is None:
                raise SigningError(reason="Could not extract raw transaction from signed result")

            raw_tx = raw_tx_attr.hex() if hasattr(raw_tx_attr, "hex") else str(raw_tx_attr)
            if not raw_tx.startswith("0x"):
                raw_tx = "0x" + raw_tx

            tx_hash_attr = signed.hash
            tx_hash = tx_hash_attr.hex() if hasattr(tx_hash_attr, "hex") else str(tx_hash_attr)
            if not tx_hash.startswith("0x"):
                tx_hash = "0x" + tx_hash

            logger.debug(f"Signed transaction on {self._chain}: hash={tx_hash[:16]}..., nonce={tx.nonce}")

            return SignedTransaction(
                raw_tx=raw_tx,
                tx_hash=tx_hash,
                unsigned_tx=tx,
            )

        except SigningError:
            raise
        except Exception as e:
            raise SigningError(reason=f"Failed to sign transaction on {self._chain}: {type(e).__name__}") from None

    # =========================================================================
    # Transaction Submission
    # =========================================================================

    async def submit_transaction(
        self,
        signed_tx: SignedTransaction,
    ) -> SubmissionResult:
        """Submit a signed transaction to the network.

        Args:
            signed_tx: Signed transaction to submit

        Returns:
            SubmissionResult with tx_hash and status

        Raises:
            NonceError: If nonce is invalid
            InsufficientFundsError: If wallet lacks funds
            GasEstimationError: If gas-related error
            SubmissionError: For other submission failures
        """
        web3 = await self._get_web3()

        try:
            # Convert raw tx to bytes
            raw_tx = signed_tx.raw_tx
            if isinstance(raw_tx, str):
                raw_tx_bytes = bytes.fromhex(raw_tx[2:] if raw_tx.startswith("0x") else raw_tx)
            else:
                raw_tx_bytes = raw_tx

            # Submit transaction
            tx_hash = await web3.eth.send_raw_transaction(raw_tx_bytes)

            logger.info(f"Transaction submitted on {self._chain}: {tx_hash.hex()}")

            return SubmissionResult(
                tx_hash=tx_hash.hex(),
                submitted=True,
            )

        except Exception as e:
            error_message = str(e).lower()
            original_error = str(e)

            # Classify and handle error
            if "nonce" in error_message:
                raise NonceError(reason=original_error) from None
            elif "insufficient" in error_message or "balance" in error_message:
                # Parse actual values from RPC error for better diagnostics
                available, required = _parse_insufficient_funds_error(original_error)
                deficit = required - available if required > available else 0

                # Log with human-readable values
                logger.error(
                    f"Insufficient funds error: have {_format_wei_as_eth(available)} ETH, "
                    f"need {_format_wei_as_eth(required)} ETH, "
                    f"deficit {_format_wei_as_eth(deficit)} ETH"
                )
                raise InsufficientFundsError(
                    required=required,
                    available=available,
                    token="ETH",
                ) from None
            elif "gas" in error_message:
                raise GasEstimationError(reason=original_error) from None
            else:
                raise SubmissionError(reason=original_error) from None

    async def wait_for_receipt(
        self,
        tx_hash: str,
        timeout: float | None = None,
    ) -> TransactionReceipt:
        """Wait for a transaction to be mined and get its receipt.

        Args:
            tx_hash: Transaction hash to wait for
            timeout: Timeout in seconds (defaults to config value)

        Returns:
            TransactionReceipt with execution details

        Raises:
            TransactionRevertedError: If transaction reverted
            SubmissionError: If timeout or other error
        """
        web3 = await self._get_web3()
        timeout_to_use = timeout if timeout else self._tx_timeout_seconds

        try:
            tx_hash_bytes = HexBytes(tx_hash)
            receipt = await web3.eth.wait_for_transaction_receipt(
                tx_hash_bytes,
                timeout=timeout_to_use,
            )

            tx_receipt = TransactionReceipt(
                tx_hash=receipt["transactionHash"].hex(),
                block_number=receipt["blockNumber"],
                block_hash=receipt["blockHash"].hex(),
                gas_used=receipt["gasUsed"],
                effective_gas_price=receipt.get("effectiveGasPrice", 0),
                status=receipt["status"],
                logs=[dict(log) for log in receipt.get("logs", [])],
                contract_address=receipt.get("contractAddress"),
                from_address=receipt.get("from"),
                to_address=receipt.get("to"),
            )

            if tx_receipt.status == 0:
                logger.warning(f"Transaction reverted on {self._chain}: {tx_hash}")
                raise TransactionRevertedError(
                    tx_hash=tx_hash,
                    gas_used=tx_receipt.gas_used,
                    block_number=tx_receipt.block_number,
                )

            logger.info(
                f"Transaction confirmed on {self._chain}: {tx_hash}, "
                f"block={tx_receipt.block_number}, gas_used={tx_receipt.gas_used}"
            )

            return tx_receipt

        except TransactionRevertedError:
            raise
        except TimeoutError:
            raise SubmissionError(
                reason=f"Timeout waiting for transaction on {self._chain}",
                tx_hash=tx_hash,
                recoverable=True,
            ) from None
        except Exception as e:
            raise SubmissionError(
                reason=f"Failed to get receipt on {self._chain}: {e}",
                tx_hash=tx_hash,
                recoverable=True,
            ) from None

    # =========================================================================
    # Execute Transaction (Full Flow)
    # =========================================================================

    async def execute_transaction(
        self,
        tx: UnsignedTransaction,
        wait_for_confirmation: bool = True,
    ) -> TransactionExecutionResult:
        """Execute a complete transaction: sign, submit, and optionally wait for receipt.

        This is the main entry point for executing transactions on this chain.

        Args:
            tx: Unsigned transaction to execute
            wait_for_confirmation: Whether to wait for the transaction to be mined

        Returns:
            TransactionExecutionResult with complete execution details
        """
        # Ensure nonce is set
        if tx.nonce is None:
            tx = UnsignedTransaction(
                to=tx.to,
                value=tx.value,
                data=tx.data,
                chain_id=tx.chain_id,
                gas_limit=tx.gas_limit,
                nonce=await self.get_next_nonce(),
                tx_type=tx.tx_type,
                from_address=tx.from_address,
                max_fee_per_gas=tx.max_fee_per_gas,
                max_priority_fee_per_gas=tx.max_priority_fee_per_gas,
                gas_price=tx.gas_price,
                metadata=tx.metadata,
            )

        nonce_used = tx.nonce

        try:
            # Sign
            signed_tx = await self.sign_transaction(tx)

            # Submit
            submission_result = await self.submit_transaction(signed_tx)

            if not submission_result.submitted:
                return TransactionExecutionResult(
                    success=False,
                    tx_hash=signed_tx.tx_hash,
                    error=submission_result.error,
                    nonce_used=nonce_used,
                )

            # Wait for confirmation if requested
            if wait_for_confirmation:
                receipt = await self.wait_for_receipt(signed_tx.tx_hash)
                return TransactionExecutionResult(
                    success=receipt.success,
                    tx_hash=signed_tx.tx_hash,
                    receipt=receipt,
                    gas_used=receipt.gas_used,
                    gas_cost_wei=receipt.gas_cost_wei,
                    nonce_used=nonce_used,
                )
            else:
                return TransactionExecutionResult(
                    success=True,
                    tx_hash=signed_tx.tx_hash,
                    nonce_used=nonce_used,
                )

        except (
            SigningError,
            NonceError,
            InsufficientFundsError,
            GasEstimationError,
            TransactionRevertedError,
        ) as e:
            return TransactionExecutionResult(
                success=False,
                tx_hash="",
                error=str(e),
                nonce_used=nonce_used,
            )
        except SubmissionError as e:
            return TransactionExecutionResult(
                success=False,
                tx_hash=e.tx_hash or "",
                error=str(e),
                nonce_used=nonce_used,
            )
        except Exception as e:
            logger.exception(f"Unexpected error executing transaction on {self._chain}")
            return TransactionExecutionResult(
                success=False,
                tx_hash="",
                error=f"Unexpected error: {e}",
                nonce_used=nonce_used,
            )

    # =========================================================================
    # Safe Wallet Execution
    # =========================================================================

    async def execute_transaction_safe(
        self,
        tx: UnsignedTransaction,
        wait_for_confirmation: bool = True,
    ) -> TransactionExecutionResult:
        """Execute a transaction via Safe wallet.

        This method requires a SafeSigner to be configured. It signs the
        transaction through the Safe and submits the wrapper transaction.

        Args:
            tx: Unsigned transaction to execute through Safe
            wait_for_confirmation: Whether to wait for the transaction to be mined

        Returns:
            TransactionExecutionResult with complete execution details

        Raises:
            ExecutionError: If Safe signer is not configured
        """
        if self._safe_signer is None:
            raise ExecutionError("Safe execution requires a SafeSigner to be configured")

        try:
            web3 = await self._get_web3()
            eoa_nonce = await self.get_next_nonce()

            # Sign via Safe signer
            signed_tx = await self._safe_signer.sign_with_web3(tx, web3, eoa_nonce, pos_in_bundle=0)

            # Submit
            submission_result = await self.submit_transaction(signed_tx)

            if not submission_result.submitted:
                return TransactionExecutionResult(
                    success=False,
                    tx_hash=signed_tx.tx_hash,
                    error=submission_result.error,
                    nonce_used=eoa_nonce,
                )

            # Wait for confirmation if requested
            if wait_for_confirmation:
                receipt = await self.wait_for_receipt(signed_tx.tx_hash)
                return TransactionExecutionResult(
                    success=receipt.success,
                    tx_hash=signed_tx.tx_hash,
                    receipt=receipt,
                    gas_used=receipt.gas_used,
                    gas_cost_wei=receipt.gas_cost_wei,
                    nonce_used=eoa_nonce,
                )
            else:
                return TransactionExecutionResult(
                    success=True,
                    tx_hash=signed_tx.tx_hash,
                    nonce_used=eoa_nonce,
                )

        except (
            SigningError,
            NonceError,
            InsufficientFundsError,
            GasEstimationError,
            TransactionRevertedError,
        ) as e:
            return TransactionExecutionResult(
                success=False,
                tx_hash="",
                error=str(e),
            )
        except SubmissionError as e:
            return TransactionExecutionResult(
                success=False,
                tx_hash=e.tx_hash or "",
                error=str(e),
            )
        except Exception as e:
            logger.exception(f"Unexpected error executing Safe transaction on {self._chain}")
            return TransactionExecutionResult(
                success=False,
                tx_hash="",
                error=f"Unexpected error: {e}",
            )

    async def execute_bundle(
        self,
        txs: list[UnsignedTransaction],
        wait_for_confirmation: bool = True,
    ) -> TransactionExecutionResult:
        """Execute multiple transactions as an atomic bundle via MultiSend.

        This method bundles multiple transactions into a single atomic
        MultiSend call through the Safe. All transactions succeed or
        fail together.

        Requires a SafeSigner to be configured.

        Args:
            txs: List of unsigned transactions to bundle
            wait_for_confirmation: Whether to wait for the transaction to be mined

        Returns:
            TransactionExecutionResult with complete execution details

        Raises:
            ExecutionError: If Safe signer is not configured or txs is empty

        Example:
            # Create multiple transactions
            txs = [approve_tx, swap_tx, add_liquidity_tx]

            # Execute atomically
            result = await executor.execute_bundle(txs)
            if result.success:
                print(f"Bundle executed: {result.tx_hash}")
        """
        if self._safe_signer is None:
            raise ExecutionError("Atomic bundle execution requires Safe mode")

        if not txs:
            raise ExecutionError("Cannot execute empty transaction bundle")

        try:
            web3 = await self._get_web3()
            eoa_nonce = await self.get_next_nonce()

            # Clear Safe nonce cache for new bundle
            self._safe_signer.clear_nonce_cache()

            # Sign the bundle via MultiSend
            signed_tx = await self._safe_signer.sign_bundle_with_web3(txs, web3, eoa_nonce, self._chain)

            logger.info(f"Executing atomic bundle on {self._chain}: {len(txs)} txs, hash={signed_tx.tx_hash[:16]}...")

            # Submit
            submission_result = await self.submit_transaction(signed_tx)

            if not submission_result.submitted:
                return TransactionExecutionResult(
                    success=False,
                    tx_hash=signed_tx.tx_hash,
                    error=submission_result.error,
                    nonce_used=eoa_nonce,
                )

            # Wait for confirmation if requested
            if wait_for_confirmation:
                receipt = await self.wait_for_receipt(signed_tx.tx_hash)
                return TransactionExecutionResult(
                    success=receipt.success,
                    tx_hash=signed_tx.tx_hash,
                    receipt=receipt,
                    gas_used=receipt.gas_used,
                    gas_cost_wei=receipt.gas_cost_wei,
                    nonce_used=eoa_nonce,
                )
            else:
                return TransactionExecutionResult(
                    success=True,
                    tx_hash=signed_tx.tx_hash,
                    nonce_used=eoa_nonce,
                )

        except (
            SigningError,
            NonceError,
            InsufficientFundsError,
            GasEstimationError,
            TransactionRevertedError,
        ) as e:
            return TransactionExecutionResult(
                success=False,
                tx_hash="",
                error=str(e),
            )
        except SubmissionError as e:
            return TransactionExecutionResult(
                success=False,
                tx_hash=e.tx_hash or "",
                error=str(e),
            )
        except Exception as e:
            logger.exception(f"Unexpected error executing bundle on {self._chain}")
            return TransactionExecutionResult(
                success=False,
                tx_hash="",
                error=f"Unexpected error: {e}",
            )

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def get_balance(self, address: str | None = None) -> int:
        """Get the ETH balance of an address.

        Args:
            address: Address to check (defaults to wallet address)

        Returns:
            Balance in wei
        """
        web3 = await self._get_web3()
        addr = address or self._wallet_address
        return await web3.eth.get_balance(web3.to_checksum_address(addr))

    async def get_balance_eth(self, address: str | None = None) -> Decimal:
        """Get the ETH balance of an address in ETH.

        Args:
            address: Address to check (defaults to wallet address)

        Returns:
            Balance in ETH
        """
        balance_wei = await self.get_balance(address)
        return Decimal(balance_wei) / Decimal(10**18)

    def __repr__(self) -> str:
        """Return string representation without exposing private key."""
        return f"ChainExecutor(chain={self._chain!r}, chain_id={self._chain_id}, wallet={self._wallet_address})"

    def __str__(self) -> str:
        """Return string representation."""
        return f"ChainExecutor({self._chain}, {self._wallet_address[:10]}...)"


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "ChainExecutor",
    "ChainExecutorConfig",
    "TransactionExecutionResult",
    "GAS_BUFFER_MULTIPLIERS",
    "DEFAULT_GAS_LIMITS",
]
