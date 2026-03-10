"""Execution Layer Interfaces for Transaction Signing, Simulation, and Submission.

This module defines the core interfaces (ABCs) for the execution layer,
enabling multiple signing backends (local EOA, cloud KMS) and submission
methods (public mempool, Flashbots, direct RPC).

Key Components:
    - Signer: Abstract base class for transaction signing
    - Submitter: Abstract base class for transaction submission
    - Simulator: Abstract base class for transaction simulation

Design Philosophy:
    - All interfaces support async operations for non-blocking execution
    - Implementations are interchangeable via dependency injection
    - Clear separation between signing (cryptographic) and submission (network)
    - Simulation is optional but follows the same async pattern

Contract Requirements:
    - Signer: Must never log, print, or expose private keys in any way
    - Submitter: Must handle retries internally with exponential backoff
    - Simulator: Must return SimulationResult even if simulation is skipped

Example:
    from almanak.framework.execution.interfaces import Signer, Submitter, Simulator

    class LocalKeySigner(Signer):
        async def sign(
            self, tx: UnsignedTransaction, chain: str
        ) -> SignedTransaction:
            # Sign with local private key
            ...

    class PublicMempoolSubmitter(Submitter):
        async def submit(
            self, txs: list[SignedTransaction]
        ) -> list[SubmissionResult]:
            # Submit via eth_sendRawTransaction
            ...
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

# =============================================================================
# Enums
# =============================================================================


class TransactionType(StrEnum):
    """EVM transaction types."""

    LEGACY = "legacy"  # Type 0: pre-EIP-1559
    EIP_1559 = "eip1559"  # Type 2: EIP-1559 with maxFeePerGas


class Chain(StrEnum):
    """Supported blockchain networks."""

    ETHEREUM = "ethereum"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    POLYGON = "polygon"
    BASE = "base"
    AVALANCHE = "avalanche"
    BNB = "bnb"
    LINEA = "linea"
    PLASMA = "plasma"
    SONIC = "sonic"
    BLAST = "blast"
    MANTLE = "mantle"
    BERACHAIN = "berachain"
    MONAD = "monad"


# =============================================================================
# Exceptions
# =============================================================================


class ExecutionError(Exception):
    """Base exception for execution layer errors.

    All execution-related exceptions inherit from this class to allow
    broad exception handling when needed.
    """

    pass


class SigningError(ExecutionError):
    """Raised when transaction signing fails.

    This exception should be raised when:
    - Private key is invalid or missing
    - Transaction fields are malformed
    - Signing algorithm encounters an error

    Note: Never include sensitive key material in error messages.

    Attributes:
        reason: Human-readable explanation of the failure
        tx_hash: Optional hash of the transaction that failed (if available)
    """

    def __init__(
        self,
        reason: str,
        tx_hash: str | None = None,
    ) -> None:
        self.reason = reason
        self.tx_hash = tx_hash
        super().__init__(f"Signing failed: {reason}")


class SimulationError(ExecutionError):
    """Raised when transaction simulation fails.

    This is distinct from a transaction that simulates successfully
    but would revert - that returns SimulationResult with success=False.

    This exception is for infrastructure failures:
    - Simulation service unavailable
    - Network timeout
    - Invalid simulation parameters

    Attributes:
        reason: Human-readable explanation of the failure
        recoverable: Whether the error is transient and can be retried
    """

    def __init__(
        self,
        reason: str,
        recoverable: bool = True,
    ) -> None:
        self.reason = reason
        self.recoverable = recoverable
        super().__init__(f"Simulation failed: {reason}")


class SubmissionError(ExecutionError):
    """Raised when transaction submission fails.

    This covers failures to submit the transaction to the network,
    not transaction reverts (which are handled separately).

    Attributes:
        reason: Human-readable explanation of the failure
        tx_hash: Optional hash if transaction was partially submitted
        recoverable: Whether the error is transient and can be retried
    """

    def __init__(
        self,
        reason: str,
        tx_hash: str | None = None,
        recoverable: bool = True,
    ) -> None:
        self.reason = reason
        self.tx_hash = tx_hash
        self.recoverable = recoverable
        super().__init__(f"Submission failed: {reason}")


class TransactionRevertedError(ExecutionError):
    """Raised when a transaction reverts during execution.

    This is raised after submission when the transaction executes
    but reverts. The transaction was mined but failed.

    Attributes:
        tx_hash: Transaction hash of the reverted tx
        revert_reason: Decoded revert reason if available
        gas_used: Gas consumed before revert
        block_number: Block where revert occurred
    """

    def __init__(
        self,
        tx_hash: str,
        revert_reason: str | None = None,
        gas_used: int | None = None,
        block_number: int | None = None,
    ) -> None:
        self.tx_hash = tx_hash
        self.revert_reason = revert_reason
        self.gas_used = gas_used
        self.block_number = block_number
        reason_str = f": {revert_reason}" if revert_reason else ""
        super().__init__(f"Transaction {tx_hash} reverted{reason_str}")


class InsufficientFundsError(ExecutionError):
    """Raised when the wallet has insufficient funds for a transaction.

    Attributes:
        required: Amount required (in wei or smallest unit)
        available: Amount available in wallet
        token: Token symbol (ETH for gas, or ERC-20 symbol)
    """

    def __init__(
        self,
        required: int,
        available: int,
        token: str = "ETH",
    ) -> None:
        self.required = required
        self.available = available
        self.token = token
        deficit = required - available
        super().__init__(f"Insufficient {token}: need {required}, have {available} (deficit: {deficit})")


class NonceError(ExecutionError):
    """Raised when there is a nonce-related issue.

    This covers:
    - Nonce too low (transaction already mined)
    - Nonce too high (gap in nonce sequence)
    - Nonce already used (replacement underpriced)

    Attributes:
        expected: Expected nonce value (if known)
        provided: Nonce value that was provided
        reason: Specific nonce error type
    """

    def __init__(
        self,
        reason: str,
        expected: int | None = None,
        provided: int | None = None,
    ) -> None:
        self.reason = reason
        self.expected = expected
        self.provided = provided
        msg = f"Nonce error: {reason}"
        if expected is not None and provided is not None:
            msg = f"{msg} (expected {expected}, got {provided})"
        super().__init__(msg)


class GasEstimationError(ExecutionError):
    """Raised when gas estimation fails.

    This typically indicates the transaction would revert if executed.

    Attributes:
        reason: Explanation of why estimation failed
        revert_data: Raw revert data if available
    """

    def __init__(
        self,
        reason: str,
        revert_data: str | None = None,
    ) -> None:
        self.reason = reason
        self.revert_data = revert_data
        super().__init__(f"Gas estimation failed: {reason}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class UnsignedTransaction:
    """An unsigned EVM transaction ready for signing.

    This dataclass represents a transaction before it has been signed.
    All fields necessary for signing must be provided.

    Attributes:
        to: Destination address (None for contract creation)
        value: Amount of native token to send (in wei)
        data: Transaction calldata (hex string with 0x prefix)
        chain_id: Chain ID for EIP-155 replay protection
        gas_limit: Maximum gas units for the transaction
        nonce: Transaction nonce (if None, will be fetched)

        For EIP-1559 transactions:
        max_fee_per_gas: Maximum total fee per gas (in wei)
        max_priority_fee_per_gas: Maximum priority fee per gas (in wei)

        For legacy transactions:
        gas_price: Gas price (in wei)

        Metadata:
        tx_type: Transaction type (legacy or eip1559)
        from_address: Sender address (for validation only, not signing)
        metadata: Additional context (e.g., intent_id, description)

    Example:
        # EIP-1559 transaction
        tx = UnsignedTransaction(
            to="0x1234....",
            value=0,
            data="0xa9059cbb...",  # ERC-20 transfer
            chain_id=42161,  # Arbitrum
            gas_limit=100000,
            max_fee_per_gas=100_000_000,  # 0.1 gwei
            max_priority_fee_per_gas=1_000_000,  # 0.001 gwei
        )

        # Legacy transaction
        tx = UnsignedTransaction(
            to="0x1234....",
            value=1_000_000_000_000_000_000,  # 1 ETH
            data="0x",
            chain_id=1,
            gas_limit=21000,
            gas_price=30_000_000_000,  # 30 gwei
            tx_type=TransactionType.LEGACY,
        )
    """

    to: str | None  # None for contract creation
    value: int
    data: str
    chain_id: int
    gas_limit: int

    # EIP-1559 fields (Type 2)
    max_fee_per_gas: int | None = None
    max_priority_fee_per_gas: int | None = None

    # Legacy fields (Type 0)
    gas_price: int | None = None

    # Optional fields
    nonce: int | None = None
    tx_type: TransactionType = TransactionType.EIP_1559
    from_address: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate transaction fields."""
        if self.tx_type == TransactionType.EIP_1559:
            if self.max_fee_per_gas is None or self.max_priority_fee_per_gas is None:
                raise ValueError("EIP-1559 transactions require max_fee_per_gas and max_priority_fee_per_gas")
        elif self.tx_type == TransactionType.LEGACY:
            if self.gas_price is None:
                raise ValueError("Legacy transactions require gas_price")

        if self.value < 0:
            raise ValueError(f"Transaction value cannot be negative: {self.value}")

        if self.gas_limit <= 0:
            raise ValueError(f"Gas limit must be positive: {self.gas_limit}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result: dict[str, Any] = {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "chain_id": self.chain_id,
            "gas_limit": self.gas_limit,
            "tx_type": self.tx_type.value,
        }
        if self.nonce is not None:
            result["nonce"] = self.nonce
        if self.from_address:
            result["from_address"] = self.from_address
        if self.metadata:
            result["metadata"] = self.metadata

        if self.tx_type == TransactionType.EIP_1559:
            result["max_fee_per_gas"] = str(self.max_fee_per_gas)
            result["max_priority_fee_per_gas"] = str(self.max_priority_fee_per_gas)
        else:
            result["gas_price"] = str(self.gas_price)

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UnsignedTransaction":
        """Create UnsignedTransaction from dictionary."""
        tx_type = TransactionType(data.get("tx_type", "eip1559"))
        return cls(
            to=data.get("to"),
            value=int(data["value"]),
            data=data["data"],
            chain_id=data["chain_id"],
            gas_limit=data["gas_limit"],
            nonce=data.get("nonce"),
            tx_type=tx_type,
            from_address=data.get("from_address"),
            metadata=data.get("metadata", {}),
            max_fee_per_gas=int(data["max_fee_per_gas"]) if data.get("max_fee_per_gas") else None,
            max_priority_fee_per_gas=int(data["max_priority_fee_per_gas"])
            if data.get("max_priority_fee_per_gas")
            else None,
            gas_price=int(data["gas_price"]) if data.get("gas_price") else None,
        )


@dataclass
class SignedTransaction:
    """A signed EVM transaction ready for submission.

    This dataclass wraps a signed transaction with its raw bytes
    and derived transaction hash.

    Attributes:
        raw_tx: RLP-encoded signed transaction (hex string with 0x prefix)
        tx_hash: Transaction hash derived from the signed tx
        unsigned_tx: The original unsigned transaction (for reference)
        signed_at: Timestamp when the transaction was signed

    Example:
        signed = SignedTransaction(
            raw_tx="0xf86c...",
            tx_hash="0xabcd...",
            unsigned_tx=unsigned_tx,
        )
    """

    raw_tx: str
    tx_hash: str
    unsigned_tx: UnsignedTransaction
    signed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "raw_tx": self.raw_tx,
            "tx_hash": self.tx_hash,
            "unsigned_tx": self.unsigned_tx.to_dict(),
            "signed_at": self.signed_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SignedTransaction":
        """Create SignedTransaction from dictionary."""
        return cls(
            raw_tx=data["raw_tx"],
            tx_hash=data["tx_hash"],
            unsigned_tx=UnsignedTransaction.from_dict(data["unsigned_tx"]),
            signed_at=datetime.fromisoformat(data["signed_at"]),
        )


@dataclass
class SimulationResult:
    """Result of a transaction simulation.

    This dataclass represents the outcome of simulating a transaction,
    whether through a full simulation service (Tenderly) or a pass-through.

    Attributes:
        success: Whether the transaction would succeed if executed
        simulated: Whether actual simulation was performed (False for pass-through)
        gas_estimates: Estimated gas for each transaction (if simulated)
        revert_reason: Decoded revert reason if simulation failed
        warnings: Non-fatal issues detected during simulation
        state_changes: State changes that would result from execution
        logs: Event logs that would be emitted
        simulation_url: Link to simulation details (e.g., Tenderly URL)

    Example:
        # Successful simulation
        result = SimulationResult(
            success=True,
            simulated=True,
            gas_estimates=[150000, 80000],
            warnings=["High slippage detected"],
        )

        # Pass-through (no simulation)
        result = SimulationResult(
            success=True,
            simulated=False,
        )

        # Failed simulation
        result = SimulationResult(
            success=False,
            simulated=True,
            revert_reason="ERC20: transfer amount exceeds balance",
        )
    """

    success: bool
    simulated: bool = True
    gas_estimates: list[int] = field(default_factory=list)
    revert_reason: str | None = None
    warnings: list[str] = field(default_factory=list)
    state_changes: list[dict[str, Any]] = field(default_factory=list)
    logs: list[dict[str, Any]] = field(default_factory=list)
    simulation_url: str | None = None
    simulator_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "simulated": self.simulated,
            "gas_estimates": self.gas_estimates,
            "revert_reason": self.revert_reason,
            "warnings": self.warnings,
            "state_changes": self.state_changes,
            "logs": self.logs,
            "simulation_url": self.simulation_url,
            "simulator_name": self.simulator_name,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SimulationResult":
        """Create SimulationResult from dictionary."""
        return cls(
            success=data["success"],
            simulated=data.get("simulated", True),
            gas_estimates=data.get("gas_estimates", []),
            revert_reason=data.get("revert_reason"),
            warnings=data.get("warnings", []),
            state_changes=data.get("state_changes", []),
            logs=data.get("logs", []),
            simulation_url=data.get("simulation_url"),
            simulator_name=data.get("simulator_name"),
        )


@dataclass
class SubmissionResult:
    """Result of a transaction submission attempt.

    Attributes:
        tx_hash: Transaction hash
        submitted: Whether submission was successful
        error: Error message if submission failed
        submitted_at: Timestamp of submission
    """

    tx_hash: str
    submitted: bool
    error: str | None = None
    submitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tx_hash": self.tx_hash,
            "submitted": self.submitted,
            "error": self.error,
            "submitted_at": self.submitted_at.isoformat(),
        }


def _sanitize_logs(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sanitize log entries by converting HexBytes to hex strings.

    Web3.py returns logs with HexBytes objects that are not JSON serializable.
    This function recursively converts them to hex strings.
    """

    def _convert_value(val: Any) -> Any:
        # Check for HexBytes (from web3.py) - add 0x prefix for EVM convention
        if hasattr(val, "hex") and callable(val.hex):
            hex_str = val.hex()
            return hex_str if hex_str.startswith("0x") else "0x" + hex_str
        elif isinstance(val, bytes):
            return "0x" + val.hex()
        elif isinstance(val, dict):
            return {k: _convert_value(v) for k, v in val.items()}
        elif isinstance(val, list | tuple):
            return [_convert_value(item) for item in val]
        return val

    return [_convert_value(log) for log in logs]


@dataclass
class TransactionReceipt:
    """Receipt from a mined transaction.

    Attributes:
        tx_hash: Transaction hash
        block_number: Block where transaction was included
        block_hash: Hash of the block
        gas_used: Actual gas consumed
        effective_gas_price: Actual gas price paid
        status: 1 for success, 0 for revert
        logs: Event logs emitted
        contract_address: Address of created contract (if deployment)
        from_address: Sender address
        to_address: Recipient address
    """

    tx_hash: str
    block_number: int
    block_hash: str
    gas_used: int
    effective_gas_price: int
    status: int  # 1 = success, 0 = revert
    logs: list[dict[str, Any]] = field(default_factory=list)
    contract_address: str | None = None
    from_address: str | None = None
    to_address: str | None = None

    @property
    def success(self) -> bool:
        """Check if transaction succeeded."""
        return self.status == 1

    @property
    def gas_cost_wei(self) -> int:
        """Calculate total gas cost in wei."""
        return self.gas_used * self.effective_gas_price

    @property
    def gas_cost_eth(self) -> Decimal:
        """Calculate total gas cost in ETH."""
        return Decimal(self.gas_cost_wei) / Decimal(10**18)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "gas_used": self.gas_used,
            "effective_gas_price": str(self.effective_gas_price),
            "status": self.status,
            "logs": _sanitize_logs(self.logs),
            "contract_address": self.contract_address,
            "from_address": self.from_address,
            "to_address": self.to_address,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransactionReceipt":
        """Create TransactionReceipt from dictionary."""
        return cls(
            tx_hash=data["tx_hash"],
            block_number=data["block_number"],
            block_hash=data["block_hash"],
            gas_used=data["gas_used"],
            effective_gas_price=int(data["effective_gas_price"]),
            status=data["status"],
            logs=data.get("logs", []),
            contract_address=data.get("contract_address"),
            from_address=data.get("from_address"),
            to_address=data.get("to_address"),
        )


# =============================================================================
# Abstract Base Classes
# =============================================================================


class Signer(ABC):
    """Abstract base class for transaction signing.

    A Signer is responsible for cryptographically signing transactions
    using a private key. The key may be stored locally, in a hardware
    wallet, or in a cloud KMS.

    SECURITY CONTRACT:
    - NEVER log, print, or include private keys in error messages
    - NEVER expose private key bytes through any method or property
    - ALWAYS validate transaction fields before signing
    - ALWAYS derive wallet address from key at initialization

    Implementations must handle:
    - EIP-1559 (type 2) transactions
    - Legacy (type 0) transactions for chains that need it
    - Transaction field validation before signing
    - Proper RLP encoding for the transaction type

    Example:
        class LocalKeySigner(Signer):
            def __init__(self, private_key: str):
                # Validate and store key securely
                self._account = Account.from_key(private_key)

            async def sign(
                self, tx: UnsignedTransaction, chain: str
            ) -> SignedTransaction:
                # Build transaction dict
                tx_dict = {
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
                else:
                    tx_dict["gasPrice"] = tx.gas_price

                # Sign the transaction
                signed = self._account.sign_transaction(tx_dict)

                return SignedTransaction(
                    raw_tx=signed.rawTransaction.hex(),
                    tx_hash=signed.hash.hex(),
                    unsigned_tx=tx,
                )

            @property
            def address(self) -> str:
                return self._account.address
    """

    @abstractmethod
    async def sign(
        self,
        tx: UnsignedTransaction,
        chain: str,
    ) -> SignedTransaction:
        """Sign a transaction.

        This method cryptographically signs the transaction using the
        signer's private key. The implementation must validate transaction
        fields before signing.

        Args:
            tx: Unsigned transaction to sign
            chain: Chain name (e.g., "arbitrum", "ethereum") for validation

        Returns:
            SignedTransaction containing the raw signed bytes and hash

        Raises:
            SigningError: If signing fails (invalid fields, key issues)
            ValueError: If transaction fields are malformed
        """
        pass

    @property
    @abstractmethod
    def address(self) -> str:
        """Return the wallet address associated with this signer.

        The address is derived from the private key at initialization
        time and should be cached for efficiency.

        Returns:
            Checksummed Ethereum address (0x-prefixed)
        """
        pass

    async def sign_batch(
        self,
        txs: list[UnsignedTransaction],
        chain: str,
    ) -> list[SignedTransaction]:
        """Sign multiple transactions.

        Default implementation signs sequentially. Override for
        batch optimization if supported by the signing backend.

        Args:
            txs: List of unsigned transactions to sign
            chain: Chain name for validation

        Returns:
            List of signed transactions in the same order

        Raises:
            SigningError: If any signing operation fails
        """
        return [await self.sign(tx, chain) for tx in txs]


class Submitter(ABC):
    """Abstract base class for transaction submission.

    A Submitter is responsible for broadcasting signed transactions
    to the blockchain network. Different implementations support
    different submission methods (public mempool, Flashbots, etc.).

    Implementations must handle:
    - Connection errors with retry and backoff
    - Nonce errors (too low, already used)
    - Insufficient funds errors
    - Gas errors (price too low, limit too low)

    The contract for implementations:
    1. On success: Return SubmissionResult with tx_hash
    2. On connection error: Retry with backoff, then raise SubmissionError
    3. On nonce error: Raise NonceError with details
    4. On insufficient funds: Raise InsufficientFundsError with balance info
    5. On gas error: Raise GasEstimationError with details

    Example:
        class PublicMempoolSubmitter(Submitter):
            def __init__(self, rpc_url: str):
                self._rpc_url = rpc_url
                self._web3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))

            async def submit(
                self, txs: list[SignedTransaction]
            ) -> list[SubmissionResult]:
                results = []
                for tx in txs:
                    try:
                        tx_hash = await self._web3.eth.send_raw_transaction(
                            tx.raw_tx
                        )
                        results.append(SubmissionResult(
                            tx_hash=tx_hash.hex(),
                            submitted=True,
                        ))
                    except Exception as e:
                        results.append(SubmissionResult(
                            tx_hash=tx.tx_hash,
                            submitted=False,
                            error=str(e),
                        ))
                return results

            async def get_receipt(
                self, tx_hash: str, timeout: float = 120.0
            ) -> TransactionReceipt:
                receipt = await self._web3.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=timeout
                )
                return TransactionReceipt(
                    tx_hash=tx_hash,
                    block_number=receipt["blockNumber"],
                    # ... etc
                )
    """

    @abstractmethod
    async def submit(
        self,
        txs: list[SignedTransaction],
    ) -> list[SubmissionResult]:
        """Submit signed transactions to the network.

        This method broadcasts the transactions to the blockchain network.
        Implementations should handle retries internally for transient errors.

        Args:
            txs: List of signed transactions to submit

        Returns:
            List of SubmissionResult in the same order as input.
            Each result indicates success/failure for that transaction.

        Raises:
            SubmissionError: For fatal submission failures
            NonceError: If nonce is invalid
            InsufficientFundsError: If wallet lacks funds
        """
        pass

    @abstractmethod
    async def get_receipt(
        self,
        tx_hash: str,
        timeout: float = 120.0,
    ) -> TransactionReceipt:
        """Wait for and retrieve a transaction receipt.

        This method polls the network for the transaction receipt,
        waiting up to the specified timeout for the transaction to be mined.

        Args:
            tx_hash: Transaction hash to wait for
            timeout: Maximum seconds to wait (default 120)

        Returns:
            TransactionReceipt with execution details

        Raises:
            TransactionRevertedError: If transaction was mined but reverted
            SubmissionError: If receipt cannot be retrieved within timeout
        """
        pass

    async def get_receipts(
        self,
        tx_hashes: list[str],
        timeout: float = 120.0,
    ) -> list[TransactionReceipt]:
        """Wait for and retrieve multiple transaction receipts.

        Default implementation waits for receipts sequentially.
        Override for concurrent receipt polling if beneficial.

        Args:
            tx_hashes: List of transaction hashes
            timeout: Maximum seconds to wait per transaction

        Returns:
            List of TransactionReceipt in the same order as input

        Raises:
            TransactionRevertedError: If any transaction reverted
            SubmissionError: If any receipt cannot be retrieved
        """
        return [await self.get_receipt(tx_hash, timeout) for tx_hash in tx_hashes]


class Simulator(ABC):
    """Abstract base class for transaction simulation.

    A Simulator pre-validates transactions before submission by
    simulating their execution. This helps detect issues like
    reverts, insufficient gas, or unexpected state changes.

    Implementations can range from full simulation services (Tenderly)
    to pass-through implementations that skip simulation.

    The contract for implementations:
    1. Always return SimulationResult (never raise for valid input)
    2. Set simulated=False if simulation was skipped
    3. Set success=False with revert_reason if simulation shows revert
    4. Include gas_estimates if simulation provides them
    5. Include warnings for non-fatal issues (high slippage, etc.)

    State Overrides (SAFE Wallet Support):
        For SAFE wallet simulations, the simulator may need to override state
        to properly simulate transactions that will be executed by the SAFE.
        The state_overrides parameter allows setting ETH balance and storage
        for specific addresses during simulation.

        Format: {"0xAddress": {"balance": "0xHexWei"}}

        This is particularly important for SAFE wallets where:
        - The EOA signs the transaction but doesn't hold the tokens
        - The SAFE wallet holds the tokens and executes the swap
        - Simulation needs to reflect the SAFE's balances, not the EOA's

    Example:
        class DirectSimulator(Simulator):
            '''Pass-through simulator that skips actual simulation.'''

            async def simulate(
                self,
                txs: list[UnsignedTransaction],
                chain: str,
                state_overrides: Optional[dict[str, Any]] = None,
            ) -> SimulationResult:
                # No actual simulation - pass through
                return SimulationResult(
                    success=True,
                    simulated=False,
                )

        class TenderlySimulator(Simulator):
            '''Full simulation via Tenderly API.'''

            async def simulate(
                self,
                txs: list[UnsignedTransaction],
                chain: str,
                state_overrides: Optional[dict[str, Any]] = None,
            ) -> SimulationResult:
                # Call Tenderly simulation API with state overrides
                response = await self._simulate_bundle(
                    txs, chain, state_overrides=state_overrides
                )

                if response.status == "FAILED":
                    return SimulationResult(
                        success=False,
                        simulated=True,
                        revert_reason=response.error_message,
                    )

                return SimulationResult(
                    success=True,
                    simulated=True,
                    gas_estimates=response.gas_estimates,
                    state_changes=response.state_changes,
                    simulation_url=response.dashboard_url,
                )
    """

    @property
    def name(self) -> str:
        """Return the simulator name for logging/debugging.

        Returns:
            Human-readable simulator name
        """
        return self.__class__.__name__

    def supports_chain(self, chain: str) -> bool:
        """Check if this simulator supports a given chain.

        Override in implementations that have chain restrictions.
        Default implementation returns True for all chains.

        Args:
            chain: Chain name (lowercase)

        Returns:
            True if the simulator supports this chain
        """
        return True

    @abstractmethod
    async def simulate(
        self,
        txs: list[UnsignedTransaction],
        chain: str,
        state_overrides: dict[str, Any] | None = None,
    ) -> SimulationResult:
        """Simulate transaction execution.

        This method simulates the execution of one or more transactions
        without actually submitting them to the network. The simulation
        result indicates whether the transactions would succeed.

        Args:
            txs: List of unsigned transactions to simulate
            chain: Chain name (e.g., "arbitrum") for chain-specific behavior
            state_overrides: Optional state overrides for SAFE wallet simulation.
                Format: {"0xAddress": {"balance": "0xHexWei", "storage": {...}}}
                Used to set ETH balance for SAFE wallets during simulation.
                Not all simulators support this (Alchemy does not).

        Returns:
            SimulationResult indicating success/failure and details

        Raises:
            SimulationError: Only for infrastructure failures, not tx failures
        """
        pass


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Enums
    "TransactionType",
    "Chain",
    # Data classes
    "UnsignedTransaction",
    "SignedTransaction",
    "SimulationResult",
    "SubmissionResult",
    "TransactionReceipt",
    # Abstract base classes
    "Signer",
    "Submitter",
    "Simulator",
    # Exceptions
    "ExecutionError",
    "SigningError",
    "SimulationError",
    "SubmissionError",
    "TransactionRevertedError",
    "InsufficientFundsError",
    "NonceError",
    "GasEstimationError",
]
