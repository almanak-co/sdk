"""Public Mempool Transaction Submitter.

This module implements the Submitter ABC for submitting transactions
to the public mempool via eth_sendRawTransaction RPC call.

Key Features:
    - Async transaction submission via Web3
    - Exponential backoff with jitter for connection errors
    - Clear error handling for nonce, gas, and balance issues
    - Receipt polling with configurable timeout
    - Support for Anvil fork testing

Error Handling:
    - Connection errors: Retry with backoff (up to max_retries)
    - Nonce errors: Raise NonceError with expected/provided nonces
    - Insufficient funds: Raise InsufficientFundsError with balance info
    - Gas errors: Raise GasEstimationError or SubmissionError with details

Example:
    from almanak.framework.execution.submitter import PublicMempoolSubmitter

    submitter = PublicMempoolSubmitter(
        rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
        max_retries=3,
        timeout_seconds=120,
    )

    # Submit transactions
    results = await submitter.submit([signed_tx_1, signed_tx_2])

    # Wait for receipts
    receipts = await submitter.get_receipts([r.tx_hash for r in results if r.submitted])
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from hexbytes import HexBytes
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.exceptions import TransactionNotFound

from almanak.framework.execution.interfaces import (
    GasEstimationError,
    InsufficientFundsError,
    NonceError,
    SignedTransaction,
    SubmissionError,
    SubmissionResult,
    Submitter,
    TransactionReceipt,
    TransactionRevertedError,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Error Message Patterns
# =============================================================================


# Nonce-related error patterns
NONCE_ERROR_PATTERNS = [
    r"nonce too low",
    r"nonce too high",
    r"replacement transaction underpriced",
    r"already known",
    r"known transaction",
]

# Insufficient funds error patterns
INSUFFICIENT_FUNDS_PATTERNS = [
    r"insufficient funds",
    r"insufficient balance",
    r"not enough funds",
]

# Gas-related error patterns
GAS_ERROR_PATTERNS = [
    r"max fee per gas less than block base fee",
    r"intrinsic gas too low",
    r"gas price too low",
    r"exceeds block gas limit",
    r"gas limit too low",
]

# RPC in-flight transaction limit patterns (Alchemy delegated accounts)
INFLIGHT_LIMIT_PATTERNS = [
    r"in-flight transaction limit",
    r"gapped-nonce tx from delegated",
]

# Connection/transient error patterns (should retry)
CONNECTION_ERROR_PATTERNS = [
    r"connection refused",
    r"timeout",
    r"server error",
    r"bad gateway",
    r"service unavailable",
    r"network error",
    r"connection reset",
    r"econnrefused",
    r"econnreset",
    r"etimedout",
    r"header not found",  # RPC node sync issue, not a contract revert (VIB-525)
    r"missing trie node",  # RPC node pruning issue
]


# =============================================================================
# Revert Reason Decoding
# =============================================================================


# Standard Solidity error selectors
ERROR_SELECTORS: dict[str, str] = {
    "0x08c379a0": "Error(string)",  # Standard revert with message
    "0x4e487b71": "Panic(uint256)",  # Panic codes
}

# Panic code descriptions (from Solidity docs)
PANIC_CODES: dict[int, str] = {
    0x00: "Generic compiler panic",
    0x01: "Assert failed",
    0x11: "Arithmetic overflow/underflow",
    0x12: "Division or modulo by zero",
    0x21: "Invalid enum conversion",
    0x22: "Storage byte array encoding error",
    0x31: "pop() on empty array",
    0x32: "Array index out of bounds",
    0x41: "Memory allocation too much",
    0x51: "Called uninitialized internal function",
}

# Common custom error selectors (4 bytes)
KNOWN_CUSTOM_ERRORS: dict[str, str] = {
    # Uniswap V3
    "0x0a061d77": "InsufficientOutputAmount()",
    "0x39d35496": "InvalidPool()",
    "0x4e487b71": "Panic(uint256)",
    # ERC-20 (legacy labels, superseded by OpenZeppelin entries below where applicable)
    "0xcf479181": "InsufficientBalance()",
    # Common DEX errors
    "0x8baa579f": "InvalidSignature()",
    "0x00000000": "Unknown()",
    # PancakeSwap V3
    "0xce30421c": "TooLittleReceived()",
    "0x675cae38": "TooMuchRequested()",
    # OpenZeppelin Address library
    "0x1425ea42": "FailedInnerCall()",
    "0xd6bda275": "FailedCall()",
    "0xcd786059": "AddressInsufficientBalance(address account)",
    "0x9996b315": "AddressEmptyCode(address target)",
    # OpenZeppelin ERC-20
    "0xe450d38c": "ERC20InsufficientBalance(address sender, uint256 balance, uint256 needed)",
    "0xfb8f41b2": "ERC20InsufficientAllowance(address spender, uint256 allowance, uint256 needed)",
    "0x96c6fd1e": "ERC20InvalidSender(address sender)",
    "0xec442f05": "ERC20InvalidReceiver(address receiver)",
    # OpenZeppelin Ownable / Access
    "0x118cdaa7": "OwnableUnauthorizedAccount(address account)",
    "0x1e4fbdf7": "OwnableInvalidOwner(address owner)",
    # Aave V3 Pool custom errors
    "0x2c5211c6": "InvalidAmount()",
    "0x90cd6f24": "ReserveInactive()",
    "0xd37f5f1c": "ReservePaused()",
    "0x6d305815": "ReserveFrozen()",
    "0x77a6a896": "BorrowCapExceeded()",
    "0xf58f733a": "SupplyCapExceeded()",
    "0xcdd36a97": "CallerNotPoolAdmin()",
    "0x930bb771": "HealthFactorNotBelowThreshold()",
    "0x979b5ce8": "CollateralCannotBeLiquidated()",
    "0x3a23d825": "InsufficientCollateral()",
    "0xf0788fb2": "NoDebtOfSelectedType()",
    "0xdff88f51": "SameBlockBorrowRepay()",
    # Compound V3 Comet custom errors
    "0xe273b446": "BorrowTooSmall()",
    "0x14c5f7b6": "NotCollateralized()",
    "0x945e9268": "InsufficientReserves()",
    "0x9e87fac8": "Paused()",
    "0x82b42900": "Unauthorized()",
    "0xe7a3dfa0": "TransferInFailed()",
    "0xcefaffeb": "TransferOutFailed()",
    "0xfd1ee349": "BadPrice()",
    "0xfa6ad355": "TooMuchSlippage()",
    # General
    "0x": "EmptyRevertData()",
}


# =============================================================================
# Health Metrics
# =============================================================================


@dataclass
class SubmitterHealthMetrics:
    """Health metrics for the submitter.

    Tracks submission success rate, latency, and error counts for observability.

    Attributes:
        total_submissions: Total number of submission attempts
        successful_submissions: Number of successful submissions
        failed_submissions: Number of failed submissions
        total_latency_ms: Cumulative latency for all submissions
        connection_errors: Count of connection/transient errors
        nonce_errors: Count of nonce-related errors
        gas_errors: Count of gas-related errors
        insufficient_funds_errors: Count of insufficient funds errors
        last_error: Most recent error message
        last_error_at: Timestamp of most recent error
    """

    total_submissions: int = 0
    successful_submissions: int = 0
    failed_submissions: int = 0
    total_latency_ms: float = 0.0
    connection_errors: int = 0
    nonce_errors: int = 0
    gas_errors: int = 0
    insufficient_funds_errors: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        if self.total_submissions == 0:
            return 100.0
        return (self.successful_submissions / self.total_submissions) * 100.0

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if self.successful_submissions == 0:
            return 0.0
        return self.total_latency_ms / self.successful_submissions

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "total_submissions": self.total_submissions,
            "successful_submissions": self.successful_submissions,
            "failed_submissions": self.failed_submissions,
            "success_rate": self.success_rate,
            "average_latency_ms": self.average_latency_ms,
            "connection_errors": self.connection_errors,
            "nonce_errors": self.nonce_errors,
            "gas_errors": self.gas_errors,
            "insufficient_funds_errors": self.insufficient_funds_errors,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
        }


# =============================================================================
# Public Mempool Submitter
# =============================================================================


class PublicMempoolSubmitter(Submitter):
    """Submitter implementation for public mempool via eth_sendRawTransaction.

    This submitter broadcasts signed transactions to the blockchain network
    through the public mempool. It handles retries for transient errors and
    provides clear error handling for various failure scenarios.

    Attributes:
        rpc_url: RPC endpoint URL (credentials masked in logs)
        max_retries: Maximum retry attempts for connection errors (default 3)
        timeout_seconds: Receipt polling timeout in seconds (default 120)
        base_delay: Base delay for exponential backoff in seconds (default 1.0)
        max_delay: Maximum delay cap for backoff in seconds (default 32.0)

    SECURITY CONTRACT:
    - Never log full RPC URLs containing API keys
    - Mask sensitive parts of URLs in all log messages

    Example:
        submitter = PublicMempoolSubmitter(
            rpc_url="https://arb-mainnet.g.alchemy.com/v2/YOUR_API_KEY",
            max_retries=3,
            timeout_seconds=120,
        )

        # Submit a signed transaction
        results = await submitter.submit([signed_tx])

        # Wait for receipt
        if results[0].submitted:
            receipt = await submitter.get_receipt(results[0].tx_hash)
    """

    def __init__(
        self,
        rpc_url: str,
        max_retries: int = 3,
        timeout_seconds: float = 120.0,
        base_delay: float = 1.0,
        max_delay: float = 32.0,
    ) -> None:
        """Initialize the public mempool submitter.

        Args:
            rpc_url: RPC endpoint URL
            max_retries: Maximum retry attempts for transient errors
            timeout_seconds: Receipt polling timeout
            base_delay: Base delay for exponential backoff
            max_delay: Maximum delay cap for backoff

        Raises:
            ValueError: If rpc_url is empty or invalid
        """
        if not rpc_url:
            raise ValueError("RPC URL cannot be empty")

        self._rpc_url = rpc_url
        self._max_retries = max_retries
        self._timeout_seconds = timeout_seconds
        self._base_delay = base_delay
        self._max_delay = max_delay

        # Initialize Web3 with async provider
        self._web3: AsyncWeb3 | None = None

        # Health metrics
        self._metrics = SubmitterHealthMetrics()

        logger.info(
            f"PublicMempoolSubmitter initialized: rpc={self._mask_rpc_url(rpc_url)}, "
            f"max_retries={max_retries}, timeout={timeout_seconds}s"
        )

    def _mask_rpc_url(self, url: str) -> str:
        """Mask sensitive parts of RPC URL for safe logging.

        Hides API keys and credentials from URLs to prevent accidental exposure.

        Args:
            url: RPC URL possibly containing API keys

        Returns:
            URL with sensitive parts masked
        """
        # Mask everything after the last slash if it looks like an API key
        # Example: https://arb-mainnet.g.alchemy.com/v2/abc123 -> https://arb-mainnet.g.alchemy.com/v2/***
        if "/" in url:
            parts = url.rsplit("/", 1)
            if len(parts) == 2 and len(parts[1]) > 8:
                # Likely an API key, mask it
                return f"{parts[0]}/***"
        return url

    async def _get_web3(self) -> AsyncWeb3:
        """Get or create the AsyncWeb3 instance.

        Lazily initializes the Web3 connection to avoid blocking the constructor.

        Returns:
            AsyncWeb3 instance connected to the RPC endpoint
        """
        if self._web3 is None:
            self._web3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_url))
        return self._web3

    def _classify_error(self, error_message: str) -> str:
        """Classify an error message into a category.

        Args:
            error_message: Error message from RPC response

        Returns:
            Error category: "nonce", "insufficient_funds", "gas",
            "inflight_limit", "connection", or "unknown"
        """
        error_lower = error_message.lower()

        for pattern in NONCE_ERROR_PATTERNS:
            if re.search(pattern, error_lower):
                return "nonce"

        for pattern in INSUFFICIENT_FUNDS_PATTERNS:
            if re.search(pattern, error_lower):
                return "insufficient_funds"

        for pattern in GAS_ERROR_PATTERNS:
            if re.search(pattern, error_lower):
                return "gas"

        for pattern in INFLIGHT_LIMIT_PATTERNS:
            if re.search(pattern, error_lower):
                return "inflight_limit"

        for pattern in CONNECTION_ERROR_PATTERNS:
            if re.search(pattern, error_lower):
                return "connection"

        return "unknown"

    def _extract_nonce_info(self, error_message: str) -> tuple[int | None, int | None]:
        """Extract expected and provided nonce from error message.

        Attempts to parse nonce values from common RPC error formats.

        Args:
            error_message: Error message containing nonce information

        Returns:
            Tuple of (expected_nonce, provided_nonce), either may be None
        """
        expected = None
        provided = None

        # Try to match patterns like "expected 5, got 3" or "nonce: 3, expected: 5"
        expected_match = re.search(r"expected[:\s]+(\d+)", error_message.lower())
        if expected_match:
            expected = int(expected_match.group(1))

        provided_match = re.search(r"(?:got|provided|nonce)[:\s]+(\d+)", error_message.lower())
        if provided_match:
            provided = int(provided_match.group(1))

        return expected, provided

    def _parse_insufficient_funds_values(self, error_message: str) -> tuple[int, int]:
        """Parse 'have' and 'want' values from RPC insufficient funds error.

        Parses error messages like:
        'insufficient funds for gas * price + value: address 0x... have 123 want 456'

        Args:
            error_message: Error message from RPC response

        Returns:
            Tuple of (available, required) in wei. Returns (0, 0) if parsing fails.
        """
        # Pattern: "have <number> want <number>"
        match = re.search(r"have\s+(\d+)\s+want\s+(\d+)", error_message)
        if match:
            available = int(match.group(1))
            required = int(match.group(2))
            return available, required

        return 0, 0

    def _calculate_backoff_delay(self, attempt: int) -> float:
        """Calculate backoff delay with jitter.

        Uses exponential backoff with random jitter to prevent thundering herd.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        # Exponential backoff: base_delay * 2^attempt
        delay = self._base_delay * (2**attempt)

        # Cap at max_delay
        delay = min(delay, self._max_delay)

        # Add jitter: random value between 0 and delay
        jitter = random.uniform(0, delay * 0.5)

        return delay + jitter

    async def _submit_single(
        self,
        signed_tx: SignedTransaction,
    ) -> SubmissionResult:
        """Submit a single transaction with retry logic.

        Args:
            signed_tx: Signed transaction to submit

        Returns:
            SubmissionResult indicating success or failure
        """
        web3 = await self._get_web3()
        start_time = datetime.now(UTC)

        for attempt in range(self._max_retries + 1):
            try:
                # Convert hex string to bytes if needed
                raw_tx = signed_tx.raw_tx
                if isinstance(raw_tx, str):
                    raw_tx_bytes = bytes.fromhex(raw_tx[2:] if raw_tx.startswith("0x") else raw_tx)
                else:
                    raw_tx_bytes = raw_tx

                # Submit the transaction
                tx_hash = await web3.eth.send_raw_transaction(raw_tx_bytes)

                # Calculate latency
                latency_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000

                # Update metrics
                self._metrics.total_submissions += 1
                self._metrics.successful_submissions += 1
                self._metrics.total_latency_ms += latency_ms

                logger.info(f"Transaction submitted: tx_hash={tx_hash.hex()}, latency={latency_ms:.1f}ms")

                return SubmissionResult(
                    tx_hash=tx_hash.hex(),
                    submitted=True,
                )

            except Exception as e:
                error_message = self._get_error_message(e)
                error_category = self._classify_error(error_message)

                # Update error metrics
                self._metrics.last_error = error_message
                self._metrics.last_error_at = datetime.now(UTC)

                # Handle based on error category
                if error_category == "nonce":
                    self._metrics.nonce_errors += 1
                    expected, provided = self._extract_nonce_info(error_message)
                    logger.warning(
                        f"Nonce error submitting tx: {error_message}, expected={expected}, provided={provided}"
                    )
                    # Nonce errors are not retryable - raise immediately
                    raise NonceError(
                        reason=error_message,
                        expected=expected,
                        provided=provided,
                    ) from None

                elif error_category == "insufficient_funds":
                    self._metrics.insufficient_funds_errors += 1
                    logger.warning(f"Insufficient funds error: {error_message}")
                    # Parse have/want values from error message
                    available, required = self._parse_insufficient_funds_values(error_message)
                    # Not retryable - raise immediately
                    raise InsufficientFundsError(
                        required=required,
                        available=available,
                        token="ETH",
                    ) from None

                elif error_category == "gas":
                    self._metrics.gas_errors += 1
                    logger.warning(f"Gas error submitting tx: {error_message}")
                    # Gas errors are typically not retryable
                    raise GasEstimationError(reason=error_message) from None

                elif error_category == "inflight_limit":
                    # RPC in-flight limit (e.g. Alchemy delegated accounts).
                    # Retryable -- the prior TX may confirm and free a slot.
                    self._metrics.connection_errors += 1
                    if attempt < self._max_retries:
                        delay = self._calculate_backoff_delay(attempt)
                        logger.warning(
                            f"In-flight TX limit hit (attempt {attempt + 1}/{self._max_retries + 1}): "
                            f"{error_message}, retrying in {delay:.1f}s"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"In-flight TX limit after {self._max_retries + 1} attempts: {error_message}")
                        self._metrics.total_submissions += 1
                        self._metrics.failed_submissions += 1
                        raise SubmissionError(
                            reason=f"RPC in-flight transaction limit: {error_message}",
                            tx_hash=signed_tx.tx_hash,
                            recoverable=True,
                        ) from None

                elif error_category == "connection":
                    self._metrics.connection_errors += 1
                    if attempt < self._max_retries:
                        delay = self._calculate_backoff_delay(attempt)
                        logger.warning(
                            f"Connection error (attempt {attempt + 1}/{self._max_retries + 1}): "
                            f"{error_message}, retrying in {delay:.1f}s"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Connection error after {self._max_retries + 1} attempts: {error_message}")
                        self._metrics.total_submissions += 1
                        self._metrics.failed_submissions += 1
                        return SubmissionResult(
                            tx_hash=signed_tx.tx_hash,
                            submitted=False,
                            error=f"Connection failed after {self._max_retries + 1} attempts: {error_message}",
                        )

                else:
                    # Unknown error - don't retry
                    logger.error(f"Unknown error submitting tx: {error_message}")
                    self._metrics.total_submissions += 1
                    self._metrics.failed_submissions += 1
                    return SubmissionResult(
                        tx_hash=signed_tx.tx_hash,
                        submitted=False,
                        error=error_message,
                    )

        # Should not reach here, but handle gracefully
        self._metrics.total_submissions += 1
        self._metrics.failed_submissions += 1
        return SubmissionResult(
            tx_hash=signed_tx.tx_hash,
            submitted=False,
            error="Max retries exceeded",
        )

    def _get_error_message(self, error: Exception) -> str:
        """Extract error message from various exception types.

        Handles different RPC error formats and exception structures.

        Args:
            error: Exception to extract message from

        Returns:
            Human-readable error message
        """
        # Handle web3 RPC errors with dict structure
        if error.args and isinstance(error.args[0], dict):
            error_dict = error.args[0]
            if "message" in error_dict:
                return error_dict["message"]

        # Handle standard exception
        return str(error)

    async def submit(
        self,
        txs: list[SignedTransaction],
    ) -> list[SubmissionResult]:
        """Submit signed transactions to the public mempool.

        Submits transactions sequentially (to preserve nonce order) and
        returns results for each transaction.

        Args:
            txs: List of signed transactions to submit

        Returns:
            List of SubmissionResult in the same order as input

        Raises:
            NonceError: If a nonce error occurs (caller should handle)
            InsufficientFundsError: If wallet lacks funds
            GasEstimationError: If gas-related error occurs
        """
        if not txs:
            return []

        results: list[SubmissionResult] = []

        for tx in txs:
            result = await self._submit_single(tx)
            results.append(result)

            # Stop on critical failures that would affect subsequent transactions
            if not result.submitted:
                # For non-submitted transactions, we still need to add them to results
                # but we continue trying subsequent transactions unless it's a nonce error
                # (nonce errors would have raised an exception)
                logger.warning(f"Transaction {tx.tx_hash} failed to submit: {result.error}")

        return results

    async def submit_sequential(
        self,
        txs: list[SignedTransaction],
        receipt_timeout: float = 120.0,
    ) -> tuple[list[SubmissionResult], list[TransactionReceipt]]:
        """Submit transactions sequentially, confirming each before sending the next.

        This avoids hitting RPC in-flight transaction limits (e.g. Alchemy's
        2-TX limit for delegated accounts on Base) by ensuring only one
        transaction is in-flight at any time.

        The flow for each TX is: submit -> wait for receipt -> submit next.

        On failure, partial results (already-confirmed TXs) are attached to
        the raised exception as ``partial_results`` and ``partial_receipts``
        attributes so the caller can record them for retry safety.

        Args:
            txs: List of signed transactions to submit (order preserved).
            receipt_timeout: Seconds to wait for each receipt.

        Returns:
            Tuple of (submission_results, receipts) in the same order as input.

        Raises:
            NonceError: If a nonce error occurs.
            InsufficientFundsError: If wallet lacks funds.
            GasEstimationError: If gas-related error occurs.
            TransactionRevertedError: If a transaction reverts on-chain.
            SubmissionError: If receipt cannot be retrieved.
        """
        if not txs:
            return [], []

        submission_results: list[SubmissionResult] = []
        receipts: list[TransactionReceipt] = []

        def _attach_partial(exc: Exception) -> None:
            """Attach partial results to an exception for upstream recovery."""
            exc.partial_results = submission_results  # type: ignore[attr-defined]
            exc.partial_receipts = receipts  # type: ignore[attr-defined]

        try:
            for i, tx in enumerate(txs):
                logger.info(f"Sequential submit: TX {i + 1}/{len(txs)}")

                # Submit this TX
                result = await self._submit_single(tx)
                submission_results.append(result)

                if not result.submitted:
                    # Determine recoverability from the error message.
                    # Connection and in-flight limit errors are transient.
                    recoverable = bool(
                        result.error
                        and any(
                            indicator in result.error.lower()
                            for indicator in ("connection", "in-flight", "inflight", "timeout")
                        )
                    )
                    raise SubmissionError(
                        reason=f"Sequential TX {i + 1}/{len(txs)} failed to submit: {result.error}",
                        tx_hash=tx.tx_hash,
                        recoverable=recoverable,
                    )

                # Wait for receipt before submitting next TX
                receipt = await self.get_receipt(result.tx_hash, timeout=receipt_timeout)
                receipts.append(receipt)

                logger.info(
                    f"Sequential submit: TX {i + 1}/{len(txs)} confirmed "
                    f"(block={receipt.block_number}, gas={receipt.gas_used})"
                )

        except Exception as exc:
            _attach_partial(exc)
            raise

        return submission_results, receipts

    def _decode_revert_data(self, revert_data: str | bytes) -> str:
        """Decode revert data into a human-readable message.

        Handles standard Solidity errors, panic codes, and custom errors.

        Args:
            revert_data: Raw revert data (hex string or bytes)

        Returns:
            Human-readable revert reason
        """
        if not revert_data:
            return "Empty revert data"

        # Convert to hex string if needed
        if isinstance(revert_data, bytes):
            hex_data = "0x" + revert_data.hex()
        else:
            hex_data = revert_data if revert_data.startswith("0x") else "0x" + revert_data

        # Need at least 4 bytes for a selector
        if len(hex_data) < 10:
            return f"Invalid revert data (too short): {hex_data}"

        selector = hex_data[:10].lower()

        # Check for standard Error(string) - 0x08c379a0
        if selector == "0x08c379a0":
            try:
                # Decode ABI-encoded string
                # Format: selector (4) + offset (32) + length (32) + string data
                if len(hex_data) >= 138:  # 10 + 64 + 64
                    # Get string length from bytes 68-132 (offset 36-68 in hex chars after 0x)
                    length_hex = hex_data[74:138]
                    string_length = int(length_hex, 16)
                    # Get string data
                    string_start = 138
                    string_end = string_start + (string_length * 2)
                    if len(hex_data) >= string_end:
                        string_hex = hex_data[string_start:string_end]
                        message = bytes.fromhex(string_hex).decode("utf-8", errors="replace")
                        return f"Error: {message}"
            except (ValueError, IndexError, UnicodeDecodeError) as e:
                logger.debug(f"Failed to decode Error(string): {e}")
            return f"Error(string) - failed to decode: {hex_data[:100]}..."

        # Check for Panic(uint256) - 0x4e487b71
        if selector == "0x4e487b71":
            try:
                if len(hex_data) >= 74:  # 10 + 64
                    panic_code = int(hex_data[10:74], 16)
                    description = PANIC_CODES.get(panic_code, "Unknown panic code")
                    return f"Panic({panic_code}): {description}"
            except (ValueError, IndexError) as e:
                logger.debug(f"Failed to decode Panic: {e}")
            return f"Panic - failed to decode: {hex_data[:100]}..."

        # Check for known custom errors
        if selector in KNOWN_CUSTOM_ERRORS:
            return f"Custom error: {KNOWN_CUSTOM_ERRORS[selector]}"

        # Unknown error - return raw selector and truncated data
        return f"Unknown revert (selector={selector}): {hex_data[:100]}{'...' if len(hex_data) > 100 else ''}"

    async def _extract_revert_reason(
        self,
        tx_hash: str,
        block_number: int,
    ) -> str | None:
        """Extract revert reason by replaying the transaction with eth_call.

        When a transaction reverts, we can replay it using eth_call to get
        the actual revert reason from the contract.

        Args:
            tx_hash: Hash of the reverted transaction
            block_number: Block number where the transaction was included

        Returns:
            Decoded revert reason string, or None if extraction failed
        """
        try:
            web3 = await self._get_web3()

            # Get the original transaction data
            tx = await web3.eth.get_transaction(HexBytes(tx_hash))
            if tx is None:
                logger.debug(f"Could not fetch transaction {tx_hash} for revert reason extraction")
                return None

            # Build the call params to replay the transaction
            # Use type: ignore for web3 TxParams compatibility
            call_params = {
                "from": tx.get("from"),
                "to": tx.get("to"),
                "value": tx.get("value", 0),
                "data": tx.get("input", tx.get("data", "0x")),
            }

            # Include gas if available
            if tx.get("gas"):
                call_params["gas"] = tx.get("gas")

            # Replay at the block where it failed
            # We use block_number - 1 to get the state right before the tx was included
            # or the same block to see the exact revert
            try:
                # Try eth_call at the same block
                await web3.eth.call(call_params, block_identifier=block_number)  # type: ignore[arg-type]
                # If this succeeds, something is wrong (tx should revert)
                logger.debug(f"eth_call did not revert for tx {tx_hash}")
                return None

            except Exception as call_error:  # noqa: BLE001 - Intentionally broad: eth_call can raise various RPC/web3 exceptions when replaying reverts
                # Extract revert data from the error
                revert_data = self._extract_revert_data_from_error(call_error)
                if revert_data:
                    return self._decode_revert_data(revert_data)

                # Try to get message directly from error
                error_msg = self._get_error_message(call_error)
                if "execution reverted" in error_msg.lower():
                    # Sometimes the error message contains the revert reason
                    return error_msg

                return f"Reverted (no detailed reason): {error_msg[:200]}"

        except Exception as e:  # noqa: BLE001 - Intentionally broad: revert reason extraction is best-effort and should not fail the transaction
            logger.debug(f"Failed to extract revert reason for {tx_hash}: {e}")
            return None

    def _extract_revert_data_from_error(self, error: Exception) -> str | None:
        """Extract raw revert data from various RPC error formats.

        Different RPC providers format revert errors differently.
        This method handles common formats.

        Args:
            error: Exception from eth_call

        Returns:
            Raw revert data hex string, or None if not found
        """
        # Handle web3 RPC errors with dict structure
        if error.args and isinstance(error.args[0], dict):
            error_dict = error.args[0]

            # Alchemy/Infura format: {"code": 3, "data": "0x...", "message": "..."}
            if "data" in error_dict:
                data = error_dict["data"]
                if isinstance(data, str) and data.startswith("0x"):
                    return data

            # Some providers nest the data
            if "error" in error_dict and isinstance(error_dict["error"], dict):
                nested = error_dict["error"]
                if "data" in nested:
                    data = nested["data"]
                    if isinstance(data, str) and data.startswith("0x"):
                        return data

        # Handle ContractLogicError from web3.py
        if hasattr(error, "data"):
            data = error.data
            if isinstance(data, str | bytes):
                if isinstance(data, bytes):
                    return "0x" + data.hex()
                return data if data.startswith("0x") else "0x" + data

        # Try to extract from error message (some providers include it)
        error_str = str(error)

        # Pattern: execution reverted: 0x...
        hex_match = re.search(r"0x[0-9a-fA-F]+", error_str)
        if hex_match:
            hex_data = hex_match.group(0)
            if len(hex_data) >= 10:  # At least selector
                return hex_data

        return None

    async def get_receipt(
        self,
        tx_hash: str,
        timeout: float = 120.0,
    ) -> TransactionReceipt:
        """Wait for and retrieve a transaction receipt.

        Polls the network for the transaction receipt until it's mined
        or the timeout is reached.

        Args:
            tx_hash: Transaction hash to wait for
            timeout: Maximum seconds to wait (default 120)

        Returns:
            TransactionReceipt with execution details

        Raises:
            TransactionRevertedError: If transaction was mined but reverted
            SubmissionError: If receipt cannot be retrieved within timeout
        """
        web3 = await self._get_web3()
        timeout_to_use = timeout if timeout > 0 else self._timeout_seconds

        logger.info(f"Waiting for receipt: tx_hash={tx_hash}, timeout={timeout_to_use}s")

        try:
            # Use Web3's built-in wait_for_transaction_receipt
            # Convert tx_hash to HexBytes for proper typing
            tx_hash_bytes = HexBytes(tx_hash)
            receipt = await web3.eth.wait_for_transaction_receipt(
                tx_hash_bytes,
                timeout=timeout_to_use,
            )

            # Defensive check - receipt should never be None but handle gracefully
            if receipt is None:
                logger.error(f"Got None receipt for tx_hash={tx_hash}")
                raise SubmissionError(
                    reason=f"Received null receipt for transaction {tx_hash}",
                    tx_hash=tx_hash,
                    recoverable=True,
                )

            # Convert to our TransactionReceipt dataclass
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
                # Extract revert reason by replaying the transaction
                revert_reason = await self._extract_revert_reason(
                    tx_hash=tx_hash,
                    block_number=tx_receipt.block_number,
                )
                logger.warning(f"Transaction reverted: tx_hash={tx_hash}, reason={revert_reason or 'Unknown'}")
                raise TransactionRevertedError(
                    tx_hash=tx_hash,
                    revert_reason=revert_reason,
                    gas_used=tx_receipt.gas_used,
                    block_number=tx_receipt.block_number,
                )

            logger.info(
                f"Transaction confirmed: tx_hash={tx_hash}, "
                f"block={tx_receipt.block_number}, gas_used={tx_receipt.gas_used}"
            )
            return tx_receipt

        except TimeoutError:
            logger.error(f"Timeout waiting for receipt: tx_hash={tx_hash}")
            raise SubmissionError(
                reason=f"Timeout waiting for transaction receipt after {timeout_to_use}s",
                tx_hash=tx_hash,
                recoverable=True,
            ) from None

        except TransactionNotFound:
            logger.error(f"Transaction not found: tx_hash={tx_hash}")
            raise SubmissionError(
                reason=f"Transaction {tx_hash} not found on chain",
                tx_hash=tx_hash,
                recoverable=True,
            ) from None

        except TransactionRevertedError:
            # Re-raise our custom error
            raise

        except Exception as e:
            error_message = self._get_error_message(e)
            logger.error(f"Error getting receipt for {tx_hash}: {error_message}")
            raise SubmissionError(
                reason=f"Failed to get receipt: {error_message}",
                tx_hash=tx_hash,
                recoverable=True,
            ) from None

    async def get_receipts(
        self,
        tx_hashes: list[str],
        timeout: float = 120.0,
    ) -> list[TransactionReceipt]:
        """Wait for and retrieve multiple transaction receipts.

        Fetches receipts concurrently for better performance.

        Args:
            tx_hashes: List of transaction hashes
            timeout: Maximum seconds to wait per transaction

        Returns:
            List of TransactionReceipt in the same order as input

        Raises:
            TransactionRevertedError: If any transaction reverted
            SubmissionError: If any receipt cannot be retrieved
        """
        if not tx_hashes:
            return []

        # Fetch receipts concurrently
        tasks = [self.get_receipt(tx_hash, timeout) for tx_hash in tx_hashes]
        return await asyncio.gather(*tasks)

    @property
    def metrics(self) -> SubmitterHealthMetrics:
        """Get health metrics for the submitter.

        Returns:
            SubmitterHealthMetrics with current statistics
        """
        return self._metrics

    def __repr__(self) -> str:
        """Return string representation (safe, no credentials)."""
        return (
            f"PublicMempoolSubmitter("
            f"rpc={self._mask_rpc_url(self._rpc_url)}, "
            f"max_retries={self._max_retries}, "
            f"timeout={self._timeout_seconds}s)"
        )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "PublicMempoolSubmitter",
    "SubmitterHealthMetrics",
]
