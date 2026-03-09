"""Tests for PublicMempoolSubmitter.

This test suite covers:
- Initialization with valid/invalid RPC URLs
- Transaction submission (success and various error scenarios)
- Receipt polling with timeout handling
- Error classification and handling
- Retry logic with exponential backoff
- Health metrics tracking
"""

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.interfaces import (
    GasEstimationError,
    InsufficientFundsError,
    NonceError,
    SignedTransaction,
    SubmissionError,
    TransactionReceipt,
    TransactionRevertedError,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter, SubmitterHealthMetrics

T = TypeVar("T")


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    """Helper to run async functions in sync tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_web3() -> MagicMock:
    """Create a mock Web3 instance."""
    web3 = MagicMock()
    web3.eth = AsyncMock()
    return web3


@pytest.fixture
def sample_unsigned_tx() -> UnsignedTransaction:
    """Create a sample unsigned transaction."""
    return UnsignedTransaction(
        to="0x1234567890123456789012345678901234567890",
        value=1_000_000_000_000_000_000,  # 1 ETH
        data="0x",
        chain_id=1,
        gas_limit=21000,
        nonce=0,
        max_fee_per_gas=30_000_000_000,
        max_priority_fee_per_gas=1_000_000_000,
        tx_type=TransactionType.EIP_1559,
    )


@pytest.fixture
def sample_signed_tx(sample_unsigned_tx: UnsignedTransaction) -> SignedTransaction:
    """Create a sample signed transaction."""
    # Valid hex raw transaction (just repeated pattern for testing)
    return SignedTransaction(
        raw_tx="0x" + "ab" * 100,  # 200 hex chars
        tx_hash="0x" + "cd" * 32,  # 64 hex chars
        unsigned_tx=sample_unsigned_tx,
    )


# =============================================================================
# Initialization Tests
# =============================================================================


class TestPublicMempoolSubmitterInit:
    """Tests for PublicMempoolSubmitter initialization."""

    def test_init_with_valid_rpc_url(self) -> None:
        """Test initialization with a valid RPC URL."""
        submitter = PublicMempoolSubmitter(rpc_url="https://mainnet.infura.io/v3/YOUR_API_KEY")

        assert submitter._rpc_url == "https://mainnet.infura.io/v3/YOUR_API_KEY"
        assert submitter._max_retries == 3  # default
        assert submitter._timeout_seconds == 120.0  # default

    def test_init_with_custom_parameters(self) -> None:
        """Test initialization with custom parameters."""
        submitter = PublicMempoolSubmitter(
            rpc_url="https://mainnet.infura.io/v3/YOUR_API_KEY",
            max_retries=5,
            timeout_seconds=60.0,
            base_delay=2.0,
            max_delay=64.0,
        )

        assert submitter._max_retries == 5
        assert submitter._timeout_seconds == 60.0
        assert submitter._base_delay == 2.0
        assert submitter._max_delay == 64.0

    def test_init_with_empty_rpc_url(self) -> None:
        """Test initialization with empty RPC URL raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            PublicMempoolSubmitter(rpc_url="")

        assert "RPC URL cannot be empty" in str(exc_info.value)

    def test_rpc_url_masking(self) -> None:
        """Test that RPC URLs are properly masked in logs and repr."""
        submitter = PublicMempoolSubmitter(rpc_url="https://mainnet.infura.io/v3/abc123def456ghi789")

        masked = submitter._mask_rpc_url(submitter._rpc_url)
        assert "abc123def456ghi789" not in masked
        assert "***" in masked
        assert "mainnet.infura.io" in masked

    def test_repr_does_not_expose_api_key(self) -> None:
        """Test that repr doesn't expose API keys."""
        submitter = PublicMempoolSubmitter(rpc_url="https://mainnet.infura.io/v3/super_secret_api_key")

        repr_str = repr(submitter)
        assert "super_secret_api_key" not in repr_str
        assert "***" in repr_str


# =============================================================================
# Error Classification Tests
# =============================================================================


class TestErrorClassification:
    """Tests for error classification."""

    def test_classify_nonce_too_low(self) -> None:
        """Test classification of nonce too low error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("nonce too low") == "nonce"

    def test_classify_nonce_too_high(self) -> None:
        """Test classification of nonce too high error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("nonce too high") == "nonce"

    def test_classify_replacement_underpriced(self) -> None:
        """Test classification of replacement underpriced error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("replacement transaction underpriced") == "nonce"

    def test_classify_already_known(self) -> None:
        """Test classification of already known error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("already known") == "nonce"

    def test_classify_insufficient_funds(self) -> None:
        """Test classification of insufficient funds error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("insufficient funds for gas") == "insufficient_funds"

    def test_classify_gas_price_too_low(self) -> None:
        """Test classification of gas price too low error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("max fee per gas less than block base fee") == "gas"

    def test_classify_intrinsic_gas_too_low(self) -> None:
        """Test classification of intrinsic gas too low error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("intrinsic gas too low") == "gas"

    def test_classify_connection_error(self) -> None:
        """Test classification of connection error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("connection refused") == "connection"

    def test_classify_timeout_error(self) -> None:
        """Test classification of timeout error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("timeout") == "connection"

    def test_classify_unknown_error(self) -> None:
        """Test classification of unknown error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        assert submitter._classify_error("something totally unexpected") == "unknown"


# =============================================================================
# Nonce Info Extraction Tests
# =============================================================================


class TestNonceInfoExtraction:
    """Tests for nonce information extraction from error messages."""

    def test_extract_expected_nonce(self) -> None:
        """Test extraction of expected nonce."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        expected, provided = submitter._extract_nonce_info("nonce too low: expected 5, got 3")

        assert expected == 5
        assert provided == 3

    def test_extract_nonce_different_format(self) -> None:
        """Test extraction with different error format."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        expected, provided = submitter._extract_nonce_info("nonce: 3, expected: 5")

        assert expected == 5
        assert provided == 3

    def test_extract_nonce_no_info(self) -> None:
        """Test extraction when no nonce info present."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        expected, provided = submitter._extract_nonce_info("nonce too low")

        assert expected is None
        assert provided is None


# =============================================================================
# Backoff Calculation Tests
# =============================================================================


class TestBackoffCalculation:
    """Tests for exponential backoff calculation."""

    def test_backoff_increases_exponentially(self) -> None:
        """Test that backoff increases with each attempt."""
        submitter = PublicMempoolSubmitter(
            rpc_url="https://example.com",
            base_delay=1.0,
            max_delay=32.0,
        )

        # Get multiple delays (without jitter for testing)
        delays: list[float] = []
        for attempt in range(5):
            # Run multiple times to get average (jitter makes it non-deterministic)
            total: float = 0.0
            for _ in range(100):
                total += submitter._calculate_backoff_delay(attempt)
            delays.append(total / 100)

        # Each delay should be roughly double the previous (accounting for jitter)
        assert delays[1] > delays[0]
        assert delays[2] > delays[1]
        assert delays[3] > delays[2]

    def test_backoff_capped_at_max_delay(self) -> None:
        """Test that backoff is capped at max_delay."""
        submitter = PublicMempoolSubmitter(
            rpc_url="https://example.com",
            base_delay=1.0,
            max_delay=8.0,
        )

        # Very high attempt number should still be capped
        delay = submitter._calculate_backoff_delay(10)

        # With max_delay=8 and up to 50% jitter, max is 12
        assert delay <= 12.0


# =============================================================================
# Submission Tests
# =============================================================================


class TestSubmission:
    """Tests for transaction submission."""

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_success(self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction) -> None:
        """Test successful transaction submission."""
        # Setup mock
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(return_value=bytes.fromhex("abcdef1234567890" * 4))
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        results = run_async(submitter.submit([sample_signed_tx]))

        assert len(results) == 1
        assert results[0].submitted is True
        assert results[0].tx_hash is not None
        assert submitter._metrics.successful_submissions == 1

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_multiple_transactions(
        self, mock_web3_class: MagicMock, sample_unsigned_tx: UnsignedTransaction
    ) -> None:
        """Test submitting multiple transactions."""
        # Setup mock
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        counter = [0]

        async def mock_send(*args: Any, **kwargs: Any) -> bytes:
            counter[0] += 1
            return bytes.fromhex(f"{'ab' * 31}{counter[0]:02x}")

        mock_web3.eth.send_raw_transaction = mock_send
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        # Create multiple signed transactions
        signed_txs = []
        for i in range(3):
            unsigned = UnsignedTransaction(
                to=sample_unsigned_tx.to,
                value=sample_unsigned_tx.value,
                data=sample_unsigned_tx.data,
                chain_id=sample_unsigned_tx.chain_id,
                gas_limit=sample_unsigned_tx.gas_limit,
                nonce=i,
                max_fee_per_gas=sample_unsigned_tx.max_fee_per_gas,
                max_priority_fee_per_gas=sample_unsigned_tx.max_priority_fee_per_gas,
            )
            signed_txs.append(
                SignedTransaction(
                    raw_tx=f"0x{'ab' * 50}",
                    tx_hash=f"0x{'cd' * 31}{i:02x}",
                    unsigned_tx=unsigned,
                )
            )

        results = run_async(submitter.submit(signed_txs))

        assert len(results) == 3
        assert all(r.submitted for r in results)
        assert submitter._metrics.successful_submissions == 3

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_empty_list(self, mock_web3_class: MagicMock) -> None:
        """Test submitting empty list returns empty list."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")

        results = run_async(submitter.submit([]))

        assert results == []

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_nonce_error_raises(self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction) -> None:
        """Test that nonce errors raise NonceError."""
        # Setup mock to raise nonce error
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(
            side_effect=Exception({"message": "nonce too low: expected 5, got 3"})
        )
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        with pytest.raises(NonceError) as exc_info:
            run_async(submitter.submit([sample_signed_tx]))

        assert exc_info.value.expected == 5
        assert exc_info.value.provided == 3
        assert submitter._metrics.nonce_errors == 1

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_insufficient_funds_raises(
        self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction
    ) -> None:
        """Test that insufficient funds errors raise InsufficientFundsError."""
        # Setup mock to raise insufficient funds error
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(side_effect=Exception("insufficient funds for gas"))
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        with pytest.raises(InsufficientFundsError):
            run_async(submitter.submit([sample_signed_tx]))

        assert submitter._metrics.insufficient_funds_errors == 1

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_gas_error_raises(self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction) -> None:
        """Test that gas errors raise GasEstimationError."""
        # Setup mock to raise gas error
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(
            side_effect=Exception("max fee per gas less than block base fee")
        )
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        with pytest.raises(GasEstimationError):
            run_async(submitter.submit([sample_signed_tx]))

        assert submitter._metrics.gas_errors == 1

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_connection_error_retries(
        self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction
    ) -> None:
        """Test that connection errors are retried."""
        # Setup mock to fail twice then succeed
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        call_count = [0]

        async def mock_send(*args: Any, **kwargs: Any) -> bytes:
            call_count[0] += 1
            if call_count[0] <= 2:
                raise Exception("connection refused")
            return bytes.fromhex("abcdef1234567890" * 4)

        mock_web3.eth.send_raw_transaction = mock_send
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(
            rpc_url="https://example.com",
            max_retries=3,
            base_delay=0.01,  # Fast for testing
        )
        submitter._web3 = mock_web3

        results = run_async(submitter.submit([sample_signed_tx]))

        assert len(results) == 1
        assert results[0].submitted is True
        assert call_count[0] == 3  # 2 failures + 1 success
        assert submitter._metrics.connection_errors == 2

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_connection_error_max_retries_exceeded(
        self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction
    ) -> None:
        """Test that connection errors fail after max retries."""
        # Setup mock to always fail
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(side_effect=Exception("connection refused"))
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(
            rpc_url="https://example.com",
            max_retries=2,
            base_delay=0.01,
        )
        submitter._web3 = mock_web3

        results = run_async(submitter.submit([sample_signed_tx]))

        assert len(results) == 1
        assert results[0].submitted is False
        assert "Connection failed after" in (results[0].error or "")
        assert submitter._metrics.connection_errors == 3  # Initial + 2 retries

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_submit_unknown_error_returns_failure(
        self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction
    ) -> None:
        """Test that unknown errors return failure result."""
        # Setup mock to raise unknown error
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(side_effect=Exception("something unexpected happened"))
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        results = run_async(submitter.submit([sample_signed_tx]))

        assert len(results) == 1
        assert results[0].submitted is False
        assert "something unexpected happened" in (results[0].error or "")


# =============================================================================
# Receipt Polling Tests
# =============================================================================


class TestReceiptPolling:
    """Tests for receipt polling."""

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_get_receipt_success(self, mock_web3_class: MagicMock) -> None:
        """Test successful receipt retrieval."""
        # Setup mock receipt
        mock_receipt = {
            "transactionHash": bytes.fromhex("abcdef1234567890" * 4),
            "blockNumber": 12345,
            "blockHash": bytes.fromhex("1234567890abcdef" * 4),
            "gasUsed": 21000,
            "effectiveGasPrice": 30_000_000_000,
            "status": 1,
            "logs": [],
            "contractAddress": None,
            "from": "0x1234567890123456789012345678901234567890",
            "to": "0x0987654321098765432109876543210987654321",
        }

        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value=mock_receipt)
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        receipt = run_async(
            submitter.get_receipt("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab")
        )

        assert isinstance(receipt, TransactionReceipt)
        assert receipt.block_number == 12345
        assert receipt.gas_used == 21000
        assert receipt.status == 1
        assert receipt.success is True

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_get_receipt_reverted_raises(self, mock_web3_class: MagicMock) -> None:
        """Test that reverted transactions raise TransactionRevertedError."""
        # Setup mock receipt with status 0 (reverted)
        mock_receipt = {
            "transactionHash": bytes.fromhex("abcdef1234567890" * 4),
            "blockNumber": 12345,
            "blockHash": bytes.fromhex("1234567890abcdef" * 4),
            "gasUsed": 50000,
            "effectiveGasPrice": 30_000_000_000,
            "status": 0,  # Reverted
            "logs": [],
            "contractAddress": None,
            "from": "0x1234567890123456789012345678901234567890",
            "to": "0x0987654321098765432109876543210987654321",
        }

        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(return_value=mock_receipt)
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        with pytest.raises(TransactionRevertedError) as exc_info:
            run_async(submitter.get_receipt("0xabcdef"))

        assert exc_info.value.gas_used == 50000
        assert exc_info.value.block_number == 12345

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_get_receipt_timeout_raises(self, mock_web3_class: MagicMock) -> None:
        """Test that timeout raises SubmissionError."""
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.wait_for_transaction_receipt = AsyncMock(side_effect=TimeoutError())
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        with pytest.raises(SubmissionError) as exc_info:
            run_async(submitter.get_receipt("0xabcdef", timeout=1.0))

        assert "Timeout" in str(exc_info.value)
        assert exc_info.value.recoverable is True

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_get_receipts_concurrent(self, mock_web3_class: MagicMock) -> None:
        """Test that get_receipts fetches concurrently."""
        call_count = [0]

        async def mock_wait(*args: Any, **kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            await asyncio.sleep(0.01)  # Small delay
            return {
                "transactionHash": bytes.fromhex("abcdef1234567890" * 4),
                "blockNumber": 12345 + call_count[0],
                "blockHash": bytes.fromhex("1234567890abcdef" * 4),
                "gasUsed": 21000,
                "effectiveGasPrice": 30_000_000_000,
                "status": 1,
                "logs": [],
            }

        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.wait_for_transaction_receipt = mock_wait
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        tx_hashes = [f"0x{'ab' * 31}{i:02x}" for i in range(3)]
        receipts = run_async(submitter.get_receipts(tx_hashes))

        assert len(receipts) == 3
        # All receipts should be retrieved
        assert all(isinstance(r, TransactionReceipt) for r in receipts)


# =============================================================================
# Health Metrics Tests
# =============================================================================


class TestHealthMetrics:
    """Tests for health metrics tracking."""

    def test_initial_metrics(self) -> None:
        """Test initial metrics values."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        metrics = submitter.metrics

        assert metrics.total_submissions == 0
        assert metrics.successful_submissions == 0
        assert metrics.failed_submissions == 0
        assert metrics.success_rate == 100.0  # No failures
        assert metrics.average_latency_ms == 0.0

    def test_metrics_success_rate_calculation(self) -> None:
        """Test success rate calculation."""
        metrics = SubmitterHealthMetrics(
            total_submissions=10,
            successful_submissions=8,
            failed_submissions=2,
        )

        assert metrics.success_rate == 80.0

    def test_metrics_average_latency_calculation(self) -> None:
        """Test average latency calculation."""
        metrics = SubmitterHealthMetrics(
            successful_submissions=5,
            total_latency_ms=500.0,
        )

        assert metrics.average_latency_ms == 100.0

    def test_metrics_to_dict(self) -> None:
        """Test metrics serialization."""
        metrics = SubmitterHealthMetrics(
            total_submissions=10,
            successful_submissions=8,
            failed_submissions=2,
            total_latency_ms=1000.0,
            connection_errors=1,
            nonce_errors=1,
        )

        metrics_dict = metrics.to_dict()

        assert metrics_dict["total_submissions"] == 10
        assert metrics_dict["successful_submissions"] == 8
        assert metrics_dict["success_rate"] == 80.0
        assert metrics_dict["average_latency_ms"] == 125.0
        assert metrics_dict["connection_errors"] == 1
        assert metrics_dict["nonce_errors"] == 1

    @patch("src.execution.submitter.public.AsyncWeb3")
    def test_metrics_updated_on_submission(
        self, mock_web3_class: MagicMock, sample_signed_tx: SignedTransaction
    ) -> None:
        """Test that metrics are updated after submission."""
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(return_value=bytes.fromhex("abcdef1234567890" * 4))
        mock_web3_class.return_value = mock_web3

        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")
        submitter._web3 = mock_web3

        # Submit transaction
        run_async(submitter.submit([sample_signed_tx]))

        # Check metrics updated
        metrics = submitter.metrics
        assert metrics.total_submissions == 1
        assert metrics.successful_submissions == 1
        assert metrics.total_latency_ms > 0


# =============================================================================
# Error Message Extraction Tests
# =============================================================================


class TestErrorMessageExtraction:
    """Tests for error message extraction."""

    def test_extract_from_dict_error(self) -> None:
        """Test extraction from dict-style error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")

        error = Exception({"code": -32000, "message": "nonce too low"})
        message = submitter._get_error_message(error)

        assert message == "nonce too low"

    def test_extract_from_string_error(self) -> None:
        """Test extraction from string error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")

        error = Exception("something went wrong")
        message = submitter._get_error_message(error)

        assert message == "something went wrong"

    def test_extract_from_empty_dict(self) -> None:
        """Test extraction from empty dict error."""
        submitter = PublicMempoolSubmitter(rpc_url="https://example.com")

        error = Exception({})
        message = submitter._get_error_message(error)

        # Should fall back to str representation
        assert "{}" in message
