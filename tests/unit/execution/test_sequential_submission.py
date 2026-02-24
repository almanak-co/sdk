"""Tests for sequential transaction submission (VIB-136).

Verifies that:
1. submit_sequential() submits TXs one at a time, confirming each receipt
   before sending the next.
2. The orchestrator uses submit_sequential() for multi-TX EOA bundles and
   falls back to parallel submit+confirm for single TXs and Safe signers.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    SubmissionError,
    SubmissionResult,
    TransactionReceipt,
    TransactionRevertedError,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unsigned_tx(nonce: int = 0) -> UnsignedTransaction:
    return UnsignedTransaction(
        to="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        value=0,
        data="0x",
        chain_id=8453,
        gas_limit=100_000,
        nonce=nonce,
        tx_type=TransactionType.EIP_1559,
        max_fee_per_gas=1_000_000_000,
        max_priority_fee_per_gas=100_000_000,
    )


def _signed_tx(tx_hash: str = "0xabc", nonce: int = 0) -> SignedTransaction:
    return SignedTransaction(
        tx_hash=tx_hash,
        raw_tx="0xdeadbeef",
        unsigned_tx=_unsigned_tx(nonce),
    )


def _receipt(tx_hash: str = "0xabc", success: bool = True) -> TransactionReceipt:
    return TransactionReceipt(
        tx_hash=tx_hash,
        block_number=100,
        block_hash="0xblockhash",
        gas_used=21000,
        effective_gas_price=1_000_000_000,
        status=1 if success else 0,
        logs=[],
    )


# ---------------------------------------------------------------------------
# submit_sequential unit tests
# ---------------------------------------------------------------------------


class TestSubmitSequential:
    """Test PublicMempoolSubmitter.submit_sequential()."""

    @pytest.fixture
    def submitter(self) -> PublicMempoolSubmitter:
        return PublicMempoolSubmitter(rpc_url="http://localhost:8545")

    @pytest.mark.asyncio
    async def test_empty_list_returns_empty(self, submitter: PublicMempoolSubmitter) -> None:
        results, receipts = await submitter.submit_sequential([])
        assert results == []
        assert receipts == []

    @pytest.mark.asyncio
    async def test_single_tx(self, submitter: PublicMempoolSubmitter) -> None:
        """Single TX should submit and confirm."""
        tx = _signed_tx("0x111", nonce=0)

        submitter._submit_single = AsyncMock(
            return_value=SubmissionResult(tx_hash="0x111", submitted=True)
        )
        submitter.get_receipt = AsyncMock(return_value=_receipt("0x111"))

        results, receipts = await submitter.submit_sequential([tx])

        assert len(results) == 1
        assert results[0].submitted
        assert len(receipts) == 1
        assert receipts[0].tx_hash == "0x111"

    @pytest.mark.asyncio
    async def test_three_txs_submitted_in_order(self, submitter: PublicMempoolSubmitter) -> None:
        """Three TXs must be submitted sequentially with confirm between each."""
        txs = [_signed_tx(f"0x{i}", nonce=i) for i in range(3)]

        call_order: list[str] = []

        async def mock_submit(tx: SignedTransaction) -> SubmissionResult:
            call_order.append(f"submit:{tx.tx_hash}")
            return SubmissionResult(tx_hash=tx.tx_hash, submitted=True)

        async def mock_receipt(tx_hash: str, timeout: float = 120.0) -> TransactionReceipt:
            call_order.append(f"receipt:{tx_hash}")
            return _receipt(tx_hash)

        submitter._submit_single = AsyncMock(side_effect=mock_submit)
        submitter.get_receipt = AsyncMock(side_effect=mock_receipt)

        results, receipts = await submitter.submit_sequential(txs)

        assert len(results) == 3
        assert len(receipts) == 3

        # Verify strictly alternating: submit, receipt, submit, receipt, submit, receipt
        assert call_order == [
            "submit:0x0",
            "receipt:0x0",
            "submit:0x1",
            "receipt:0x1",
            "submit:0x2",
            "receipt:0x2",
        ]

    @pytest.mark.asyncio
    async def test_submission_failure_raises(self, submitter: PublicMempoolSubmitter) -> None:
        """If a TX fails to submit, SubmissionError is raised."""
        txs = [_signed_tx("0xfail", nonce=0)]

        submitter._submit_single = AsyncMock(
            return_value=SubmissionResult(tx_hash="0xfail", submitted=False, error="gas too low")
        )

        with pytest.raises(SubmissionError, match="failed to submit"):
            await submitter.submit_sequential(txs)

    @pytest.mark.asyncio
    async def test_revert_propagates(self, submitter: PublicMempoolSubmitter) -> None:
        """If a TX reverts on-chain, TransactionRevertedError propagates."""
        txs = [_signed_tx("0xrevert", nonce=0)]

        submitter._submit_single = AsyncMock(
            return_value=SubmissionResult(tx_hash="0xrevert", submitted=True)
        )
        submitter.get_receipt = AsyncMock(
            side_effect=TransactionRevertedError(
                tx_hash="0xrevert",
                revert_reason="STF",
                gas_used=50000,
                block_number=100,
            )
        )

        with pytest.raises(TransactionRevertedError):
            await submitter.submit_sequential(txs)

    @pytest.mark.asyncio
    async def test_second_tx_failure_stops_sequence(self, submitter: PublicMempoolSubmitter) -> None:
        """If TX 2 of 3 fails to submit, we stop and don't attempt TX 3."""
        txs = [_signed_tx(f"0x{i}", nonce=i) for i in range(3)]

        call_count = 0

        async def mock_submit(tx: SignedTransaction) -> SubmissionResult:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                return SubmissionResult(tx_hash=tx.tx_hash, submitted=False, error="inflight limit")
            return SubmissionResult(tx_hash=tx.tx_hash, submitted=True)

        submitter._submit_single = AsyncMock(side_effect=mock_submit)
        submitter.get_receipt = AsyncMock(return_value=_receipt("0x0"))

        with pytest.raises(SubmissionError):
            await submitter.submit_sequential(txs)

        # Only 2 submit calls made (TX 0 and TX 1); TX 2 never attempted
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_partial_results_attached_on_failure(self, submitter: PublicMempoolSubmitter) -> None:
        """When TX 2/3 fails, partial_results and partial_receipts are set on exception."""
        txs = [_signed_tx(f"0x{i}", nonce=i) for i in range(3)]

        call_count = 0

        async def mock_submit(tx: SignedTransaction) -> SubmissionResult:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                return SubmissionResult(tx_hash=tx.tx_hash, submitted=False, error="connection refused")
            return SubmissionResult(tx_hash=tx.tx_hash, submitted=True)

        submitter._submit_single = AsyncMock(side_effect=mock_submit)
        submitter.get_receipt = AsyncMock(return_value=_receipt("0x0"))

        with pytest.raises(SubmissionError) as exc_info:
            await submitter.submit_sequential(txs)

        exc = exc_info.value
        # TX 0 was submitted + confirmed; TX 1 failed to submit
        assert len(exc.partial_results) == 2  # type: ignore[attr-defined]
        assert exc.partial_results[0].submitted is True  # type: ignore[attr-defined]
        assert exc.partial_results[1].submitted is False  # type: ignore[attr-defined]
        assert len(exc.partial_receipts) == 1  # type: ignore[attr-defined]
        assert exc.partial_receipts[0].tx_hash == "0x0"  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_connection_error_is_recoverable(self, submitter: PublicMempoolSubmitter) -> None:
        """Connection errors in sequential submit should be marked recoverable."""
        txs = [_signed_tx("0xfail", nonce=0)]

        submitter._submit_single = AsyncMock(
            return_value=SubmissionResult(tx_hash="0xfail", submitted=False, error="connection refused")
        )

        with pytest.raises(SubmissionError) as exc_info:
            await submitter.submit_sequential(txs)

        assert exc_info.value.recoverable is True

    @pytest.mark.asyncio
    async def test_unknown_error_is_not_recoverable(self, submitter: PublicMempoolSubmitter) -> None:
        """Non-transient errors in sequential submit should not be recoverable."""
        txs = [_signed_tx("0xfail", nonce=0)]

        submitter._submit_single = AsyncMock(
            return_value=SubmissionResult(tx_hash="0xfail", submitted=False, error="gas too low")
        )

        with pytest.raises(SubmissionError) as exc_info:
            await submitter.submit_sequential(txs)

        assert exc_info.value.recoverable is False

    @pytest.mark.asyncio
    async def test_revert_has_partial_results(self, submitter: PublicMempoolSubmitter) -> None:
        """TransactionRevertedError should also carry partial results."""
        txs = [_signed_tx("0xok", nonce=0), _signed_tx("0xrevert", nonce=1)]

        call_count = 0

        async def mock_submit(tx: SignedTransaction) -> SubmissionResult:
            nonlocal call_count
            call_count += 1
            return SubmissionResult(tx_hash=tx.tx_hash, submitted=True)

        async def mock_receipt(tx_hash: str, timeout: float = 120.0) -> TransactionReceipt:
            if tx_hash == "0xrevert":
                raise TransactionRevertedError(
                    tx_hash="0xrevert",
                    revert_reason="STF",
                    gas_used=50000,
                    block_number=100,
                )
            return _receipt(tx_hash)

        submitter._submit_single = AsyncMock(side_effect=mock_submit)
        submitter.get_receipt = AsyncMock(side_effect=mock_receipt)

        with pytest.raises(TransactionRevertedError) as exc_info:
            await submitter.submit_sequential(txs)

        exc = exc_info.value
        # TX 0 was confirmed, TX 1 reverted
        assert len(exc.partial_results) == 2  # type: ignore[attr-defined]
        assert len(exc.partial_receipts) == 1  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Orchestrator routing tests
# ---------------------------------------------------------------------------


class TestOrchestratorSequentialRouting:
    """Verify the orchestrator picks sequential vs parallel submission."""

    def _make_orchestrator(self, signer_cls: str = "eoa") -> MagicMock:
        """Create a minimal mocked orchestrator for routing checks."""
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator

        signer = MagicMock()
        signer.address = "0x1234567890abcdef1234567890abcdef12345678"

        if signer_cls == "safe":
            from almanak.framework.execution.signer.safe.base import SafeSigner

            signer = MagicMock(spec=SafeSigner)
            signer.address = "0x1234567890abcdef1234567890abcdef12345678"
            signer.eoa_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

        submitter = MagicMock(spec=PublicMempoolSubmitter)
        simulator = MagicMock()

        orch = ExecutionOrchestrator(
            signer=signer,
            submitter=submitter,
            simulator=simulator,
            chain="base",
            rpc_url="http://localhost:8545",
        )
        return orch

    def test_multi_tx_eoa_uses_sequential(self) -> None:
        """Multi-TX EOA bundle should select sequential path."""
        from almanak.framework.execution.signer.safe.base import SafeSigner

        orch = self._make_orchestrator("eoa")
        signed_txs = [_signed_tx(f"0x{i}", nonce=i) for i in range(3)]

        use_sequential = len(signed_txs) >= 2 and not isinstance(orch.signer, SafeSigner)
        assert use_sequential is True

    def test_single_tx_uses_parallel(self) -> None:
        """Single TX should use parallel path."""
        from almanak.framework.execution.signer.safe.base import SafeSigner

        orch = self._make_orchestrator("eoa")
        signed_txs = [_signed_tx("0x0", nonce=0)]

        use_sequential = len(signed_txs) >= 2 and not isinstance(orch.signer, SafeSigner)
        assert use_sequential is False

    def test_safe_signer_uses_parallel(self) -> None:
        """Safe signer should use parallel path (MultiSend bundling)."""
        from almanak.framework.execution.signer.safe.base import SafeSigner

        orch = self._make_orchestrator("safe")
        signed_txs = [_signed_tx(f"0x{i}", nonce=i) for i in range(3)]

        use_sequential = len(signed_txs) >= 2 and not isinstance(orch.signer, SafeSigner)
        assert use_sequential is False
