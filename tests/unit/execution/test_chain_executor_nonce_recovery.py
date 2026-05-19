"""Regression tests for the parallel NONCE_ERROR recovery path in ChainExecutor.

PR #2358 originally fixed the silent-fund-stranding bug in the
PublicMempoolSubmitter only. The Claude pr-auditor flagged a second instance
of the same bug pattern in
``almanak.framework.execution.chain_executor.ChainExecutor.submit_transaction``
(line 775-776), which was the multichain orchestrator's parallel submission
path. Both paths now share the same receipt-recovery logic — these tests
guarantee the chain_executor path stays correct.

The bug class: when ``send_raw_transaction`` raises with "nonce too low: tx:
N state: N+1", the chain state has advanced past our nonce because our own
prior submission actually landed. The previous code raised ``NonceError``
unconditionally, causing the runner's retry to re-issue the intent against
an empty wallet — the original on-chain position became a zombie that
teardown never closed.

Observed on Arbitrum mainnet 2026-05-18, lp_triple strategy, stranding
~$3.50 in NFT 5495063 (manually recovered via direct cast calls).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.chain_executor import ChainExecutor
from almanak.framework.execution.interfaces import (
    NonceError,
    SignedTransaction,
    SubmissionResult,
    TransactionRevertedError,
    UnsignedTransaction,
)

# Anvil's well-known default account #0 — used only as a signing key the
# test constructor accepts; no actual network calls happen.
_TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


def _run(coro):
    # ``asyncio.run`` creates a fresh loop, runs the coroutine, and closes
    # the loop on exit — no resource leak across multiple test invocations.
    return asyncio.run(coro)


def _make_signed_tx() -> SignedTransaction:
    unsigned = UnsignedTransaction(
        to="0x0000000000000000000000000000000000000001",
        value=0,
        data="0x",
        chain_id=42161,
        gas_limit=100_000,
        nonce=1635,
        max_fee_per_gas=10**9,
        max_priority_fee_per_gas=10**8,
    )
    return SignedTransaction(
        raw_tx="0x" + "ab" * 100,
        tx_hash="0x" + "cd" * 32,
        unsigned_tx=unsigned,
        signed_at=datetime.now(UTC),
    )


class TestChainExecutorNonceRecovery:
    """Mirror of test_public.py::TestSubmission nonce-recovery cases for the
    parallel ChainExecutor.submit_transaction path."""

    def _make_executor(self, mock_web3: MagicMock) -> ChainExecutor:
        executor = ChainExecutor(
            chain="arbitrum",
            rpc_url="https://example.com",
            private_key=_TEST_PRIVATE_KEY,
        )
        executor._web3 = mock_web3
        return executor

    @patch("almanak.framework.execution.chain_executor.AsyncWeb3")
    def test_nonce_too_low_with_mined_receipt_returns_success(
        self, mock_web3_class: MagicMock
    ) -> None:
        """Receipt with status=1 means our prior submission landed — recover."""
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(
            side_effect=Exception({"message": "nonce too low: tx: 1635 state: 1636"})
        )
        mock_web3.eth.get_transaction_receipt = AsyncMock(
            return_value={"status": 1, "blockNumber": 464118091}
        )
        mock_web3_class.return_value = mock_web3

        signed = _make_signed_tx()
        executor = self._make_executor(mock_web3)

        result = _run(executor.submit_transaction(signed))

        assert isinstance(result, SubmissionResult)
        assert result.submitted is True
        assert result.tx_hash == signed.tx_hash
        # Confirm the lookup was made against OUR signed tx hash — guards
        # against future regressions that hash-collide on a different tx.
        mock_web3.eth.get_transaction_receipt.assert_awaited_once_with(signed.tx_hash)

    @patch("almanak.framework.execution.chain_executor.AsyncWeb3")
    def test_nonce_too_low_with_reverted_receipt_raises_revert(
        self, mock_web3_class: MagicMock
    ) -> None:
        """Receipt with status=0 means our tx landed but reverted — surface revert."""
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(
            side_effect=Exception({"message": "nonce too low: tx: 5 state: 6"})
        )
        mock_web3.eth.get_transaction_receipt = AsyncMock(
            return_value={"status": 0, "blockNumber": 100, "gasUsed": 42000}
        )
        mock_web3_class.return_value = mock_web3

        signed = _make_signed_tx()
        executor = self._make_executor(mock_web3)

        with pytest.raises(TransactionRevertedError) as exc_info:
            _run(executor.submit_transaction(signed))

        assert exc_info.value.tx_hash == signed.tx_hash
        assert exc_info.value.gas_used == 42000
        assert exc_info.value.block_number == 100
        mock_web3.eth.get_transaction_receipt.assert_awaited_once_with(signed.tx_hash)

    @patch("almanak.framework.execution.chain_executor.AsyncWeb3")
    def test_nonce_too_low_with_no_receipt_raises_nonce_error(
        self, mock_web3_class: MagicMock
    ) -> None:
        """No receipt means the tx was dropped — preserve NonceError."""
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(
            side_effect=Exception({"message": "nonce too low: tx: 5 state: 6"})
        )
        mock_web3.eth.get_transaction_receipt = AsyncMock(return_value=None)
        mock_web3_class.return_value = mock_web3

        signed = _make_signed_tx()
        executor = self._make_executor(mock_web3)

        with pytest.raises(NonceError):
            _run(executor.submit_transaction(signed))

        mock_web3.eth.get_transaction_receipt.assert_awaited_once_with(signed.tx_hash)

    @patch("almanak.framework.execution.chain_executor.AsyncWeb3")
    def test_nonce_too_low_with_receipt_lookup_error_raises_nonce_error(
        self, mock_web3_class: MagicMock
    ) -> None:
        """Receipt-lookup transient errors must NOT be silently treated as success."""
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(
            side_effect=Exception({"message": "nonce too low: tx: 5 state: 6"})
        )
        mock_web3.eth.get_transaction_receipt = AsyncMock(
            side_effect=Exception("TransactionNotFound")
        )
        mock_web3_class.return_value = mock_web3

        signed = _make_signed_tx()
        executor = self._make_executor(mock_web3)

        with pytest.raises(NonceError):
            _run(executor.submit_transaction(signed))

        mock_web3.eth.get_transaction_receipt.assert_awaited_once_with(signed.tx_hash)

    @patch("almanak.framework.execution.chain_executor.AsyncWeb3")
    def test_nonce_too_high_does_not_attempt_receipt_recovery(
        self, mock_web3_class: MagicMock
    ) -> None:
        """Only 'nonce too low' carries the 'tx may have landed' ambiguity.

        'nonce too high' means a gap in the nonce sequence — the tx CANNOT
        have landed by definition. Receipt lookup must NOT be attempted.
        """
        mock_web3 = MagicMock()
        mock_web3.eth = AsyncMock()
        mock_web3.eth.send_raw_transaction = AsyncMock(
            side_effect=Exception({"message": "nonce too high"})
        )
        # If the code wrongly attempts a lookup, this would return success
        # and silently swallow the error — confirm it is NOT called.
        mock_web3.eth.get_transaction_receipt = AsyncMock(
            return_value={"status": 1, "blockNumber": 99}
        )
        mock_web3_class.return_value = mock_web3

        signed = _make_signed_tx()
        executor = self._make_executor(mock_web3)

        with pytest.raises(NonceError):
            _run(executor.submit_transaction(signed))

        mock_web3.eth.get_transaction_receipt.assert_not_called()
