"""Branch coverage for ChainExecutor execute_transaction / execute_transaction_safe /
execute_bundle.

The full-flow entry points are driven with the sign/submit/confirm seams replaced
by AsyncMock so no RPC ever happens. Covers the success paths (with and without
confirmation waits), the submitted=False early return, and each of the three
error-classification except branches (execution errors, SubmissionError with and
without a tx hash, and the unexpected-Exception fallback). No RPC access.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.chain_executor import ChainExecutor
from almanak.framework.execution.interfaces import (
    ExecutionError,
    SignedTransaction,
    SigningError,
    SubmissionError,
    SubmissionResult,
    TransactionReceipt,
    TransactionRevertedError,
    TransactionType,
    UnsignedTransaction,
)

# Anvil's first well-known dev account key (public knowledge, test-only).
_TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

_TX_HASH = "0x" + "ab" * 32


@pytest.fixture
def executor() -> ChainExecutor:
    return ChainExecutor(
        chain="arbitrum",
        rpc_url="https://example.com",
        private_key=_TEST_PRIVATE_KEY,
    )


def _unsigned(*, nonce=1, **overrides) -> UnsignedTransaction:
    from eth_utils import to_checksum_address

    fields = {
        # eth-account requires EIP-55 checksummed addresses.
        "to": to_checksum_address("0x" + "aa" * 20),
        "value": 10,
        "data": "0x",
        "chain_id": 42161,
        "gas_limit": 21000,
        "nonce": nonce,
        "tx_type": TransactionType.EIP_1559,
        "max_fee_per_gas": 100,
        "max_priority_fee_per_gas": 2,
    }
    fields.update(overrides)
    return UnsignedTransaction(**fields)


def _signed(tx: UnsignedTransaction) -> SignedTransaction:
    return SignedTransaction(raw_tx="0x02beef", tx_hash=_TX_HASH, unsigned_tx=tx)


def _receipt(*, status=1, gas_used=21000, effective_gas_price=10) -> TransactionReceipt:
    return TransactionReceipt(
        tx_hash=_TX_HASH,
        block_number=123,
        block_hash="0x" + "cd" * 32,
        gas_used=gas_used,
        effective_gas_price=effective_gas_price,
        status=status,
    )


def _wire_eoa_seams(executor: ChainExecutor, tx: UnsignedTransaction) -> None:
    """Install happy-path sign/submit/confirm mocks on the executor."""
    executor.get_next_nonce = AsyncMock(return_value=7)
    executor.sign_transaction = AsyncMock(return_value=_signed(tx))
    executor.submit_transaction = AsyncMock(return_value=SubmissionResult(tx_hash=_TX_HASH, submitted=True))
    executor.wait_for_receipt = AsyncMock(return_value=_receipt())


def _wire_safe_seams(executor: ChainExecutor, signed: SignedTransaction) -> MagicMock:
    """Install a Safe signer plus happy-path submit/confirm mocks."""
    safe_signer = MagicMock()
    safe_signer.sign_with_web3 = AsyncMock(return_value=signed)
    safe_signer.sign_bundle_with_web3 = AsyncMock(return_value=signed)
    executor._safe_signer = safe_signer
    executor._get_web3 = AsyncMock(return_value=MagicMock(name="web3"))
    executor.get_next_nonce = AsyncMock(return_value=11)
    executor.submit_transaction = AsyncMock(return_value=SubmissionResult(tx_hash=_TX_HASH, submitted=True))
    executor.wait_for_receipt = AsyncMock(return_value=_receipt())
    return safe_signer


# =============================================================================
# execute_transaction
# =============================================================================


class TestExecuteTransaction:
    def test_missing_nonce_is_filled_then_confirmed(self, executor):
        tx = _unsigned(nonce=None)
        _wire_eoa_seams(executor, tx)

        result = asyncio.run(executor.execute_transaction(tx))

        assert result.success is True
        assert result.tx_hash == _TX_HASH
        assert result.nonce_used == 7
        assert result.gas_used == 21000
        assert result.gas_cost_wei == 210000
        assert result.receipt is executor.wait_for_receipt.return_value
        # The rebuilt transaction handed to the signer carries the fetched nonce
        # and preserves every other field.
        signed_arg = executor.sign_transaction.call_args[0][0]
        assert signed_arg.nonce == 7
        assert signed_arg.to == tx.to
        assert signed_arg.gas_limit == tx.gas_limit
        executor.get_next_nonce.assert_awaited_once()

    def test_preset_nonce_without_confirmation(self, executor):
        tx = _unsigned(nonce=3)
        _wire_eoa_seams(executor, tx)

        result = asyncio.run(executor.execute_transaction(tx, wait_for_confirmation=False))

        assert result.success is True
        assert result.tx_hash == _TX_HASH
        assert result.nonce_used == 3
        assert result.receipt is None
        executor.get_next_nonce.assert_not_awaited()
        executor.wait_for_receipt.assert_not_awaited()
        # The pre-nonced transaction is signed as-is, not rebuilt.
        assert executor.sign_transaction.call_args[0][0] is tx

    def test_submission_not_submitted_returns_failure(self, executor):
        tx = _unsigned(nonce=3)
        _wire_eoa_seams(executor, tx)
        executor.submit_transaction = AsyncMock(
            return_value=SubmissionResult(tx_hash=_TX_HASH, submitted=False, error="mempool full")
        )

        result = asyncio.run(executor.execute_transaction(tx))

        assert result.success is False
        assert result.tx_hash == _TX_HASH
        assert result.error == "mempool full"
        assert result.nonce_used == 3
        executor.wait_for_receipt.assert_not_awaited()

    def test_signing_error_classified_without_tx_hash(self, executor):
        tx = _unsigned(nonce=3)
        _wire_eoa_seams(executor, tx)
        executor.sign_transaction = AsyncMock(side_effect=SigningError(reason="bad key"))

        result = asyncio.run(executor.execute_transaction(tx))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Signing failed: bad key"
        assert result.nonce_used == 3

    def test_reverted_error_classified_without_tx_hash(self, executor):
        # TransactionRevertedError shares the execution-error handler, so the
        # result deliberately reports an empty tx_hash even though the error
        # itself carries one.
        tx = _unsigned(nonce=3)
        _wire_eoa_seams(executor, tx)
        executor.wait_for_receipt = AsyncMock(side_effect=TransactionRevertedError(tx_hash=_TX_HASH, gas_used=100))

        result = asyncio.run(executor.execute_transaction(tx))

        assert result.success is False
        assert result.tx_hash == ""
        assert _TX_HASH in (result.error or "")

    def test_submission_error_propagates_tx_hash(self, executor):
        tx = _unsigned(nonce=3)
        _wire_eoa_seams(executor, tx)
        executor.wait_for_receipt = AsyncMock(side_effect=SubmissionError(reason="timeout", tx_hash=_TX_HASH))

        result = asyncio.run(executor.execute_transaction(tx))

        assert result.success is False
        assert result.tx_hash == _TX_HASH
        assert result.error == "Submission failed: timeout"
        assert result.nonce_used == 3

    def test_submission_error_without_tx_hash_yields_empty(self, executor):
        tx = _unsigned(nonce=3)
        _wire_eoa_seams(executor, tx)
        executor.submit_transaction = AsyncMock(side_effect=SubmissionError(reason="rpc down", tx_hash=None))

        result = asyncio.run(executor.execute_transaction(tx))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Submission failed: rpc down"

    def test_unexpected_exception_is_wrapped(self, executor):
        tx = _unsigned(nonce=3)
        _wire_eoa_seams(executor, tx)
        executor.submit_transaction = AsyncMock(side_effect=RuntimeError("boom"))

        result = asyncio.run(executor.execute_transaction(tx))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Unexpected error: boom"
        assert result.nonce_used == 3


# =============================================================================
# execute_transaction_safe
# =============================================================================


class TestExecuteTransactionSafe:
    def test_requires_safe_signer(self, executor):
        with pytest.raises(ExecutionError, match="requires a SafeSigner"):
            asyncio.run(executor.execute_transaction_safe(_unsigned()))

    def test_success_with_confirmation(self, executor):
        tx = _unsigned(nonce=None)
        safe_signer = _wire_safe_seams(executor, _signed(tx))

        result = asyncio.run(executor.execute_transaction_safe(tx))

        assert result.success is True
        assert result.tx_hash == _TX_HASH
        assert result.nonce_used == 11
        assert result.gas_used == 21000
        assert result.gas_cost_wei == 210000
        safe_signer.sign_with_web3.assert_awaited_once_with(
            tx, executor._get_web3.return_value, 11, pos_in_bundle=0
        )

    def test_success_without_confirmation(self, executor):
        tx = _unsigned()
        _wire_safe_seams(executor, _signed(tx))

        result = asyncio.run(executor.execute_transaction_safe(tx, wait_for_confirmation=False))

        assert result.success is True
        assert result.receipt is None
        assert result.nonce_used == 11
        executor.wait_for_receipt.assert_not_awaited()

    def test_submission_not_submitted_returns_failure(self, executor):
        tx = _unsigned()
        _wire_safe_seams(executor, _signed(tx))
        executor.submit_transaction = AsyncMock(
            return_value=SubmissionResult(tx_hash=_TX_HASH, submitted=False, error="rejected")
        )

        result = asyncio.run(executor.execute_transaction_safe(tx))

        assert result.success is False
        assert result.tx_hash == _TX_HASH
        assert result.error == "rejected"
        assert result.nonce_used == 11

    def test_signing_error_classified(self, executor):
        tx = _unsigned()
        safe_signer = _wire_safe_seams(executor, _signed(tx))
        safe_signer.sign_with_web3 = AsyncMock(side_effect=SigningError(reason="safe refused"))

        result = asyncio.run(executor.execute_transaction_safe(tx))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Signing failed: safe refused"

    def test_submission_error_propagates_tx_hash(self, executor):
        tx = _unsigned()
        _wire_safe_seams(executor, _signed(tx))
        executor.wait_for_receipt = AsyncMock(side_effect=SubmissionError(reason="timeout", tx_hash=_TX_HASH))

        result = asyncio.run(executor.execute_transaction_safe(tx))

        assert result.success is False
        assert result.tx_hash == _TX_HASH
        assert result.error == "Submission failed: timeout"

    def test_unexpected_exception_is_wrapped(self, executor):
        tx = _unsigned()
        _wire_safe_seams(executor, _signed(tx))
        executor._get_web3 = AsyncMock(side_effect=RuntimeError("no provider"))

        result = asyncio.run(executor.execute_transaction_safe(tx))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Unexpected error: no provider"


# =============================================================================
# execute_bundle
# =============================================================================


class TestExecuteBundle:
    def test_requires_safe_signer(self, executor):
        with pytest.raises(ExecutionError, match="requires Safe mode"):
            asyncio.run(executor.execute_bundle([_unsigned()]))

    def test_rejects_empty_bundle(self, executor):
        executor._safe_signer = MagicMock()
        with pytest.raises(ExecutionError, match="empty transaction bundle"):
            asyncio.run(executor.execute_bundle([]))

    def test_success_with_confirmation(self, executor):
        txs = [_unsigned(), _unsigned(nonce=2)]
        safe_signer = _wire_safe_seams(executor, _signed(txs[0]))

        result = asyncio.run(executor.execute_bundle(txs))

        assert result.success is True
        assert result.tx_hash == _TX_HASH
        assert result.nonce_used == 11
        assert result.gas_used == 21000
        assert result.gas_cost_wei == 210000
        # A fresh bundle always clears the Safe nonce cache before signing.
        safe_signer.clear_nonce_cache.assert_called_once_with()
        safe_signer.sign_bundle_with_web3.assert_awaited_once_with(
            txs, executor._get_web3.return_value, 11, "arbitrum"
        )

    def test_success_without_confirmation(self, executor):
        txs = [_unsigned()]
        _wire_safe_seams(executor, _signed(txs[0]))

        result = asyncio.run(executor.execute_bundle(txs, wait_for_confirmation=False))

        assert result.success is True
        assert result.receipt is None
        assert result.nonce_used == 11
        executor.wait_for_receipt.assert_not_awaited()

    def test_submission_not_submitted_returns_failure(self, executor):
        txs = [_unsigned()]
        _wire_safe_seams(executor, _signed(txs[0]))
        executor.submit_transaction = AsyncMock(
            return_value=SubmissionResult(tx_hash=_TX_HASH, submitted=False, error="underpriced")
        )

        result = asyncio.run(executor.execute_bundle(txs))

        assert result.success is False
        assert result.tx_hash == _TX_HASH
        assert result.error == "underpriced"
        assert result.nonce_used == 11
        executor.wait_for_receipt.assert_not_awaited()

    def test_signing_error_classified(self, executor):
        txs = [_unsigned()]
        safe_signer = _wire_safe_seams(executor, _signed(txs[0]))
        safe_signer.sign_bundle_with_web3 = AsyncMock(side_effect=SigningError(reason="role denied"))

        result = asyncio.run(executor.execute_bundle(txs))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Signing failed: role denied"

    def test_submission_error_without_tx_hash_yields_empty(self, executor):
        txs = [_unsigned()]
        _wire_safe_seams(executor, _signed(txs[0]))
        executor.submit_transaction = AsyncMock(side_effect=SubmissionError(reason="rpc down", tx_hash=None))

        result = asyncio.run(executor.execute_bundle(txs))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Submission failed: rpc down"

    def test_unexpected_exception_is_wrapped(self, executor):
        txs = [_unsigned()]
        safe_signer = _wire_safe_seams(executor, _signed(txs[0]))
        safe_signer.sign_bundle_with_web3 = AsyncMock(side_effect=RuntimeError("multisend down"))

        result = asyncio.run(executor.execute_bundle(txs))

        assert result.success is False
        assert result.tx_hash == ""
        assert result.error == "Unexpected error: multisend down"
