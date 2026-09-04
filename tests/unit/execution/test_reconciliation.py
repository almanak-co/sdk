"""Retry-safety contract for mixed-version execution-result envelopes."""

from collections.abc import Callable
from types import SimpleNamespace

import pytest

from almanak.framework.execution.chain_executor import TransactionExecutionResult
from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.multichain import ExecutionStatus, IntentExecutionResult
from almanak.framework.execution.reconciliation import (
    complete_receipt_set_error,
    failed_submission_allows_recompile,
    failed_submission_proves_revert,
    failed_submission_requires_reconciliation,
    submitted_transaction_hashes,
    successful_receipt_set_error,
)
from almanak.framework.execution.submission import (
    ReplayPolicy,
    SubmissionProvenance,
    SubmissionTransactionEvidence,
    TransactionRole,
)


def _reverted_receipt(tx_hash: str) -> dict[str, object]:
    return {
        "tx_hash": tx_hash,
        "block_number": 42,
        "block_hash": "0xblock",
        "gas_used": 21_000,
        "effective_gas_price": "1",
        "status": 0,
        "logs": [],
    }


def _solana_failed_receipt(signature: str) -> dict[str, object]:
    return {
        "signature": signature,
        "slot": 42,
        "fee_lamports": 5000,
        "success": False,
        "err": {"InstructionError": [0, "Custom"]},
        "logs": [],
    }


def _successful_evm_receipt(tx_hash: str) -> dict[str, object]:
    receipt = _reverted_receipt(tx_hash)
    receipt["status"] = 1
    return receipt


def test_mapping_envelope_and_mapping_transactions_preserve_hashes() -> None:
    result = {
        "success": False,
        "tx_hashes": ["0xAAA", "0xaaa"],
        "transaction_results": [{"tx_hash": "0xBBB"}],
        "error": "receipt mismatch",
    }

    assert submitted_transaction_hashes(result) == ("0xAAA", "0xBBB")
    assert failed_submission_requires_reconciliation(result) is True


def test_legacy_singular_hash_is_a_terminal_broadcast() -> None:
    result = SimpleNamespace(success=False, tx_hash="0xlegacy", error="timeout")

    assert submitted_transaction_hashes(result) == ("0xlegacy",)
    assert failed_submission_requires_reconciliation(result) is True


def test_success_with_hash_is_not_misclassified() -> None:
    result = {"success": True, "tx_hash": "0xconfirmed"}

    assert failed_submission_requires_reconciliation(result) is False


def test_hashless_failed_outcome_does_not_prove_safe_retry() -> None:
    """Crossing the orchestrator boundary without an identifier is ambiguous."""
    result = SimpleNamespace(success=False, error="worker crashed after submit")

    assert failed_submission_proves_revert(result) is False
    assert failed_submission_requires_reconciliation(result) is True


def test_authoritative_pre_submit_rejection_is_safe_to_retry() -> None:
    result = SimpleNamespace(
        success=False,
        error="risk guard rejected",
        submission_provenance=SubmissionProvenance.NOT_ATTEMPTED,
    )

    assert failed_submission_requires_reconciliation(result) is False


def test_not_attempted_with_retained_transaction_identity_fails_closed() -> None:
    """Hash evidence dominates a contradictory producer provenance stamp."""
    result = SimpleNamespace(
        success=False,
        error="contradictory gateway result",
        submission_provenance=SubmissionProvenance.NOT_ATTEMPTED,
        tx_hashes=["0xalreadybroadcast"],
    )

    assert failed_submission_requires_reconciliation(result) is True


def test_hashless_attempted_submission_requires_reconciliation() -> None:
    result = SimpleNamespace(
        success=False,
        error="transport lost response",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
    )

    assert failed_submission_requires_reconciliation(result) is True


def test_complete_status_zero_receipt_is_the_only_proven_retry_control() -> None:
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xreverted"],
        total_gas_used=21_000,
        receipts=[_reverted_receipt("0xreverted")],
        execution_id="exec-proven-revert",
        error="execution reverted",
    )

    assert failed_submission_proves_revert(result) is True


def _mixed_setup_action_failure(*, action_status: int = 0) -> GatewayExecutionResult:
    plan_hash = "a" * 64
    return GatewayExecutionResult(
        success=False,
        tx_hashes=["0xapprove", "0xaction"],
        total_gas_used=42_000,
        receipts=[_successful_evm_receipt("0xapprove"), {**_reverted_receipt("0xaction"), "status": action_status}],
        execution_id="exec-mixed",
        error="action reverted",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        execution_plan_hash=plan_hash,
        submission_transactions=[
            SubmissionTransactionEvidence("0xapprove", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
            SubmissionTransactionEvidence("0xaction", TransactionRole.ACTION, ReplayPolicy.NEVER),
        ],
    )


def test_plan_bound_landed_setup_and_reverted_action_allows_only_recompile() -> None:
    result = _mixed_setup_action_failure()

    assert failed_submission_allows_recompile(result, expected_plan_hash="a" * 64) is True
    assert failed_submission_proves_revert(result) is False


@pytest.mark.parametrize("mutation", ["action_landed", "plan_mismatch", "unknown_role", "not_attempted"])
def test_recompile_classifier_fails_closed_on_unsafe_or_unbound_evidence(mutation: str) -> None:
    result = _mixed_setup_action_failure(action_status=1 if mutation == "action_landed" else 0)
    expected_plan_hash = "a" * 64
    if mutation == "plan_mismatch":
        expected_plan_hash = "b" * 64
    elif mutation == "unknown_role":
        result.submission_transactions[0] = SubmissionTransactionEvidence("0xapprove")
    elif mutation == "not_attempted":
        result.submission_provenance = SubmissionProvenance.NOT_ATTEMPTED

    assert failed_submission_allows_recompile(result, expected_plan_hash=expected_plan_hash) is False


def test_atomic_safe_action_receipt_never_allows_recompile() -> None:
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xsafe"],
        total_gas_used=21_000,
        receipts=[_successful_evm_receipt("0xsafe")],
        execution_id="exec-safe",
        error="parser failed after atomic execution",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        execution_plan_hash="a" * 64,
        submission_transactions=[SubmissionTransactionEvidence("0xsafe", TransactionRole.ACTION, ReplayPolicy.NEVER)],
    )

    assert failed_submission_allows_recompile(result, expected_plan_hash="a" * 64) is False


def test_complete_validator_preserves_failed_receipt_but_success_validator_rejects_it() -> None:
    receipt = _reverted_receipt("0xreverted")

    assert complete_receipt_set_error(["0xreverted"], [receipt]) is None
    assert successful_receipt_set_error(["0xreverted"], [receipt]) is not None


def test_successful_evm_receipt_identity_accepts_gateway_prefix_normalization() -> None:
    """The gateway and client may spell the same 32-byte EVM hash with/without ``0x``."""
    hash_payload = "ab" * 32

    result = GatewayExecutionResult(
        success=True,
        tx_hashes=[f"0x{hash_payload}"],
        total_gas_used=21_000,
        receipts=[_successful_evm_receipt(hash_payload)],
        execution_id="exec-prefix-normalized",
    )

    assert result.success is True
    assert result.error is None


def test_successful_evm_receipt_identity_still_rejects_a_different_hash() -> None:
    hash_payload = "ab" * 32

    assert (
        successful_receipt_set_error(
            [f"0x{hash_payload}"],
            [_successful_evm_receipt("ac" * 32)],
        )
        == "receipt 0 identity does not match submitted transaction"
    )


def test_production_intent_wrapper_preserves_incomplete_gateway_hashes() -> None:
    inner = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xwrapped"],
        total_gas_used=0,
        receipts=[],
        execution_id="exec-1",
        error="receipt set incomplete",
    )
    result = IntentExecutionResult(
        intent=SimpleNamespace(intent_id="intent-1"),
        chain="arbitrum",
        status=ExecutionStatus.FAILED,
        tx_result=inner,
        error=inner.error,
    )

    assert submitted_transaction_hashes(result) == ("0xwrapped",)
    assert failed_submission_requires_reconciliation(result) is True


def test_fully_observed_gateway_revert_remains_normally_retryable() -> None:
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xreverted"],
        total_gas_used=21_000,
        receipts=[_reverted_receipt("0xreverted")],
        execution_id="exec-2",
        error="execution reverted",
    )

    assert failed_submission_requires_reconciliation(result) is False


def test_multi_transaction_mixed_success_and_revert_requires_reconciliation() -> None:
    successful_receipt = _reverted_receipt("0xlanded")
    successful_receipt["status"] = 1
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xlanded", "0xreverted"],
        total_gas_used=42_000,
        receipts=[successful_receipt, _reverted_receipt("0xreverted")],
        execution_id="exec-mixed",
        error="bundle partially failed",
    )

    assert failed_submission_requires_reconciliation(result) is True


@pytest.mark.parametrize(
    "receipts",
    [
        [_reverted_receipt("0xduplicate"), _reverted_receipt("0xduplicate")],
        [
            _reverted_receipt("0xduplicate"),
            {**_reverted_receipt("0xduplicate"), "status": 1},
        ],
    ],
    ids=["duplicate-reverts", "contradictory-duplicate-status"],
)
def test_duplicate_gateway_hashes_require_reconciliation(receipts: list[dict[str, object]]) -> None:
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xDUPLICATE", "0xduplicate"],
        total_gas_used=42_000,
        receipts=receipts,
        execution_id="exec-duplicate",
        error="duplicate receipt identity",
    )

    assert submitted_transaction_hashes(result) == ("0xDUPLICATE",)
    assert failed_submission_requires_reconciliation(result) is True


def test_plural_and_derived_transaction_result_echo_is_not_a_duplicate() -> None:
    receipt = _reverted_receipt("0xreverted")
    result = {
        "success": False,
        "tx_hashes": ["0xreverted"],
        "receipts": [receipt],
        "transaction_results": [{"tx_hash": "0xreverted", "receipt": receipt}],
        "error": "execution reverted",
    }

    assert failed_submission_requires_reconciliation(result) is False


@pytest.mark.parametrize(
    "receipt",
    [
        {},
        _reverted_receipt("0xwrong"),
        {key: value for key, value in _reverted_receipt("0xexpected").items() if key != "status"},
        {**_reverted_receipt("0xexpected"), "status": "unknown"},
        {key: value for key, value in _reverted_receipt("0xexpected").items() if key != "block_number"},
        {key: value for key, value in _reverted_receipt("0xexpected").items() if key != "block_hash"},
        {key: value for key, value in _reverted_receipt("0xexpected").items() if key != "gas_used"},
        {key: value for key, value in _reverted_receipt("0xexpected").items() if key != "effective_gas_price"},
        {key: value for key, value in _reverted_receipt("0xexpected").items() if key != "logs"},
    ],
    ids=[
        "empty",
        "wrong-hash",
        "missing-status",
        "invalid-status",
        "missing-block-number",
        "missing-block-hash",
        "missing-gas",
        "missing-gas-price",
        "missing-logs",
    ],
)
def test_malformed_or_mismatched_gateway_receipt_requires_reconciliation(receipt: dict[str, object]) -> None:
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xexpected"],
        total_gas_used=21_000,
        receipts=[receipt],
        execution_id="exec-malformed",
        error="receipt evidence invalid",
    )

    assert failed_submission_requires_reconciliation(result) is True


def test_production_intent_wrapper_handles_local_transaction_result() -> None:
    inner = TransactionExecutionResult(success=False, tx_hash="0xlocal", error="receipt timeout")
    result = IntentExecutionResult(
        intent=SimpleNamespace(intent_id="intent-2"),
        chain="arbitrum",
        status=ExecutionStatus.FAILED,
        tx_result=inner,
        error=inner.error,
    )

    assert submitted_transaction_hashes(result) == ("0xlocal",)
    assert failed_submission_requires_reconciliation(result) is True


def test_local_batch_hashes_preserve_compiled_order() -> None:
    approval = TransactionExecutionResult(success=True, tx_hash="0xapprove", transaction_index=0)
    action = TransactionExecutionResult(success=False, tx_hash="0xaction", transaction_index=1, error="reverted")
    batch = TransactionExecutionResult(
        success=False,
        tx_hash=action.tx_hash,
        error=action.error,
        transaction_results=[approval, action],
    )

    assert submitted_transaction_hashes(batch) == ("0xapprove", "0xaction")
    assert failed_submission_requires_reconciliation(batch) is True


def test_fully_observed_local_revert_remains_normally_retryable() -> None:
    receipt = TransactionReceipt(
        tx_hash="0xlocal-revert",
        block_number=42,
        block_hash="0xblock",
        gas_used=21_000,
        effective_gas_price=1,
        status=0,
    )
    result = TransactionExecutionResult(
        success=False,
        tx_hash=receipt.tx_hash,
        receipt=receipt,
        error="execution reverted",
    )

    assert failed_submission_requires_reconciliation(result) is False


def test_fully_observed_solana_failure_remains_normally_retryable() -> None:
    signature = "5VERv8NMHKRYsGeYfVb9oKzvoHvU9vE3yo9Xq2Gj8j3B8VeqiZLzQQDCbPVmXNgTjEFGdYkhNmj1PYqC7GsQzXvA"
    result = SimpleNamespace(
        success=False,
        tx_hashes=[signature],
        receipts=[_solana_failed_receipt(signature)],
        chain_family="SOLANA",
        error="transaction failed on-chain",
    )

    assert failed_submission_requires_reconciliation(result) is False


@pytest.mark.parametrize(
    "mutation",
    [
        lambda receipt: receipt.update(signature="different"),
        lambda receipt: receipt.pop("slot"),
        lambda receipt: receipt.pop("fee_lamports"),
        lambda receipt: receipt.pop("err"),
        lambda receipt: receipt.pop("logs"),
    ],
)
def test_partial_solana_failure_requires_reconciliation(
    mutation: Callable[[dict[str, object]], object],
) -> None:
    signature = "solana-signature"
    receipt = _solana_failed_receipt(signature)
    mutation(receipt)
    result = SimpleNamespace(success=False, tx_hashes=[signature], receipts=[receipt], error="failed")

    assert failed_submission_requires_reconciliation(result) is True


def test_successful_evm_receipt_set_rejects_duplicate_and_mismatched_identity() -> None:
    receipt_a = {**_reverted_receipt("0xaaa"), "status": 1}
    receipt_b = {**_reverted_receipt("0xbbb"), "status": 1}

    assert successful_receipt_set_error(["0xaaa", "0xAAA"], [receipt_a, receipt_a]) is not None
    assert successful_receipt_set_error(["0xaaa"], [receipt_b]) is not None
