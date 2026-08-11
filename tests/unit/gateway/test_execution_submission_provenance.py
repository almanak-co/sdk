"""Wire and receipt invariants for execution submission provenance."""

from types import SimpleNamespace

import pytest

from almanak.framework.execution.gateway_orchestrator import _submission_provenance_from_proto
from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.orchestrator import TransactionResult
from almanak.framework.execution.submission import SubmissionProvenance
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import (
    ReceiptSetSerializationError,
    _serialize_evm_transaction_results,
    _submission_provenance_to_proto,
)


def _receipt(status: int) -> TransactionReceipt:
    return TransactionReceipt(
        tx_hash="0xabc",
        block_number=42,
        block_hash="0xblock",
        gas_used=21_000,
        effective_gas_price=1,
        status=status,
        logs=[],
    )


def test_complete_status_zero_receipt_serializes_for_strategy_reconciliation() -> None:
    hashes, payload = _serialize_evm_transaction_results(
        [TransactionResult(tx_hash="0xabc", success=False, receipt=_receipt(0))]
    )

    assert hashes == ["0xabc"]
    assert b'"status": 0' in payload


def test_non_binary_receipt_status_fails_closed() -> None:
    with pytest.raises(ReceiptSetSerializationError):
        _serialize_evm_transaction_results(
            [TransactionResult(tx_hash="0xabc", success=False, receipt=_receipt(2))]
        )


def test_legacy_or_unknown_proto_value_is_unspecified() -> None:
    assert _submission_provenance_from_proto(SimpleNamespace()) is SubmissionProvenance.UNSPECIFIED
    assert (
        _submission_provenance_from_proto(SimpleNamespace(submission_provenance=999))
        is SubmissionProvenance.UNSPECIFIED
    )


@pytest.mark.parametrize(
    ("internal", "wire"),
    [
        (SubmissionProvenance.UNSPECIFIED, gateway_pb2.SUBMISSION_PROVENANCE_UNSPECIFIED),
        (SubmissionProvenance.NOT_ATTEMPTED, gateway_pb2.SUBMISSION_PROVENANCE_NOT_ATTEMPTED),
        (SubmissionProvenance.ATTEMPTED, gateway_pb2.SUBMISSION_PROVENANCE_ATTEMPTED),
    ],
)
def test_submission_provenance_round_trips_wire(internal: SubmissionProvenance, wire: int) -> None:
    assert _submission_provenance_to_proto(internal) == wire
    assert _submission_provenance_from_proto(SimpleNamespace(submission_provenance=wire)) is internal
