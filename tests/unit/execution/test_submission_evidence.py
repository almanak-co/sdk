"""Plan-bound submission-role evidence contracts."""

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.submission import (
    ReplayPolicy,
    SubmissionTransactionEvidence,
    TransactionRole,
    certify_submission_transactions,
    execution_plan_hash,
)
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import _submission_transactions_to_proto


def _bundle(approval_data: str = "0x095ea7b3" + "00" * 64) -> dict:
    return {
        "intent_type": "SWAP",
        "transactions": [
            {"tx_type": "approve", "data": approval_data, "value": "0x0", "to": "0x1"},
            {"tx_type": "swap", "data": "0x12345678", "value": "0x0", "to": "0x2"},
        ],
    }


def test_certification_requires_canonical_approval_calldata_and_zero_value() -> None:
    evidence = certify_submission_transactions(_bundle(), ["0xaaa", "0xbbb"], transaction_indices=[0, 1])

    assert evidence == [
        SubmissionTransactionEvidence("0xaaa", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
        SubmissionTransactionEvidence("0xbbb", TransactionRole.ACTION, ReplayPolicy.NEVER),
    ]

    wrong_selector = certify_submission_transactions(
        _bundle("0xdeadbeef"), ["0xaaa", "0xbbb"], transaction_indices=[0, 1]
    )
    assert wrong_selector[0].role is TransactionRole.ACTION
    native_value_bundle = _bundle()
    native_value_bundle["transactions"][0]["value"] = "0x1"
    assert (
        certify_submission_transactions(native_value_bundle, ["0xaaa"], transaction_indices=[0])[0].role
        is TransactionRole.ACTION
    )


def test_partial_evidence_requires_explicit_non_positional_alignment() -> None:
    unbound = certify_submission_transactions(_bundle(), ["0xaction"])
    bound = certify_submission_transactions(_bundle(), ["0xaction"], transaction_indices=[1])

    assert unbound == [SubmissionTransactionEvidence("0xaction", TransactionRole.UNKNOWN, ReplayPolicy.NEVER)]
    assert bound == [SubmissionTransactionEvidence("0xaction", TransactionRole.ACTION, ReplayPolicy.NEVER)]


def test_partial_approval_evidence_cannot_authorize_recompile_of_an_unaccounted_action() -> None:
    evidence = certify_submission_transactions(_bundle(), ["0xapprove"], transaction_indices=[0])

    assert evidence == [SubmissionTransactionEvidence("0xapprove", TransactionRole.UNKNOWN, ReplayPolicy.NEVER)]


def test_duplicate_or_out_of_range_indices_fail_closed() -> None:
    duplicate = certify_submission_transactions(
        _bundle(),
        ["0xfirst", "0xsecond"],
        transaction_indices=[0, 0],
    )
    out_of_range = certify_submission_transactions(_bundle(), ["0xunknown"], transaction_indices=[2])

    assert all(item.role is TransactionRole.UNKNOWN for item in duplicate)
    assert out_of_range == [SubmissionTransactionEvidence("0xunknown", TransactionRole.UNKNOWN, ReplayPolicy.NEVER)]


def test_duplicate_transaction_id_with_distinct_indices_fails_closed() -> None:
    evidence = certify_submission_transactions(
        _bundle(),
        ["0xduplicate", "0xDUPLICATE"],
        transaction_indices=[0, 1],
    )

    assert all(item.role is TransactionRole.UNKNOWN for item in evidence)
    assert all(item.replay_policy is ReplayPolicy.NEVER for item in evidence)


def test_duplicate_transaction_id_uses_canonical_evm_identity() -> None:
    evidence = certify_submission_transactions(
        _bundle(),
        ["0xAbC", "abc"],
        transaction_indices=[0, 1],
    )

    assert all(item.role is TransactionRole.UNKNOWN for item in evidence)
    assert all(item.replay_policy is ReplayPolicy.NEVER for item in evidence)


def test_atomic_safe_batch_is_action_if_any_logical_member_is_action() -> None:
    evidence = certify_submission_transactions(_bundle(), ["0xsafe"], atomic_batch=True)

    assert evidence == [SubmissionTransactionEvidence("0xsafe", TransactionRole.ACTION, ReplayPolicy.NEVER)]


def test_atomic_safe_batch_is_setup_only_when_every_member_is_idempotent_setup() -> None:
    bundle = _bundle()
    bundle["transactions"] = [
        bundle["transactions"][0],
        {**bundle["transactions"][0], "tx_type": "approve_reset"},
    ]

    evidence = certify_submission_transactions(bundle, ["0xsafe"], atomic_batch=True)

    assert evidence == [
        SubmissionTransactionEvidence("0xsafe", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY)
    ]


def test_atomic_safe_batch_rejects_prefix_only_transaction_id() -> None:
    bundle = _bundle()
    bundle["transactions"] = [bundle["transactions"][0]]

    evidence = certify_submission_transactions(bundle, ["0x"], atomic_batch=True)

    assert evidence == [SubmissionTransactionEvidence("0x", TransactionRole.UNKNOWN, ReplayPolicy.NEVER)]


def test_plan_hash_binds_order_and_exact_transaction_bytes() -> None:
    original = _bundle()
    reordered = {**original, "transactions": list(reversed(original["transactions"]))}
    changed = _bundle("0x095ea7b3" + "01" * 64)

    assert len(execution_plan_hash(original)) == 64
    assert execution_plan_hash(original) != execution_plan_hash(reordered)
    assert execution_plan_hash(original) != execution_plan_hash(changed)


def test_proto_and_gateway_result_preserve_role_policy_and_plan_binding() -> None:
    internal = [
        SubmissionTransactionEvidence("0xaaa", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
        SubmissionTransactionEvidence("0xbbb", TransactionRole.ACTION, ReplayPolicy.NEVER),
    ]
    wire = gateway_pb2.ExecutionResult(
        execution_plan_hash="a" * 64,
        submission_transactions=_submission_transactions_to_proto(internal),
    )

    assert wire.submission_transactions[0].role == gateway_pb2.EXECUTION_TRANSACTION_ROLE_SETUP_APPROVAL
    assert wire.submission_transactions[0].replay_policy == gateway_pb2.REPLAY_POLICY_RECOMPILE_ONLY
    result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xaaa", "0xbbb"],
        total_gas_used=0,
        receipts=[],
        execution_id="",
        execution_plan_hash=wire.execution_plan_hash,
        submission_transactions=[
            SubmissionTransactionEvidence(
                item.tx_id,
                TransactionRole.SETUP_APPROVAL
                if item.role == gateway_pb2.EXECUTION_TRANSACTION_ROLE_SETUP_APPROVAL
                else TransactionRole.ACTION,
                ReplayPolicy.RECOMPILE_ONLY
                if item.replay_policy == gateway_pb2.REPLAY_POLICY_RECOMPILE_ONLY
                else ReplayPolicy.NEVER,
            )
            for item in wire.submission_transactions
        ],
    )

    assert result.to_dict()["execution_plan_hash"] == "a" * 64
    assert result.to_dict()["submission_transactions"] == [item.to_dict() for item in internal]
    assert result.to_outcome().submission_transactions == internal
