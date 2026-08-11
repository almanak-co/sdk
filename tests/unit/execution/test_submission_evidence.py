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
    evidence = certify_submission_transactions(_bundle(), ["0xaaa", "0xbbb"])

    assert evidence == [
        SubmissionTransactionEvidence("0xaaa", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
        SubmissionTransactionEvidence("0xbbb", TransactionRole.ACTION, ReplayPolicy.NEVER),
    ]

    wrong_selector = certify_submission_transactions(_bundle("0xdeadbeef"), ["0xaaa", "0xbbb"])
    assert wrong_selector[0].role is TransactionRole.ACTION
    native_value_bundle = _bundle()
    native_value_bundle["transactions"][0]["value"] = "0x1"
    assert certify_submission_transactions(native_value_bundle, ["0xaaa"])[0].role is TransactionRole.ACTION


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
