"""Complete-plan recovery refuses approval-only and outer-Safe success."""

import json
from dataclasses import replace
from pathlib import Path

import pytest
from eth_utils import keccak

from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.plan_completion import PlanCompletionUnproven, prove_completed_evm_plan
from almanak.framework.execution.submission import SubmissionProvenance, SubmissionTransactionEvidence

APPROVAL = "0x" + "11" * 32
ACTION = "0x" + "22" * 32
SAFE = "0x" + "33" * 20


def receipt(tx_hash: str, **kwargs) -> TransactionReceipt:
    return TransactionReceipt(
        tx_hash=tx_hash,
        block_number=7,
        block_hash="0x" + "44" * 32,
        gas_used=21_000,
        effective_gas_price=6,
        status=1,
        **kwargs,
    )


def evidence(tx_hash=ACTION, indices=(0,), count=1, safe="") -> SubmissionTransactionEvidence:
    return SubmissionTransactionEvidence(tx_hash, plan_indices=indices, plan_transaction_count=count, safe_address=safe)


def prove(items=None, observations=None, **kwargs):
    return prove_completed_evm_plan(
        **{
            "expected_plan_hash": "a" * 64,
            "observed_plan_hash": "a" * 64,
            "provenance": SubmissionProvenance.ATTEMPTED,
            "submitted_tx_ids": [ACTION],
            "evidence": items if items is not None else [evidence()],
            "receipts": observations if observations is not None else [receipt(ACTION)],
            **kwargs,
        }
    )


def safe_log(event="ExecutionSuccess", indexed=False, address=SAFE):
    module = "Module" in event
    topics = ["0x" + keccak(text=f"{event}({'address' if module else 'bytes32,uint256'})").hex()]
    if module:
        topics.append("0x" + "00" * 12 + "55" * 20)
        data = "0x"
    elif indexed:
        topics.append("0x" + "66" * 32)
        data = "0x" + "00" * 32
    else:
        data = "0x" + "66" * 32 + "00" * 32
    return {"address": address, "topics": topics, "data": data}


def test_shuffled_receipts_are_returned_in_original_plan_order_without_inventing_fees():
    approval, action = receipt(APPROVAL), receipt(ACTION)
    result = prove(
        [evidence(ACTION, (1,), 2), evidence(APPROVAL, (0,), 2)],
        [action, approval],
        submitted_tx_ids=[ACTION, APPROVAL],
    )
    assert result == (approval, action)
    assert result[0].l1_fee_wei is None


def test_unprefixed_signed_hash_survives_gateway_and_checkpoint_before_proof():
    from almanak.framework.execution.gateway_orchestrator import _execution_result_from_proto
    from almanak.framework.execution.submission import certify_submission_transactions
    from almanak.framework.runner.runner_models import StepSubmissionEvidence
    from almanak.gateway.proto import gateway_pb2
    from almanak.gateway.services.execution_service import _submission_transactions_to_proto

    signed_hash = ACTION.removeprefix("0x")
    certified = certify_submission_transactions(
        {"transactions": [{"tx_type": "swap"}]}, [signed_hash], transaction_indices=[0]
    )
    wire = gateway_pb2.ExecutionResult(
        execution_plan_hash="a" * 64,
        submission_provenance=gateway_pb2.SUBMISSION_PROVENANCE_ATTEMPTED,
        tx_hashes=[ACTION],
        submission_transactions=_submission_transactions_to_proto(certified),
    )
    decoded = _execution_result_from_proto(wire, chain="bsc", expected_plan_hash="a" * 64)
    marker = StepSubmissionEvidence(
        step_index=0,
        chain="bsc",
        execution_plan_hash=decoded.execution_plan_hash,
        submission_provenance=decoded.submission_provenance,
        submitted_transaction_ids=decoded.tx_hashes,
        submission_transactions=decoded.submission_transactions,
    )
    restored = StepSubmissionEvidence.from_dict(marker.to_dict())
    assert prove(items=restored.submission_transactions) == (receipt(ACTION),)


def test_prefix_variants_do_not_hide_duplicate_submitted_transactions():
    with pytest.raises(PlanCompletionUnproven):
        prove(submitted_tx_ids=[ACTION, ACTION.removeprefix("0x")])


def test_real_anvil_canonical_payload_completes_its_original_two_transaction_plan():
    from almanak.framework.execution.receipt_observation import decode_canonical_receipt
    from almanak.framework.runner.runner_models import StepSubmissionEvidence
    from almanak.gateway.proto import gateway_pb2

    fixture = json.loads((Path(__file__).parent / "fixtures/bstocks_canonical_recovery.json").read_text())
    marker = StepSubmissionEvidence.from_dict(fixture["submission_evidence"])
    observations = []
    for status in fixture["canonical_statuses"]:
        raw = status["receipt"]
        wire = gateway_pb2.TxStatus(
            status=status["status"],
            block_number=raw["block_number"],
            gas_used=raw["gas_used"],
            canonical_receipt=json.dumps(raw).encode(),
        )
        observations.append(decode_canonical_receipt(wire, status["tx"]))
    ordered = prove_completed_evm_plan(
        expected_plan_hash=marker.execution_plan_hash,
        observed_plan_hash=marker.execution_plan_hash,
        provenance=marker.submission_provenance,
        submitted_tx_ids=marker.submitted_transaction_ids,
        evidence=marker.submission_transactions,
        receipts=list(reversed(observations)),
    )
    assert ordered == tuple(observations)
    assert sum(item.gas_cost_wei for item in ordered) == 259325000000000
    assert len(ordered[1].logs) == 5


@pytest.mark.parametrize("identity", ["11" * 31, "gg" * 32, " " + "11" * 32])
def test_malformed_unprefixed_transaction_evidence_is_refused(identity):
    with pytest.raises(PlanCompletionUnproven):
        prove(items=[evidence(identity)])


@pytest.mark.parametrize(
    "changes",
    [
        {"observed_plan_hash": "b" * 64},
        {"expected_plan_hash": ""},
        {"provenance": SubmissionProvenance.UNSPECIFIED},
        {"provenance": SubmissionProvenance.NOT_ATTEMPTED},
        {"submitted_tx_ids": []},
        {"submitted_tx_ids": [ACTION, ACTION]},
    ],
)
def test_unknown_or_contradictory_plan_identity_is_not_success(changes):
    with pytest.raises(PlanCompletionUnproven):
        prove(**changes)


@pytest.mark.parametrize(
    "item",
    [
        evidence(APPROVAL, (0,), 2),
        evidence(ACTION, (), 0),
        evidence(ACTION, (0,), True),
        evidence(ACTION, (True,), 1),
        evidence(ACTION, (1,), 1),
        evidence(ACTION, (0, 0), 1),
        evidence(ACTION, (0, 1), 2),
        evidence(ACTION, (0,), 2**32 - 1),
    ],
)
def test_successful_retained_hash_does_not_prove_complete_original_plan(item):
    with pytest.raises(PlanCompletionUnproven):
        prove([item], [receipt(item.tx_id)], submitted_tx_ids=[item.tx_id])


@pytest.mark.parametrize(
    "items",
    [
        [evidence(APPROVAL, (0,), 2), evidence(ACTION, (0,), 2)],
        [evidence(APPROVAL, (0,), 2), evidence(ACTION, (1,), 3)],
        [evidence(APPROVAL, (0,), 2), evidence(APPROVAL, (1,), 2)],
        [evidence(APPROVAL, (0,), 2), evidence(ACTION, (1,), 2, SAFE)],
    ],
)
def test_ambiguous_coverage_is_not_recovered(items):
    with pytest.raises(PlanCompletionUnproven):
        prove(items, [receipt(APPROVAL), receipt(ACTION)], submitted_tx_ids=[APPROVAL, ACTION])


@pytest.mark.parametrize(
    "observations",
    [
        [],
        [receipt(APPROVAL)],
        [receipt(ACTION), receipt(ACTION)],
        [replace(receipt(ACTION), status=0)],
        [replace(receipt(ACTION), block_hash="0x01")],
        [replace(receipt(ACTION), gas_used=-1)],
    ],
)
def test_incomplete_or_reverted_receipts_cannot_advance_completion(observations):
    with pytest.raises(PlanCompletionUnproven):
        prove(observations=observations)


@pytest.mark.parametrize(
    "event,indexed",
    [
        ("ExecutionSuccess", False),
        ("ExecutionSuccess", True),
        ("ExecutionFromModuleSuccess", True),
    ],
)
def test_safe_inner_success_for_supported_event_layouts(event, indexed):
    observed = receipt(ACTION, logs=[safe_log(event, indexed)])
    assert prove([evidence(safe=SAFE)], [observed]) == (observed,)
    assert prove([evidence(indices=(0, 1), count=2, safe=SAFE)], [observed]) == (observed,)


@pytest.mark.parametrize(
    "logs",
    [
        [],
        [safe_log("ExecutionFailure")],
        [safe_log("ExecutionFromModuleFailure")],
        [safe_log(address="0x" + "99" * 20)],
        [safe_log(), safe_log()],
        [safe_log(), safe_log("ExecutionFailure")],
        [{**safe_log(), "removed": True}],
        [{**safe_log(), "data": "0x01"}],
        [{**safe_log(), "topics": []}],
        [{**safe_log(), "topics": [safe_log()["topics"][0], "0x00"]}],
    ],
)
def test_outer_safe_success_is_insufficient_without_unique_valid_inner_success(logs):
    with pytest.raises(PlanCompletionUnproven):
        prove([evidence(safe=SAFE)], [receipt(ACTION, logs=logs)])
