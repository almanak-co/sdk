"""Receipt capability skew and envelope validation at the strategy boundary."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator
from almanak.framework.execution.interfaces import SubmissionError, TransactionRevertedError
from almanak.framework.execution.receipt_observation import decode_canonical_receipt
from almanak.gateway.proto import gateway_pb2

TX = "0x" + "12" * 32


def response(status=1):
    return gateway_pb2.TxStatus(
        status="confirmed" if status else "reverted",
        block_number=7,
        gas_used=21_000,
        canonical_receipt=json.dumps(
            {
                "tx_hash": TX,
                "block_number": 7,
                "block_hash": "0x" + "34" * 32,
                "gas_used": 21_000,
                "effective_gas_price": "6",
                "status": status,
                "logs": [],
            }
        ).encode(),
    )


@pytest.mark.parametrize("status", [0, 1])
def test_complete_matching_receipt_is_decoded_without_inventing_l1_fee(status):
    receipt = decode_canonical_receipt(response(status), TX)
    assert receipt.status == status
    assert receipt.gas_cost_wei == 126_000
    assert receipt.l1_fee_wei is None


@pytest.mark.parametrize("reply", [SimpleNamespace(status="confirmed"), gateway_pb2.TxStatus(status="reverted")])
def test_old_gateway_status_without_receipt_cannot_authorize_recovery(reply):
    with pytest.raises(SubmissionError) as raised:
        decode_canonical_receipt(reply, TX)
    assert raised.value.tx_hash == TX


@pytest.mark.parametrize("field,value", [("status", "pending"), ("block_number", 8), ("gas_used", 0)])
def test_envelope_contradiction_refuses_recovery(field, value):
    reply = response()
    setattr(reply, field, value)
    with pytest.raises(SubmissionError):
        decode_canonical_receipt(reply, TX)


@pytest.mark.parametrize("payload", [b"invalid", b"[]", b"{}", b"null"])
def test_malformed_payload_refuses_recovery(payload):
    reply = response()
    reply.canonical_receipt = payload
    with pytest.raises(SubmissionError):
        decode_canonical_receipt(reply, TX)


@pytest.mark.parametrize("field,value", [("tx_hash", "0x" + "99" * 32), ("block_hash", "0x00"), ("logs", None)])
def test_receipt_identity_and_completeness_cannot_be_inferred(field, value):
    reply = response()
    raw = json.loads(reply.canonical_receipt)
    raw[field] = value
    reply.canonical_receipt = json.dumps(raw).encode()
    with pytest.raises(SubmissionError):
        decode_canonical_receipt(reply, TX)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [0, 1])
async def test_gateway_orchestrator_receipt_reads_never_execute_or_sign(status):
    client = MagicMock()
    client.execution.GetTransactionStatus.return_value = response(status)
    orchestrator = GatewayExecutionOrchestrator(client, chain="bsc")
    if status:
        receipt = await orchestrator.get_receipt(TX, timeout=15)
        assert receipt.gas_cost_wei == 126_000
    else:
        with pytest.raises(TransactionRevertedError) as raised:
            await orchestrator.get_receipt(TX, timeout=15)
        assert raised.value.receipt.gas_cost_wei == 126_000
    client.execution.GetTransactionStatus.assert_called_once_with(
        gateway_pb2.TxStatusRequest(tx_hash=TX, chain="bsc"), timeout=15
    )
    client.execution.Execute.assert_not_called()
    client.execution.CompileIntent.assert_not_called()


@pytest.mark.asyncio
async def test_complete_plan_observation_uses_only_gateway_receipt_reads():
    from almanak.framework.execution.submission import SubmissionProvenance, SubmissionTransactionEvidence

    client = MagicMock()
    client.execution.GetTransactionStatus.return_value = response()
    orchestrator = GatewayExecutionOrchestrator(client, chain="bsc")
    observed = await orchestrator.get_completed_plan_receipts(
        expected_plan_hash="a" * 64,
        observed_plan_hash="a" * 64,
        provenance=SubmissionProvenance.ATTEMPTED,
        submitted_tx_ids=[TX],
        evidence=[SubmissionTransactionEvidence(TX, plan_indices=(0,), plan_transaction_count=1)],
    )
    assert [receipt.tx_hash for receipt in observed] == [TX]
    assert client.execution.method_calls[0][0] == "GetTransactionStatus"
    assert len(client.execution.method_calls) == 1


@pytest.mark.asyncio
async def test_complete_plan_observation_never_promotes_landed_approval_only():
    from almanak.framework.execution.plan_completion import PlanCompletionUnproven
    from almanak.framework.execution.submission import SubmissionProvenance, SubmissionTransactionEvidence

    client = MagicMock()
    client.execution.GetTransactionStatus.return_value = response()
    orchestrator = GatewayExecutionOrchestrator(client, chain="bsc")
    with pytest.raises(PlanCompletionUnproven):
        await orchestrator.get_completed_plan_receipts(
            expected_plan_hash="a" * 64,
            observed_plan_hash="a" * 64,
            provenance=SubmissionProvenance.ATTEMPTED,
            submitted_tx_ids=[TX],
            evidence=[SubmissionTransactionEvidence(TX, plan_indices=(0,), plan_transaction_count=2)],
        )
    client.execution.Execute.assert_not_called()


@pytest.mark.asyncio
async def test_complete_plan_observation_has_one_deadline_and_propagates_cancellation():
    import asyncio
    from unittest.mock import AsyncMock

    from almanak.framework.execution.submission import SubmissionProvenance

    async def pending(*args, **kwargs):
        await asyncio.Event().wait()

    orchestrator = GatewayExecutionOrchestrator(MagicMock(), chain="bsc")
    orchestrator.get_receipt = AsyncMock(side_effect=pending)
    arguments = {
        "expected_plan_hash": "a" * 64,
        "observed_plan_hash": "a" * 64,
        "provenance": SubmissionProvenance.ATTEMPTED,
        "submitted_tx_ids": [TX],
        "evidence": [],
    }
    with pytest.raises(TimeoutError):
        await orchestrator.get_completed_plan_receipts(**arguments, timeout=0.01)
    task = asyncio.create_task(orchestrator.get_completed_plan_receipts(**arguments))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_plan_observation_cancellation_precedes_sibling_error():
    import asyncio
    from unittest.mock import AsyncMock

    from almanak.framework.execution.submission import SubmissionProvenance

    orchestrator = GatewayExecutionOrchestrator(MagicMock(), chain="bsc")
    orchestrator.get_receipt = AsyncMock(side_effect=[SubmissionError(reason="unobserved"), asyncio.CancelledError()])
    with pytest.raises(asyncio.CancelledError):
        await orchestrator.get_completed_plan_receipts(
            expected_plan_hash="a" * 64,
            observed_plan_hash="a" * 64,
            provenance=SubmissionProvenance.ATTEMPTED,
            submitted_tx_ids=[TX, "0x" + "56" * 32],
            evidence=[],
        )
