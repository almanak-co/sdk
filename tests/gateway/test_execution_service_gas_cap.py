"""ExecutionService gas cap isolation tests."""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.framework.execution.submission import SubmissionProvenance
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer


def _request(max_gas_price_gwei: int, *, transactions: list[dict] | None = None) -> gateway_pb2.ExecuteRequest:
    return gateway_pb2.ExecuteRequest(
        action_bundle=json.dumps({"intent_type": "swap", "transactions": transactions or []}).encode("utf-8"),
        dry_run=True,
        simulation_enabled=False,
        deployment_id="s1",
        intent_id="i1",
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        max_gas_price_gwei=max_gas_price_gwei,
    )


def _result() -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        transaction_results=[],
        total_gas_used=0,
        correlation_id="cid",
        error="",
    )


class _SerializableReceipt:
    def __init__(self, tx_hash: str, status: int = 1) -> None:
        self.tx_hash = tx_hash
        self.status = status

    def to_dict(self) -> dict:
        return {
            "tx_hash": self.tx_hash,
            "block_number": 42,
            "block_hash": "0xblock",
            "status": self.status,
            "gas_used": 21_000,
            "effective_gas_price": 1,
            "logs": [],
        }


class _BrokenReceipt:
    def to_dict(self) -> dict:
        raise TypeError("deliberate receipt serialization failure")


@pytest.mark.asyncio
async def test_execute_uses_request_cap_and_resets_to_default():
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    seen_caps: list[int] = []

    async def _execute_side_effect(*_args, **_kwargs):
        seen_caps.append(orchestrator.tx_risk_config.max_gas_price_gwei)
        return _result()

    orchestrator.execute = AsyncMock(side_effect=_execute_side_effect)
    service._get_orchestrator = AsyncMock(return_value=orchestrator)

    context = MagicMock()

    response_one = await service.Execute(_request(5), context)
    response_two = await service.Execute(_request(0), context)

    assert response_one.success
    assert response_two.success
    assert seen_caps == [5, 42]
    assert orchestrator.tx_risk_config.max_gas_price_gwei == 42


@pytest.mark.asyncio
async def test_execute_serializes_concurrent_requests_per_orchestrator():
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    seen_caps: list[int] = []

    async def _execute_side_effect(*_args, **_kwargs):
        seen_caps.append(orchestrator.tx_risk_config.max_gas_price_gwei)
        await asyncio.sleep(0.05)
        return _result()

    orchestrator.execute = AsyncMock(side_effect=_execute_side_effect)
    service._get_orchestrator = AsyncMock(return_value=orchestrator)

    context = MagicMock()
    start = time.monotonic()
    await asyncio.gather(
        service.Execute(_request(5), context),
        service.Execute(_request(11), context),
    )
    elapsed = time.monotonic() - start

    assert sorted(seen_caps) == [5, 11]
    assert elapsed >= 0.09
    assert orchestrator.tx_risk_config.max_gas_price_gwei == 42


@pytest.mark.asyncio
async def test_execute_fails_closed_instead_of_skipping_one_unserializable_receipt():
    """Reachability control for the old 2 hashes / 1 receipt success response."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=True,
            transaction_results=[
                SimpleNamespace(tx_hash="0xaaa", receipt=_SerializableReceipt("0xaaa")),
                SimpleNamespace(tx_hash="0xbbb", receipt=_BrokenReceipt()),
                SimpleNamespace(tx_hash=None, receipt=None),
                SimpleNamespace(tx_hash=" ", receipt=None),
                SimpleNamespace(tx_hash=" 0xccc ", receipt=None),
            ],
            total_gas_used=42_000,
            correlation_id="cid",
            error="",
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)
    context = MagicMock()

    response = await service.Execute(_request(5), context)

    assert not response.success
    assert response.error_code == "RECEIPT_SET_INCOMPLETE"
    assert "deliberate receipt serialization failure" in response.error
    assert list(response.tx_hashes) == ["0xaaa", "0xbbb", "0xccc"]
    assert response.receipts == b""
    context.set_code.assert_not_called()


@pytest.mark.asyncio
async def test_execute_fails_closed_when_a_transaction_result_has_no_receipt():
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=True,
            transaction_results=[SimpleNamespace(tx_hash="0xaaa", receipt=None)],
            total_gas_used=21_000,
            correlation_id="cid",
            error="",
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)
    context = MagicMock()

    response = await service.Execute(_request(5), context)

    assert not response.success
    assert response.error_code == "RECEIPT_SET_INCOMPLETE"
    assert list(response.tx_hashes) == ["0xaaa"]
    assert "has no receipt" in response.error
    context.set_code.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt_hashes", [("0xbbb",), ("0xaaa", "0xAAA")])
async def test_execute_fails_closed_on_mismatched_or_duplicate_receipt_identity(receipt_hashes: tuple[str, ...]):
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()
    transaction_results = [SimpleNamespace(tx_hash="0xaaa", receipt=_SerializableReceipt(receipt_hashes[0]))]
    if len(receipt_hashes) == 2:
        transaction_results.append(SimpleNamespace(tx_hash="0xAAA", receipt=_SerializableReceipt(receipt_hashes[1])))
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=True,
            transaction_results=transaction_results,
            total_gas_used=21_000 * len(transaction_results),
            correlation_id="cid",
            error="",
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)

    response = await service.Execute(_request(5), MagicMock())

    assert not response.success
    assert response.error_code == "RECEIPT_SET_INCOMPLETE"


@pytest.mark.asyncio
async def test_execute_returns_ordered_approval_and_reverted_action_evidence():
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    approval_receipt = _SerializableReceipt("0xapprove")
    reverted_receipt = _SerializableReceipt("0xaction")
    reverted_receipt.to_dict = MagicMock(
        return_value={
            "tx_hash": "0xaction",
            "block_number": 101,
            "block_hash": "0xreverted-block",
            "status": 0,
            "gas_used": 152_000,
            "effective_gas_price": 7,
            "logs": [{"address": "0xpool", "data": "0xdead"}],
        }
    )
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=False,
            transaction_results=[
                SimpleNamespace(tx_hash="0xapprove", receipt=approval_receipt, transaction_index=0),
                SimpleNamespace(tx_hash="0xaction", receipt=reverted_receipt, transaction_index=1),
            ],
            total_gas_used=197_000,
            correlation_id="cid",
            error="swap reverted",
            submission_provenance="ATTEMPTED",
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)
    transactions = [
        {"to": "0xtoken", "data": "0x095ea7b3" + "00" * 64, "value": 0, "tx_type": "approve"},
        {"to": "0xrouter", "data": "0x12345678", "value": 0, "tx_type": "swap"},
    ]

    response = await service.Execute(_request(5, transactions=transactions), MagicMock())
    receipts = json.loads(response.receipts)

    assert not response.success
    assert response.error_code == ""
    assert list(response.tx_hashes) == ["0xapprove", "0xaction"]
    assert [receipt["status"] for receipt in receipts] == [1, 0]
    assert receipts[1]["block_hash"] == "0xreverted-block"
    assert receipts[1]["block_number"] == 101
    assert receipts[1]["gas_used"] == 152_000
    assert receipts[1]["logs"] == [{"address": "0xpool", "data": "0xdead"}]
    assert [item.role for item in response.submission_transactions] == [
        gateway_pb2.EXECUTION_TRANSACTION_ROLE_SETUP_APPROVAL,
        gateway_pb2.EXECUTION_TRANSACTION_ROLE_ACTION,
    ]


@pytest.mark.asyncio
async def test_execute_keeps_submitted_false_approval_hash_unknown_and_non_replayable():
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=False,
            transaction_results=[
                SimpleNamespace(tx_hash="0xambiguous", receipt=None, transaction_index=None),
            ],
            total_gas_used=0,
            correlation_id="cid",
            error="connection reset after broadcast attempt",
            submission_provenance="ATTEMPTED",
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)
    approval = {"to": "0xtoken", "data": "0x095ea7b3" + "00" * 64, "value": 0, "tx_type": "approve"}

    response = await service.Execute(_request(5, transactions=[approval]), MagicMock())

    assert not response.success
    assert response.error_code == "RECEIPT_SET_INCOMPLETE"
    assert list(response.tx_hashes) == ["0xambiguous"]
    assert response.submission_transactions[0].role == gateway_pb2.EXECUTION_TRANSACTION_ROLE_UNSPECIFIED
    assert response.submission_transactions[0].replay_policy == gateway_pb2.REPLAY_POLICY_NEVER


@pytest.mark.asyncio
async def test_execute_anvil_forces_simulation_and_preserves_execution_identity():
    service = ExecutionServiceServicer(GatewaySettings(network="anvil"))
    service._ensure_initialized = AsyncMock()
    orchestrator = MagicMock()
    orchestrator.signer = object()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(return_value=_result())
    service._get_orchestrator = AsyncMock(return_value=orchestrator)

    response = await service.Execute(_request(0), MagicMock())

    assert response.success
    action_bundle, exec_context = orchestrator.execute.await_args.args
    assert action_bundle.intent_type == "swap"
    assert exec_context.deployment_id == "s1"
    assert exec_context.intent_id == "i1"
    assert exec_context.chain == "arbitrum"
    assert exec_context.wallet_address == "0x1234567890123456789012345678901234567890"
    assert exec_context.simulation_enabled is True
    assert exec_context.dry_run is True


@pytest.mark.asyncio
async def test_execute_restores_gas_cap_when_orchestrator_raises():
    service = ExecutionServiceServicer(GatewaySettings())
    service._ensure_initialized = AsyncMock()
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(side_effect=RuntimeError("submission boundary failed"))
    service._get_orchestrator = AsyncMock(return_value=orchestrator)
    context = MagicMock()

    response = await service.Execute(_request(5), context)

    assert not response.success
    assert response.error == "submission boundary failed"
    assert response.error_code == "EXECUTION_FAILED"
    assert response.submission_provenance == gateway_pb2.SUBMISSION_PROVENANCE_UNSPECIFIED
    assert orchestrator.tx_risk_config.max_gas_price_gwei == 42
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    context.set_details.assert_called_once_with("submission boundary failed")


@pytest.mark.asyncio
async def test_execute_serializes_proven_revert_without_losing_submission_evidence():
    service = ExecutionServiceServicer(GatewaySettings())
    service._ensure_initialized = AsyncMock()
    orchestrator = MagicMock()
    orchestrator.signer = object()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=False,
            transaction_results=[SimpleNamespace(tx_hash="0xaaa", receipt=_SerializableReceipt("0xaaa", status=0))],
            total_gas_used=21_000,
            correlation_id="cid",
            error="transaction reverted",
            submission_provenance=SubmissionProvenance.ATTEMPTED,
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)

    response = await service.Execute(_request(5), MagicMock())

    assert not response.success
    assert list(response.tx_hashes) == ["0xaaa"]
    assert json.loads(response.receipts) == [_SerializableReceipt("0xaaa", status=0).to_dict()]
    assert response.total_gas_used == 21_000
    assert response.execution_id == "cid"
    assert response.error == "transaction reverted"
    assert response.error_code == ""
    assert response.submission_provenance == gateway_pb2.SUBMISSION_PROVENANCE_ATTEMPTED


@pytest.mark.asyncio
async def test_execute_certifies_multitransaction_safe_as_one_atomic_action():
    class SafeSignerMarker:
        pass

    service = ExecutionServiceServicer(GatewaySettings())
    service._ensure_initialized = AsyncMock()
    orchestrator = MagicMock()
    orchestrator.signer = SafeSignerMarker()
    orchestrator.tx_risk_config = SimpleNamespace(max_gas_price_gwei=42)
    orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(
            success=True,
            transaction_results=[SimpleNamespace(tx_hash="0xsafe", receipt=_SerializableReceipt("0xsafe"))],
            total_gas_used=21_000,
            correlation_id="cid",
            error="",
            submission_provenance=SubmissionProvenance.ATTEMPTED,
        )
    )
    service._get_orchestrator = AsyncMock(return_value=orchestrator)
    request = _request(5)
    request.action_bundle = json.dumps(
        {
            "intent_type": "swap",
            "transactions": [
                {"tx_type": "approve", "data": "0x095ea7b3" + "0" * 128, "value": 0},
                {"tx_type": "swap", "data": "0x1234", "value": 0},
            ],
        }
    ).encode("utf-8")

    with patch("almanak.framework.execution.signer.safe.base.SafeSigner", SafeSignerMarker):
        response = await service.Execute(request, MagicMock())

    assert response.success
    submitted_bundle = orchestrator.execute.await_args.args[0]
    assert [transaction["tx_type"] for transaction in submitted_bundle.transactions] == ["approve", "swap"]
    assert list(response.tx_hashes) == ["0xsafe"]
    assert len(response.submission_transactions) == 1
    evidence = response.submission_transactions[0]
    assert evidence.tx_id == "0xsafe"
    assert evidence.role == gateway_pb2.EXECUTION_TRANSACTION_ROLE_ACTION
    assert evidence.replay_policy == gateway_pb2.REPLAY_POLICY_NEVER
