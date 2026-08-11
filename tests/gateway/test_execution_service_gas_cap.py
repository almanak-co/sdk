"""ExecutionService gas cap isolation tests."""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer


def _request(max_gas_price_gwei: int) -> gateway_pb2.ExecuteRequest:
    return gateway_pb2.ExecuteRequest(
        action_bundle=json.dumps({"intent_type": "swap", "transactions": []}).encode("utf-8"),
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
    def __init__(self, tx_hash: str) -> None:
        self.tx_hash = tx_hash

    def to_dict(self) -> dict:
        return {
            "tx_hash": self.tx_hash,
            "block_number": 42,
            "block_hash": "0xblock",
            "status": 1,
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
    assert list(response.tx_hashes) == ["0xaaa", "0xbbb"]
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
