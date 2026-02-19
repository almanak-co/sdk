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
        strategy_id="s1",
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
