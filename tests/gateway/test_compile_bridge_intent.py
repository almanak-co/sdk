"""Gateway tests for Bridge intent compilation path."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.intents.compiler import CompilationResult, CompilationStatus
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer


@pytest.mark.asyncio
async def test_compile_intent_accepts_bridge_intent_type() -> None:
    """CompileIntent should deserialize and compile bridge intents end-to-end."""
    service = ExecutionServiceServicer(GatewaySettings())
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    compiler.compile.return_value = CompilationResult(
        status=CompilationStatus.SUCCESS,
        action_bundle=ActionBundle(
            intent_type="BRIDGE",
            transactions=[{"to": "0x1", "value": "0", "data": "0x", "gas_estimate": 1, "tx_type": "bridge"}],
            metadata={"bridge": "Across", "from_chain": "base", "to_chain": "arbitrum"},
        ),
        intent_id="i-bridge",
    )
    service._get_compiler = MagicMock(return_value=compiler)

    request = gateway_pb2.CompileIntentRequest(
        intent_type="bridge",
        intent_data=json.dumps(
            {
                "token": "USDC",
                "amount": "100",
                "from_chain": "base",
                "to_chain": "arbitrum",
            }
        ).encode("utf-8"),
        chain="base",
        wallet_address="0x1111111111111111111111111111111111111111",
    )
    context = MagicMock()

    response = await service.CompileIntent(request, context)

    assert response.success is True
    bundle = json.loads(response.action_bundle.decode("utf-8"))
    assert bundle["intent_type"] == "BRIDGE"
    assert bundle["metadata"]["bridge"] == "Across"
