"""Tests for gateway CompileIntent validation -- INVALID_ARGUMENT error handling.

Covers:
- pydantic.ValidationError (e.g., raw float for SafeDecimal field) -> INVALID_INTENT_DATA
- ValueError for unknown intent_type -> INVALID_INTENT_TYPE
"""

from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.execution_service import ExecutionServiceServicer


@pytest.fixture
def service():
    return ExecutionServiceServicer(GatewaySettings())


@pytest.fixture
def grpc_context():
    ctx = MagicMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.mark.asyncio
async def test_compile_intent_invalid_data_returns_invalid_argument(service, grpc_context):
    """A raw float in a SafeDecimal field surfaces as INVALID_ARGUMENT with INVALID_INTENT_DATA code."""
    from almanak.gateway.proto import gateway_pb2
    import json

    # max_slippage as raw float triggers SafeDecimal ValidationError
    intent_data = {
        "from_token": "USDC",
        "to_token": "ETH",
        "amount": "1000",
        "chain": "arbitrum",
        "max_slippage": 0.03,  # raw float -- Pydantic SafeDecimal rejects this
    }
    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=json.dumps(intent_data).encode(),
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
    )

    result = await service.CompileIntent(request, grpc_context)

    assert not result.success
    assert result.error_code == "INVALID_INTENT_DATA"
    grpc_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.asyncio
async def test_compile_intent_unknown_type_returns_invalid_argument(service, grpc_context):
    """An unknown intent_type surfaces as INVALID_ARGUMENT with INVALID_INTENT_TYPE code."""
    from almanak.gateway.proto import gateway_pb2
    import json

    request = gateway_pb2.CompileIntentRequest(
        intent_type="totally_made_up_intent",
        intent_data=json.dumps({}).encode(),
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
    )

    result = await service.CompileIntent(request, grpc_context)

    assert not result.success
    assert result.error_code == "INVALID_INTENT_TYPE"
    grpc_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
