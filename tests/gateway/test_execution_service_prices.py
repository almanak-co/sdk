"""ExecutionService price_map handling tests.

Validates that:
- CompileIntent applies real prices from price_map to the cached compiler
- Empty/missing price_map uses placeholder prices (backward compat)
- Compiler state is restored after compilation (cached compiler not corrupted)
- String->Decimal parsing is correct
"""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents.compiler import CompilationStatus
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer


def _make_compile_request(
    price_map: dict[str, str] | None = None,
) -> gateway_pb2.CompileIntentRequest:
    """Build a CompileIntentRequest with optional price_map."""
    intent_data = json.dumps({"token_in": "USDC", "token_out": "ETH", "amount": "100"}).encode("utf-8")
    return gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        price_map=price_map or {},
    )


def _make_compilation_result(success: bool = True):
    """Build a mock CompilationResult with real CompilationStatus."""
    bundle = MagicMock()
    bundle.to_dict.return_value = {"intent_type": "swap", "transactions": []}
    result = MagicMock()
    result.status = CompilationStatus.SUCCESS if success else CompilationStatus.FAILED
    result.action_bundle = bundle if success else None
    result.error = None if success else "compilation failed"
    return result


@pytest.mark.asyncio
async def test_compile_with_price_map_uses_real_prices():
    """Verify compiler.price_oracle is set from parsed price_map."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    compiler.price_oracle = {"ETH": Decimal("2000")}  # placeholder
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()

    # Track what prices were set during compile()
    seen_prices = {}
    seen_placeholders = {}

    original_compile = compiler.compile

    def capture_state(intent):
        seen_prices["value"] = dict(compiler.price_oracle) if compiler.price_oracle else None
        seen_placeholders["value"] = compiler._using_placeholders
        return original_compile(intent)

    compiler.compile = capture_state
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    request = _make_compile_request(price_map={"ETH": "3400.50", "USDC": "1.0001"})

    await service.CompileIntent(request, context)

    # During compile, prices should have been the parsed real prices
    assert seen_prices["value"] == {"ETH": Decimal("3400.50"), "USDC": Decimal("1.0001")}
    assert seen_placeholders["value"] is False


@pytest.mark.asyncio
async def test_compile_without_price_map_uses_placeholders():
    """Empty price_map preserves placeholder state (backward compat)."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    original_oracle = {"ETH": Decimal("2000")}
    compiler.price_oracle = original_oracle
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()

    seen_placeholders = {}

    original_compile = compiler.compile

    def capture_state(intent):
        seen_placeholders["value"] = compiler._using_placeholders
        return original_compile(intent)

    compiler.compile = capture_state
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    request = _make_compile_request(price_map={})  # empty map

    await service.CompileIntent(request, context)

    # Should still be using placeholders
    assert seen_placeholders["value"] is True


@pytest.mark.asyncio
async def test_compile_restores_price_oracle_after_call():
    """Cached compiler state is restored after compilation, even if compile raises."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    original_oracle = {"ETH": Decimal("2000")}
    compiler.price_oracle = original_oracle
    compiler._using_placeholders = True

    # Make compile raise to test finally block
    compiler.compile.side_effect = RuntimeError("compilation error")
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    request = _make_compile_request(price_map={"ETH": "3400"})

    await service.CompileIntent(request, context)

    # Compiler state should be restored even after error
    assert compiler.price_oracle is original_oracle
    assert compiler._using_placeholders is True


@pytest.mark.asyncio
async def test_compile_with_empty_price_map_uses_placeholders():
    """Explicitly empty price_map = no prices = backward compatible."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()

    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    request = _make_compile_request()  # no price_map at all

    await service.CompileIntent(request, context)

    # price_oracle should not have been modified
    assert compiler.price_oracle is None
    assert compiler._using_placeholders is True


@pytest.mark.asyncio
async def test_price_map_decimal_parsing():
    """String->Decimal conversion is precise."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()

    captured_oracle = {}

    original_compile = compiler.compile

    def capture_oracle(intent):
        captured_oracle.update(compiler.price_oracle or {})
        return original_compile(intent)

    compiler.compile = capture_oracle
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    # Test various decimal precisions
    request = _make_compile_request(
        price_map={
            "ETH": "3421.123456789",
            "BTC": "67000",
            "USDC": "0.9999",
        }
    )

    await service.CompileIntent(request, context)

    assert captured_oracle["ETH"] == Decimal("3421.123456789")
    assert captured_oracle["BTC"] == Decimal("67000")
    assert captured_oracle["USDC"] == Decimal("0.9999")


@pytest.mark.asyncio
async def test_invalid_price_map_returns_invalid_argument():
    """Invalid price_map values return INVALID_ARGUMENT, not INTERNAL."""
    import grpc

    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    context = MagicMock()
    # Price map with non-numeric value
    request = _make_compile_request(price_map={"ETH": "not_a_number"})

    result = await service.CompileIntent(request, context)

    assert result.success is False
    assert result.error_code == "INVALID_PRICE_MAP"
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.asyncio
async def test_non_finite_price_map_returns_invalid_argument():
    """NaN, Infinity, and zero prices are rejected as INVALID_ARGUMENT."""
    import grpc

    settings = GatewaySettings()

    for bad_value in ["NaN", "Infinity", "-Infinity", "0", "-1"]:
        service = ExecutionServiceServicer(settings)
        service._ensure_initialized = AsyncMock()

        context = MagicMock()
        request = _make_compile_request(price_map={"ETH": bad_value})

        result = await service.CompileIntent(request, context)

        assert result.success is False, f"Expected failure for price={bad_value}"
        assert result.error_code == "INVALID_PRICE_MAP", f"Wrong error_code for price={bad_value}"
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
