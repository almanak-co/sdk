"""Pin-down tests for CompileIntent seams ahead of the cc-reduction refactor.

Pins the behaviors that sit exactly on the decomposition boundaries: the
try/finally restore_prices contract when compile() raises, the all-tokens
coverage requirement of the mainnet self-serve gate, the branch-specific
error message when no token symbols are extractable, and the
empty-wallet-address validation skip.
"""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.intents.compiler import CompilationStatus
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer


def _request(intent_type: str = "swap", wallet: str = "0x1234567890123456789012345678901234567890"):
    intent_data = json.dumps({"token_in": "USDC", "token_out": "ETH", "amount": "100"}).encode("utf-8")
    return gateway_pb2.CompileIntentRequest(
        intent_type=intent_type,
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address=wallet,
        price_map={},
    )


def _success_result():
    bundle = MagicMock()
    bundle.to_dict.return_value = {"intent_type": "swap", "transactions": []}
    bundle.sensitive_data = None
    result = MagicMock()
    result.status = CompilationStatus.SUCCESS
    result.action_bundle = bundle
    result.error = None
    return result


def _service(network: str = "anvil") -> ExecutionServiceServicer:
    service = ExecutionServiceServicer(GatewaySettings(network=network))
    service._ensure_initialized = AsyncMock()
    service._create_intent = MagicMock(return_value=MagicMock())
    return service


def _compiler(compile_result=None):
    compiler = MagicMock()
    compiler.price_oracle = {"ETH": Decimal("2000")}
    compiler._using_placeholders = True
    if compile_result is not None:
        compiler.compile.return_value = compile_result
    return compiler


@pytest.mark.asyncio
async def test_restore_prices_called_when_compile_raises():
    """The finally-side restore runs with the captured originals on failure."""
    service = _service()
    compiler = _compiler()
    original_oracle = compiler.price_oracle
    compiler.compile.side_effect = RuntimeError("boom")
    service._get_compiler = MagicMock(return_value=compiler)

    context = MagicMock()
    result = await service.CompileIntent(_request(), context)

    assert result.success is False
    assert result.error_code == "COMPILATION_FAILED"
    context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
    compiler.restore_prices.assert_called_once_with(original_oracle, True)


@pytest.mark.asyncio
async def test_mainnet_partial_self_serve_coverage_fails_closed():
    """Prices for some-but-not-all extracted tokens must not compile."""
    service = _service(network="mainnet")
    compiler = _compiler()
    service._get_compiler = MagicMock(return_value=compiler)
    service._extract_token_symbols_from_intent = MagicMock(return_value=["USDC", "WETH"])
    service._fetch_prices_for_tokens = AsyncMock(return_value={"USDC": Decimal("1")})

    result = await service.CompileIntent(_request(), MagicMock())

    assert result.success is False
    assert result.error_code == "NO_PRICES_AVAILABLE"
    compiler.compile.assert_not_called()
    compiler.update_prices.assert_not_called()


@pytest.mark.asyncio
async def test_mainnet_no_extractable_tokens_uses_symbol_error_message():
    """Non-close intent with no extractable tokens gets the symbol-extraction error."""
    service = _service(network="mainnet")
    compiler = _compiler()
    service._get_compiler = MagicMock(return_value=compiler)
    service._extract_token_symbols_from_intent = MagicMock(return_value=[])

    result = await service.CompileIntent(_request(intent_type="withdraw"), MagicMock())

    assert result.success is False
    assert result.error_code == "NO_PRICES_AVAILABLE"
    assert "Could not extract token symbols" in result.error
    compiler.compile.assert_not_called()


@pytest.mark.asyncio
async def test_empty_wallet_address_skips_address_validation():
    """Empty wallet_address bypasses chain-address validation and still compiles."""
    service = _service()
    compiler = _compiler(_success_result())
    service._get_compiler = MagicMock(return_value=compiler)

    result = await service.CompileIntent(_request(wallet=""), MagicMock())

    assert result.success is True
    service._get_compiler.assert_called_once_with("arbitrum", "")
