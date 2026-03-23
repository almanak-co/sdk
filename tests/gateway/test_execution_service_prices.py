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
    bundle.sensitive_data = None  # prevent MagicMock auto-attr from being JSON-serialized
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

    # Make update_prices/restore_prices functional on the mock
    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices

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
    """Empty price_map preserves placeholder state (on non-mainnet networks)."""
    settings = GatewaySettings(network="anvil")
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    original_oracle = {"ETH": Decimal("2000")}
    compiler.price_oracle = original_oracle
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()

    # Make update_prices/restore_prices functional on the mock
    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices

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
@pytest.mark.parametrize(
    "intent_type",
    ["swap", "lpopen", "lp_open", "lp-open", "lpclose", "supply", "repay", "borrow", "withdraw", "perpopen", "perpclose"],
)
async def test_mainnet_no_prices_fails_for_price_sensitive_intents(intent_type):
    """On mainnet, price-sensitive intents MUST fail when no real prices available (VIB-523)."""
    settings = GatewaySettings(network="mainnet")
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    intent_data = json.dumps({"token_in": "USDC", "token_out": "ETH", "amount": "100"}).encode("utf-8")
    request = gateway_pb2.CompileIntentRequest(
        intent_type=intent_type,
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        price_map={},
    )

    result = await service.CompileIntent(request, context)

    assert result.success is False
    assert result.error_code == "NO_PRICES_AVAILABLE"
    assert "mainnet" in result.error
    compiler.compile.assert_not_called()


@pytest.mark.asyncio
async def test_mainnet_no_prices_allows_hold_intent():
    """On mainnet, non-price-sensitive intents (HOLD) still compile without prices."""
    settings = GatewaySettings(network="mainnet")
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.restore_prices = _restore_prices
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    intent_data = json.dumps({"reason": "waiting"}).encode("utf-8")
    request = gateway_pb2.CompileIntentRequest(
        intent_type="hold",
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        price_map={},
    )

    result = await service.CompileIntent(request, context)

    assert result.success is True


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

    # Make update_prices/restore_prices functional on the mock
    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices

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

    # Make update_prices/restore_prices functional on the mock
    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices

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

    # Make update_prices/restore_prices functional on the mock
    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices

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


# ---------------------------------------------------------------------------
# Self-serve price fetching tests (gateway fetches its own prices)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_token_symbols_from_intent():
    """_extract_token_symbols_from_intent extracts from_token and to_token."""
    intent = MagicMock()
    intent.from_token = "USDC"
    intent.to_token = "WETH"
    intent.token = None
    intent.collateral_token = None
    intent.borrow_token = None
    # Delete attrs that shouldn't exist so getattr falls through
    del intent.collateral_token
    del intent.borrow_token

    tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
    assert "USDC" in tokens
    assert "WETH" in tokens


@pytest.mark.asyncio
async def test_fetch_prices_returns_empty_without_market_servicer():
    """_fetch_prices_for_tokens returns empty when no market_servicer is set."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service.market_servicer = None

    result = await service._fetch_prices_for_tokens(["USDC", "WETH"])
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_prices_returns_empty_without_aggregator():
    """_fetch_prices_for_tokens returns empty when market_servicer has no _price_aggregator after init."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    mock_market = MagicMock(spec=[])  # No _price_aggregator attr
    mock_market._ensure_initialized = AsyncMock()
    service.market_servicer = mock_market

    result = await service._fetch_prices_for_tokens(["USDC", "WETH"])
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_prices_returns_decimals_from_aggregator():
    """_fetch_prices_for_tokens fetches from aggregator and returns Decimals."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)

    mock_aggregator = AsyncMock()
    price_result = MagicMock()
    price_result.price = 2100.50

    mock_aggregator.get_aggregated_price = AsyncMock(return_value=price_result)

    mock_market = MagicMock()
    mock_market._price_aggregator = mock_aggregator
    mock_market._ensure_initialized = AsyncMock()
    service.market_servicer = mock_market

    result = await service._fetch_prices_for_tokens(["WETH"])
    assert "WETH" in result
    assert isinstance(result["WETH"], Decimal)
    assert result["WETH"] == Decimal("2100.5")


@pytest.mark.asyncio
async def test_fetch_prices_handles_partial_failures():
    """_fetch_prices_for_tokens returns available prices even if some fail."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)

    mock_aggregator = AsyncMock()

    async def mock_get_price(token, quote):
        if token == "USDC":
            result = MagicMock()
            result.price = 1.0
            return result
        raise Exception("CoinGecko rate limited")

    mock_aggregator.get_aggregated_price = mock_get_price

    mock_market = MagicMock()
    mock_market._price_aggregator = mock_aggregator
    mock_market._ensure_initialized = AsyncMock()
    service.market_servicer = mock_market

    result = await service._fetch_prices_for_tokens(["USDC", "UNKNOWN_TOKEN"])
    assert "USDC" in result
    assert "UNKNOWN_TOKEN" not in result


@pytest.mark.asyncio
async def test_mainnet_self_serve_prices_used_when_no_price_map():
    """On mainnet, self-served prices are used for compilation when price_map is empty."""
    settings = GatewaySettings(network="mainnet")
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    # Set up compiler
    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()

    captured_prices = {}

    def _update_prices(prices):
        captured_prices.update(prices)
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices
    service._get_compiler = MagicMock(return_value=compiler)

    # Create a swap intent mock with from_token/to_token
    mock_intent = MagicMock()
    mock_intent.from_token = "USDC"
    mock_intent.to_token = "WETH"
    service._create_intent = MagicMock(return_value=mock_intent)

    # Set up market servicer with mock aggregator
    mock_aggregator = AsyncMock()

    async def mock_get_price(token, quote):
        prices = {"USDC": 1.0, "WETH": 2100.0}
        result = MagicMock()
        result.price = prices.get(token, 0)
        return result

    mock_aggregator.get_aggregated_price = mock_get_price
    mock_market = MagicMock()
    mock_market._price_aggregator = mock_aggregator
    mock_market._ensure_initialized = AsyncMock()
    service.market_servicer = mock_market

    context = MagicMock()
    intent_data = json.dumps({"from_token": "USDC", "to_token": "WETH", "amount": "100"}).encode("utf-8")
    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        price_map={},  # empty — triggers self-serve
    )

    result = await service.CompileIntent(request, context)

    assert result.success is True
    assert "USDC" in captured_prices
    assert "WETH" in captured_prices
    assert isinstance(captured_prices["USDC"], Decimal)


@pytest.mark.asyncio
async def test_mainnet_no_market_servicer_still_fails():
    """On mainnet without market_servicer, NO_PRICES_AVAILABLE error is returned."""
    settings = GatewaySettings(network="mainnet")
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()
    service.market_servicer = None  # No market service available

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    service._get_compiler = MagicMock(return_value=compiler)

    mock_intent = MagicMock()
    mock_intent.from_token = "USDC"
    mock_intent.to_token = "WETH"
    service._create_intent = MagicMock(return_value=mock_intent)

    context = MagicMock()
    intent_data = json.dumps({"from_token": "USDC", "to_token": "WETH", "amount": "100"}).encode("utf-8")
    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        price_map={},
    )

    result = await service.CompileIntent(request, context)

    assert result.success is False
    assert result.error_code == "NO_PRICES_AVAILABLE"
    compiler.compile.assert_not_called()
