"""ExecutionService price_map handling tests.

Validates that:
- CompileIntent applies real prices from price_map to the cached compiler
- Empty/missing price_map uses placeholder prices (backward compat)
- Compiler state is restored after compilation (cached compiler not corrupted)
- String->Decimal parsing is correct
"""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.tokens import TokenRef
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import CompilationResult
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle
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
    ["swap", "lpopen", "lp_open", "lp-open", "supply", "repay", "borrow", "withdraw", "perpopen"],
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
@pytest.mark.parametrize("intent_type", ["lpclose", "lp_close", "perpclose", "perp_close"])
async def test_mainnet_close_intents_bypass_price_gate(intent_type):
    """Close-type intents (LP_CLOSE, PERP_CLOSE) bypass the price gate on mainnet.

    These operations (decreaseLiquidity/collect, close position) don't need prices
    for slippage calculation. They should compile even when no prices are available.
    """
    settings = GatewaySettings(network="mainnet")
    service = ExecutionServiceServicer(settings)
    service._ensure_initialized = AsyncMock()

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.compile.return_value = _make_compilation_result()
    service._get_compiler = MagicMock(return_value=compiler)
    service._create_intent = MagicMock(return_value=MagicMock())

    context = MagicMock()
    intent_data = json.dumps({"position_id": "12345", "pool": "0xABC"}).encode("utf-8")
    request = gateway_pb2.CompileIntentRequest(
        intent_type=intent_type,
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        price_map={},
    )

    result = await service.CompileIntent(request, context)

    # Close intents should proceed to compilation, not be blocked by price gate
    assert result.success is True
    compiler.compile.assert_called_once()


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

    result = await service._fetch_prices_for_tokens(["USDC", "WETH"], "arbitrum")
    assert result.prices == {}
    assert result.sources == {}
    assert result.peg_tokens == frozenset()


@pytest.mark.asyncio
async def test_fetch_prices_returns_empty_without_aggregator():
    """_fetch_prices_for_tokens returns empty when market_servicer has no _price_aggregator."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)
    service.market_servicer = MagicMock(spec=[])  # No _price_aggregator attr

    result = await service._fetch_prices_for_tokens(["USDC", "WETH"], "arbitrum")
    assert result.prices == {}
    assert result.sources == {}
    assert result.peg_tokens == frozenset()


@pytest.mark.asyncio
async def test_fetch_prices_returns_decimals_from_aggregator():
    """_fetch_prices_for_tokens fetches from aggregator and returns Decimals."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)

    mock_aggregator = AsyncMock()
    price_result = MagicMock()
    price_result.price = 2100.50
    price_result.source = "aggregated"

    mock_aggregator.get_aggregated_price = AsyncMock(return_value=price_result)

    mock_market = MagicMock()
    mock_market._ensure_initialized = AsyncMock()
    mock_market._price_aggregator = mock_aggregator
    mock_market._aggregator_for = MagicMock(return_value=mock_aggregator)
    mock_market._resolve_token_for_pricing = AsyncMock(return_value=None)
    service.market_servicer = mock_market

    result = await service._fetch_prices_for_tokens(["WETH"], "arbitrum")
    assert "WETH" in result.prices
    assert isinstance(result.prices["WETH"], Decimal)
    assert result.prices["WETH"] == Decimal("2100.5")
    assert result.sources == {"WETH": "aggregated"}
    assert result.peg_tokens == frozenset()


@pytest.mark.asyncio
async def test_fetch_prices_handles_partial_failures(caplog):
    """_fetch_prices_for_tokens returns available prices even if some fail."""
    settings = GatewaySettings()
    service = ExecutionServiceServicer(settings)

    mock_aggregator = AsyncMock()

    async def mock_get_price(token, quote, **kwargs):
        if token == "USDC":
            result = MagicMock()
            result.price = 1.0
            result.source = "aggregated"
            return result
        raise Exception("CoinGecko rate limited")

    mock_aggregator.get_aggregated_price = mock_get_price

    mock_market = MagicMock()
    mock_market._ensure_initialized = AsyncMock()
    mock_market._price_aggregator = mock_aggregator
    mock_market._aggregator_for = MagicMock(return_value=mock_aggregator)
    mock_market._resolve_token_for_pricing = AsyncMock(return_value=None)
    service.market_servicer = mock_market

    with caplog.at_level("WARNING", logger="almanak.gateway.services.execution_service"):
        result = await service._fetch_prices_for_tokens(["USDC", "UNKNOWN_TOKEN"], "arbitrum")
    assert "USDC" in result.prices
    assert "UNKNOWN_TOKEN" not in result.prices
    assert "Self-serve price fetch failed for UNKNOWN_TOKEN on arbitrum" in caplog.text


@pytest.mark.asyncio
async def test_gateway_synthetic_peg_stamps_exact_compile_provenance():
    """A self-served $1 fallback remains synthetic through compilation."""
    settings = GatewaySettings(network="mainnet")
    service = ExecutionServiceServicer(settings)
    service._extract_token_symbols_from_intent = MagicMock(return_value=["USDC"])

    usdc_address = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    token_ref = TokenRef(chain="arbitrum", address=usdc_address, decimals=6, symbol="USDC")
    resolved = SimpleNamespace(token_ref=token_ref)
    identity = f"arbitrum:{usdc_address}"
    price_result = SimpleNamespace(price=Decimal("1"), source="aggregated", peg_tokens=(identity,))
    aggregator = MagicMock()
    aggregator.get_aggregated_price = AsyncMock(return_value=price_result)
    market = MagicMock()
    market._ensure_initialized = AsyncMock()
    market._aggregator_for = MagicMock(return_value=aggregator)
    market._resolve_token_for_pricing = AsyncMock(return_value=resolved)
    service.market_servicer = market

    compiler = IntentCompiler(
        chain="arbitrum",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    gate_error = await service._enforce_mainnet_price_gate(compiler, MagicMock(), "supply")

    assert gate_error is None
    assert compiler.price_oracle == {"USDC": Decimal("1")}
    compiled = CompilationResult(
        status=CompilationStatus.SUCCESS,
        action_bundle=ActionBundle(intent_type="TEST"),
    )
    with patch.object(compiler, "_compile_intent", return_value=compiled):
        result = compiler.compile(SimpleNamespace(intent_type=IntentType.HOLD))  # type: ignore[arg-type]

    assert result.used_peg is True
    assert result.peg_tokens == [identity]
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["price_provenance"] == {
        "used_peg": True,
        "peg_tokens": [identity],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["foreign_identity", "identity_free_legacy"])
async def test_gateway_rejects_unauditable_synthetic_price(case: str):
    """Foreign or identity-free synthetic $1 marks must fail the mainnet gate."""
    settings = GatewaySettings(network="mainnet")
    service = ExecutionServiceServicer(settings)
    service._extract_token_symbols_from_intent = MagicMock(return_value=["USDC"])

    usdc_address = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    token_ref = TokenRef(chain="arbitrum", address=usdc_address, decimals=6, symbol="USDC")
    if case == "foreign_identity":
        resolved = SimpleNamespace(token_ref=token_ref)
        source = "aggregated"
        peg_tokens = ("base:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",)
    else:
        resolved = None
        source = "stablecoin_fallback"
        peg_tokens = ()

    aggregator = MagicMock()
    aggregator.get_aggregated_price = AsyncMock(
        return_value=SimpleNamespace(price=Decimal("1"), source=source, peg_tokens=peg_tokens)
    )
    market = MagicMock()
    market._ensure_initialized = AsyncMock()
    market._aggregator_for = MagicMock(return_value=aggregator)
    market._resolve_token_for_pricing = AsyncMock(return_value=resolved)
    service.market_servicer = market

    compiler = IntentCompiler(
        chain="arbitrum",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    gate_error = await service._enforce_mainnet_price_gate(compiler, MagicMock(), "supply")

    assert gate_error is not None
    assert gate_error.success is False
    assert gate_error.error_code == "NO_PRICES_AVAILABLE"
    assert compiler._using_placeholders is True
    assert compiler._pending_peg_fallbacks == set()


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
    compiler.chain = "arbitrum"
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

    async def mock_get_price(token, quote, **kwargs):
        prices = {"USDC": 1.0, "WETH": 2100.0}
        result = MagicMock()
        result.price = prices.get(token, 0)
        return result

    mock_aggregator.get_aggregated_price = mock_get_price
    mock_market = MagicMock()
    mock_market._ensure_initialized = AsyncMock()
    mock_market._price_aggregator = mock_aggregator
    mock_market._aggregator_for = MagicMock(return_value=mock_aggregator)
    mock_market._resolve_token_for_pricing = AsyncMock(return_value=None)
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


class TestExtractTokenSymbolsFromIntent:
    """Unit tests for _extract_token_symbols_from_intent pool parsing."""

    def test_extracts_from_pool_string(self):
        """Pool string like 'WETH/USDC/500' yields token symbols."""
        intent = MagicMock(spec=[])  # no auto-attrs
        intent.pool = "WETH/USDC/500"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert "500" not in tokens  # fee tier is numeric, excluded

    def test_extracts_from_token_fields(self):
        """Standard from_token/to_token fields are extracted."""
        intent = MagicMock(spec=[])
        intent.from_token = "USDC"
        intent.to_token = "ETH"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert tokens == ["USDC", "ETH"]

    def test_no_tokens_from_position_only_intent(self):
        """LP_CLOSE with only position_id and address-form pool returns empty."""
        intent = MagicMock(spec=[])
        intent.position_id = "12345"
        intent.pool = "0xABCDEF1234567890"  # address, no "/" separator
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert tokens == []

    def test_deduplicates_pool_and_field_tokens(self):
        """Tokens from pool string don't duplicate those from fields."""
        intent = MagicMock(spec=[])
        intent.from_token = "WETH"
        intent.pool = "WETH/USDC/500"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert tokens.count("WETH") == 1
        assert "USDC" in tokens

    def test_pool_field_skips_volatile_suffix(self):
        """Regression: Aerodrome 'volatile' suffix must not leak as a token.

        Observed in staging deployment b3f41304: 'WETH/USDC/volatile' caused
        'No Chainlink feed for VOLATILE on base' errors during self-serve
        price prefetch because 'volatile' was split out and queried as a
        token symbol.
        """
        intent = MagicMock(spec=[])
        intent.pool = "WETH/USDC/volatile"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert "volatile" not in tokens
        assert "VOLATILE" not in tokens
        assert tokens == ["WETH", "USDC"]

    def test_pool_field_skips_stable_suffix(self):
        intent = MagicMock(spec=[])
        intent.pool = "USDC/USDT/stable"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert "stable" not in tokens
        assert tokens == ["USDC", "USDT"]

    def test_pool_field_skips_concentrated_suffix(self):
        intent = MagicMock(spec=[])
        intent.pool = "WETH/USDC/concentrated"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert "concentrated" not in tokens
        assert tokens == ["WETH", "USDC"]

    def test_pool_field_skips_cl_suffix(self):
        """Aerodrome Slipstream 'cl' suffix."""
        intent = MagicMock(spec=[])
        intent.pool = "WETH/USDC/cl"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert "cl" not in tokens
        assert tokens == ["WETH", "USDC"]

    def test_extracts_token_in_and_token_out(self):
        """Delegating to the canonical parser picks up token_in/token_out.

        The previous hand-rolled parser only looked at
        (from_token, to_token, token, collateral_token, borrow_token), so
        any intent carrying token_in/token_out (e.g., some CLOB flows)
        silently missed price prefetch. Delegation fixes this.
        """
        intent = MagicMock(spec=[])
        intent.token_in = "USDC"
        intent.token_out = "WETH"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert "USDC" in tokens
        assert "WETH" in tokens

    def test_extracts_token_a_and_token_b(self):
        """Canonical parser also covers token_a / token_b (LP intents)."""
        intent = MagicMock(spec=[])
        intent.token_a = "USDC"
        intent.token_b = "WETH"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert "USDC" in tokens
        assert "WETH" in tokens

    def test_recurses_into_callback_intents(self):
        """Flash-loan callback intents now get their tokens extracted too."""
        cb = MagicMock(spec=[])
        cb.from_token = "WETH"
        cb.to_token = "USDC"
        intent = MagicMock(spec=[])
        intent.token = "WETH"
        intent.callback_intents = [cb]
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        # WETH appears in both levels but deduped
        assert tokens.count("WETH") == 1
        assert "USDC" in tokens

    def test_pool_field_skips_fiat_quote(self):
        """Regression: pool 'BTC/USD' must not leak USD into price prefetch.

        Observed on BSC staging (2026-04-22): perp market descriptors like
        'BTC/USD' caused the gateway price prefetch to query USD/USD, which
        has no Chainlink feed and no on-chain representation.
        """
        intent = MagicMock(spec=[])
        intent.pool = "BTC/USD"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert tokens == ["BTC"]
        assert "USD" not in tokens

    def test_token_field_skips_fiat_quote(self):
        """token_out='USD' must not leak as a fiat quote."""
        intent = MagicMock(spec=[])
        intent.token_in = "WETH"
        intent.token_out = "USD"
        tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent)
        assert tokens == ["WETH"]
        assert "USD" not in tokens
