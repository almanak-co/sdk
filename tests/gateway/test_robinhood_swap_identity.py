"""Contract identity must survive gateway swap preparation."""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.tokens import ResolvedToken, create_token_resolver
from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import ExecutionServiceServicer

USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
HOOD = "0x32ac8c1d7672667d5ebdea22935f7b06fc8d496f"
WALLET = "0x1234567890123456789012345678901234567890"


def _intent():
    return SwapIntent(from_token=USDG, to_token=HOOD, amount=Decimal("9"), protocol="uniswap_v3", chain="robinhood")


def test_swap_price_prefetch_preserves_unknown_contracts():
    tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(_intent(), default_chain="robinhood")
    assert tokens == [USDG, HOOD]


@pytest.mark.asyncio
async def test_gateway_resolves_unknown_swap_contract_before_sync_compile(tmp_path):
    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    hood = ResolvedToken(
        symbol="HOOD",
        address=HOOD,
        decimals=18,
        chain="robinhood",
        chain_id=4663,
        source="on_chain",
        is_verified=False,
    )
    usdg = resolver.resolve(USDG, "robinhood")
    market = MagicMock()
    market._resolve_token_for_pricing = AsyncMock(side_effect=lambda token, chain: {USDG: usdg, HOOD: hood}[token])
    service = ExecutionServiceServicer(GatewaySettings(chains=["robinhood"], network="mainnet"))
    service.market_servicer = market
    compiler = IntentCompiler(
        chain="robinhood",
        wallet_address=WALLET,
        price_oracle={},
        token_resolver=resolver,
        config=IntentCompilerConfig(allow_placeholder_prices=False, managed_fork=False),
    )

    def compile_at_metadata_boundary(*, intent):
        token = compiler._resolve_token(intent.to_token)
        assert token is not None, "Gateway resolved metadata must reach the synchronous compiler"
        assert token.address.lower() == HOOD
        assert token.decimals == 18
        assert compiler._require_token_price_for(token) == Decimal("111")
        return CompilationResult(status=CompilationStatus.SUCCESS, action_bundle=ActionBundle(intent_type="SWAP"))

    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=json.dumps(_intent().serialize()).encode(),
        chain="robinhood",
        wallet_address=WALLET,
        price_map={USDG: "1", HOOD: "111"},
    )
    with (
        patch.object(service, "_get_compiler", return_value=compiler),
        patch.object(compiler, "compile", side_effect=compile_at_metadata_boundary),
    ):
        result = await service.CompileIntent(request, MagicMock())
    assert result.success, result.error


@pytest.mark.asyncio
@pytest.mark.parametrize("self_served", [True, False])
async def test_unknown_contract_cannot_use_foreign_symbol_price(tmp_path, self_served):
    from datetime import UTC, datetime

    from almanak.framework.data.interfaces import PriceResult

    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    hood = ResolvedToken(
        symbol="HOOD", address=HOOD, decimals=18, chain="robinhood", chain_id=4663, source="on_chain", is_verified=False
    )
    market = MagicMock()
    market._ensure_initialized = AsyncMock()
    market._resolve_token_for_pricing = AsyncMock(return_value=hood)
    aggregator = MagicMock()
    aggregator.get_aggregated_price = AsyncMock(
        return_value=PriceResult(
            price=Decimal("111"), source="contract_feed", timestamp=datetime.now(UTC), confidence=1
        )
        if self_served
        else None
    )
    market._aggregator_for.return_value = aggregator
    service = ExecutionServiceServicer(GatewaySettings(chains=["robinhood"], network="mainnet"))
    service.market_servicer = market
    compiler = IntentCompiler(
        chain="robinhood",
        wallet_address=WALLET,
        price_oracle={},
        token_resolver=resolver,
        config=IntentCompilerConfig(allow_placeholder_prices=False, managed_fork=False),
    )
    original_prices = compiler.price_oracle

    def compile_with_contract_price(*, intent):
        token = compiler._resolve_token(HOOD)
        assert compiler._require_token_price_for(token) == Decimal("111")
        return CompilationResult(status=CompilationStatus.SUCCESS, action_bundle=ActionBundle(intent_type="SWAP"))

    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=json.dumps(_intent().serialize()).encode(),
        chain="robinhood",
        wallet_address=WALLET,
        price_map={USDG: "1", "HOOD": "0.00068"},
    )
    with (
        patch.object(service, "_get_compiler", return_value=compiler),
        patch.object(compiler, "compile", side_effect=compile_with_contract_price) as compile_mock,
    ):
        result = await service.CompileIntent(request, MagicMock())
    assert result.success is self_served
    if not self_served:
        assert result.error_code == "NO_PRICES_AVAILABLE"
        assert HOOD in result.error
        compile_mock.assert_not_called()
    aggregator.get_aggregated_price.assert_awaited_once()
    assert aggregator.get_aggregated_price.call_args.args == (HOOD, "USD")
    assert compiler.price_oracle == original_prices
    from almanak.framework.data.tokens.exceptions import TokenNotFoundError

    with pytest.raises(TokenNotFoundError):
        resolver.resolve(HOOD, "robinhood", skip_gateway=True)


@pytest.mark.asyncio
async def test_metadata_from_another_contract_is_refused(tmp_path):
    from almanak.gateway.services.swap_token_preparation import SwapTokenPreparationError, discover_swap_tokens

    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    foreign = ResolvedToken(
        symbol="HOOD",
        address=WALLET,
        decimals=18,
        chain="robinhood",
        chain_id=4663,
        source="on_chain",
        is_verified=False,
    )
    with pytest.raises(SwapTokenPreparationError, match="No matching on-chain"):
        await discover_swap_tokens([HOOD], "robinhood", resolver, AsyncMock(return_value=foreign))


@pytest.mark.asyncio
async def test_walletless_swap_fails_before_compiler_initialization():
    service = ExecutionServiceServicer(GatewaySettings())
    service._ensure_initialized = AsyncMock()
    result = await service.CompileIntent(
        gateway_pb2.CompileIntentRequest(intent_type="swap", chain="robinhood"), MagicMock()
    )
    assert result.error_code == "MISSING_WALLET"
    assert "--wallet" in result.error
    service._ensure_initialized.assert_not_called()


def test_walletless_hold_validation_is_unchanged():
    service = ExecutionServiceServicer(GatewaySettings())
    _, _, invalid = service._validate_compile_request(
        gateway_pb2.CompileIntentRequest(intent_type="hold", chain="robinhood"), MagicMock()
    )
    assert invalid is None


@pytest.mark.asyncio
@pytest.mark.parametrize("quote,success", [(81165739476056398, True), (1000000000000000, False)])
@pytest.mark.parametrize("token", [HOOD, HOOD.upper(), f"eip155:4663/erc20:{HOOD}"])
async def test_real_v3_compilation_preserves_contract_and_impact_guard(tmp_path, quote, success, token):
    from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler

    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    hood = ResolvedToken(
        symbol="ETH", address=HOOD, decimals=18, chain="robinhood", chain_id=4663, source="on_chain", is_verified=False
    )
    market = MagicMock()
    market._resolve_token_for_pricing = AsyncMock(return_value=hood)
    service = ExecutionServiceServicer(GatewaySettings(chains=["robinhood"], network="mainnet"))
    service.market_servicer = market
    compiler = IntentCompiler(
        chain="robinhood",
        wallet_address=WALLET,
        price_oracle={},
        token_resolver=resolver,
        config=IntentCompilerConfig(allow_placeholder_prices=False, managed_fork=False),
    )

    def quote_swap(**kwargs):
        adapter = kwargs["adapter"]
        adapter._cached_fee = 10000
        adapter.last_fee_selection = {"selected_fee_tier": 10000}
        return quote

    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=json.dumps(
            SwapIntent(
                from_token=USDG, to_token=token, amount=Decimal("9"), protocol="uniswap_v3", chain="robinhood"
            ).serialize()
        ).encode(),
        chain="robinhood",
        wallet_address=WALLET,
        price_map={USDG: "1", HOOD: "111"},
    )
    with (
        patch.object(service, "_get_compiler", return_value=compiler),
        patch.object(compiler, "_build_approve_tx", return_value=[]),
        patch.object(UniswapV3Compiler, "_quote_swap_via_registry", side_effect=quote_swap),
        patch.object(UniswapV3Compiler, "_validate_swap_pool_after_fee_selection", return_value=None),
    ):
        result = await service.CompileIntent(request, MagicMock())
    assert result.success is success, result.error
    if success:
        bundle = json.loads(result.action_bundle)
        assert bundle["metadata"]["to_token"]["address"].lower() == HOOD
        assert bundle["metadata"]["to_token"]["decimals"] == 18
        assert bundle["metadata"]["to_token"]["is_native"] is False
        assert bundle["metadata"]["amount_in"] == "9000000"
        assert int(bundle["metadata"]["min_amount_out"]) > 0
    else:
        assert "Price impact too high" in result.error
        assert "reference-price contract identity" in result.error


def test_native_sentinel_keeps_registry_authorized_price_symbol():
    from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

    intent = SwapIntent(from_token=NATIVE_SENTINEL, to_token=USDG, amount=Decimal("0.001"), chain="robinhood")
    tokens = ExecutionServiceServicer._extract_token_symbols_from_intent(intent, default_chain="robinhood")
    assert tokens == ["ETH", USDG]


@pytest.mark.parametrize("token", [f"eip155:4663/erc20:{HOOD}", HOOD.upper()])
def test_caip_erc20_named_eth_is_not_native(tmp_path, token):
    resolver = create_token_resolver(cache_file=str(tmp_path / "tokens.json"))
    foreign = ResolvedToken(
        symbol="ETH", address=HOOD, decimals=18, chain="robinhood", chain_id=4663, source="on_chain", is_verified=False
    )
    compiler = IntentCompiler(chain="robinhood", wallet_address=WALLET, price_oracle={}, token_resolver=resolver)
    with resolver.scoped_metadata([foreign]):
        info = compiler._resolve_token(token)
        assert info.address.lower() == HOOD
        assert info.is_native is False


@pytest.mark.asyncio
async def test_symbol_removal_returns_actionable_client_error(tmp_path):
    import grpc

    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    compiler = IntentCompiler(chain="robinhood", wallet_address=WALLET, token_resolver=resolver, price_oracle={})
    service = ExecutionServiceServicer(GatewaySettings(chains=["robinhood"], network="mainnet"))
    intent = SwapIntent(from_token="USDG", to_token=HOOD, amount=Decimal("9"), chain="robinhood")
    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=json.dumps(intent.serialize()).encode(),
        chain="robinhood",
        wallet_address=WALLET,
    )
    context = MagicMock()
    with (
        patch.object(service, "_get_compiler", return_value=compiler),
        patch("almanak.framework.data.tokens.deprecation.SDK_VERSION", "3.0.0"),
    ):
        result = await service.CompileIntent(request, context)
    assert not result.success
    assert result.error_code == "INVALID_TOKEN"
    assert "address" in result.error.lower()
    context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.asyncio
async def test_preparation_suppresses_internal_symbol_warning(tmp_path):
    import warnings

    from almanak.framework.data.tokens import SymbolTokenResolutionWarning
    from almanak.gateway.services.swap_token_preparation import discover_swap_tokens

    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await discover_swap_tokens(["USDG"], "robinhood", resolver, None)
    assert not [item for item in caught if issubclass(item.category, SymbolTokenResolutionWarning)]


@pytest.mark.asyncio
async def test_foreign_intent_chain_is_invalid_argument(tmp_path):
    import grpc

    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    compiler = IntentCompiler(chain="robinhood", wallet_address=WALLET, token_resolver=resolver, price_oracle={})
    service = ExecutionServiceServicer(GatewaySettings(chains=["robinhood"], network="mainnet"))
    intent = SwapIntent(from_token=USDG, to_token=HOOD, amount=Decimal("9"), chain="arbitrum")
    request = gateway_pb2.CompileIntentRequest(
        intent_type="swap",
        intent_data=json.dumps(intent.serialize()).encode(),
        chain="robinhood",
        wallet_address=WALLET,
    )
    context = MagicMock()
    with patch.object(service, "_get_compiler", return_value=compiler):
        result = await service.CompileIntent(request, context)
    assert not result.success
    assert result.error_code == "INVALID_CHAIN"
    context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.asyncio
async def test_preparation_symbol_removal_is_not_metadata_miss(tmp_path):
    from almanak.gateway.services.swap_token_preparation import SwapTokenPreparationError, discover_swap_tokens

    resolver = create_token_resolver(cache_file=tmp_path / "tokens.json")
    discovery = AsyncMock()
    with patch("almanak.framework.data.tokens.deprecation.SDK_VERSION", "3.0.0"):
        with pytest.raises(SwapTokenPreparationError, match="contract address") as caught:
            await discover_swap_tokens(["USDG"], "robinhood", resolver, discovery)
    assert caught.value.code == "INVALID_TOKEN"
    discovery.assert_not_awaited()
