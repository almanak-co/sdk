"""Unit tests for the Fluid SWAP compiler (Phase 1, VIB-5029).

The Fluid compiler is SWAP-only and routerless: the per-pair pool resolved
on-chain is both the approve spender and the swap target. These tests mock
the SDK boundary (``FluidCompiler._build_sdk``) — the real on-chain
behaviour is covered by ``tests/intents/*/test_fluid_swap.py``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.base.compiler import SwapCompilerContext
from almanak.connectors.fluid.compiler import FluidCompiler
from almanak.connectors.fluid.sdk import FluidMinAmountError, FluidSDKError
from almanak.framework.intents.compiler_models import CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import LPOpenIntent, SwapIntent

POOL = "0x3C0441B42195F4aD6aa9a0978E06096ea616CDa7"
WALLET = "0x2222222222222222222222222222222222222222"
USDC_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT_ADDR = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"


def _token(symbol: str, address: str, decimals: int = 6, is_native: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        address=address,
        decimals=decimals,
        is_native=is_native,
        to_dict=lambda: {"symbol": symbol, "address": address, "decimals": decimals},
    )


def _services(from_token, to_token) -> MagicMock:
    services = MagicMock()
    services.resolve_token.side_effect = lambda t: {
        from_token.symbol: from_token,
        to_token.symbol: to_token,
    }.get(t)
    services.calculate_expected_output.return_value = 50_000_000
    services.build_approve_tx.return_value = [
        TransactionData(
            to=from_token.address,
            value=0,
            data="0x095ea7b3" + "00" * 64,
            gas_estimate=46_000,
            tx_type="approve",
            description="approve",
        )
    ]
    services.format_amount.side_effect = lambda amount, decimals: str(amount)
    return services


def _ctx(services: MagicMock, **overrides) -> SwapCompilerContext:
    defaults = dict(
        chain="arbitrum",
        wallet_address=WALLET,
        rpc_url="http://localhost:8545",
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=None,
        price_oracle={},
        cache={},
        services=services,
    )
    defaults.update(overrides)
    return SwapCompilerContext(**defaults)


def _mock_sdk(quote: int = 50_037_813, pool=(POOL, True)) -> MagicMock:
    sdk = MagicMock()
    sdk.find_pool_for_pair.return_value = pool
    sdk.get_swap_quote.return_value = quote
    sdk.build_swap_tx.side_effect = lambda **kw: {
        "to": kw["dex_address"],
        "data": "0x2668dfaa" + "00" * 128,
        "value": kw["value"],
        "gas": 250_000,
    }
    return sdk


def _swap_intent(**overrides) -> SwapIntent:
    defaults = dict(
        from_token="USDC",
        to_token="USDT",
        amount=Decimal("50"),
        max_slippage=Decimal("0.01"),
        protocol="fluid",
        chain="arbitrum",
    )
    defaults.update(overrides)
    return SwapIntent(**defaults)


class TestCompileSwapSuccess:
    def _compile(self, sdk=None, ctx=None, intent=None):
        from_token = _token("USDC", USDC_ADDR)
        to_token = _token("USDT", USDT_ADDR)
        services = _services(from_token, to_token)
        ctx = ctx or _ctx(services)
        sdk = sdk or _mock_sdk()
        with patch.object(FluidCompiler, "_build_sdk", return_value=sdk):
            return FluidCompiler().compile_swap(ctx, intent or _swap_intent()), sdk

    def test_success_builds_approve_plus_swap(self):
        result, sdk = self._compile()
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["approve", "swap"]
        assert result.transactions[-1].to == POOL

    def test_metadata_carries_pool_and_direction(self):
        result, _ = self._compile()
        md = result.action_bundle.metadata
        assert md["protocol"] == "fluid"
        assert md["pool"] == POOL
        assert md["swap0to1"] is True
        assert int(md["min_amount_out"]) > 0

    def test_min_out_uses_safer_quote(self):
        # Resolver quote (50_037_813) above oracle (50_000_000): slippage
        # basis is the LOWER oracle figure (choose_safer_quote semantics).
        result, _ = self._compile()
        min_out = int(result.action_bundle.metadata["min_amount_out"])
        assert min_out == int(50_000_000 * Decimal("0.99"))

    def test_swap_calldata_targets_swap_in(self):
        result, sdk = self._compile()
        sdk.build_swap_tx.assert_called_once()
        kwargs = sdk.build_swap_tx.call_args.kwargs
        assert kwargs["dex_address"] == POOL
        assert kwargs["swap0to1"] is True
        assert kwargs["amount_in"] == 50_000_000
        assert kwargs["to"] == WALLET
        assert kwargs["value"] == 0


class TestCompileSwapNative:
    def test_native_input_skips_approve_and_sets_value(self):
        eth = _token("ETH", "0x0000000000000000000000000000000000000000", decimals=18, is_native=True)
        usdc = _token("USDC", USDC_ADDR)
        services = _services(eth, usdc)
        sdk = _mock_sdk(pool=(POOL, False))
        intent = _swap_intent(from_token="ETH", to_token="USDC", amount=Decimal("0.01"))
        with patch.object(FluidCompiler, "_build_sdk", return_value=sdk):
            result = FluidCompiler().compile_swap(_ctx(services), intent)
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["swap"]
        assert result.transactions[0].value == 10**16
        # Pool lookup used Fluid's native sentinel, not WETH
        from almanak.connectors.fluid.sdk import FLUID_NATIVE_TOKEN

        sdk.find_pool_for_pair.assert_called_once_with(FLUID_NATIVE_TOKEN, USDC_ADDR)


class TestCompileSwapFailures:
    def _compile(self, sdk, intent=None, **ctx_overrides):
        from_token = _token("USDC", USDC_ADDR)
        to_token = _token("USDT", USDT_ADDR)
        services = _services(from_token, to_token)
        with patch.object(FluidCompiler, "_build_sdk", return_value=sdk):
            return FluidCompiler().compile_swap(_ctx(services, **ctx_overrides), intent or _swap_intent())

    def test_no_pool_fails_with_per_pair_explanation(self):
        result = self._compile(_mock_sdk(pool=None))
        assert result.status is CompilationStatus.FAILED
        assert "No Fluid DEX pool exists" in result.error

    def test_pool_enumeration_failure_mentions_rpc(self):
        sdk = _mock_sdk()
        sdk.find_pool_for_pair.side_effect = FluidSDKError("enumeration failed")
        result = self._compile(sdk)
        assert result.status is CompilationStatus.FAILED
        assert "pool not found" in result.error.lower()

    def test_limit_gated_quote_is_retryable_failure(self):
        sdk = _mock_sdk()
        sdk.get_swap_quote.side_effect = FluidMinAmountError("limits")
        result = self._compile(sdk)
        assert result.status is CompilationStatus.FAILED
        assert "retryable" in result.error

    def test_quote_failure_fails_closed_without_placeholders(self):
        # Resolver quote unavailable + placeholder prices disallowed =>
        # refuse to compile on oracle price alone (QUOTER_MISSING_FAIL_CLOSED).
        # allow_placeholder_prices=False pins the production posture; the
        # _ctx default of True is the permissive test default.
        sdk = _mock_sdk()
        sdk.get_swap_quote.side_effect = FluidSDKError("boom")
        result = self._compile(sdk, allow_placeholder_prices=False)
        assert result.status is CompilationStatus.FAILED
        assert "Refusing to compile" in result.error

    def test_no_transport_fails(self):
        from_token = _token("USDC", USDC_ADDR)
        to_token = _token("USDT", USDT_ADDR)
        services = _services(from_token, to_token)
        ctx = _ctx(services, rpc_url=None, gateway_client=None)
        result = FluidCompiler().compile_swap(ctx, _swap_intent())
        assert result.status is CompilationStatus.FAILED
        assert "pool not found" in result.error.lower()

    def test_unknown_token_fails(self):
        sdk = _mock_sdk()
        result = self._compile(sdk, intent=_swap_intent(from_token="NOPE"))
        assert result.status is CompilationStatus.FAILED
        assert "Unknown token" in result.error

    def test_amount_all_must_be_resolved(self):
        sdk = _mock_sdk()
        result = self._compile(sdk, intent=_swap_intent(amount="all"))
        assert result.status is CompilationStatus.FAILED
        assert "amount='all'" in result.error


class TestCompileDispatch:
    def test_lp_open_unsupported(self):
        intent = LPOpenIntent(
            pool="0x1111111111111111111111111111111111111111",
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="fluid",
            chain="arbitrum",
        )
        services = MagicMock()
        result = FluidCompiler().compile(_ctx(services), intent)
        assert result.status is CompilationStatus.FAILED

    def test_class_shape_swap_and_lending(self):
        from almanak.framework.intents.vocabulary import IntentType

        # SWAP (Phase 1, VIB-5029) + fToken SUPPLY/WITHDRAW (Phase 2, VIB-5030).
        assert FluidCompiler.intents == frozenset({IntentType.SWAP, IntentType.SUPPLY, IntentType.WITHDRAW})
        assert FluidCompiler.chains == frozenset({"arbitrum", "base", "ethereum", "polygon"})
        # Lending is scoped to the Phase-0-validated chains.
        assert FluidCompiler.LENDING_CHAINS == frozenset({"arbitrum", "base"})
