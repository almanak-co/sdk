"""Unit tests for Fluid DEX swap deprecation guard (VIB-2822).

All 20 Fluid DEX T1 pools on Arbitrum currently reject swaps at any amount
(FluidDexSwapTooSmall / FluidDexLiquidityLimit). The compiler fails fast
with a clear error so strategy authors don't waste time debugging
protocol-level reverts.
"""

from decimal import Decimal

from almanak import IntentCompiler, IntentCompilerConfig, SwapIntent
from almanak.framework.intents.compiler_models import CompilationStatus

_BASE_PRICES = {
    "USDC": Decimal("1"),
    "USDT": Decimal("1"),
    "ETH": Decimal("3400"),
    "WETH": Decimal("3400"),
}


class TestFluidSwapDeprecationGuard:
    """VIB-2822: protocol='fluid' SWAP must fail fast with a clear error."""

    def test_fluid_swap_fails_with_clear_error(self):
        """Any fluid SWAP must be rejected at compile time with a VIB-2822 message."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle=_BASE_PRICES,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount=Decimal("10"),
            max_slippage=Decimal("0.01"),
            protocol="fluid",
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
        assert "VIB-2822" in result.error
        assert "Fluid DEX connector is disabled" in result.error

    def test_fluid_swap_fails_fast_without_rpc(self):
        """Guard fires before any network/RPC lookup — no gateway needed."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle=_BASE_PRICES,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=Decimal("0.001"),
            max_slippage=Decimal("0.01"),
            protocol="fluid",
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        # Tighten assertions so the test can't pass on an unrelated RPC/config
        # failure: the VIB-2822 guard message must be present in the error,
        # which proves the deprecation path fired before any network lookup.
        assert result.status == CompilationStatus.FAILED
        assert result.action_bundle is None
        assert result.error is not None
        assert "VIB-2822" in result.error
        assert "Fluid DEX connector is disabled" in result.error
