"""Compile-time regression guard for the disabled Fluid DEX connector on Arbitrum.

VIB-2822: ``IntentCompiler._compile_swap_fluid`` (in ``almanak/framework/intents/compiler.py``)
unconditionally returns ``CompilationStatus.FAILED`` because every Arbitrum
Fluid DEX T1 pool currently reverts (``FluidDexSwapTooSmall`` /
``FluidDexLiquidityLimit``). The compile-time short-circuit is the correct
production state — these tests lock that state in. If someone re-enables
Fluid without fixing the underlying protocol issue, this guard fails loudly.

Compile-only — no Anvil, no orchestrator, no network. Replaces an earlier
on-chain xfail harness that pretended an integration existed.
"""

from decimal import Decimal

import pytest

from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# A non-empty stub oracle keeps ``IntentCompiler.__init__`` from raising about
# placeholder prices. The Fluid swap path returns FAILED before any pricing
# code runs, so the values are immaterial.
_STUB_PRICE_ORACLE: dict[str, Decimal] = {
    "USDC": Decimal("1"),
    "USDT": Decimal("1"),
}

# Any well-formed EVM address is fine — the compile path returns FAILED before
# wallet state is consulted.
_STUB_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


# =============================================================================
# Compile-time regression guards
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestFluidSwapCompileGuard:
    """Pin the disabled-Fluid contract at the compile boundary.

    These tests assert that ``SwapIntent(protocol="fluid", chain="arbitrum")``
    fails compilation with a VIB-2822-mentioning error. They do NOT exercise
    Layers 2-4 (execution, receipt parsing, balance deltas) because there is
    no on-chain integration to exercise — Fluid is hard-disabled at the
    compile boundary by design.
    """

    # Fluid is hard-disabled at the compile boundary (VIB-2822); Layers 2-4
    # (execution, receipt parsing, balance deltas) are intentionally absent.
    # See module docstring for full rationale.
    @pytest.mark.parametrize(  # noqa: layers
        "from_token,to_token",
        [
            ("USDC", "USDT"),
            ("USDT", "USDC"),
        ],
        ids=["usdc_to_usdt", "usdt_to_usdc"],
    )
    def test_compile_fails_when_fluid_disabled(
        self, from_token: str, to_token: str
    ) -> None:
        """Fluid swap compilation must fail with a VIB-2822 message.

        Asserts:
        1. ``CompilationResult.status == CompilationStatus.FAILED``
        2. The error message mentions VIB-2822, "Fluid", and "disabled" so a
           regression that re-enables Fluid without diagnosing the underlying
           protocol issue surfaces immediately.
        3. No ``ActionBundle`` is produced.
        """
        intent = SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount=Decimal("1"),
            max_slippage=Decimal("0.05"),
            protocol="fluid",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=_STUB_WALLET,
            price_oracle=_STUB_PRICE_ORACLE,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED, (
            f"Fluid is hard-disabled at compile time (VIB-2822); expected "
            f"CompilationStatus.FAILED but got {result.status} "
            f"(error={result.error!r}). If Fluid was intentionally re-enabled, "
            f"update or remove this guard *and* document why the underlying "
            f"FluidDexSwapTooSmall / FluidDexLiquidityLimit issue is resolved."
        )
        assert result.action_bundle is None, (
            "Fluid disabled-state must not emit an ActionBundle; the compile "
            "short-circuit returns FAILED before any tx is built."
        )
        assert result.error, "FAILED result must carry an error message"
        error = result.error
        assert "VIB-2822" in error, (
            f"Error message must reference VIB-2822 so a regression points at "
            f"the right ticket. Got: {error!r}"
        )
        assert "Fluid" in error, (
            f"Error message must name the connector. Got: {error!r}"
        )
        assert "disabled" in error.lower(), (
            f"Error message must communicate that the connector is disabled "
            f"(not e.g. 'misconfigured'). Got: {error!r}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
