"""Tests for flash loan callback amount='all' resolution.

Verifies that the flash loan compiler resolves amount='all' in callback
intents using the estimated output from the previous callback, instead
of failing with "amount='all' must be resolved before compilation."

Fixes VIB-784: balancer_flash_arb compilation_error on arbitrum.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.intents import Intent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)


@pytest.fixture
def compiler():
    """Create a compiler with placeholder prices enabled (no RPC needed).

    Patches _is_wallet_contract to return True (contract wallet) so that
    the EOA guard in _compile_flash_loan does not block compilation.
    These tests exercise callback amount resolution, not wallet type checks.
    """
    config = IntentCompilerConfig(allow_placeholder_prices=True)
    c = IntentCompiler(chain="arbitrum", config=config)
    with patch.object(c, "_is_wallet_contract", return_value=True):
        yield c


class TestFlashLoanCallbackAmountAll:
    """Test amount='all' resolution in flash loan callback intents."""

    def test_flash_loan_with_amount_all_callback_compiles(self, compiler):
        """Flash loan with amount='all' in second callback should compile successfully.

        This is the exact pattern used by the balancer_flash_arb demo strategy:
        borrow USDC -> swap USDC->WETH -> swap WETH->USDC (amount='all') -> repay.
        """
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("1000"),
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount="all",
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
            ],
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"Flash loan with amount='all' callback should compile. Error: {result.error}"
        )
        assert result.action_bundle is not None

    def test_flash_loan_amount_all_first_callback_uses_borrowed_amount(self, compiler):
        """amount='all' in the first callback should use the flash loan's borrowed amount.

        The flash loan borrows 1000 USDC, so callback 1 with amount='all' and
        from_token='USDC' should resolve to 1000 USDC.
        """
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount="all",
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount="all",
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
            ],
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"Flash loan with amount='all' in first callback should compile "
            f"(seeded from borrowed amount). Error: {result.error}"
        )
        assert result.action_bundle is not None

    def test_flash_loan_explicit_amounts_still_work(self, compiler):
        """Flash loan with explicit amounts in all callbacks should still compile."""
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("1000"),
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount=Decimal("0.5"),
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
            ],
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"Flash loan with explicit amounts should compile. Error: {result.error}"
        )

    def test_flash_loan_aave_provider_with_amount_all(self, compiler):
        """amount='all' resolution works with Aave provider too."""
        intent = Intent.flash_loan(
            provider="aave",
            token="USDC",
            amount=Decimal("500"),
            callback_intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("500"),
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount="all",
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
            ],
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"Aave flash loan with amount='all' callback should compile. Error: {result.error}"
        )

    def test_flash_loan_amount_all_token_mismatch_fails(self, compiler):
        """amount='all' should fail when from_token doesn't match previous output token.

        If flash loan borrows USDC but callback 1 uses from_token='WETH' with amount='all',
        that's a token mismatch -- the compiler should reject it with a clear error.
        """
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount="all",
                    max_slippage=Decimal("0.01"),
                    protocol="uniswap_v3",
                ),
            ],
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "amount='all' expects token 'USDC'" in result.error
        assert "from_token is 'WETH'" in result.error
