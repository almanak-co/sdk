"""Unit tests for unwrap native pre-flight balance check.

VIB-1146: The compiler should check wallet balance before building an unwrap
transaction, rather than letting it revert on-chain with a cryptic
"ERC20: burn amount exceeds balance" error.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import UnwrapNativeIntent


@pytest.fixture()
def compiler():
    """Compiler for Arbitrum with placeholder prices."""
    return IntentCompiler(
        chain="arbitrum",
        wallet_address="0x" + "a" * 40,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestUnwrapBalanceCheck:
    """Verify pre-flight balance check for unwrap_native intent."""

    def test_insufficient_balance_returns_clear_error(self, compiler):
        """When wallet has less WETH than requested, error should state balances."""
        intent = UnwrapNativeIntent(
            token="WETH",
            amount=Decimal("0.004"),
            chain="arbitrum",
        )

        # Mock: wallet has 0.001 WETH (1e15 wei)
        with patch.object(compiler, "_query_erc20_balance", return_value=10**15):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "Insufficient WETH balance" in result.error
        assert "0.001" in result.error
        assert "0.004" in result.error

    def test_zero_balance_returns_clear_error(self, compiler):
        """When wallet has 0 WETH, error should indicate zero balance."""
        intent = UnwrapNativeIntent(
            token="WETH",
            amount=Decimal("0.004"),
            chain="arbitrum",
        )

        with patch.object(compiler, "_query_erc20_balance", return_value=0):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "Insufficient WETH balance" in result.error

    def test_sufficient_balance_compiles_successfully(self, compiler):
        """When wallet has enough WETH, compilation should succeed."""
        intent = UnwrapNativeIntent(
            token="WETH",
            amount=Decimal("0.004"),
            chain="arbitrum",
        )

        # Mock: wallet has 0.1 WETH (1e17 wei)
        with patch.object(compiler, "_query_erc20_balance", return_value=10**17):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"

    def test_balance_query_failure_still_compiles(self, compiler):
        """If balance query fails (returns None), compilation should proceed."""
        intent = UnwrapNativeIntent(
            token="WETH",
            amount=Decimal("0.004"),
            chain="arbitrum",
        )

        # Mock: balance query fails (no gateway)
        with patch.object(compiler, "_query_erc20_balance", return_value=None):
            result = compiler.compile(intent)

        # Should still compile (can't verify, let on-chain handle it)
        assert result.status.value == "SUCCESS"

    def test_amount_all_skips_preflight_check(self, compiler):
        """amount='all' uses balance directly, no extra pre-flight needed."""
        intent = UnwrapNativeIntent(
            token="WETH",
            amount="all",
            chain="arbitrum",
        )

        # Mock: wallet has some WETH
        with patch.object(compiler, "_query_erc20_balance", return_value=10**17) as balance_mock:
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        # amount="all" path queries balance once (for the unwrap amount), but NOT for preflight
        assert balance_mock.call_count == 1
