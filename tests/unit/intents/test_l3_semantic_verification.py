"""Unit tests for L3 semantic verification helpers.

Tests the assert_swap_semantic_match() helper without requiring Anvil or on-chain execution.
Uses mock swap_result objects to verify the semantic checks work correctly.
"""

from dataclasses import dataclass
from decimal import Decimal

import pytest

from tests.intents.conftest import assert_swap_semantic_match


@dataclass
class MockSwapResult:
    """Mock swap result matching receipt parser output shape."""

    amount_in: int = 0
    amount_out: int = 0
    amount_in_decimal: Decimal = Decimal("0")
    amount_out_decimal: Decimal = Decimal("0")
    effective_price: Decimal | None = None
    token_in: str | None = None
    token_out: str | None = None


class TestAssertSwapSemanticMatch:
    """Test L3 semantic verification for swap intents."""

    def test_exact_amount_match_passes(self):
        """Swap result matching intent amount exactly should pass."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("2000"),
        )
        assert_swap_semantic_match(
            intent_amount=Decimal("100"),
            intent_from_token="USDC",
            intent_to_token="WETH",
            swap_result=result,
        )

    def test_amount_within_tolerance_passes(self):
        """Swap result within tolerance (2%) should pass."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("101"),  # 1% over
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("2000"),
        )
        assert_swap_semantic_match(
            intent_amount=Decimal("100"),
            intent_from_token="USDC",
            intent_to_token="WETH",
            swap_result=result,
            tolerance_bps=200,
        )

    def test_amount_exceeds_tolerance_fails(self):
        """Swap result exceeding tolerance should fail."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("110"),  # 10% over
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("2000"),
        )
        with pytest.raises(AssertionError, match="L3 semantic: receipt amount_in"):
            assert_swap_semantic_match(
                intent_amount=Decimal("100"),
                intent_from_token="USDC",
                intent_to_token="WETH",
                swap_result=result,
                tolerance_bps=200,
            )

    def test_zero_amount_out_fails(self):
        """Zero output amount should fail (no-op guard)."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0"),
            effective_price=Decimal("2000"),
        )
        with pytest.raises(AssertionError, match="L3 semantic: receipt amount_out must be positive"):
            assert_swap_semantic_match(
                intent_amount=Decimal("100"),
                intent_from_token="USDC",
                intent_to_token="WETH",
                swap_result=result,
            )

    def test_zero_effective_price_fails(self):
        """Zero effective price should fail."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("0"),
        )
        with pytest.raises(AssertionError, match="L3 semantic: effective_price must be positive"):
            assert_swap_semantic_match(
                intent_amount=Decimal("100"),
                intent_from_token="USDC",
                intent_to_token="WETH",
                swap_result=result,
            )

    def test_negative_effective_price_fails(self):
        """Negative effective price should fail."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("-1"),
        )
        with pytest.raises(AssertionError, match="L3 semantic: effective_price must be positive"):
            assert_swap_semantic_match(
                intent_amount=Decimal("100"),
                intent_from_token="USDC",
                intent_to_token="WETH",
                swap_result=result,
            )

    def test_custom_tolerance(self):
        """Custom tolerance should be respected."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("103"),  # 3% over
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("2000"),
        )
        # 5% tolerance — should pass
        assert_swap_semantic_match(
            intent_amount=Decimal("100"),
            intent_from_token="USDC",
            intent_to_token="WETH",
            swap_result=result,
            tolerance_bps=500,
        )
        # 1% tolerance — should fail
        with pytest.raises(AssertionError, match="L3 semantic"):
            assert_swap_semantic_match(
                intent_amount=Decimal("100"),
                intent_from_token="USDC",
                intent_to_token="WETH",
                swap_result=result,
                tolerance_bps=100,
            )

    def test_zero_amount_in_fails(self):
        """Zero input amount should fail (parser regression guard)."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("0"),
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("2000"),
        )
        with pytest.raises(AssertionError, match="L3 semantic: receipt amount_in must be positive"):
            assert_swap_semantic_match(
                intent_amount=Decimal("100"),
                intent_from_token="USDC",
                intent_to_token="WETH",
                swap_result=result,
            )

    def test_missing_amount_in_decimal_skips_check(self):
        """If amount_in_decimal is None, amount check is skipped."""
        result = MockSwapResult(
            amount_out_decimal=Decimal("0.05"),
            effective_price=Decimal("2000"),
        )
        result.amount_in_decimal = None  # Explicitly None, not zero
        # Should not raise — amount check skipped
        assert_swap_semantic_match(
            intent_amount=Decimal("100"),
            intent_from_token="USDC",
            intent_to_token="WETH",
            swap_result=result,
        )

    def test_missing_effective_price_skips_check(self):
        """If effective_price is None, price check is skipped."""
        result = MockSwapResult(
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.05"),
            effective_price=None,
        )
        assert_swap_semantic_match(
            intent_amount=Decimal("100"),
            intent_from_token="USDC",
            intent_to_token="WETH",
            swap_result=result,
        )
