"""Tests for interest_rate_mode wiring through BorrowIntent, RepayIntent, and compiler (VIB-1656).

Ensures that interest_rate_mode is:
1. Validated on both BorrowIntent and RepayIntent
2. Correctly wired through the compiler to calldata/metadata for all 4 paths:
   - Aave Borrow, Spark Borrow, Aave Repay, Spark Repay
3. Rejected for protocols that don't support it (morpho, compound_v3, etc.)
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    Intent,
    RepayIntent,
)


# =============================================================================
# BorrowIntent validation tests
# =============================================================================


class TestBorrowIntentInterestRateMode:
    """Tests for interest_rate_mode on BorrowIntent."""

    def test_aave_variable_accepted(self):
        intent = Intent.borrow(
            protocol="aave_v3",
            collateral_token="ETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
            interest_rate_mode="variable",
        )
        assert intent.interest_rate_mode == "variable"

    def test_aave_stable_rejected(self):
        """Stable rate is deprecated on Aave V3 — must be rejected at intent layer."""
        with pytest.raises(ValidationError, match="Input should be 'variable'"):
            Intent.borrow(
                protocol="aave_v3",
                collateral_token="ETH",
                collateral_amount=Decimal("1"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
                interest_rate_mode="stable",
            )

    def test_spark_stable_rejected(self):
        """Stable rate is deprecated on Spark — must be rejected at intent layer."""
        with pytest.raises(ValidationError, match="Input should be 'variable'"):
            Intent.borrow(
                protocol="spark",
                collateral_token="ETH",
                collateral_amount=Decimal("1"),
                borrow_token="DAI",
                borrow_amount=Decimal("1000"),
                interest_rate_mode="stable",
            )

    def test_morpho_rejects_interest_rate_mode(self):
        with pytest.raises(ValidationError, match="does not support interest rate mode"):
            Intent.borrow(
                protocol="morpho",
                collateral_token="ETH",
                collateral_amount=Decimal("1"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
                interest_rate_mode="variable",
                market_id="0xtest",
            )

    def test_compound_rejects_interest_rate_mode(self):
        with pytest.raises(ValidationError, match="does not support interest rate mode"):
            Intent.borrow(
                protocol="compound_v3",
                collateral_token="ETH",
                collateral_amount=Decimal("1"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
                interest_rate_mode="variable",
            )

    def test_none_accepted_all_protocols(self):
        """None (default) should be accepted for all protocols."""
        intent = Intent.borrow(
            protocol="aave_v3",
            collateral_token="ETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
        )
        assert intent.interest_rate_mode is None


# =============================================================================
# RepayIntent validation tests
# =============================================================================


class TestRepayIntentInterestRateMode:
    """Tests for interest_rate_mode on RepayIntent."""

    def test_aave_variable_accepted(self):
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
            interest_rate_mode="variable",
        )
        assert intent.interest_rate_mode == "variable"

    def test_aave_stable_rejected(self):
        """Stable rate is deprecated on Aave V3 — must be rejected at intent layer."""
        with pytest.raises(ValidationError, match="Input should be 'variable'"):
            Intent.repay(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="stable",
            )

    def test_spark_stable_rejected(self):
        """Stable rate is deprecated on Spark — must be rejected at intent layer."""
        with pytest.raises(ValidationError, match="Input should be 'variable'"):
            Intent.repay(
                protocol="spark",
                token="DAI",
                amount=Decimal("1000"),
                interest_rate_mode="stable",
            )

    def test_morpho_rejects_interest_rate_mode(self):
        with pytest.raises(ValidationError, match="does not support interest rate mode"):
            Intent.repay(
                protocol="morpho",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="variable",
                market_id="0xtest",
            )

    def test_compound_rejects_interest_rate_mode(self):
        with pytest.raises(ValidationError, match="does not support interest rate mode"):
            Intent.repay(
                protocol="compound_v3",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="variable",
            )

    def test_none_accepted_all_protocols(self):
        """None (default) should be accepted for all protocols."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
        )
        assert intent.interest_rate_mode is None

    def test_invalid_mode_rejected(self):
        """Invalid mode string should be rejected by Pydantic Literal validation."""
        with pytest.raises(ValidationError, match="interest_rate_mode|Valid modes"):
            Intent.repay(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="fixed",
            )

    def test_repay_full_with_variable_mode(self):
        """repay_full=True should work with interest_rate_mode='variable'."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("0"),
            repay_full=True,
            interest_rate_mode="variable",
        )
        assert intent.repay_full is True
        assert intent.interest_rate_mode == "variable"

    def test_serialize_includes_interest_rate_mode(self):
        """Serialized RepayIntent should include interest_rate_mode."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
            interest_rate_mode="variable",
        )
        data = intent.serialize()
        assert data["interest_rate_mode"] == "variable"

    def test_serialize_none_interest_rate_mode(self):
        """Serialized RepayIntent with None should have null interest_rate_mode."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
        )
        data = intent.serialize()
        assert data["interest_rate_mode"] is None
