"""Tests for Intent.repay() API — optional amount when repay_full=True.

Covers the footgun where callers using repay_full=True had to explicitly pass
amount=Decimal("0") even though the amount is ignored. Omitting it raised a
TypeError that was typically swallowed by a bare except in decide(), causing
silent HOLD instead of REPAY.
"""

from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import Intent, RepayIntent


class TestRepayFullOptionalAmount:
    """Intent.repay() should not require amount when repay_full=True."""

    def test_repay_full_without_amount_defaults_to_zero(self):
        """repay_full=True should allow omitting amount, defaulting to Decimal('0')."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            repay_full=True,
        )
        assert isinstance(intent, RepayIntent)
        assert intent.repay_full is True
        assert intent.amount == Decimal("0")

    def test_repay_full_with_explicit_zero_still_works(self):
        """Existing callers passing amount=Decimal('0') explicitly should continue to work."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("0"),
            repay_full=True,
        )
        assert intent.repay_full is True
        assert intent.amount == Decimal("0")

    def test_repay_full_without_amount_with_interest_rate_mode(self):
        """repay_full=True without amount should work alongside interest_rate_mode."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            repay_full=True,
            interest_rate_mode="variable",
        )
        assert intent.repay_full is True
        assert intent.amount == Decimal("0")
        assert intent.interest_rate_mode == "variable"

    def test_repay_full_without_amount_morpho_blue(self):
        """repay_full=True without amount should work for morpho_blue."""
        intent = Intent.repay(
            protocol="morpho_blue",
            token="USDC",
            repay_full=True,
            market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        )
        assert intent.repay_full is True
        assert intent.amount == Decimal("0")

    def test_repay_full_without_amount_compound_v3(self):
        """repay_full=True without amount should work for compound_v3."""
        intent = Intent.repay(
            protocol="compound_v3",
            token="USDC",
            repay_full=True,
        )
        assert intent.repay_full is True
        assert intent.amount == Decimal("0")

    def test_repay_not_full_requires_amount(self):
        """When repay_full=False (default), omitting amount should raise a helpful ValueError."""
        with pytest.raises(ValueError, match="amount is required when repay_full=False"):
            Intent.repay(
                protocol="aave_v3",
                token="USDC",
            )

    def test_repay_not_full_explicit_false_requires_amount(self):
        """When repay_full=False explicitly, omitting amount should raise ValueError."""
        with pytest.raises(ValueError, match="amount is required when repay_full=False"):
            Intent.repay(
                protocol="aave_v3",
                token="USDC",
                repay_full=False,
            )

    def test_repay_with_explicit_amount_not_full(self):
        """Normal repay with explicit amount should still work."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
        )
        assert intent.amount == Decimal("500")
        assert intent.repay_full is False

    def test_repay_with_amount_all_not_full(self):
        """Repay with amount='all' should still work for chained operations."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount="all",
        )
        assert intent.amount == "all"
        assert intent.repay_full is False

    def test_repay_full_without_amount_with_chain(self):
        """repay_full=True without amount should work with chain parameter."""
        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            repay_full=True,
            chain="avalanche",
        )
        assert intent.repay_full is True
        assert intent.amount == Decimal("0")
        assert intent.chain == "avalanche"
