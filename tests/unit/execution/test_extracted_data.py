"""Tests for extracted data models (SwapAmounts, etc.)."""

from decimal import Decimal

import pytest

from almanak.framework.execution.extracted_data import SwapAmounts


class TestSwapAmountsHumanAliases:
    """Test that amount_in_human / amount_out_human aliases work (VIB-295)."""

    def test_amount_in_human_returns_decimal(self):
        """amount_in_human should alias amount_in_decimal."""
        sa = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
        )
        assert sa.amount_in_human == Decimal("1.0")
        assert sa.amount_in_human == sa.amount_in_decimal

    def test_amount_out_human_returns_decimal(self):
        """amount_out_human should alias amount_out_decimal."""
        sa = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
        )
        assert sa.amount_out_human == Decimal("0.5")
        assert sa.amount_out_human == sa.amount_out_decimal

    def test_invalid_attribute_still_raises(self):
        """Non-aliased attributes should still raise AttributeError."""
        sa = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
        )
        with pytest.raises(AttributeError):
            _ = sa.nonexistent_field

    def test_original_fields_still_work(self):
        """Original amount_in_decimal / amount_out_decimal fields still work."""
        sa = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("0.5"),
        )
        assert sa.amount_in_decimal == Decimal("1.0")
        assert sa.amount_out_decimal == Decimal("0.5")
