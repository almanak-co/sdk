"""Tests for extracted data models (SwapAmounts, etc.)."""

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from almanak.framework.execution.extracted_data import LPOpenData, ProtocolFees, SwapAmounts


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


class TestProtocolFees:
    """Tests for the ProtocolFees dataclass (VIB-3204)."""

    def test_only_total_usd_required(self):
        """VIB-3204 audit fix: all-None components requires total_usd == 0.

        The prior version of this test constructed
        ``ProtocolFees(total_usd=Decimal("0.05"))`` with all components
        ``None`` — now rejected by ``__post_init__`` because it violates
        the "total equals sum of populated components" invariant. The
        test is renamed to exercise the vacuous (all-zero) shape that
        remains valid.
        """
        fees = ProtocolFees(total_usd=Decimal("0"))
        assert fees.total_usd == Decimal("0")
        assert fees.swap_fee_usd is None
        assert fees.lp_fee_usd is None
        assert fees.lending_origination_fee_usd is None
        assert fees.vault_fee_usd is None
        assert fees.perp_fee_usd is None

    def test_to_dict_preserves_numeric_precision_as_strings(self):
        fees = ProtocolFees(
            total_usd=Decimal("0.123456"),
            swap_fee_usd=Decimal("0.1"),
            lp_fee_usd=Decimal("0.023456"),
        )
        out = fees.to_dict()
        assert out == {
            "total_usd": "0.123456",
            "swap_fee_usd": "0.1",
            "lp_fee_usd": "0.023456",
            "lending_origination_fee_usd": None,
            "vault_fee_usd": None,
            "perp_fee_usd": None,
            "unavailable_reason": None,  # VIB-3495: new field, None for measured fees
        }

    def test_to_dict_distinguishes_zero_from_none(self):
        """ProtocolFees lets callers tell 'measured to be zero' from
        'not measured' — to_dict must preserve that distinction."""
        fees = ProtocolFees(
            total_usd=Decimal(0),
            lending_origination_fee_usd=Decimal(0),
        )
        out = fees.to_dict()
        assert out["lending_origination_fee_usd"] == "0"
        assert out["swap_fee_usd"] is None

    def test_frozen_dataclass(self):
        """ProtocolFees is immutable."""
        fees = ProtocolFees(total_usd=Decimal(0))
        with pytest.raises(FrozenInstanceError):
            fees.total_usd = Decimal(1)  # type: ignore[misc]


class TestLPOpenDataToDict:
    def test_to_dict_preserves_measured_zero_amount(self):
        """VIB-5032 — Empty != Zero at serialization. A single-sided LP_OPEN's
        unfunded leg is a MEASURED zero; to_dict must emit "0" (measured), not
        null (unmeasured), or the typed LPOpenEventPayload Decimal field rejects
        it downstream and blocks Accountant cells G6/G13/LP4."""
        data = LPOpenData(position_id=0, liquidity=945, amount0=0, amount1=50_000_000)
        out = data.to_dict()
        assert out["amount0"] == "0", "measured-zero leg must serialize as '0', not null"
        assert out["amount1"] == "50000000"
        assert out["liquidity"] == "945"

    def test_to_dict_preserves_unmeasured_none(self):
        """A genuinely unmeasured leg (None) still serializes to null."""
        data = LPOpenData(position_id=0, liquidity=None, amount0=None, amount1=None)
        out = data.to_dict()
        assert out["amount0"] is None
        assert out["amount1"] is None
        assert out["liquidity"] is None

    def test_total_usd_invariant_enforced(self):
        """VIB-3204 audit fix: total_usd must equal sum of populated components.

        Without this invariant, consumers couldn't distinguish
        "measured to be zero" from "fields not populated" — systematically
        under-attributing swap costs in PnL attribution.
        """
        # Populated components whose sum != total_usd -> rejected.
        with pytest.raises(ValueError, match="sum of populated components"):
            ProtocolFees(total_usd=Decimal("0"), swap_fee_usd=Decimal("0.05"))

        # Populated components whose sum == total_usd -> accepted.
        fees = ProtocolFees(total_usd=Decimal("0.15"), swap_fee_usd=Decimal("0.1"), lp_fee_usd=Decimal("0.05"))
        assert fees.total_usd == Decimal("0.15")

        # All None + total_usd == 0 -> accepted (vacuously true).
        fees = ProtocolFees(total_usd=Decimal("0"))
        assert fees.total_usd == Decimal("0")

        # All None + total_usd != 0 -> rejected.
        with pytest.raises(ValueError, match="sum of populated components"):
            ProtocolFees(total_usd=Decimal("0.1"))
