"""Tests for token normalization utilities.

This test suite covers:
- normalize() function for converting raw amounts to human-readable
- denormalize() function for converting human-readable to raw
- Edge cases: 0, very large numbers, 6 vs 18 decimals
- Error handling for invalid inputs
"""

from decimal import Decimal

import pytest

from almanak.framework.data.tokens import (
    denormalize,
    normalize,
)

# =============================================================================
# Test normalize()
# =============================================================================


class TestNormalize:
    """Tests for the normalize function."""

    def test_normalize_eth_18_decimals(self) -> None:
        """Test normalizing 1 ETH (18 decimals)."""
        wei = 1_000_000_000_000_000_000
        result = normalize(wei, 18)
        assert result == Decimal("1")

    def test_normalize_usdc_6_decimals(self) -> None:
        """Test normalizing 1 USDC (6 decimals)."""
        raw = 1_000_000
        result = normalize(raw, 6)
        assert result == Decimal("1")

    def test_normalize_wbtc_8_decimals(self) -> None:
        """Test normalizing 0.5 WBTC (8 decimals)."""
        raw = 50_000_000
        result = normalize(raw, 8)
        assert result == Decimal("0.5")

    def test_normalize_zero(self) -> None:
        """Test normalizing zero amount."""
        result = normalize(0, 18)
        assert result == Decimal("0")

    def test_normalize_fractional(self) -> None:
        """Test normalizing a fractional amount."""
        # 0.1 ETH in wei
        wei = 100_000_000_000_000_000
        result = normalize(wei, 18)
        assert result == Decimal("0.1")

    def test_normalize_zero_decimals(self) -> None:
        """Test normalizing with 0 decimals (raw == human-readable)."""
        result = normalize(42, 0)
        assert result == Decimal("42")

    def test_normalize_negative_decimals_raises(self) -> None:
        """Test that negative decimals raises ValueError."""
        with pytest.raises(ValueError, match="cannot be negative"):
            normalize(1000, -1)

    def test_normalize_excessive_decimals_raises(self) -> None:
        """Test that decimals > 77 raises ValueError."""
        with pytest.raises(ValueError, match="cannot exceed 77"):
            normalize(1000, 78)

    def test_normalize_very_small_amount(self) -> None:
        """Test normalizing 1 wei (smallest ETH unit)."""
        result = normalize(1, 18)
        assert result == Decimal("1E-18") or result == Decimal("0.000000000000000001")

    def test_normalize_very_large_amount(self) -> None:
        """Test normalizing a very large amount."""
        # 1 billion ETH in wei
        wei = 1_000_000_000 * 10**18
        result = normalize(wei, 18)
        assert result == Decimal("1000000000")


# =============================================================================
# Test denormalize()
# =============================================================================


class TestDenormalize:
    """Tests for the denormalize function."""

    def test_denormalize_eth(self) -> None:
        """Test denormalizing 1 ETH to wei."""
        result = denormalize(Decimal("1"), 18)
        assert result == 1_000_000_000_000_000_000

    def test_denormalize_usdc(self) -> None:
        """Test denormalizing 1 USDC to raw units."""
        result = denormalize(Decimal("1"), 6)
        assert result == 1_000_000

    def test_denormalize_wbtc(self) -> None:
        """Test denormalizing 0.5 WBTC to satoshis."""
        result = denormalize(Decimal("0.5"), 8)
        assert result == 50_000_000

    def test_denormalize_zero(self) -> None:
        """Test denormalizing zero."""
        result = denormalize(Decimal("0"), 18)
        assert result == 0

    def test_denormalize_zero_decimals(self) -> None:
        """Test denormalizing with 0 decimals."""
        result = denormalize(Decimal("42"), 0)
        assert result == 42

    def test_denormalize_negative_decimals_raises(self) -> None:
        """Test that negative decimals raises ValueError."""
        with pytest.raises(ValueError, match="cannot be negative"):
            denormalize(Decimal("1"), -1)

    def test_denormalize_excessive_decimals_raises(self) -> None:
        """Test that decimals > 77 raises ValueError."""
        with pytest.raises(ValueError, match="cannot exceed 77"):
            denormalize(Decimal("1"), 78)


# =============================================================================
# Test roundtrip consistency
# =============================================================================


class TestRoundtrip:
    """Tests for normalize/denormalize roundtrip consistency."""

    def test_roundtrip_eth(self) -> None:
        """Test ETH roundtrip: normalize -> denormalize."""
        original_wei = 1_500_000_000_000_000_000
        normalized = normalize(original_wei, 18)
        denormalized = denormalize(normalized, 18)
        assert denormalized == original_wei

    def test_roundtrip_usdc(self) -> None:
        """Test USDC roundtrip."""
        original_raw = 1_500_000
        normalized = normalize(original_raw, 6)
        denormalized = denormalize(normalized, 6)
        assert denormalized == original_raw

    def test_roundtrip_large_amount(self) -> None:
        """Test roundtrip with very large amount."""
        # 10 billion ETH equivalent in wei
        original_wei = 10_000_000_000 * 10**18
        normalized = normalize(original_wei, 18)
        denormalized = denormalize(normalized, 18)
        assert denormalized == original_wei

    def test_different_decimals_same_human_value(self) -> None:
        """Test that same human value produces different raw values for different decimals."""
        one_eth_wei = denormalize(Decimal("1"), 18)
        one_usdc_raw = denormalize(Decimal("1"), 6)
        one_wbtc_raw = denormalize(Decimal("1"), 8)

        # All represent "1" but with vastly different raw values
        assert one_eth_wei == 1_000_000_000_000_000_000
        assert one_usdc_raw == 1_000_000
        assert one_wbtc_raw == 100_000_000

        # Verify they're not equal to each other
        assert one_eth_wei != one_usdc_raw
        assert one_usdc_raw != one_wbtc_raw
