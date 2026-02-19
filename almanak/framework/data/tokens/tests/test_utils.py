"""Tests for token normalization utilities.

This test suite covers:
- normalize() function for converting raw amounts to human-readable
- denormalize() function for converting human-readable to raw
- normalize_token() function for registry-based normalization
- denormalize_token() function for registry-based denormalization
- Edge cases: 0, very large numbers, 6 vs 18 decimals
- Error handling for invalid inputs
"""

from decimal import Decimal

import pytest

from almanak.framework.data.tokens import (
    Token,
    TokenRegistry,
    denormalize,
    denormalize_token,
    get_default_registry,
    normalize,
    normalize_token,
)

# =============================================================================
# Test normalize()
# =============================================================================


class TestNormalize:
    """Tests for the normalize function."""

    def test_normalize_zero(self) -> None:
        """Test normalizing zero amount."""
        assert normalize(0, 18) == Decimal("0")
        assert normalize(0, 6) == Decimal("0")
        assert normalize(0, 0) == Decimal("0")

    def test_normalize_one_eth(self) -> None:
        """Test normalizing 1 ETH (10^18 wei)."""
        wei = 1_000_000_000_000_000_000
        result = normalize(wei, 18)
        assert result == Decimal("1")

    def test_normalize_one_usdc(self) -> None:
        """Test normalizing 1 USDC (10^6 units)."""
        raw = 1_000_000
        result = normalize(raw, 6)
        assert result == Decimal("1")

    def test_normalize_one_wbtc(self) -> None:
        """Test normalizing 1 WBTC (10^8 satoshis)."""
        raw = 100_000_000
        result = normalize(raw, 8)
        assert result == Decimal("1")

    def test_normalize_fractional_eth(self) -> None:
        """Test normalizing fractional ETH."""
        # 0.5 ETH
        wei = 500_000_000_000_000_000
        result = normalize(wei, 18)
        assert result == Decimal("0.5")

        # 0.001 ETH (1 finney)
        wei = 1_000_000_000_000_000
        result = normalize(wei, 18)
        assert result == Decimal("0.001")

    def test_normalize_fractional_usdc(self) -> None:
        """Test normalizing fractional USDC."""
        # 1.50 USDC
        raw = 1_500_000
        result = normalize(raw, 6)
        assert result == Decimal("1.5")

        # 0.01 USDC (1 cent)
        raw = 10_000
        result = normalize(raw, 6)
        assert result == Decimal("0.01")

    def test_normalize_very_large_number(self) -> None:
        """Test normalizing very large amounts (whale territory)."""
        # 1 billion ETH (extreme case)
        wei = 1_000_000_000 * 10**18
        result = normalize(wei, 18)
        assert result == Decimal("1000000000")

        # 1 trillion USDC
        raw = 1_000_000_000_000 * 10**6
        result = normalize(raw, 6)
        assert result == Decimal("1000000000000")

    def test_normalize_very_small_number(self) -> None:
        """Test normalizing very small amounts (dust)."""
        # 1 wei
        result = normalize(1, 18)
        assert result == Decimal("1E-18")

        # 1 raw USDC unit
        result = normalize(1, 6)
        assert result == Decimal("0.000001")

    def test_normalize_zero_decimals(self) -> None:
        """Test normalizing with 0 decimals (some NFTs, special tokens)."""
        result = normalize(100, 0)
        assert result == Decimal("100")

    def test_normalize_high_decimals(self) -> None:
        """Test normalizing with high decimal count."""
        # 24 decimals (some tokens use this)
        raw = 10**24
        result = normalize(raw, 24)
        assert result == Decimal("1")

    def test_normalize_negative_decimals_raises(self) -> None:
        """Test that negative decimals raises ValueError."""
        with pytest.raises(ValueError, match="cannot be negative"):
            normalize(1000, -1)

    def test_normalize_excessive_decimals_raises(self) -> None:
        """Test that decimals > 77 raises ValueError."""
        with pytest.raises(ValueError, match="cannot exceed 77"):
            normalize(1000, 78)

    def test_normalize_preserves_precision(self) -> None:
        """Test that Decimal precision is preserved."""
        # 123.456789012345678901234567 ETH (very precise)
        wei = 123456789012345678901234567
        result = normalize(wei, 18)
        # Should preserve all significant digits
        assert result == Decimal("123456789.012345678901234567")


# =============================================================================
# Test denormalize()
# =============================================================================


class TestDenormalize:
    """Tests for the denormalize function."""

    def test_denormalize_zero(self) -> None:
        """Test denormalizing zero amount."""
        assert denormalize(Decimal("0"), 18) == 0
        assert denormalize(Decimal("0"), 6) == 0
        assert denormalize(Decimal("0"), 0) == 0

    def test_denormalize_one_eth(self) -> None:
        """Test denormalizing 1 ETH to wei."""
        result = denormalize(Decimal("1"), 18)
        assert result == 1_000_000_000_000_000_000

    def test_denormalize_one_usdc(self) -> None:
        """Test denormalizing 1 USDC to raw units."""
        result = denormalize(Decimal("1"), 6)
        assert result == 1_000_000

    def test_denormalize_one_wbtc(self) -> None:
        """Test denormalizing 1 WBTC to satoshis."""
        result = denormalize(Decimal("1"), 8)
        assert result == 100_000_000

    def test_denormalize_fractional(self) -> None:
        """Test denormalizing fractional amounts."""
        # 0.5 ETH
        result = denormalize(Decimal("0.5"), 18)
        assert result == 500_000_000_000_000_000

        # 1.50 USDC
        result = denormalize(Decimal("1.5"), 6)
        assert result == 1_500_000

    def test_denormalize_very_large_number(self) -> None:
        """Test denormalizing very large amounts."""
        # 1 billion USDC
        result = denormalize(Decimal("1000000000"), 6)
        assert result == 1_000_000_000_000_000

    def test_denormalize_very_small_number(self) -> None:
        """Test denormalizing very small amounts."""
        # 1 satoshi worth of WBTC
        result = denormalize(Decimal("0.00000001"), 8)
        assert result == 1

    def test_denormalize_zero_decimals(self) -> None:
        """Test denormalizing with 0 decimals."""
        result = denormalize(Decimal("100"), 0)
        assert result == 100

    def test_denormalize_negative_decimals_raises(self) -> None:
        """Test that negative decimals raises ValueError."""
        with pytest.raises(ValueError, match="cannot be negative"):
            denormalize(Decimal("1"), -1)

    def test_denormalize_excessive_decimals_raises(self) -> None:
        """Test that decimals > 77 raises ValueError."""
        with pytest.raises(ValueError, match="cannot exceed 77"):
            denormalize(Decimal("1"), 78)

    def test_denormalize_truncates_excess_precision(self) -> None:
        """Test that excess precision beyond decimals is truncated."""
        # 1.0000001 USDC (7 decimal places, but USDC has 6)
        # Should truncate to 1000000 (losing the 0.0000001)
        result = denormalize(Decimal("1.0000001"), 6)
        assert result == 1_000_000


# =============================================================================
# Test round-trip consistency
# =============================================================================


class TestRoundTrip:
    """Test that normalize and denormalize are inverse operations."""

    def test_round_trip_eth(self) -> None:
        """Test round-trip for ETH amounts."""
        original_wei = 1_234_567_890_123_456_789
        normalized = normalize(original_wei, 18)
        denormalized = denormalize(normalized, 18)
        assert denormalized == original_wei

    def test_round_trip_usdc(self) -> None:
        """Test round-trip for USDC amounts."""
        original_raw = 1_234_567
        normalized = normalize(original_raw, 6)
        denormalized = denormalize(normalized, 6)
        assert denormalized == original_raw

    def test_round_trip_wbtc(self) -> None:
        """Test round-trip for WBTC amounts."""
        original_raw = 12_345_678
        normalized = normalize(original_raw, 8)
        denormalized = denormalize(normalized, 8)
        assert denormalized == original_raw

    def test_round_trip_large_amount(self) -> None:
        """Test round-trip for large amounts."""
        # 10 billion ETH equivalent in wei
        original_wei = 10_000_000_000 * 10**18
        normalized = normalize(original_wei, 18)
        denormalized = denormalize(normalized, 18)
        assert denormalized == original_wei


# =============================================================================
# Test normalize_token() and denormalize_token()
# =============================================================================


class TestNormalizeToken:
    """Tests for registry-based normalization functions."""

    @pytest.fixture
    def registry(self) -> TokenRegistry:
        """Create a registry with test tokens."""
        return get_default_registry()

    def test_normalize_token_eth(self, registry: TokenRegistry) -> None:
        """Test normalizing ETH using registry."""
        wei = 1_000_000_000_000_000_000
        result = normalize_token(wei, "ETH", registry)
        assert result == Decimal("1")

    def test_normalize_token_usdc(self, registry: TokenRegistry) -> None:
        """Test normalizing USDC using registry (6 decimals)."""
        raw = 1_000_000
        result = normalize_token(raw, "USDC", registry)
        assert result == Decimal("1")

    def test_normalize_token_usdt(self, registry: TokenRegistry) -> None:
        """Test normalizing USDT using registry (6 decimals)."""
        raw = 2_500_000
        result = normalize_token(raw, "USDT", registry)
        assert result == Decimal("2.5")

    def test_normalize_token_wbtc(self, registry: TokenRegistry) -> None:
        """Test normalizing WBTC using registry (8 decimals)."""
        raw = 100_000_000
        result = normalize_token(raw, "WBTC", registry)
        assert result == Decimal("1")

    def test_normalize_token_case_insensitive(self, registry: TokenRegistry) -> None:
        """Test that token lookup is case-insensitive."""
        wei = 1_000_000_000_000_000_000
        assert normalize_token(wei, "eth", registry) == Decimal("1")
        assert normalize_token(wei, "ETH", registry) == Decimal("1")
        assert normalize_token(wei, "Eth", registry) == Decimal("1")

    def test_normalize_token_not_found_raises(self, registry: TokenRegistry) -> None:
        """Test that unknown token raises KeyError."""
        with pytest.raises(KeyError, match="not found in registry"):
            normalize_token(1000, "UNKNOWN_TOKEN", registry)

    def test_denormalize_token_eth(self, registry: TokenRegistry) -> None:
        """Test denormalizing ETH using registry."""
        result = denormalize_token(Decimal("1"), "ETH", registry)
        assert result == 1_000_000_000_000_000_000

    def test_denormalize_token_usdc(self, registry: TokenRegistry) -> None:
        """Test denormalizing USDC using registry."""
        result = denormalize_token(Decimal("1"), "USDC", registry)
        assert result == 1_000_000

    def test_denormalize_token_wbtc(self, registry: TokenRegistry) -> None:
        """Test denormalizing WBTC using registry."""
        result = denormalize_token(Decimal("0.5"), "WBTC", registry)
        assert result == 50_000_000

    def test_denormalize_token_not_found_raises(self, registry: TokenRegistry) -> None:
        """Test that unknown token raises KeyError."""
        with pytest.raises(KeyError, match="not found in registry"):
            denormalize_token(Decimal("1"), "UNKNOWN_TOKEN", registry)


# =============================================================================
# Test with custom registry
# =============================================================================


class TestCustomRegistry:
    """Tests using a custom registry."""

    def test_custom_token_normalization(self) -> None:
        """Test normalization with a custom token registry."""
        # Create custom registry
        registry = TokenRegistry()
        custom_token = Token(
            symbol="CUSTOM",
            name="Custom Token",
            decimals=12,
            addresses={"ethereum": "0x1234"},
            coingecko_id=None,
            is_stablecoin=False,
        )
        registry.register(custom_token)

        # Test with 12 decimals
        raw = 1_000_000_000_000  # 10^12
        result = normalize_token(raw, "CUSTOM", registry)
        assert result == Decimal("1")

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
