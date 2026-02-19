"""Unit tests for Morpho flash loan provider selection."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.flash_loan.selector import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_SUPPORTED_CHAINS,
    MORPHO_SUPPORTED_TOKENS,
    PROVIDER_FEES_BPS,
    FlashLoanSelector,
    NoProviderAvailableError,
)


# =========================================================================
# Morpho Provider Constants Tests
# =========================================================================


class TestMorphoConstants:
    """Test Morpho-related constants are correctly defined."""

    def test_morpho_zero_fee(self):
        assert PROVIDER_FEES_BPS["morpho"] == 0

    def test_morpho_supported_on_ethereum(self):
        assert "ethereum" in MORPHO_SUPPORTED_CHAINS

    def test_morpho_supported_on_base(self):
        assert "base" in MORPHO_SUPPORTED_CHAINS

    def test_morpho_not_on_arbitrum(self):
        assert "arbitrum" not in MORPHO_SUPPORTED_CHAINS

    def test_morpho_address_ethereum(self):
        assert MORPHO_BLUE_ADDRESSES["ethereum"] == "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

    def test_morpho_tokens_ethereum(self):
        tokens = MORPHO_SUPPORTED_TOKENS["ethereum"]
        assert "USDC" in tokens
        assert "WETH" in tokens
        assert "USDe" in tokens
        assert "sUSDe" in tokens


# =========================================================================
# Morpho Provider Evaluation Tests
# =========================================================================


class TestMorphoProviderEvaluation:
    """Test FlashLoanSelector with Morpho provider."""

    def test_morpho_available_on_ethereum(self):
        selector = FlashLoanSelector(chain="ethereum")
        info = selector.get_provider_info("morpho", "USDC", Decimal("1000000"))
        assert info.is_available is True
        assert info.fee_bps == 0
        assert info.fee_amount == Decimal("0")
        assert info.pool_address == MORPHO_BLUE_ADDRESSES["ethereum"]

    def test_morpho_available_on_base(self):
        selector = FlashLoanSelector(chain="base")
        info = selector.get_provider_info("morpho", "USDC", Decimal("1000000"))
        assert info.is_available is True

    def test_morpho_unavailable_on_arbitrum(self):
        selector = FlashLoanSelector(chain="arbitrum")
        info = selector.get_provider_info("morpho", "USDC", Decimal("1000000"))
        assert info.is_available is False
        assert "not available" in info.unavailable_reason

    def test_morpho_unsupported_token(self):
        selector = FlashLoanSelector(chain="ethereum")
        info = selector.get_provider_info("morpho", "SHIB", Decimal("1000"))
        assert info.is_available is False
        assert "not supported" in info.unavailable_reason

    def test_morpho_zero_fee_for_any_amount(self):
        selector = FlashLoanSelector(chain="ethereum")
        info = selector.get_provider_info("morpho", "USDC", Decimal("100000000"))
        assert info.fee_amount == Decimal("0")


# =========================================================================
# Selection Integration Tests
# =========================================================================


class TestMorphoSelection:
    """Test that Morpho is properly included in provider selection."""

    def test_morpho_selected_for_fee_priority_on_ethereum(self):
        selector = FlashLoanSelector(chain="ethereum")
        result = selector.select_provider(
            token="USDe",  # Only available on Morpho, not Aave/Balancer
            amount=Decimal("1000000"),
            priority="fee",
        )
        # USDe is only on Morpho, so it must be selected
        assert result.provider == "morpho"
        assert result.fee_bps == 0

    def test_morpho_evaluated_in_providers_list(self):
        selector = FlashLoanSelector(chain="ethereum")
        result = selector.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            priority="fee",
        )
        providers = {p.provider for p in result.providers_evaluated}
        assert "morpho" in providers
        assert "aave" in providers
        assert "balancer" in providers

    def test_is_token_supported_with_morpho(self):
        selector = FlashLoanSelector(chain="ethereum")
        assert selector.is_token_supported("USDe", "morpho") is True
        assert selector.is_token_supported("SHIB", "morpho") is False

    def test_is_token_supported_any_provider(self):
        selector = FlashLoanSelector(chain="ethereum")
        # sUSDe is on Morpho but not on Aave/Balancer
        assert selector.is_token_supported("sUSDe") is True

    def test_get_supported_tokens_includes_morpho(self):
        selector = FlashLoanSelector(chain="ethereum")
        all_tokens = selector.get_supported_tokens()
        morpho_tokens = selector.get_supported_tokens("morpho")
        assert morpho_tokens.issubset(all_tokens)
        assert "USDe" in morpho_tokens

    def test_morpho_tokens_on_unsupported_chain(self):
        selector = FlashLoanSelector(chain="arbitrum")
        tokens = selector.get_supported_tokens("morpho")
        assert len(tokens) == 0


# =========================================================================
# Flash Loan Selection Priority Tests
# =========================================================================


class TestMorphoSelectionPriority:
    """Test that Morpho competes correctly against other providers."""

    def test_zero_fee_providers_compete(self):
        """Morpho and Balancer both have zero fees; gas/reliability should differentiate."""
        selector = FlashLoanSelector(chain="ethereum")
        result = selector.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            priority="fee",
        )
        # Both Balancer and Morpho have zero fees
        # Selection depends on gas/reliability scores
        assert result.is_success is True
        assert result.fee_bps == 0  # Winner should have zero fee

    def test_gas_priority_prefers_morpho(self):
        """Morpho has lowest gas estimate (200k vs 250k vs 300k)."""
        selector = FlashLoanSelector(chain="ethereum")
        result = selector.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            priority="gas",
        )
        # Morpho has lowest gas (200k), should score well for gas priority
        assert result.is_success is True
        morpho_info = next(p for p in result.providers_evaluated if p.provider == "morpho")
        assert morpho_info.gas_estimate == 200000

    def test_reliability_priority(self):
        """Aave has highest reliability (0.98), Morpho second (0.97)."""
        selector = FlashLoanSelector(chain="ethereum")
        result = selector.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            priority="reliability",
        )
        assert result.is_success is True
        # Verify reliability scores are reasonable
        for p in result.providers_evaluated:
            if p.is_available:
                assert p.reliability_score > 0.9
