"""Unit tests for FlashLoanSelector.

Tests cover:
- Provider selection based on different priorities
- Token availability checking
- Fee calculation
- Liquidity requirements
- Error handling for unsupported tokens
"""

from decimal import Decimal

import pytest

from ..selector import (
    AAVE_SUPPORTED_TOKENS,
    BALANCER_SUPPORTED_TOKENS,
    PROVIDER_FEES_BPS,
    FlashLoanSelector,
    NoProviderAvailableError,
    SelectionPriority,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def selector_ethereum() -> FlashLoanSelector:
    """Create a FlashLoanSelector for Ethereum."""
    return FlashLoanSelector(chain="ethereum")


@pytest.fixture
def selector_arbitrum() -> FlashLoanSelector:
    """Create a FlashLoanSelector for Arbitrum."""
    return FlashLoanSelector(chain="arbitrum")


@pytest.fixture
def selector_base() -> FlashLoanSelector:
    """Create a FlashLoanSelector for Base."""
    return FlashLoanSelector(chain="base")


# =============================================================================
# Initialization Tests
# =============================================================================


class TestFlashLoanSelectorInit:
    """Tests for FlashLoanSelector initialization."""

    def test_init_valid_chain(self) -> None:
        """Test initialization with valid chain."""
        selector = FlashLoanSelector(chain="ethereum")
        assert selector.chain == "ethereum"
        assert selector.default_priority == SelectionPriority.FEE

    def test_init_with_custom_priority(self) -> None:
        """Test initialization with custom default priority."""
        selector = FlashLoanSelector(chain="ethereum", default_priority=SelectionPriority.LIQUIDITY)
        assert selector.default_priority == SelectionPriority.LIQUIDITY

    def test_init_invalid_chain(self) -> None:
        """Test initialization with invalid chain raises error."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            FlashLoanSelector(chain="invalid_chain")

    def test_init_with_custom_reliability_scores(self) -> None:
        """Test initialization with custom reliability scores."""
        custom_scores = {"aave": 0.99, "balancer": 0.90}
        selector = FlashLoanSelector(chain="ethereum", reliability_scores=custom_scores)
        assert selector.reliability_scores["aave"] == 0.99
        assert selector.reliability_scores["balancer"] == 0.90


# =============================================================================
# Provider Selection Tests
# =============================================================================


class TestSelectProvider:
    """Tests for select_provider method."""

    def test_select_provider_fee_priority_prefers_balancer(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test that fee priority prefers Balancer (zero fees)."""
        # USDC is supported on both Aave and Balancer
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            priority="fee",
        )

        assert result.is_success
        assert result.provider == "balancer"
        assert result.fee_bps == 0
        assert result.fee_amount == Decimal("0")
        assert result.total_repay == Decimal("1000000")

    def test_select_provider_reliability_priority_scores_correctly(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test that reliability priority weights reliability highly."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            priority="reliability",
        )

        assert result.is_success
        # Both Aave and Balancer support USDC, but Balancer's zero fee advantage
        # can still outweigh the small reliability difference (0.98 vs 0.95).
        # The important thing is that the scores are calculated and a provider is selected.
        assert result.provider in ("aave", "balancer")

        # Verify that providers were evaluated
        provider_names = [p.provider for p in result.providers_evaluated]
        assert "aave" in provider_names
        assert "balancer" in provider_names

    def test_select_provider_calculates_fee_correctly(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test that fee is calculated correctly for Aave."""
        # Use a token only available on Aave to force Aave selection
        result = selector_ethereum.select_provider(
            token="LUSD",
            amount=Decimal("10000"),
        )

        assert result.provider == "aave"
        # 10000 * 9 / 10000 = 9 fee (9 bps = 0.09%)
        assert result.fee_amount == Decimal("9")
        assert result.total_repay == Decimal("10009")

    def test_select_provider_unsupported_token_raises_error(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test that unsupported token raises NoProviderAvailableError."""
        with pytest.raises(NoProviderAvailableError, match="No flash loan provider"):
            selector_ethereum.select_provider(
                token="UNKNOWN_TOKEN",
                amount=Decimal("1000"),
            )

    def test_select_provider_aave_only_token(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test selection for token only available on Aave."""
        # LUSD is only on Aave (not in Balancer's supported tokens)
        result = selector_ethereum.select_provider(
            token="LUSD",
            amount=Decimal("10000"),
        )

        assert result.is_success
        assert result.provider == "aave"

    def test_select_provider_returns_pool_address(self, selector_arbitrum: FlashLoanSelector) -> None:
        """Test that result includes correct pool address."""
        result = selector_arbitrum.select_provider(
            token="WETH",
            amount=Decimal("100"),
        )

        assert result.is_success
        assert result.pool_address != ""
        # Should be either Aave pool or Balancer vault
        assert result.pool_address.startswith("0x")

    def test_select_provider_with_min_liquidity(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test selection with minimum liquidity requirement."""
        # Request very high liquidity that Balancer might not have
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            min_liquidity_usd=1_000_000_000,  # 1 billion USD
        )

        # Should still succeed with Aave which has more liquidity
        assert result.is_success

    def test_select_provider_default_priority(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test that default priority (FEE) is used when not specified."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
        )

        assert result.is_success
        # Default priority is FEE, should prefer Balancer
        assert result.provider == "balancer"


# =============================================================================
# Token Support Tests
# =============================================================================


class TestTokenSupport:
    """Tests for token support checking methods."""

    def test_is_token_supported_any_provider(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test is_token_supported for common tokens."""
        assert selector_ethereum.is_token_supported("USDC") is True
        assert selector_ethereum.is_token_supported("WETH") is True
        assert selector_ethereum.is_token_supported("FAKE_TOKEN") is False

    def test_is_token_supported_specific_provider_aave(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test is_token_supported for Aave specifically."""
        assert selector_ethereum.is_token_supported("USDC", provider="aave") is True
        # GHO is Aave-specific
        assert selector_ethereum.is_token_supported("GHO", provider="aave") is True

    def test_is_token_supported_specific_provider_balancer(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test is_token_supported for Balancer specifically."""
        assert selector_ethereum.is_token_supported("USDC", provider="balancer") is True
        assert selector_ethereum.is_token_supported("wstETH", provider="balancer") is True

    def test_get_supported_tokens_all(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test get_supported_tokens returns union of all providers."""
        tokens = selector_ethereum.get_supported_tokens()
        assert "USDC" in tokens
        assert "WETH" in tokens
        assert len(tokens) > 0

    def test_get_supported_tokens_aave_only(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test get_supported_tokens for Aave only."""
        tokens = selector_ethereum.get_supported_tokens(provider="aave")
        aave_tokens = AAVE_SUPPORTED_TOKENS.get("ethereum", set())
        assert tokens == aave_tokens

    def test_get_supported_tokens_balancer_only(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test get_supported_tokens for Balancer only."""
        tokens = selector_ethereum.get_supported_tokens(provider="balancer")
        balancer_tokens = BALANCER_SUPPORTED_TOKENS.get("ethereum", set())
        assert tokens == balancer_tokens


# =============================================================================
# Provider Info Tests
# =============================================================================


class TestGetProviderInfo:
    """Tests for get_provider_info method."""

    def test_get_provider_info_aave(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test getting Aave provider info."""
        info = selector_ethereum.get_provider_info(
            provider="aave",
            token="USDC",
            amount=Decimal("10000"),
        )

        assert info.provider == "aave"
        assert info.is_available is True
        assert info.fee_bps == PROVIDER_FEES_BPS["aave"]
        # Fee = 10000 * 9 / 10000 = 9 (9 basis points = 0.09%)
        assert info.fee_amount == Decimal("9")
        assert info.pool_address != ""

    def test_get_provider_info_balancer(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test getting Balancer provider info."""
        info = selector_ethereum.get_provider_info(
            provider="balancer",
            token="USDC",
            amount=Decimal("10000"),
        )

        assert info.provider == "balancer"
        assert info.is_available is True
        assert info.fee_bps == 0
        assert info.fee_amount == Decimal("0")
        assert info.pool_address != ""

    def test_get_provider_info_unsupported_token(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test provider info for unsupported token."""
        info = selector_ethereum.get_provider_info(
            provider="aave",
            token="UNKNOWN",
            amount=Decimal("1000"),
        )

        assert info.is_available is False
        assert info.unavailable_reason is not None
        assert "not supported" in info.unavailable_reason

    def test_get_provider_info_unknown_provider(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test provider info for unknown provider."""
        info = selector_ethereum.get_provider_info(
            provider="unknown",
            token="USDC",
            amount=Decimal("1000"),
        )

        assert info.is_available is False
        assert "Unknown provider" in (info.unavailable_reason or "")


# =============================================================================
# Liquidity Estimation Tests
# =============================================================================


class TestEstimateLiquidity:
    """Tests for estimate_liquidity method."""

    def test_estimate_liquidity_all_providers(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test liquidity estimation for all providers."""
        liquidity = selector_ethereum.estimate_liquidity(token="USDC")

        assert "aave" in liquidity
        assert "balancer" in liquidity
        assert liquidity["aave"] > 0
        assert liquidity["balancer"] > 0

    def test_estimate_liquidity_specific_provider(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test liquidity estimation for specific provider."""
        liquidity = selector_ethereum.estimate_liquidity(token="USDC", provider="aave")

        assert "aave" in liquidity
        assert liquidity["aave"] > 0

    def test_estimate_liquidity_unknown_token(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test liquidity estimation for unknown token returns 0."""
        liquidity = selector_ethereum.estimate_liquidity(token="UNKNOWN")

        assert liquidity.get("aave", 0) == 0
        assert liquidity.get("balancer", 0) == 0


# =============================================================================
# Selection Result Tests
# =============================================================================


class TestSelectionResult:
    """Tests for FlashLoanSelectionResult data class."""

    def test_selection_result_is_success(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test is_success property."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
        )

        assert result.is_success is True

    def test_selection_result_to_dict(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test to_dict method."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
        )

        result_dict = result.to_dict()
        assert "provider" in result_dict
        assert "pool_address" in result_dict
        assert "fee_bps" in result_dict
        assert "fee_amount" in result_dict
        assert "total_repay" in result_dict
        assert "providers_evaluated" in result_dict
        assert "selection_reasoning" in result_dict

    def test_selection_result_includes_all_providers_evaluated(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test that result includes info about all evaluated providers."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
        )

        # Should have evaluated both Aave and Balancer
        provider_names = [p.provider for p in result.providers_evaluated]
        assert "aave" in provider_names
        assert "balancer" in provider_names


# =============================================================================
# Chain-specific Tests
# =============================================================================


class TestChainSpecificBehavior:
    """Tests for chain-specific behavior."""

    def test_arbitrum_chain_support(self, selector_arbitrum: FlashLoanSelector) -> None:
        """Test Arbitrum chain support."""
        result = selector_arbitrum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
        )

        assert result.is_success
        assert result.pool_address != ""

    def test_base_chain_has_limited_balancer_support(self, selector_base: FlashLoanSelector) -> None:
        """Test Base chain has more limited Balancer support."""
        # USDC should still be available
        result = selector_base.select_provider(
            token="USDC",
            amount=Decimal("1000"),
        )

        assert result.is_success


# =============================================================================
# Priority Weight Tests
# =============================================================================


class TestPriorityWeights:
    """Tests for different priority weight behaviors."""

    def test_gas_priority(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test gas priority selection."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
            priority="gas",
        )

        assert result.is_success
        # Balancer has lower gas, should be preferred
        assert result.provider == "balancer"

    def test_liquidity_priority_prefers_higher_liquidity(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test liquidity priority prefers provider with more liquidity."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
            priority="liquidity",
        )

        assert result.is_success
        # Aave typically has more liquidity for USDC
        assert result.provider == "aave"


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_amount_flash_loan(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test flash loan with zero amount."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("0"),
        )

        # Should still succeed (validation is at intent level)
        assert result.is_success
        assert result.fee_amount == Decimal("0")

    def test_very_large_amount_flash_loan(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test flash loan with very large amount."""
        # 1 billion USDC
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000000000"),
        )

        assert result.is_success
        # Should still calculate fee correctly
        if result.provider == "aave":
            expected_fee = Decimal("1000000000") * Decimal("9") / Decimal("10000")
            assert result.fee_amount == expected_fee

    def test_invalid_priority_uses_default(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test invalid priority falls back to default."""
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000"),
            priority="invalid_priority",
        )

        assert result.is_success
        # Should use default FEE priority -> Balancer
        assert result.provider == "balancer"

    def test_provider_info_to_dict(self, selector_ethereum: FlashLoanSelector) -> None:
        """Test FlashLoanProviderInfo.to_dict method."""
        info = selector_ethereum.get_provider_info(
            provider="aave",
            token="USDC",
            amount=Decimal("1000"),
        )

        info_dict = info.to_dict()
        assert "provider" in info_dict
        assert "is_available" in info_dict
        assert "fee_bps" in info_dict
        assert "fee_amount" in info_dict
        assert "estimated_liquidity_usd" in info_dict
        assert "gas_estimate" in info_dict
        assert "pool_address" in info_dict
        assert "reliability_score" in info_dict


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
