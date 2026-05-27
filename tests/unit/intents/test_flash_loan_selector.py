"""Unit tests for FlashLoanSelector.

Tests cover:
- Provider selection across different priorities
- Token availability checking
- Fee calculation
- Liquidity requirements
- Error handling for unsupported tokens
"""

from decimal import Decimal

import pytest

from almanak.connectors.aave_v3.flash_loan_provider import (
    AAVE_V3_FLASH_LOAN_FEE_BPS,
    AAVE_V3_SUPPORTED_TOKENS,
    AaveFlashLoanProvider,
)
from almanak.connectors.balancer_v2.flash_loan_provider import (
    BALANCER_FLASH_LOAN_FEE_BPS,
    BALANCER_SUPPORTED_TOKENS,
    BalancerFlashLoanProvider,
)
from almanak.connectors.morpho_blue.flash_loan_provider import (
    MorphoFlashLoanProvider,
)
from almanak.framework.intents.flash_loan_selector import (
    FlashLoanSelector,
    NoProviderAvailableError,
    SelectionPriority,
)


def _all_providers() -> list:
    return [
        AaveFlashLoanProvider(),
        BalancerFlashLoanProvider(),
        MorphoFlashLoanProvider(),
    ]


def _expected_aave_fee(amount: Decimal) -> Decimal:
    """Compute the expected Aave V3 flash-loan fee from the connector constant.

    Keeps test expectations in sync with ``AAVE_V3_FLASH_LOAN_FEE_BPS`` instead
    of hard-coding ``9`` in multiple places.
    """
    return amount * Decimal(AAVE_V3_FLASH_LOAN_FEE_BPS) / Decimal("10000")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def selector_ethereum() -> FlashLoanSelector:
    return FlashLoanSelector(chain="ethereum", providers=_all_providers())


@pytest.fixture
def selector_arbitrum() -> FlashLoanSelector:
    return FlashLoanSelector(chain="arbitrum", providers=_all_providers())


@pytest.fixture
def selector_base() -> FlashLoanSelector:
    return FlashLoanSelector(chain="base", providers=_all_providers())


# =============================================================================
# Initialization
# =============================================================================


class TestFlashLoanSelectorInit:
    def test_init_valid_chain(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        assert selector.chain == "ethereum"
        assert selector.default_priority == SelectionPriority.FEE
        assert len(selector.providers) == 3

    def test_init_with_custom_priority(self) -> None:
        selector = FlashLoanSelector(
            chain="ethereum",
            providers=_all_providers(),
            default_priority=SelectionPriority.LIQUIDITY,
        )
        assert selector.default_priority == SelectionPriority.LIQUIDITY

    def test_unknown_chain_yields_no_provider(self) -> None:
        # The abstract selector accepts any chain; each provider's quote()
        # marks itself unavailable when the chain is unsupported. Result:
        # the selector surfaces NoProviderAvailableError, not ValueError.
        selector = FlashLoanSelector(chain="not-a-chain", providers=_all_providers())
        with pytest.raises(NoProviderAvailableError):
            selector.select_provider(token="USDC", amount=Decimal("1000"))


# =============================================================================
# Provider selection
# =============================================================================


class TestSelectProvider:
    def test_fee_priority_prefers_zero_fee(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(
            token="USDC", amount=Decimal("1000000"), priority="fee"
        )
        assert result.is_success
        assert result.provider in ("balancer", "morpho")
        assert result.fee_bps == 0
        assert result.fee_amount == Decimal("0")
        assert result.total_repay == Decimal("1000000")

    def test_reliability_priority_evaluates_all(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(
            token="USDC", amount=Decimal("1000000"), priority="reliability"
        )
        assert result.is_success
        assert result.provider in ("aave", "balancer", "morpho")
        provider_names = [p.provider for p in result.providers_evaluated]
        assert {"aave", "balancer", "morpho"}.issubset(provider_names)

    def test_aave_only_token_yields_aave(self, selector_ethereum: FlashLoanSelector) -> None:
        # LUSD is in Aave's supported set on Ethereum but not Balancer's or Morpho's.
        amount = Decimal("10000")
        result = selector_ethereum.select_provider(token="LUSD", amount=amount)
        assert result.is_success
        assert result.provider == "aave"
        expected_fee = _expected_aave_fee(amount)
        assert result.fee_amount == expected_fee
        assert result.total_repay == amount + expected_fee

    def test_unsupported_token_raises(self, selector_ethereum: FlashLoanSelector) -> None:
        with pytest.raises(NoProviderAvailableError, match="No flash loan provider"):
            selector_ethereum.select_provider(token="UNKNOWN_TOKEN", amount=Decimal("1000"))

    def test_pool_address_returned(self, selector_arbitrum: FlashLoanSelector) -> None:
        result = selector_arbitrum.select_provider(token="WETH", amount=Decimal("100"))
        assert result.is_success
        assert result.pool_address.startswith("0x")

    def test_min_liquidity(self, selector_ethereum: FlashLoanSelector) -> None:
        # Aave Ethereum/USDC liquidity snapshot is 2B; require 1B and ensure success.
        result = selector_ethereum.select_provider(
            token="USDC",
            amount=Decimal("1000000"),
            min_liquidity_usd=1_000_000_000,
        )
        assert result.is_success

    def test_default_priority_is_fee(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(token="USDC", amount=Decimal("1000"))
        assert result.is_success
        assert result.provider in ("balancer", "morpho")
        assert result.fee_bps == 0


# =============================================================================
# Token support
# =============================================================================


class TestTokenSupport:
    def test_any_provider_supports_common_tokens(self, selector_ethereum: FlashLoanSelector) -> None:
        assert selector_ethereum.is_token_supported("USDC") is True
        assert selector_ethereum.is_token_supported("WETH") is True
        assert selector_ethereum.is_token_supported("FAKE_TOKEN") is False

    def test_specific_provider_aave(self, selector_ethereum: FlashLoanSelector) -> None:
        assert selector_ethereum.is_token_supported("USDC", provider="aave") is True
        # GHO is Aave-specific in the static allowlists.
        assert selector_ethereum.is_token_supported("GHO", provider="aave") is True

    def test_specific_provider_balancer(self, selector_ethereum: FlashLoanSelector) -> None:
        assert selector_ethereum.is_token_supported("USDC", provider="balancer") is True
        assert selector_ethereum.is_token_supported("wstETH", provider="balancer") is True

    def test_per_connector_constants_unchanged(self) -> None:
        # Sanity check that the constants moved with their connectors.
        assert "USDC" in AAVE_V3_SUPPORTED_TOKENS["ethereum"]
        assert "USDC" in BALANCER_SUPPORTED_TOKENS["ethereum"]


# =============================================================================
# Provider info
# =============================================================================


class TestGetProviderInfo:
    def test_aave_info(self, selector_ethereum: FlashLoanSelector) -> None:
        info = selector_ethereum.get_provider_info(
            provider="aave", token="USDC", amount=Decimal("10000")
        )
        assert info.provider == "aave"
        assert info.is_available is True
        assert info.fee_bps == AAVE_V3_FLASH_LOAN_FEE_BPS
        assert info.fee_amount == _expected_aave_fee(Decimal("10000"))
        assert info.pool_address != ""

    def test_balancer_info(self, selector_ethereum: FlashLoanSelector) -> None:
        info = selector_ethereum.get_provider_info(
            provider="balancer", token="USDC", amount=Decimal("10000")
        )
        assert info.provider == "balancer"
        assert info.is_available is True
        assert info.fee_bps == BALANCER_FLASH_LOAN_FEE_BPS
        assert info.fee_amount == Decimal("0")
        assert info.pool_address != ""

    def test_unsupported_token(self, selector_ethereum: FlashLoanSelector) -> None:
        info = selector_ethereum.get_provider_info(
            provider="aave", token="UNKNOWN", amount=Decimal("1000")
        )
        assert info.is_available is False
        assert info.unavailable_reason is not None
        assert "not supported" in info.unavailable_reason

    def test_unknown_provider(self, selector_ethereum: FlashLoanSelector) -> None:
        info = selector_ethereum.get_provider_info(
            provider="unknown", token="USDC", amount=Decimal("1000")
        )
        assert info.is_available is False
        assert "Unknown provider" in (info.unavailable_reason or "")


# =============================================================================
# Result shape
# =============================================================================


class TestSelectionResult:
    def test_is_success(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(token="USDC", amount=Decimal("1000"))
        assert result.is_success is True

    def test_to_dict(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(token="USDC", amount=Decimal("1000"))
        d = result.to_dict()
        for key in (
            "provider",
            "pool_address",
            "fee_bps",
            "fee_amount",
            "total_repay",
            "providers_evaluated",
            "selection_reasoning",
        ):
            assert key in d

    def test_providers_evaluated_complete(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(token="USDC", amount=Decimal("1000"))
        names = [p.provider for p in result.providers_evaluated]
        assert {"aave", "balancer", "morpho"}.issubset(names)

    def test_provider_info_to_dict(self, selector_ethereum: FlashLoanSelector) -> None:
        info = selector_ethereum.get_provider_info(
            provider="aave", token="USDC", amount=Decimal("1000")
        )
        d = info.to_dict()
        for key in (
            "provider",
            "is_available",
            "fee_bps",
            "fee_amount",
            "estimated_liquidity_usd",
            "gas_estimate",
            "pool_address",
            "reliability_score",
        ):
            assert key in d


# =============================================================================
# Chain-specific behaviour
# =============================================================================


class TestChainSpecificBehavior:
    def test_arbitrum(self, selector_arbitrum: FlashLoanSelector) -> None:
        result = selector_arbitrum.select_provider(token="USDC", amount=Decimal("1000"))
        assert result.is_success
        assert result.pool_address != ""

    def test_base_limited_balancer_support(self, selector_base: FlashLoanSelector) -> None:
        result = selector_base.select_provider(token="USDC", amount=Decimal("1000"))
        assert result.is_success


# =============================================================================
# Priority weights
# =============================================================================


class TestPriorityWeights:
    def test_gas_priority(self, selector_ethereum: FlashLoanSelector) -> None:
        # Per-connector gas estimates: aave 300k, balancer 250k, morpho 200k.
        result = selector_ethereum.select_provider(
            token="USDC", amount=Decimal("1000"), priority="gas"
        )
        assert result.is_success
        assert result.provider == "morpho"

    def test_liquidity_priority_prefers_aave(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(
            token="USDC", amount=Decimal("1000"), priority="liquidity"
        )
        assert result.is_success
        assert result.provider == "aave"


# =============================================================================
# Edge cases
# =============================================================================


class TestEdgeCases:
    def test_zero_amount(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(token="USDC", amount=Decimal("0"))
        assert result.is_success
        assert result.fee_amount == Decimal("0")

    def test_very_large_amount(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(
            token="USDC", amount=Decimal("1000000000")
        )
        assert result.is_success
        if result.provider == "aave":
            expected_fee = _expected_aave_fee(Decimal("1000000000"))
            assert result.fee_amount == expected_fee

    def test_invalid_priority_falls_back_to_default(self, selector_ethereum: FlashLoanSelector) -> None:
        result = selector_ethereum.select_provider(
            token="USDC", amount=Decimal("1000"), priority="invalid_priority"
        )
        assert result.is_success
        assert result.provider in ("balancer", "morpho")
        assert result.fee_bps == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
