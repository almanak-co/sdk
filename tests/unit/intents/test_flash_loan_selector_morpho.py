"""Unit tests for Morpho flash-loan provider integration."""

from decimal import Decimal

import pytest

from almanak.connectors.aave_v3.flash_loan_provider import AaveFlashLoanProvider
from almanak.connectors.balancer_v2.flash_loan_provider import BalancerFlashLoanProvider
from almanak.connectors.morpho_blue.flash_loan_provider import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_FLASH_LOAN_FEE_BPS,
    MORPHO_FLASH_LOAN_GAS_ESTIMATE,
    MORPHO_SUPPORTED_CHAINS,
    MORPHO_SUPPORTED_TOKENS,
    MorphoFlashLoanProvider,
)
from almanak.framework.intents.flash_loan_selector import FlashLoanSelector


def _all_providers() -> list:
    return [
        AaveFlashLoanProvider(),
        BalancerFlashLoanProvider(),
        MorphoFlashLoanProvider(),
    ]


# =========================================================================
# Morpho provider constants
# =========================================================================


class TestMorphoConstants:
    def test_morpho_zero_fee(self) -> None:
        assert MORPHO_FLASH_LOAN_FEE_BPS == 0

    def test_morpho_supported_on_ethereum(self) -> None:
        assert "ethereum" in MORPHO_SUPPORTED_CHAINS

    def test_morpho_supported_on_base(self) -> None:
        assert "base" in MORPHO_SUPPORTED_CHAINS

    def test_morpho_not_on_arbitrum(self) -> None:
        assert "arbitrum" not in MORPHO_SUPPORTED_CHAINS

    def test_morpho_address_ethereum(self) -> None:
        assert MORPHO_BLUE_ADDRESSES["ethereum"] == "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

    def test_morpho_tokens_ethereum(self) -> None:
        tokens = MORPHO_SUPPORTED_TOKENS["ethereum"]
        assert {"USDC", "WETH", "USDe", "sUSDe"}.issubset(tokens)


# =========================================================================
# Morpho provider evaluation via the selector
# =========================================================================


class TestMorphoProviderEvaluation:
    def test_available_on_ethereum(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        info = selector.get_provider_info("morpho", "USDC", Decimal("1000000"))
        assert info.is_available is True
        assert info.fee_bps == 0
        assert info.fee_amount == Decimal("0")
        assert info.pool_address == MORPHO_BLUE_ADDRESSES["ethereum"]

    def test_available_on_base(self) -> None:
        selector = FlashLoanSelector(chain="base", providers=_all_providers())
        info = selector.get_provider_info("morpho", "USDC", Decimal("1000000"))
        assert info.is_available is True

    def test_unavailable_on_arbitrum(self) -> None:
        selector = FlashLoanSelector(chain="arbitrum", providers=_all_providers())
        info = selector.get_provider_info("morpho", "USDC", Decimal("1000000"))
        assert info.is_available is False
        assert info.unavailable_reason is not None
        assert "not enabled" in info.unavailable_reason

    def test_unsupported_token(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        info = selector.get_provider_info("morpho", "SHIB", Decimal("1000"))
        assert info.is_available is False
        assert info.unavailable_reason is not None
        assert "not supported" in info.unavailable_reason

    def test_zero_fee_for_any_amount(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        info = selector.get_provider_info("morpho", "USDC", Decimal("100000000"))
        assert info.fee_amount == Decimal("0")


# =========================================================================
# Selection integration
# =========================================================================


class TestMorphoSelection:
    def test_morpho_only_token_selected(self) -> None:
        # USDe is in Morpho's allowlist only.
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        result = selector.select_provider(
            token="USDe", amount=Decimal("1000000"), priority="fee"
        )
        assert result.provider == "morpho"
        assert result.fee_bps == 0

    def test_morpho_in_evaluated_list(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        result = selector.select_provider(
            token="USDC", amount=Decimal("1000000"), priority="fee"
        )
        providers = {p.provider for p in result.providers_evaluated}
        assert {"aave", "balancer", "morpho"}.issubset(providers)

    def test_is_token_supported_with_morpho(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        assert selector.is_token_supported("USDe", "morpho") is True
        assert selector.is_token_supported("SHIB", "morpho") is False

    def test_is_token_supported_any(self) -> None:
        # sUSDe is Morpho-only on Ethereum.
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        assert selector.is_token_supported("sUSDe") is True


# =========================================================================
# Priority weights including Morpho
# =========================================================================


class TestMorphoSelectionPriority:
    def test_zero_fee_winner(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        result = selector.select_provider(
            token="USDC", amount=Decimal("1000000"), priority="fee"
        )
        assert result.is_success
        assert result.fee_bps == 0

    def test_gas_priority_morpho_estimate(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        result = selector.select_provider(
            token="USDC", amount=Decimal("1000000"), priority="gas"
        )
        assert result.is_success
        morpho_info = next(p for p in result.providers_evaluated if p.provider == "morpho")
        assert morpho_info.gas_estimate == MORPHO_FLASH_LOAN_GAS_ESTIMATE

    def test_reliability_priority(self) -> None:
        selector = FlashLoanSelector(chain="ethereum", providers=_all_providers())
        result = selector.select_provider(
            token="USDC", amount=Decimal("1000000"), priority="reliability"
        )
        assert result.is_success
        for p in result.providers_evaluated:
            if p.is_available:
                assert p.reliability_score > 0.9
