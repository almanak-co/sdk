"""Tests for Balancer flash loan intent creation and serialization.

Verifies that FlashLoanIntent with provider="balancer" can be created,
serialized, and deserialized correctly. This tests the intent layer
(not on-chain execution) -- the first kitchenloop test of Balancer intents.
"""

from decimal import Decimal

import pytest

from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import FlashLoanIntent, IntentType, SwapIntent


class TestBalancerFlashLoanIntent:
    """Test FlashLoanIntent creation with Balancer provider."""

    def test_create_balancer_flash_loan_intent(self):
        """Create a basic Balancer flash loan intent."""
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("1000"),
                    max_slippage=Decimal("0.01"),
                    protocol="enso",
                ),
            ],
            chain="arbitrum",
        )

        assert isinstance(intent, FlashLoanIntent)
        assert intent.provider == "balancer"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("1000")
        assert intent.chain == "arbitrum"
        assert intent.intent_type == IntentType.FLASH_LOAN
        assert len(intent.callback_intents) == 1

    def test_balancer_flash_loan_zero_fee(self):
        """Verify Balancer flash loan has no fee field (zero fee)."""
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("5000"),
            callback_intents=[
                Intent.swap("USDC", "WETH", amount=Decimal("5000"), max_slippage=Decimal("0.01"), protocol="enso"),
                Intent.swap("WETH", "USDC", amount="all", max_slippage=Decimal("0.01"), protocol="enso"),
            ],
            chain="arbitrum",
        )

        assert intent.provider == "balancer"
        assert len(intent.callback_intents) == 2

    def test_balancer_flash_loan_round_trip_serialization(self):
        """Serialize and deserialize a Balancer flash loan intent."""
        original = Intent.flash_loan(
            provider="balancer",
            token="WETH",
            amount=Decimal("10"),
            callback_intents=[
                Intent.swap("WETH", "USDC", amount=Decimal("10"), max_slippage=Decimal("0.005"), protocol="enso"),
            ],
            chain="ethereum",
        )

        serialized = original.serialize()
        assert serialized["type"] == "FLASH_LOAN"
        assert serialized["provider"] == "balancer"
        assert serialized["token"] == "WETH"

        deserialized = FlashLoanIntent.deserialize(serialized)
        assert deserialized.provider == "balancer"
        assert deserialized.token == "WETH"
        assert deserialized.amount == Decimal("10")
        assert len(deserialized.callback_intents) == 1

    def test_callback_intents_are_swap_intents(self):
        """Verify callback intents are properly typed SwapIntents."""
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap("USDC", "WETH", amount=Decimal("1000"), max_slippage=Decimal("0.01"), protocol="enso"),
            ],
            chain="arbitrum",
        )

        callback = intent.callback_intents[0]
        assert isinstance(callback, SwapIntent)
        assert callback.from_token == "USDC"
        assert callback.to_token == "WETH"

    def test_flash_loan_requires_callback_intents(self):
        """Flash loan with empty callbacks should raise validation error."""
        with pytest.raises(ValueError, match="callback intent"):
            Intent.flash_loan(
                provider="balancer",
                token="USDC",
                amount=Decimal("1000"),
                callback_intents=[],
                chain="arbitrum",
            )
