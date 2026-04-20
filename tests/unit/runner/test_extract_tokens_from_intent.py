"""Tests for _extract_tokens_from_intent including FlashLoanIntent callback recursion.

Verifies that token extraction for price pre-fetching recurses into
FlashLoanIntent callback_intents so the runner pre-warms all referenced
tokens (not just the flash loan's borrow token).

Fixes VIB-1282: balancer_flash_arb compilation_error on arbitrum.
"""

from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.runner.strategy_runner import _extract_tokens_from_intent


class TestExtractTokensFromIntent:
    """Test token extraction for price pre-fetching."""

    def test_swap_intent_extracts_both_tokens(self):
        intent = Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens
        assert "WETH" in tokens

    def test_flash_loan_extracts_borrow_token(self):
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="DAI",
                    amount=Decimal("1000"),
                    max_slippage=Decimal("0.01"),
                ),
            ],
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens

    def test_flash_loan_extracts_callback_tokens(self):
        """FlashLoanIntent should extract tokens from callback_intents.

        This is the exact pattern from balancer_flash_arb:
        borrow USDC -> swap USDC->WETH -> swap WETH->USDC (amount='all').
        Without recursion, WETH would be missing from the extracted tokens,
        causing _require_token_price("WETH") to fail at compile time.
        """
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
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount="all",
                    max_slippage=Decimal("0.01"),
                    protocol="enso",
                ),
            ],
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens
        assert "WETH" in tokens

    def test_flash_loan_deduplicates_tokens(self):
        """Tokens appearing multiple times should be deduplicated."""
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
                ),
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount="all",
                    max_slippage=Decimal("0.01"),
                ),
            ],
        )
        tokens = _extract_tokens_from_intent(intent)
        assert len(tokens) == len(set(tokens)), "Tokens should be deduplicated"

    def test_hold_intent_extracts_no_tokens(self):
        intent = Intent.hold(reason="waiting")
        tokens = _extract_tokens_from_intent(intent)
        assert tokens == []
