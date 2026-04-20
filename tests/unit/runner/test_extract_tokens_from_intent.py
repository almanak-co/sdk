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

    def test_lp_intent_skips_fee_tier_in_pool_string(self):
        """Pool strings like 'WETH/USDC/500' should not extract '500' as a token."""
        intent = Intent.lp_open(
            pool="WETH/USDC/500",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert "500" not in tokens

    def test_lp_intent_skips_various_fee_tiers(self):
        """Fee tiers (500, 3000, 10000) and bin steps (20) should all be filtered."""
        for fee in ["500", "3000", "10000", "20"]:
            intent = Intent.lp_open(
                pool=f"WETH/USDT/{fee}",
                amount0=Decimal("1"),
                amount1=Decimal("2000"),
                range_lower=Decimal("1500"),
                range_upper=Decimal("2500"),
            )
            tokens = _extract_tokens_from_intent(intent)
            assert fee not in tokens, f"Fee tier '{fee}' should not be extracted as a token"

    def test_lp_intent_skips_volatile_pool_type(self):
        """VIB-1642: pool string 'WETH/USDC/volatile' must not extract 'volatile' as a token."""
        intent = Intent.lp_open(
            pool="WETH/USDC/volatile",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert "volatile" not in tokens
        assert "VOLATILE" not in tokens

    def test_lp_intent_skips_stable_pool_type(self):
        """Pool string 'USDC/DAI/stable' must not extract 'stable' as a token."""
        intent = Intent.lp_open(
            pool="USDC/DAI/stable",
            amount0=Decimal("1000"),
            amount1=Decimal("1000"),
            range_lower=Decimal("0.99"),
            range_upper=Decimal("1.01"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "USDC" in tokens
        assert "DAI" in tokens
        assert "stable" not in tokens

    def test_lp_intent_skips_concentrated_pool_type(self):
        """Pool string with 'concentrated' or 'cl' suffix must be filtered."""
        for suffix in ["concentrated", "cl"]:
            intent = Intent.lp_open(
                pool=f"WETH/USDC/{suffix}",
                amount0=Decimal("1"),
                amount1=Decimal("2000"),
                range_lower=Decimal("1500"),
                range_upper=Decimal("2500"),
            )
            tokens = _extract_tokens_from_intent(intent)
            assert "WETH" in tokens
            assert "USDC" in tokens
            assert suffix not in tokens, f"Pool type '{suffix}' should not be extracted as a token"

    def test_lp_intent_skips_pool_type_with_trailing_slash(self):
        """Pool string with trailing slash must still filter the pool-type suffix."""
        intent = Intent.lp_open(
            pool="WETH/USDC/volatile/",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
        )
        tokens = _extract_tokens_from_intent(intent)
        assert "WETH" in tokens
        assert "USDC" in tokens
        assert "volatile" not in tokens
