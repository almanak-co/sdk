"""Tests for Jupiter Lend Receipt Parser."""

from decimal import Decimal

from almanak.framework.connectors.jupiter_lend.receipt_parser import JupiterLendReceiptParser, LendingAmounts


def _make_receipt(pre_balances, post_balances):
    """Helper to build a Solana transaction receipt with token balances."""
    return {
        "meta": {
            "preTokenBalances": pre_balances,
            "postTokenBalances": post_balances,
        },
        "success": True,
    }


USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"


class TestBalanceDeltaExtraction:
    def test_extract_deposit_amounts(self):
        receipt = _make_receipt(
            pre_balances=[
                {
                    "accountIndex": 0,
                    "mint": USDC_MINT,
                    "uiTokenAmount": {"amount": "100000000", "decimals": 6},
                }
            ],
            post_balances=[
                {
                    "accountIndex": 0,
                    "mint": USDC_MINT,
                    "uiTokenAmount": {"amount": "0", "decimals": 6},
                }
            ],
        )
        parser = JupiterLendReceiptParser()
        result = parser.extract_supply_amounts(receipt)
        assert result is not None
        assert result.amount == Decimal("100")
        assert result.amount_raw == 100_000_000
        assert result.token == USDC_MINT
        assert result.action == "deposit"

    def test_extract_borrow_amounts(self):
        receipt = _make_receipt(
            pre_balances=[
                {
                    "accountIndex": 0,
                    "mint": USDC_MINT,
                    "uiTokenAmount": {"amount": "0", "decimals": 6},
                }
            ],
            post_balances=[
                {
                    "accountIndex": 0,
                    "mint": USDC_MINT,
                    "uiTokenAmount": {"amount": "50000000", "decimals": 6},
                }
            ],
        )
        parser = JupiterLendReceiptParser()
        result = parser.extract_borrow_amounts(receipt)
        assert result is not None
        assert result.amount == Decimal("50")
        assert result.action == "borrow"

    def test_extract_with_9_decimals(self):
        """Test SOL amounts with 9 decimals."""
        receipt = _make_receipt(
            pre_balances=[
                {
                    "accountIndex": 0,
                    "mint": SOL_MINT,
                    "uiTokenAmount": {"amount": "5000000000", "decimals": 9},
                }
            ],
            post_balances=[
                {
                    "accountIndex": 0,
                    "mint": SOL_MINT,
                    "uiTokenAmount": {"amount": "3000000000", "decimals": 9},
                }
            ],
        )
        parser = JupiterLendReceiptParser()
        result = parser.extract_withdraw_amounts(receipt)
        assert result is not None
        assert result.amount == Decimal("2")
        assert result.amount_raw == 2_000_000_000

    def test_multiple_token_changes_returns_largest_normalized(self):
        """Largest delta uses normalized (decimal-adjusted) amounts, not raw."""
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000", "decimals": 6}},
                {"accountIndex": 1, "mint": SOL_MINT, "uiTokenAmount": {"amount": "1000000000", "decimals": 9}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0", "decimals": 6}},
                {"accountIndex": 1, "mint": SOL_MINT, "uiTokenAmount": {"amount": "5000000000", "decimals": 9}},
            ],
        )
        parser = JupiterLendReceiptParser()
        result = parser.extract_supply_amounts(receipt)
        assert result is not None
        # USDC delta = 100 tokens, SOL delta = 4 tokens -> USDC wins (normalized)
        assert result.token == USDC_MINT
        assert result.amount == Decimal("100")

    def test_no_balance_changes(self):
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100", "decimals": 6}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100", "decimals": 6}},
            ],
        )
        parser = JupiterLendReceiptParser()
        result = parser.extract_supply_amounts(receipt)
        assert result is None

    def test_empty_receipt(self):
        parser = JupiterLendReceiptParser()
        result = parser.extract_supply_amounts({})
        assert result is None

    def test_no_token_balances(self):
        receipt = {"meta": {}}
        parser = JupiterLendReceiptParser()
        result = parser.extract_supply_amounts(receipt)
        assert result is None


class TestParseReceipt:
    def test_parse_receipt_returns_all_actions(self):
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000", "decimals": 6}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0", "decimals": 6}},
            ],
        )
        parser = JupiterLendReceiptParser()
        # With no action specified, no specific extraction is performed
        result = parser.parse_receipt(receipt)
        assert result["success"] is True
        # When action is empty, no extraction keys are populated
        assert "supply_amounts" not in result
        assert "borrow_amounts" not in result
        assert "repay_amounts" not in result
        assert "withdraw_amounts" not in result

    def test_parse_receipt_with_action_filter(self):
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000", "decimals": 6}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0", "decimals": 6}},
            ],
        )
        parser = JupiterLendReceiptParser()
        result = parser.parse_receipt(receipt, action="deposit")
        assert "supply_amounts" in result
        assert "borrow_amounts" not in result

    def test_parse_receipt_snake_case_fields(self):
        """Test that TransactionReceipt.to_dict() format is also parsed."""
        receipt = {
            "pre_token_balances": [
                {"account_index": 0, "mint": USDC_MINT, "ui_token_amount": {"amount": "100000000", "decimals": 6}},
            ],
            "post_token_balances": [
                {"account_index": 0, "mint": USDC_MINT, "ui_token_amount": {"amount": "50000000", "decimals": 6}},
            ],
            "success": True,
        }
        parser = JupiterLendReceiptParser()
        result = parser.extract_supply_amounts(receipt)
        assert result is not None
        assert result.amount == Decimal("50")
        assert result.amount_raw == 50_000_000
