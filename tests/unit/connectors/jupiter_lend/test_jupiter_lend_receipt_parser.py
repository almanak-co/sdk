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
        """Supply = tokens leaving wallet (pre > post)."""
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
        """Borrow = tokens arriving at wallet (post > pre)."""
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

    def test_extract_withdraw_with_9_decimals(self):
        """Withdraw = tokens arriving (post > pre), test with 9-decimal SOL."""
        receipt = _make_receipt(
            pre_balances=[
                {
                    "accountIndex": 0,
                    "mint": SOL_MINT,
                    "uiTokenAmount": {"amount": "3000000000", "decimals": 9},
                }
            ],
            post_balances=[
                {
                    "accountIndex": 0,
                    "mint": SOL_MINT,
                    "uiTokenAmount": {"amount": "5000000000", "decimals": 9},
                }
            ],
        )
        parser = JupiterLendReceiptParser()
        result = parser.extract_withdraw_amounts(receipt)
        assert result is not None
        assert result.amount == Decimal("2")
        assert result.amount_raw == 2_000_000_000
        assert result.action == "withdraw"

    def test_repay_with_9_decimals(self):
        """Repay = tokens leaving wallet (pre > post)."""
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
        result = parser.extract_repay_amounts(receipt)
        assert result is not None
        assert result.amount == Decimal("2")
        assert result.amount_raw == 2_000_000_000
        assert result.action == "repay"

    def test_direction_filtering_supply_ignores_inflow(self):
        """Supply (outflow) should NOT match tokens arriving at wallet."""
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0", "decimals": 6}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "50000000", "decimals": 6}},
            ],
        )
        parser = JupiterLendReceiptParser()
        # USDC is arriving (inflow), supply expects outflow -> None
        result = parser.extract_supply_amounts(receipt)
        assert result is None

    def test_direction_filtering_borrow_ignores_outflow(self):
        """Borrow (inflow) should NOT match tokens leaving wallet."""
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000", "decimals": 6}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0", "decimals": 6}},
            ],
        )
        parser = JupiterLendReceiptParser()
        # USDC is leaving (outflow), borrow expects inflow -> None
        result = parser.extract_borrow_amounts(receipt)
        assert result is None

    def test_multi_token_supply_returns_outflow_only(self):
        """In a supply+borrow tx, supply extracts only the outflow token."""
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
        # USDC left (outflow), SOL arrived (inflow)
        supply = parser.extract_supply_amounts(receipt)
        assert supply is not None
        assert supply.token == USDC_MINT
        assert supply.amount == Decimal("100")
        assert supply.action == "deposit"

        borrow = parser.extract_borrow_amounts(receipt)
        assert borrow is not None
        assert borrow.token == SOL_MINT
        assert borrow.amount == Decimal("4")
        assert borrow.action == "borrow"

    def test_multi_token_repay_and_withdraw(self):
        """In a repay+withdraw tx, repay extracts outflow, withdraw extracts inflow."""
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "50000000", "decimals": 6}},
                {"accountIndex": 1, "mint": SOL_MINT, "uiTokenAmount": {"amount": "5000000000", "decimals": 9}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0", "decimals": 6}},
                {"accountIndex": 1, "mint": SOL_MINT, "uiTokenAmount": {"amount": "8000000000", "decimals": 9}},
            ],
        )
        parser = JupiterLendReceiptParser()
        repay = parser.extract_repay_amounts(receipt)
        assert repay is not None
        assert repay.token == USDC_MINT
        assert repay.amount == Decimal("50")

        withdraw = parser.extract_withdraw_amounts(receipt)
        assert withdraw is not None
        assert withdraw.token == SOL_MINT
        assert withdraw.amount == Decimal("3")

    def test_token_mint_filter(self):
        """Optional token_mint parameter extracts delta for specific token only."""
        receipt = _make_receipt(
            pre_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000", "decimals": 6}},
                {"accountIndex": 1, "mint": SOL_MINT, "uiTokenAmount": {"amount": "5000000000", "decimals": 9}},
            ],
            post_balances=[
                {"accountIndex": 0, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0", "decimals": 6}},
                {"accountIndex": 1, "mint": SOL_MINT, "uiTokenAmount": {"amount": "3000000000", "decimals": 9}},
            ],
        )
        parser = JupiterLendReceiptParser()
        # Both tokens are outflow; filter to SOL only
        result = parser.extract_supply_amounts(receipt, token_mint=SOL_MINT)
        assert result is not None
        assert result.token == SOL_MINT
        assert result.amount == Decimal("2")

        # Filter to non-existent mint
        result = parser.extract_supply_amounts(receipt, token_mint="NonExistentMint123")
        assert result is None

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
