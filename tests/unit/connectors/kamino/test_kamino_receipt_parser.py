"""Tests for KaminoReceiptParser (VIB-370).

Verifies balance-delta extraction from Solana transaction receipts.
"""

from decimal import Decimal

import pytest

from almanak.framework.connectors.kamino.receipt_parser import KaminoReceiptParser


def _make_receipt(pre_balances, post_balances):
    """Helper to build a minimal Solana transaction receipt."""
    return {
        "meta": {
            "preTokenBalances": pre_balances,
            "postTokenBalances": post_balances,
        }
    }


def _balance_entry(account_index, mint, amount_raw, decimals=6):
    """Helper to build a token balance entry."""
    return {
        "accountIndex": account_index,
        "mint": mint,
        "uiTokenAmount": {
            "amount": str(amount_raw),
            "decimals": decimals,
        },
    }


USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"


class TestBalanceDeltaExtraction:
    """KaminoReceiptParser extracts amounts from balance deltas."""

    def test_deposit_extracts_supply_amount(self):
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, USDC_MINT, 1000_000_000, 6)],  # 1000 USDC
            post_balances=[_balance_entry(0, USDC_MINT, 900_000_000, 6)],  # 900 USDC (100 deposited)
        )

        result = parser.extract_supply_amounts(receipt)

        assert result is not None
        assert result.amount == Decimal("100")
        assert result.amount_raw == 100_000_000
        assert result.token == USDC_MINT
        assert result.action == "deposit"

    def test_borrow_extracts_borrow_amount(self):
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, USDC_MINT, 0, 6)],
            post_balances=[_balance_entry(0, USDC_MINT, 500_000_000, 6)],  # 500 USDC received
        )

        result = parser.extract_borrow_amounts(receipt)

        assert result is not None
        assert result.amount == Decimal("500")
        assert result.action == "borrow"

    def test_no_meta_returns_none(self):
        parser = KaminoReceiptParser()
        result = parser.extract_supply_amounts({})
        assert result is None

    def test_no_balance_changes_returns_none(self):
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, USDC_MINT, 1000, 6)],
            post_balances=[_balance_entry(0, USDC_MINT, 1000, 6)],
        )

        result = parser.extract_supply_amounts(receipt)
        assert result is None

    def test_multiple_tokens_returns_largest_delta(self):
        """When multiple tokens change, return the one with the largest delta."""
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[
                _balance_entry(0, USDC_MINT, 1000_000_000, 6),  # 1000 USDC
                _balance_entry(1, SOL_MINT, 5_000_000_000, 9),  # 5 SOL
            ],
            post_balances=[
                _balance_entry(0, USDC_MINT, 900_000_000, 6),  # 900 USDC (-100)
                _balance_entry(1, SOL_MINT, 4_999_000_000, 9),  # 4.999 SOL (-0.001)
            ],
        )

        result = parser.extract_supply_amounts(receipt)

        assert result is not None
        # 100 USDC (100_000_000 raw) > 0.001 SOL (1_000_000 raw)
        assert result.token == USDC_MINT
        assert result.amount_raw == 100_000_000

    def test_sol_9_decimals_withdraw(self):
        """SOL uses 9 decimals. Withdraw = tokens arriving (post > pre)."""
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, SOL_MINT, 9_000_000_000, 9)],  # 9 SOL
            post_balances=[_balance_entry(0, SOL_MINT, 10_000_000_000, 9)],  # 10 SOL (1 arrived)
        )

        result = parser.extract_withdraw_amounts(receipt)

        assert result is not None
        assert result.amount == Decimal("1")
        assert result.amount_raw == 1_000_000_000

    def test_sol_9_decimals_repay(self):
        """SOL uses 9 decimals. Repay = tokens leaving (pre > post)."""
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, SOL_MINT, 10_000_000_000, 9)],  # 10 SOL
            post_balances=[_balance_entry(0, SOL_MINT, 9_000_000_000, 9)],  # 9 SOL (1 left)
        )

        result = parser.extract_repay_amounts(receipt)

        assert result is not None
        assert result.amount == Decimal("1")
        assert result.amount_raw == 1_000_000_000

    def test_direction_filtering_supply_ignores_inflow(self):
        """Supply (outflow) should not match tokens arriving at wallet."""
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, USDC_MINT, 0, 6)],
            post_balances=[_balance_entry(0, USDC_MINT, 500_000_000, 6)],
        )

        result = parser.extract_supply_amounts(receipt)
        assert result is None

    def test_multi_token_supply_and_borrow(self):
        """In a supply+borrow tx, supply returns outflow, borrow returns inflow."""
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[
                _balance_entry(0, USDC_MINT, 1000_000_000, 6),  # 1000 USDC
                _balance_entry(1, SOL_MINT, 0, 9),  # 0 SOL
            ],
            post_balances=[
                _balance_entry(0, USDC_MINT, 0, 6),  # 0 USDC (all supplied)
                _balance_entry(1, SOL_MINT, 5_000_000_000, 9),  # 5 SOL (borrowed)
            ],
        )

        supply = parser.extract_supply_amounts(receipt)
        assert supply is not None
        assert supply.token == USDC_MINT
        assert supply.amount == Decimal("1000")

        borrow = parser.extract_borrow_amounts(receipt)
        assert borrow is not None
        assert borrow.token == SOL_MINT
        assert borrow.amount == Decimal("5")

    def test_token_mint_filter(self):
        """Optional token_mint parameter extracts delta for specific token only."""
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[
                _balance_entry(0, USDC_MINT, 1000_000_000, 6),
                _balance_entry(1, SOL_MINT, 5_000_000_000, 9),
            ],
            post_balances=[
                _balance_entry(0, USDC_MINT, 0, 6),
                _balance_entry(1, SOL_MINT, 3_000_000_000, 9),
            ],
        )

        # Both outflow; filter to SOL
        result = parser.extract_supply_amounts(receipt, token_mint=SOL_MINT)
        assert result is not None
        assert result.token == SOL_MINT
        assert result.amount == Decimal("2")
