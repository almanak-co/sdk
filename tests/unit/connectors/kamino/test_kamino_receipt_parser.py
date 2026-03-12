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

    def test_sol_9_decimals(self):
        """SOL uses 9 decimals."""
        parser = KaminoReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, SOL_MINT, 10_000_000_000, 9)],  # 10 SOL
            post_balances=[_balance_entry(0, SOL_MINT, 9_000_000_000, 9)],  # 9 SOL
        )

        result = parser.extract_withdraw_amounts(receipt)

        assert result is not None
        assert result.amount == Decimal("1")
        assert result.amount_raw == 1_000_000_000
