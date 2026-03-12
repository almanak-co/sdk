"""Tests for RaydiumReceiptParser (VIB-371).

Verifies:
1. Position NFT extraction from receipts
2. Liquidity extraction (balance deltas)
3. LP close data extraction
4. Edge cases (empty receipts, single-sided deposits)
"""

from decimal import Decimal

import pytest

from almanak.framework.connectors.raydium.receipt_parser import RaydiumReceiptParser

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
NFT_MINT = "6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump"


def _make_receipt(pre_balances, post_balances, log_messages=None):
    """Build a minimal Solana transaction receipt."""
    return {
        "meta": {
            "preTokenBalances": pre_balances,
            "postTokenBalances": post_balances,
            "logMessages": log_messages or [],
        }
    }


def _balance_entry(account_index, mint, amount_raw, decimals=6):
    """Build a token balance entry."""
    return {
        "accountIndex": account_index,
        "mint": mint,
        "uiTokenAmount": {
            "amount": str(amount_raw),
            "decimals": decimals,
        },
    }


class TestExtractPositionId:
    """extract_position_id() finds the NFT mint from receipts."""

    def test_new_nft_detected(self):
        """NFT that appears in post but not pre is the position NFT."""
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[
                _balance_entry(0, SOL_MINT, 10_000_000_000, 9),
                _balance_entry(1, USDC_MINT, 1000_000_000, 6),
            ],
            post_balances=[
                _balance_entry(0, SOL_MINT, 9_000_000_000, 9),
                _balance_entry(1, USDC_MINT, 850_000_000, 6),
                _balance_entry(2, NFT_MINT, 1, 0),  # New NFT!
            ],
        )

        result = parser.extract_position_id(receipt)
        assert result == NFT_MINT

    def test_no_new_mint_returns_none(self):
        """No new mints means no position ID found."""
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, USDC_MINT, 1000, 6)],
            post_balances=[_balance_entry(0, USDC_MINT, 900, 6)],
        )

        result = parser.extract_position_id(receipt)
        assert result is None

    def test_no_meta_returns_none(self):
        parser = RaydiumReceiptParser()
        assert parser.extract_position_id({}) is None

    def test_non_nft_new_token_ignored(self):
        """A new token with amount != 1 is not an NFT."""
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[],
            post_balances=[
                _balance_entry(0, USDC_MINT, 500_000_000, 6),  # New but not NFT
            ],
        )

        result = parser.extract_position_id(receipt)
        assert result is None


class TestExtractLiquidity:
    """extract_liquidity() finds deposited amounts from balance deltas."""

    def test_dual_token_deposit(self):
        """Both SOL and USDC decrease = dual-token LP deposit."""
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[
                _balance_entry(0, SOL_MINT, 10_000_000_000, 9),   # 10 SOL
                _balance_entry(1, USDC_MINT, 1000_000_000, 6),    # 1000 USDC
            ],
            post_balances=[
                _balance_entry(0, SOL_MINT, 9_000_000_000, 9),    # 9 SOL (-1)
                _balance_entry(1, USDC_MINT, 850_000_000, 6),     # 850 USDC (-150)
                _balance_entry(2, NFT_MINT, 1, 0),                # Position NFT
            ],
        )

        result = parser.extract_liquidity(receipt)

        assert result is not None
        assert "amount_a_raw" in result
        assert "amount_b_raw" in result
        # Check that we found both tokens
        assert result["amount_a_raw"] > 0
        assert result["amount_b_raw"] > 0
        # NFT should be included
        assert result.get("position_nft_mint") == NFT_MINT

    def test_single_sided_deposit(self):
        """Only one token changes — single-sided LP."""
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[
                _balance_entry(0, USDC_MINT, 1000_000_000, 6),
            ],
            post_balances=[
                _balance_entry(0, USDC_MINT, 800_000_000, 6),  # -200 USDC
            ],
        )

        result = parser.extract_liquidity(receipt)

        assert result is not None
        assert result["amount_a_raw"] == 200_000_000
        assert "amount_b_raw" not in result

    def test_no_changes_returns_none(self):
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, USDC_MINT, 1000, 6)],
            post_balances=[_balance_entry(0, USDC_MINT, 1000, 6)],
        )

        result = parser.extract_liquidity(receipt)
        assert result is None


class TestExtractLPCloseData:
    """extract_lp_close_data() finds received amounts from LP close."""

    def test_dual_token_withdrawal(self):
        """Both tokens increase = LP close withdrawal."""
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[
                _balance_entry(0, SOL_MINT, 5_000_000_000, 9),
                _balance_entry(1, USDC_MINT, 500_000_000, 6),
            ],
            post_balances=[
                _balance_entry(0, SOL_MINT, 6_000_000_000, 9),   # +1 SOL
                _balance_entry(1, USDC_MINT, 650_000_000, 6),    # +150 USDC
            ],
        )

        result = parser.extract_lp_close_data(receipt)

        assert result is not None
        assert "amount_a_received_raw" in result
        assert "amount_b_received_raw" in result
        assert result["amount_a_received_raw"] > 0
        assert result["amount_b_received_raw"] > 0

    def test_no_positive_deltas_returns_none(self):
        """If no tokens increase, there's no close data."""
        parser = RaydiumReceiptParser()

        receipt = _make_receipt(
            pre_balances=[_balance_entry(0, USDC_MINT, 1000_000_000, 6)],
            post_balances=[_balance_entry(0, USDC_MINT, 800_000_000, 6)],
        )

        result = parser.extract_lp_close_data(receipt)
        assert result is None

    def test_empty_receipt_returns_none(self):
        parser = RaydiumReceiptParser()
        assert parser.extract_lp_close_data({}) is None
