"""Tests for Jupiter receipt parser (balance-delta extraction)."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.jupiter.receipt_parser import (
    WSOL_MINT,
    JupiterReceiptParser,
)
from almanak.framework.execution.extracted_data import SwapAmounts


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WALLET = "WalletPubKey123456789abcdefghijklmno"


@pytest.fixture
def parser():
    return JupiterReceiptParser(wallet_address=WALLET, chain="solana")


def make_receipt(
    pre_balances: list[dict],
    post_balances: list[dict],
    success: bool = True,
    fee_payer: str = "",
) -> dict:
    """Helper to build a mock Solana transaction receipt."""
    receipt = {
        "success": success,
        "signature": "mock_sig_123",
        "pre_token_balances": pre_balances,
        "post_token_balances": post_balances,
    }
    if fee_payer:
        receipt["fee_payer"] = fee_payer
    return receipt


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestJupiterReceiptParserInit:
    def test_supported_extractions(self, parser):
        assert "swap_amounts" in parser.SUPPORTED_EXTRACTIONS

    def test_wallet_from_kwargs(self):
        p = JupiterReceiptParser(wallet_address="my_wallet")
        assert p._wallet_address == "my_wallet"

    def test_default_chain(self):
        p = JupiterReceiptParser()
        assert p._chain == "solana"


class TestExtractSwapAmounts:
    @patch("almanak.framework.connectors.jupiter.receipt_parser.JupiterReceiptParser._resolve_decimals")
    def test_basic_swap_usdc_to_sol(self, mock_decimals, parser):
        """Test extracting a simple USDC -> WSOL swap."""
        mock_decimals.side_effect = lambda mint: {USDC_MINT: 6, WSOL_MINT: 9}.get(mint)

        receipt = make_receipt(
            pre_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000"}},  # 100 USDC
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "1000000000"}},  # 1 SOL
            ],
            post_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},  # 0 USDC
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "1666666666"}},  # 1.666 SOL
            ],
        )

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 100_000_000  # 100 USDC in smallest units
        assert amounts.amount_out == 666_666_666  # ~0.666 SOL gained
        assert amounts.token_in == USDC_MINT
        assert amounts.token_out == WSOL_MINT
        assert amounts.amount_in_decimal == Decimal("100")
        assert amounts.amount_out_decimal == Decimal("0.666666666")

    @patch("almanak.framework.connectors.jupiter.receipt_parser.JupiterReceiptParser._resolve_decimals")
    def test_swap_sol_to_usdc(self, mock_decimals, parser):
        """Test extracting a WSOL -> USDC swap."""
        mock_decimals.side_effect = lambda mint: {USDC_MINT: 6, WSOL_MINT: 9}.get(mint)

        receipt = make_receipt(
            pre_balances=[
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "5000000000"}},  # 5 SOL
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
            ],
            post_balances=[
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "4000000000"}},  # 4 SOL
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "150000000"}},  # 150 USDC
            ],
        )

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.token_in == WSOL_MINT
        assert amounts.token_out == USDC_MINT
        assert amounts.amount_in == 1_000_000_000  # 1 SOL
        assert amounts.amount_out == 150_000_000  # 150 USDC

    def test_failed_transaction_returns_none(self, parser):
        receipt = make_receipt(
            pre_balances=[],
            post_balances=[],
            success=False,
        )
        assert parser.extract_swap_amounts(receipt) is None

    def test_empty_balances_returns_none(self, parser):
        receipt = make_receipt(pre_balances=[], post_balances=[])
        assert parser.extract_swap_amounts(receipt) is None

    def test_no_wallet_returns_none(self):
        """Parser with no wallet address and receipt with no wallet info."""
        p = JupiterReceiptParser()
        receipt = make_receipt(
            pre_balances=[
                {"owner": "someone", "mint": USDC_MINT, "uiTokenAmount": {"amount": "100"}},
            ],
            post_balances=[
                {"owner": "someone", "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
            ],
        )
        assert p.extract_swap_amounts(receipt) is None

    def test_wallet_from_receipt_fee_payer(self):
        """Parser picks up wallet from receipt's fee_payer field."""
        p = JupiterReceiptParser()
        receipt = make_receipt(
            pre_balances=[
                {"owner": "fee_payer_wallet", "mint": USDC_MINT, "uiTokenAmount": {"amount": "100"}},
                {"owner": "fee_payer_wallet", "mint": WSOL_MINT, "uiTokenAmount": {"amount": "0"}},
            ],
            post_balances=[
                {"owner": "fee_payer_wallet", "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": "fee_payer_wallet", "mint": WSOL_MINT, "uiTokenAmount": {"amount": "50"}},
            ],
            fee_payer="fee_payer_wallet",
        )

        with patch.object(JupiterReceiptParser, "_resolve_decimals", return_value=6):
            amounts = p.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 100
        assert amounts.amount_out == 50

    @patch("almanak.framework.connectors.jupiter.receipt_parser.JupiterReceiptParser._resolve_decimals")
    def test_ignores_other_wallets(self, mock_decimals, parser):
        """Only tracks balance changes for the configured wallet."""
        mock_decimals.side_effect = lambda mint: {USDC_MINT: 6, WSOL_MINT: 9}.get(mint)

        receipt = make_receipt(
            pre_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000"}},
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": "other_wallet", "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": "other_wallet", "mint": WSOL_MINT, "uiTokenAmount": {"amount": "999999"}},
            ],
            post_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "666000000"}},
                {"owner": "other_wallet", "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000"}},
                {"owner": "other_wallet", "mint": WSOL_MINT, "uiTokenAmount": {"amount": "0"}},
            ],
        )

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.token_in == USDC_MINT
        assert amounts.token_out == WSOL_MINT
        assert amounts.amount_in == 100_000_000  # Wallet's USDC decrease
        assert amounts.amount_out == 666_000_000  # Wallet's SOL increase

    @patch("almanak.framework.connectors.jupiter.receipt_parser.JupiterReceiptParser._resolve_decimals")
    def test_simplified_balance_format(self, mock_decimals, parser):
        """Test with simplified amount format (no uiTokenAmount wrapper)."""
        mock_decimals.side_effect = lambda mint: {USDC_MINT: 6, WSOL_MINT: 9}.get(mint)

        receipt = make_receipt(
            pre_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "amount": "1000000"},
                {"owner": WALLET, "mint": WSOL_MINT, "amount": "0"},
            ],
            post_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "amount": "0"},
                {"owner": WALLET, "mint": WSOL_MINT, "amount": "6666666"},
            ],
        )

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 1_000_000
        assert amounts.amount_out == 6_666_666

    @patch("almanak.framework.connectors.jupiter.receipt_parser.JupiterReceiptParser._resolve_decimals")
    def test_unresolvable_output_decimals_returns_none(self, mock_decimals, parser):
        """If output token decimals can't be resolved, return None."""
        mock_decimals.side_effect = lambda mint: {USDC_MINT: 6}.get(mint)  # No WSOL decimals

        receipt = make_receipt(
            pre_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100"}},
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "0"}},
            ],
            post_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "50"}},
            ],
        )

        assert parser.extract_swap_amounts(receipt) is None

    @patch("almanak.framework.connectors.jupiter.receipt_parser.JupiterReceiptParser._resolve_decimals")
    def test_new_mint_in_post_only(self, mock_decimals, parser):
        """Handle case where output mint doesn't exist in pre_balances."""
        mock_decimals.side_effect = lambda mint: {USDC_MINT: 6, WSOL_MINT: 9}.get(mint)

        receipt = make_receipt(
            pre_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100000000"}},
                # No WSOL entry in pre-balances
            ],
            post_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "666666666"}},
            ],
        )

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.token_out == WSOL_MINT
        assert amounts.amount_out == 666_666_666


class TestParseReceipt:
    @patch("almanak.framework.connectors.jupiter.receipt_parser.JupiterReceiptParser._resolve_decimals")
    def test_parse_receipt_returns_dict(self, mock_decimals, parser):
        mock_decimals.return_value = 6

        receipt = make_receipt(
            pre_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "100"}},
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "0"}},
            ],
            post_balances=[
                {"owner": WALLET, "mint": USDC_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": WALLET, "mint": WSOL_MINT, "uiTokenAmount": {"amount": "50"}},
            ],
        )

        result = parser.parse_receipt(receipt)
        assert result["success"] is True
        assert result["signature"] == "mock_sig_123"
        assert result["swap_amounts"] is not None


class TestBuildBalanceMap:
    def test_rpc_format(self):
        balances = [
            {"owner": "wallet1", "mint": "mintA", "uiTokenAmount": {"amount": "100"}},
            {"owner": "wallet1", "mint": "mintB", "uiTokenAmount": {"amount": "200"}},
        ]
        result = JupiterReceiptParser._build_balance_map(balances)
        assert result[("wallet1", "mintA")] == 100
        assert result[("wallet1", "mintB")] == 200

    def test_simplified_format(self):
        balances = [
            {"owner": "wallet1", "mint": "mintA", "amount": "300"},
        ]
        result = JupiterReceiptParser._build_balance_map(balances)
        assert result[("wallet1", "mintA")] == 300

    def test_missing_fields_skipped(self):
        balances = [
            {"owner": "", "mint": "mintA", "amount": "100"},  # Empty owner
            {"owner": "wallet1", "mint": "", "amount": "100"},  # Empty mint
            {"owner": "wallet1", "mint": "mintA", "amount": "invalid"},  # Invalid amount
        ]
        result = JupiterReceiptParser._build_balance_map(balances)
        assert len(result) == 0

    def test_empty_list(self):
        result = JupiterReceiptParser._build_balance_map([])
        assert result == {}
