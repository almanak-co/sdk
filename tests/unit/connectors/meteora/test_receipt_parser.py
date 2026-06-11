"""Characterization tests for MeteoraReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser. Do not change parser source in this PR.

Meteora uses Solana-style receipts (plain dicts with preTokenBalances /
postTokenBalances), NOT EVM receipt format. Synthetic Solana-shaped fixtures
are used here — no network or gateway calls are needed.
"""

import pytest

from almanak.connectors.meteora.receipt_parser import MeteoraReceiptParser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def token_balance(account_index: int, mint: str, amount: int, decimals: int) -> dict:
    return {
        "accountIndex": account_index,
        "mint": mint,
        "uiTokenAmount": {"amount": str(amount), "decimals": decimals},
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parser():
    return MeteoraReceiptParser()


# Open receipt: MintX (6 decimals, acct 1) decreases 600_000,
#               MintY (9 decimals, acct 2) decreases 200_000_000.
# MintY raw delta = -200_000_000 (most negative) -> token_x
# MintX raw delta = -600_000 (less negative)     -> token_y
OPEN_RECEIPT = {
    "metadata": {"position_address": "PosAddr111"},
    "meta": {
        "preTokenBalances": [
            token_balance(1, "MintX", 1_000_000, 6),
            token_balance(2, "MintY", 500_000_000, 9),
        ],
        "postTokenBalances": [
            token_balance(1, "MintX", 400_000, 6),
            token_balance(2, "MintY", 300_000_000, 9),
        ],
    },
}

CLOSE_RECEIPT = {
    "meta": {
        "preTokenBalances": [
            token_balance(1, "MintA", 0, 6),
            token_balance(2, "MintB", 0, 9),
        ],
        "postTokenBalances": [
            token_balance(1, "MintA", 5_000_000, 6),
            token_balance(2, "MintB", 2_000_000_000, 9),
        ],
    },
}


# ---------------------------------------------------------------------------
# extract_position_id
# ---------------------------------------------------------------------------


class TestExtractPositionId:
    def test_position_id_from_metadata(self, parser):
        assert parser.extract_position_id(OPEN_RECEIPT) == "PosAddr111"

    def test_position_id_fallback_branch_always_returns_none(self, parser):
        """The fallback branch (scan logMessages for InitializePosition) has a
        loop that only 'break's without returning an address. It always returns None.
        This pins the current behavior — the metadata path is the only working path."""
        fallback_receipt = {
            "meta": {
                "logMessages": ["Program log: InitializePosition SomeAddr"],
                "preTokenBalances": [],
                "postTokenBalances": [],
            }
        }
        assert parser.extract_position_id(fallback_receipt) is None

    def test_position_id_none_when_no_metadata_and_no_meta(self, parser):
        assert parser.extract_position_id({}) is None


# ---------------------------------------------------------------------------
# extract_liquidity (open / add position)
# ---------------------------------------------------------------------------


class TestExtractLiquidity:
    def test_token_x_is_most_negative_delta(self, parser):
        """MintY raw delta = -200_000_000 (most negative) becomes token_x.
        MintX raw delta = -600_000 (less negative) becomes token_y."""
        result = parser.extract_liquidity(OPEN_RECEIPT)

        assert result is not None
        assert result["token_x_mint"] == "MintY"
        assert result["amount_x_raw"] == 200_000_000
        assert result["amount_x"] == "0.2"  # 200_000_000 / 10^9
        assert result["token_y_mint"] == "MintX"
        assert result["amount_y_raw"] == 600_000
        assert result["amount_y"] == "0.6"  # 600_000 / 10^6
        assert result["position_address"] == "PosAddr111"

    def test_empty_meta_returns_none(self, parser):
        assert parser.extract_liquidity({"meta": {}}) is None

    def test_missing_meta_returns_none(self, parser):
        assert parser.extract_liquidity({}) is None


# ---------------------------------------------------------------------------
# extract_lp_close_data (remove position)
# ---------------------------------------------------------------------------


class TestExtractLpCloseData:
    def test_largest_positive_delta_is_token_x(self, parser):
        """MintB raw delta = +2_000_000_000 (largest) -> token_x.
        MintA raw delta = +5_000_000 -> token_y."""
        result = parser.extract_lp_close_data(CLOSE_RECEIPT)

        assert result is not None
        assert result["token_x_mint"] == "MintB"
        assert result["amount_x_received_raw"] == 2_000_000_000
        assert result["amount_x_received"] == "2"  # 2_000_000_000 / 10^9
        assert result["token_y_mint"] == "MintA"
        assert result["amount_y_received_raw"] == 5_000_000
        assert result["amount_y_received"] == "5"  # 5_000_000 / 10^6

    def test_empty_meta_returns_none(self, parser):
        assert parser.extract_lp_close_data({"meta": {}}) is None


# ---------------------------------------------------------------------------
# _compute_balance_deltas: same mint across two accountIndexes aggregated
# ---------------------------------------------------------------------------


class TestBalanceDeltaAggregation:
    def test_same_mint_two_accounts_aggregated(self, parser):
        """Account 1: delta=-300_000; Account 3: delta=-300_000.
        Combined MintX delta should be -600_000."""
        receipt = {
            "metadata": {"position_address": "PosAddr222"},
            "meta": {
                "preTokenBalances": [
                    token_balance(1, "MintX", 1_000_000, 6),
                    token_balance(3, "MintX", 500_000, 6),
                ],
                "postTokenBalances": [
                    token_balance(1, "MintX", 700_000, 6),
                    token_balance(3, "MintX", 200_000, 6),
                ],
            },
        }
        result = parser.extract_liquidity(receipt)

        assert result is not None
        assert result["token_x_mint"] == "MintX"
        assert result["amount_x_raw"] == 600_000
        assert result["amount_x"] == "0.6"


# ---------------------------------------------------------------------------
# parse_receipt shape
# ---------------------------------------------------------------------------


class TestParseReceipt:
    def test_parse_receipt_returns_expected_keys(self, parser):
        result = parser.parse_receipt(OPEN_RECEIPT)

        assert set(result.keys()) == {"position_id", "liquidity", "lp_close_data", "success"}
        assert result["success"] is True
        assert result["position_id"] == "PosAddr111"
        assert result["liquidity"] is not None
        assert result["lp_close_data"] is None  # no positive deltas in open receipt

    def test_parse_receipt_success_field_from_receipt(self, parser):
        receipt = {**OPEN_RECEIPT, "success": False}
        result = parser.parse_receipt(receipt)
        assert result["success"] is False
