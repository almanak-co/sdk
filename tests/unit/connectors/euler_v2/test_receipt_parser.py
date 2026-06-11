"""Characterization tests for EulerV2ReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser. Do not change parser source in this PR.
"""

import pytest

from almanak.connectors.euler_v2.receipt_parser import (
    BORROW_TOPIC,
    DEPOSIT_TOPIC,
    REPAY_TOPIC,
    WITHDRAW_TOPIC,
    EulerV2ReceiptParser,
)


# ---------------------------------------------------------------------------
# Hex helpers
# ---------------------------------------------------------------------------


def word(v: int) -> str:
    """One 32-byte ABI word as 64 hex chars (no 0x)."""
    return f"{v:064x}"


def addr_topic(a: str) -> str:
    """Address as an indexed topic (0x-prefixed 32-byte word)."""
    return "0x" + a.lower().replace("0x", "").zfill(64)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VAULT = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
USER = "0x1111111111111111111111111111111111111111"
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parser():
    return EulerV2ReceiptParser(underlying_decimals=6)


def make_deposit_log(assets: int, shares: int, address: str = VAULT) -> dict:
    return {
        "address": address,
        "topics": [DEPOSIT_TOPIC, addr_topic(USER), addr_topic(USER)],
        "data": "0x" + word(assets) + word(shares),
        "logIndex": 0,
    }


def make_withdraw_log(assets: int, shares: int, address: str = VAULT) -> dict:
    return {
        "address": address,
        "topics": [WITHDRAW_TOPIC, addr_topic(USER), addr_topic(USER), addr_topic(USER)],
        "data": "0x" + word(assets) + word(shares),
        "logIndex": 1,
    }


def make_borrow_log(assets: int, address: str = VAULT) -> dict:
    return {
        "address": address,
        "topics": [BORROW_TOPIC, addr_topic(USER)],
        "data": "0x" + word(assets),
        "logIndex": 2,
    }


def make_repay_log(assets: int, address: str = VAULT) -> dict:
    return {
        "address": address,
        "topics": [REPAY_TOPIC, addr_topic(USER)],
        "data": "0x" + word(assets),
        "logIndex": 3,
    }


def make_receipt(logs: list) -> dict:
    return {"transactionHash": TX_HASH, "blockNumber": 12345, "logs": logs}


# ---------------------------------------------------------------------------
# Deposit
# ---------------------------------------------------------------------------


class TestDeposit:
    def test_deposit_parses_assets_and_shares(self, parser):
        receipt = make_receipt([make_deposit_log(100_000_000, 95_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.deposit_amount == 100_000_000
        assert result.deposit_shares == 95_000_000
        assert result.events == [{"event": "Deposit", "assets": 100_000_000, "shares": 95_000_000}]

    def test_two_deposit_logs_sum(self, parser):
        receipt = make_receipt([
            make_deposit_log(100_000_000, 95_000_000),
            make_deposit_log(50_000_000, 47_500_000),
        ])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.deposit_amount == 150_000_000
        assert result.deposit_shares == 142_500_000
        assert len(result.events) == 2


# ---------------------------------------------------------------------------
# Withdraw
# ---------------------------------------------------------------------------


class TestWithdraw:
    def test_withdraw_parses_assets_and_shares(self, parser):
        receipt = make_receipt([make_withdraw_log(80_000_000, 78_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.withdraw_amount == 80_000_000
        assert result.withdraw_shares == 78_000_000
        assert result.events == [{"event": "Withdraw", "assets": 80_000_000, "shares": 78_000_000}]


# ---------------------------------------------------------------------------
# Borrow
# ---------------------------------------------------------------------------


class TestBorrow:
    def test_borrow_parses_single_word(self, parser):
        receipt = make_receipt([make_borrow_log(200_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.borrow_amount == 200_000_000
        assert result.events == [{"event": "Borrow", "assets": 200_000_000}]


# ---------------------------------------------------------------------------
# Repay
# ---------------------------------------------------------------------------


class TestRepay:
    def test_repay_parses_single_word(self, parser):
        receipt = make_receipt([make_repay_log(150_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.repay_amount == 150_000_000
        assert result.events == [{"event": "Repay", "assets": 150_000_000}]


# ---------------------------------------------------------------------------
# Empty / error cases
# ---------------------------------------------------------------------------


class TestEmptyAndError:
    def test_empty_logs_returns_failure_with_error(self, parser):
        result = parser.parse_receipt({"logs": []})

        assert result.success is False
        assert result.error == "No logs in receipt"
        assert result.deposit_amount == 0
        assert result.withdraw_amount == 0
        assert result.borrow_amount == 0
        assert result.repay_amount == 0

    def test_unknown_topic0_only_returns_failure_no_error(self, parser):
        unknown_log = {
            "address": VAULT,
            "topics": ["0xdeadbeef" + "00" * 28],
            "data": "0x" + word(999),
            "logIndex": 0,
        }
        result = parser.parse_receipt(make_receipt([unknown_log]))

        assert result.success is False
        assert result.error is None
        assert result.deposit_amount == 0

    def test_truncated_deposit_data_skips_event(self, parser):
        # Only 1 word (64 hex chars) — _parse_deposit_event requires len >= 128
        truncated_log = {
            "address": VAULT,
            "topics": [DEPOSIT_TOPIC, addr_topic(USER), addr_topic(USER)],
            "data": "0x" + word(100_000_000),  # 64 hex chars only
            "logIndex": 0,
        }
        result = parser.parse_receipt(make_receipt([truncated_log]))

        assert result.success is False
        assert result.deposit_amount == 0
        assert result.events == []

    def test_vault_address_filter_mismatch_filters_out_no_retry(self, parser):
        """When vault_address filter is given and log address does not match, it is
        dropped. Unlike silo_v2, euler_v2 does NOT retry without the filter."""
        other_address = "0xBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBb"
        receipt = make_receipt([make_deposit_log(100_000_000, 95_000_000, address=other_address)])
        result = parser.parse_receipt(receipt, vault_address=VAULT)

        assert result.success is False
        assert result.deposit_amount == 0


# ---------------------------------------------------------------------------
# Extraction methods
# ---------------------------------------------------------------------------


class TestExtractionMethods:
    def test_extract_supply_amount_returns_deposit_amount(self, parser):
        receipt = make_receipt([make_deposit_log(100_000_000, 95_000_000)])
        assert parser.extract_supply_amount(receipt) == 100_000_000

    def test_extract_supply_amount_returns_none_on_empty_logs(self, parser):
        assert parser.extract_supply_amount({"logs": []}) is None

    def test_extract_borrow_amount(self, parser):
        receipt = make_receipt([make_borrow_log(200_000_000)])
        assert parser.extract_borrow_amount(receipt) == 200_000_000

    def test_extract_borrow_amount_none_when_no_borrow(self, parser):
        receipt = make_receipt([make_deposit_log(100_000_000, 90_000_000)])
        assert parser.extract_borrow_amount(receipt) is None

    def test_extract_withdraw_amount(self, parser):
        receipt = make_receipt([make_withdraw_log(80_000_000, 78_000_000)])
        assert parser.extract_withdraw_amount(receipt) == 80_000_000

    def test_extract_repay_amount(self, parser):
        receipt = make_receipt([make_repay_log(150_000_000)])
        assert parser.extract_repay_amount(receipt) == 150_000_000
