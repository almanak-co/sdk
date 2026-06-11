"""Characterization tests for SiloV2ReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser. Do not change parser source in this PR.
"""

import pytest

from almanak.connectors.silo_v2.receipt_parser import (
    BORROW_TOPIC,
    DEPOSIT_TOPIC,
    REPAY_TOPIC,
    WITHDRAW_TOPIC,
    SiloV2ReceiptParser,
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

SILO = "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
OTHER = "0xBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBb"
USER = "0x1111111111111111111111111111111111111111"
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parser():
    return SiloV2ReceiptParser(underlying_decimals=6)


def make_deposit_log(assets: int, shares: int, address: str = SILO) -> dict:
    return {
        "address": address,
        "topics": [DEPOSIT_TOPIC, addr_topic(USER), addr_topic(USER)],
        "data": "0x" + word(assets) + word(shares),
        "logIndex": 0,
    }


def make_withdraw_log(assets: int, shares: int, address: str = SILO) -> dict:
    return {
        "address": address,
        "topics": [WITHDRAW_TOPIC, addr_topic(USER), addr_topic(USER), addr_topic(USER)],
        "data": "0x" + word(assets) + word(shares),
        "logIndex": 1,
    }


def make_borrow_log(assets: int, shares: int, address: str = SILO) -> dict:
    return {
        "address": address,
        "topics": [BORROW_TOPIC, addr_topic(USER), addr_topic(USER), addr_topic(USER)],
        "data": "0x" + word(assets) + word(shares),
        "logIndex": 2,
    }


def make_repay_log(assets: int, shares: int, address: str = SILO) -> dict:
    return {
        "address": address,
        "topics": [REPAY_TOPIC, addr_topic(USER), addr_topic(USER)],
        "data": "0x" + word(assets) + word(shares),
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


# ---------------------------------------------------------------------------
# Borrow
# ---------------------------------------------------------------------------


class TestBorrow:
    def test_borrow_parses_assets_and_shares(self, parser):
        receipt = make_receipt([make_borrow_log(200_000_000, 195_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.borrow_amount == 200_000_000
        assert result.borrow_shares == 195_000_000
        assert result.events == [{"event": "Borrow", "assets": 200_000_000, "shares": 195_000_000}]


# ---------------------------------------------------------------------------
# Repay
# ---------------------------------------------------------------------------


class TestRepay:
    def test_repay_parses_assets_and_shares(self, parser):
        receipt = make_receipt([make_repay_log(150_000_000, 148_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.repay_amount == 150_000_000
        assert result.repay_shares == 148_000_000
        assert result.events == [{"event": "Repay", "assets": 150_000_000, "shares": 148_000_000}]


# ---------------------------------------------------------------------------
# Retry-without-filter behavior (distinctive to silo_v2)
# ---------------------------------------------------------------------------


class TestRetryWithoutFilter:
    def test_silo_address_filter_mismatch_retries_and_counts_event(self, parser):
        """Deposit log address != silo_address triggers retry without filter.
        The event is STILL counted (success=True). This is distinctive behavior
        vs euler_v2 which does NOT retry."""
        receipt = make_receipt([make_deposit_log(100_000_000, 95_000_000, address=OTHER)])
        # Filtered parse by SILO won't match OTHER; retry without filter will find it.
        result = parser.parse_receipt(receipt, silo_address=SILO)

        assert result.success is True
        assert result.deposit_amount == 100_000_000

    def test_no_filter_finds_event_normally(self, parser):
        receipt = make_receipt([make_deposit_log(100_000_000, 95_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.deposit_amount == 100_000_000


# ---------------------------------------------------------------------------
# Empty / error cases
# ---------------------------------------------------------------------------


class TestEmptyAndError:
    def test_empty_logs_returns_failure_with_error(self, parser):
        result = parser.parse_receipt({"logs": []})

        assert result.success is False
        assert result.error == "No logs in receipt"
        assert result.deposit_amount == 0

    def test_unknown_topic0_only_returns_failure_no_error(self, parser):
        unknown_log = {
            "address": SILO,
            "topics": ["0xdeadbeef" + "00" * 28],
            "data": "0x" + word(999),
            "logIndex": 0,
        }
        result = parser.parse_receipt(make_receipt([unknown_log]))

        assert result.success is False
        assert result.error is None

    def test_truncated_deposit_data_skips_event(self, parser):
        # Only 1 word — requires >= 128 hex chars
        truncated_log = {
            "address": SILO,
            "topics": [DEPOSIT_TOPIC, addr_topic(USER), addr_topic(USER)],
            "data": "0x" + word(100_000_000),
            "logIndex": 0,
        }
        result = parser.parse_receipt(make_receipt([truncated_log]))

        assert result.success is False
        assert result.deposit_amount == 0


# ---------------------------------------------------------------------------
# Extraction methods
# ---------------------------------------------------------------------------


class TestExtractionMethods:
    def test_extract_supply_amount(self, parser):
        receipt = make_receipt([make_deposit_log(100_000_000, 95_000_000)])
        assert parser.extract_supply_amount(receipt) == 100_000_000

    def test_extract_supply_amount_none_on_empty(self, parser):
        assert parser.extract_supply_amount({"logs": []}) is None

    def test_extract_borrow_amount(self, parser):
        receipt = make_receipt([make_borrow_log(200_000_000, 195_000_000)])
        assert parser.extract_borrow_amount(receipt) == 200_000_000

    def test_extract_withdraw_amount(self, parser):
        receipt = make_receipt([make_withdraw_log(80_000_000, 78_000_000)])
        assert parser.extract_withdraw_amount(receipt) == 80_000_000

    def test_extract_repay_amount(self, parser):
        receipt = make_receipt([make_repay_log(150_000_000, 148_000_000)])
        assert parser.extract_repay_amount(receipt) == 150_000_000
