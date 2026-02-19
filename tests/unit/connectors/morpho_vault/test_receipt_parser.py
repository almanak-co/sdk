"""Tests for MetaMorpho Vault Receipt Parser."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.morpho_vault.receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    MetaMorphoEvent,
    MetaMorphoEventType,
    MetaMorphoReceiptParser,
    ParseResult,
    TransferEventData,
    VaultDepositEventData,
    VaultWithdrawEventData,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_deposit_log(
    sender="0x" + "11" * 20,
    owner="0x" + "22" * 20,
    assets=1_000_000,  # 1 USDC (6 dec)
    shares=999_000_000_000_000_000,  # ~0.999 shares (18 dec)
    contract_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
):
    """Create a mock Deposit event log."""
    return {
        "address": contract_address,
        "topics": [
            EVENT_TOPICS["Deposit"],
            "0x" + "0" * 24 + sender[2:],
            "0x" + "0" * 24 + owner[2:],
        ],
        "data": "0x" + hex(assets)[2:].zfill(64) + hex(shares)[2:].zfill(64),
    }


def _make_withdraw_log(
    sender="0x" + "11" * 20,
    receiver="0x" + "22" * 20,
    owner="0x" + "33" * 20,
    assets=500_000,
    shares=499_000_000_000_000_000,
    contract_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
):
    """Create a mock Withdraw event log."""
    return {
        "address": contract_address,
        "topics": [
            EVENT_TOPICS["Withdraw"],
            "0x" + "0" * 24 + sender[2:],
            "0x" + "0" * 24 + receiver[2:],
            "0x" + "0" * 24 + owner[2:],
        ],
        "data": "0x" + hex(assets)[2:].zfill(64) + hex(shares)[2:].zfill(64),
    }


def _make_transfer_log(
    from_addr="0x" + "11" * 20,
    to_addr="0x" + "22" * 20,
    amount=1_000_000,
    contract_address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
):
    """Create a mock Transfer event log."""
    return {
        "address": contract_address,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "0" * 24 + from_addr[2:],
            "0x" + "0" * 24 + to_addr[2:],
        ],
        "data": "0x" + hex(amount)[2:].zfill(64),
    }


def _make_receipt(logs, tx_hash="0x" + "aa" * 32, block_number=19_000_000):
    """Create a mock transaction receipt."""
    return {
        "transactionHash": tx_hash,
        "blockNumber": block_number,
        "logs": logs,
    }


# =============================================================================
# Constants
# =============================================================================


class TestConstants:
    def test_event_topics_not_empty(self):
        assert len(EVENT_TOPICS) >= 4

    def test_topic_to_event_inverse(self):
        for name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT[topic] == name

    def test_event_name_to_type(self):
        assert EVENT_NAME_TO_TYPE["Deposit"] == MetaMorphoEventType.DEPOSIT
        assert EVENT_NAME_TO_TYPE["Withdraw"] == MetaMorphoEventType.WITHDRAW
        assert EVENT_NAME_TO_TYPE["Transfer"] == MetaMorphoEventType.TRANSFER
        assert EVENT_NAME_TO_TYPE["Approval"] == MetaMorphoEventType.APPROVAL


# =============================================================================
# Parser - Deposit
# =============================================================================


class TestParseDeposit:
    def test_parse_deposit_event(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([_make_deposit_log()])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == MetaMorphoEventType.DEPOSIT
        assert event.event_name == "Deposit"
        assert "sender" in event.data
        assert "owner" in event.data
        assert "assets" in event.data
        assert "shares" in event.data

    def test_extract_deposit_data(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([_make_deposit_log(assets=1_000_000, shares=999_000_000_000_000_000)])
        data = parser.extract_deposit_data(receipt)

        assert data is not None
        assert data["assets"] == 1_000_000
        assert data["shares"] == 999_000_000_000_000_000
        assert "share_price_raw" in data

    def test_extract_deposit_data_none_for_no_deposit(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([_make_transfer_log()])
        data = parser.extract_deposit_data(receipt)
        assert data is None


# =============================================================================
# Parser - Withdraw
# =============================================================================


class TestParseWithdraw:
    def test_parse_withdraw_event(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([_make_withdraw_log()])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == MetaMorphoEventType.WITHDRAW
        assert "sender" in event.data
        assert "receiver" in event.data
        assert "owner" in event.data

    def test_extract_redeem_data(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([_make_withdraw_log(assets=500_000, shares=499_000_000_000_000_000)])
        data = parser.extract_redeem_data(receipt)

        assert data is not None
        assert data["shares_burned"] == 499_000_000_000_000_000
        assert data["assets_received"] == 500_000

    def test_extract_redeem_data_none_for_no_withdraw(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([_make_deposit_log()])
        data = parser.extract_redeem_data(receipt)
        assert data is None


# =============================================================================
# Parser - Transfer
# =============================================================================


class TestParseTransfer:
    def test_parse_transfer_event(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([_make_transfer_log(amount=5_000_000)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_type == MetaMorphoEventType.TRANSFER
        assert event.data["amount"] == str(5_000_000)


# =============================================================================
# Parser - Multiple Events
# =============================================================================


class TestMultipleEvents:
    def test_parse_deposit_with_transfers(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([
            _make_transfer_log(),  # ERC20 transfer
            _make_deposit_log(),  # Vault deposit
            _make_transfer_log(),  # Share mint
        ])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 3

        event_types = [e.event_type for e in result.events]
        assert MetaMorphoEventType.DEPOSIT in event_types
        assert MetaMorphoEventType.TRANSFER in event_types


# =============================================================================
# Parser - Edge Cases
# =============================================================================


class TestEdgeCases:
    def test_empty_receipt(self):
        parser = MetaMorphoReceiptParser()
        result = parser.parse_receipt({"logs": []})
        assert result.success is True
        assert len(result.events) == 0

    def test_no_logs_key(self):
        parser = MetaMorphoReceiptParser()
        result = parser.parse_receipt({})
        assert result.success is True
        assert len(result.events) == 0

    def test_unknown_topic_skipped(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([{
            "address": "0x" + "00" * 20,
            "topics": ["0x" + "ff" * 32],
            "data": "0x",
        }])
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 0

    def test_log_with_no_topics_skipped(self):
        parser = MetaMorphoReceiptParser()
        receipt = _make_receipt([{
            "address": "0x" + "00" * 20,
            "topics": [],
            "data": "0x",
        }])
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 0

    def test_bytes_transaction_hash(self):
        parser = MetaMorphoReceiptParser()
        tx_hash_bytes = bytes.fromhex("aa" * 32)
        receipt = {
            "transactionHash": tx_hash_bytes,
            "blockNumber": 19_000_000,
            "logs": [],
        }
        result = parser.parse_receipt(receipt)
        assert result.transaction_hash == "0x" + "aa" * 32

    def test_hex_string_block_number(self):
        parser = MetaMorphoReceiptParser()
        receipt = {
            "transactionHash": "0x" + "bb" * 32,
            "blockNumber": "0x1234",
            "logs": [],
        }
        result = parser.parse_receipt(receipt)
        assert result.block_number == 0x1234

    def test_bytes_topic(self):
        parser = MetaMorphoReceiptParser()
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Transfer"][2:])
        receipt = _make_receipt([{
            "address": "0x" + "00" * 20,
            "topics": [
                topic_bytes,
                "0x" + "0" * 24 + "11" * 20,
                "0x" + "0" * 24 + "22" * 20,
            ],
            "data": "0x" + hex(1000)[2:].zfill(64),
        }])
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == MetaMorphoEventType.TRANSFER


# =============================================================================
# Data Classes
# =============================================================================


class TestEventDataClasses:
    def test_deposit_event_data_to_dict(self):
        data = VaultDepositEventData(
            sender="0x" + "11" * 20,
            owner="0x" + "22" * 20,
            assets=Decimal("1000000"),
            shares=Decimal("999000000000000000"),
        )
        d = data.to_dict()
        assert d["sender"] == "0x" + "11" * 20
        assert d["assets"] == "1000000"

    def test_withdraw_event_data_to_dict(self):
        data = VaultWithdrawEventData(
            sender="0x" + "11" * 20,
            receiver="0x" + "22" * 20,
            owner="0x" + "33" * 20,
            assets=Decimal("500000"),
            shares=Decimal("499000000000000000"),
        )
        d = data.to_dict()
        assert d["receiver"] == "0x" + "22" * 20
        assert d["owner"] == "0x" + "33" * 20

    def test_transfer_event_data_to_dict(self):
        data = TransferEventData(
            from_address="0x" + "11" * 20,
            to_address="0x" + "22" * 20,
            amount=Decimal("5000000"),
        )
        d = data.to_dict()
        assert d["from"] == "0x" + "11" * 20
        assert d["amount"] == "5000000"


class TestParseResult:
    def test_to_dict(self):
        result = ParseResult(
            success=True,
            events=[],
            transaction_hash="0x" + "aa" * 32,
            block_number=19_000_000,
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["events"] == []
        assert d["error"] is None

    def test_failed_result(self):
        result = ParseResult(success=False, error="parse failed")
        d = result.to_dict()
        assert d["success"] is False
        assert d["error"] == "parse failed"


class TestMetaMorphoEvent:
    def test_to_dict(self):
        event = MetaMorphoEvent(
            event_type=MetaMorphoEventType.DEPOSIT,
            event_name="Deposit",
            log_index=0,
            transaction_hash="0x" + "aa" * 32,
            block_number=19_000_000,
            contract_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            data={"sender": "0x...", "assets": "1000000"},
        )
        d = event.to_dict()
        assert d["event_type"] == "DEPOSIT"
        assert d["log_index"] == 0
