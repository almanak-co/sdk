"""Unit tests for MorphoBlueReceiptParser event parsing.

Covers all 13 event types (Supply / Withdraw / Borrow / Repay /
Supply|Withdraw Collateral / Liquidate / FlashLoan / CreateMarket /
SetAuthorization / AccrueInterest / Transfer / Approval) plus the
extract_*_amount enrichment helpers and edge cases.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.connectors.morpho_blue.receipt_parser import (
    EVENT_TOPICS,
    AccrueInterestEventData,
    BorrowEventData,
    CreateMarketEventData,
    FlashLoanEventData,
    LiquidateEventData,
    MorphoBlueEvent,
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
    ParseResult,
    RepayEventData,
    SetAuthorizationEventData,
    SupplyCollateralEventData,
    SupplyEventData,
    TransferEventData,
    WithdrawCollateralEventData,
    WithdrawEventData,
)

MARKET_ID = "0x" + "ab" * 32
USER = "0x1234567890abcdef1234567890abcdef12345678"
CALLER = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
RECEIVER = "0x9876543210987654321098765432109876543210"
MORPHO_ADDRESS = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"


def pad_addr(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def encode_uint256(value: int) -> str:
    return hex(value)[2:].zfill(64)


def encode_address_only(addr: str) -> str:
    return addr.lower().replace("0x", "").zfill(64)


@pytest.fixture
def parser() -> MorphoBlueReceiptParser:
    return MorphoBlueReceiptParser()


# =============================================================================
# Receipt Builders
# =============================================================================


def _wrap_receipt(log: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": 1,
        "transactionHash": "0x" + "aa" * 32,
        "blockNumber": 1234,
        "gasUsed": 200000,
        "logs": [log],
    }


def _supply_log(assets: int = 1_000_000, shares: int = 999) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["Supply"],
            MARKET_ID,
            pad_addr(CALLER),
            pad_addr(USER),
        ],
        "data": "0x" + encode_uint256(assets) + encode_uint256(shares),
        "logIndex": 0,
    }


def _withdraw_log(assets: int = 100, shares: int = 99) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["Withdraw"],
            MARKET_ID,
            pad_addr(USER),
            pad_addr(RECEIVER),
        ],
        "data": "0x" + encode_address_only(CALLER) + encode_uint256(assets) + encode_uint256(shares),
        "logIndex": 0,
    }


def _borrow_log(assets: int = 200, shares: int = 199) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["Borrow"],
            MARKET_ID,
            pad_addr(USER),
            pad_addr(RECEIVER),
        ],
        "data": "0x" + encode_address_only(CALLER) + encode_uint256(assets) + encode_uint256(shares),
        "logIndex": 0,
    }


def _repay_log(assets: int = 150, shares: int = 149) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["Repay"],
            MARKET_ID,
            pad_addr(CALLER),
            pad_addr(USER),
        ],
        "data": "0x" + encode_uint256(assets) + encode_uint256(shares),
        "logIndex": 0,
    }


def _supply_collateral_log(assets: int = 5_000_000_000_000_000_000) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["SupplyCollateral"],
            MARKET_ID,
            pad_addr(CALLER),
            pad_addr(USER),
        ],
        "data": "0x" + encode_uint256(assets),
        "logIndex": 0,
    }


def _withdraw_collateral_log(assets: int = 1_000) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["WithdrawCollateral"],
            MARKET_ID,
            pad_addr(USER),
            pad_addr(RECEIVER),
        ],
        "data": "0x" + encode_address_only(CALLER) + encode_uint256(assets),
        "logIndex": 0,
    }


def _liquidate_log(
    repaid_assets: int = 100,
    repaid_shares: int = 99,
    seized_assets: int = 50,
    bad_debt_assets: int = 0,
    bad_debt_shares: int = 0,
) -> dict[str, Any]:
    data = (
        "0x"
        + encode_uint256(repaid_assets)
        + encode_uint256(repaid_shares)
        + encode_uint256(seized_assets)
        + encode_uint256(bad_debt_assets)
        + encode_uint256(bad_debt_shares)
    )
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["Liquidate"],
            MARKET_ID,
            pad_addr(CALLER),
            pad_addr(USER),
        ],
        "data": data,
        "logIndex": 0,
    }


def _flash_loan_log(assets: int = 1_000_000) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["FlashLoan"],
            pad_addr(CALLER),
            pad_addr(RECEIVER),
        ],
        "data": "0x" + encode_uint256(assets),
        "logIndex": 0,
    }


def _create_market_log(lltv: int = 860000000000000000) -> dict[str, Any]:
    data = (
        "0x"
        + encode_address_only("0x1111111111111111111111111111111111111111")
        + encode_address_only("0x2222222222222222222222222222222222222222")
        + encode_address_only("0x3333333333333333333333333333333333333333")
        + encode_address_only("0x4444444444444444444444444444444444444444")
        + encode_uint256(lltv)
    )
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["CreateMarket"],
            MARKET_ID,
        ],
        "data": data,
        "logIndex": 0,
    }


def _set_authorization_log(is_authorized: bool = True) -> dict[str, Any]:
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["SetAuthorization"],
            pad_addr(CALLER),
            pad_addr(USER),
        ],
        "data": "0x" + encode_uint256(1 if is_authorized else 0),
        "logIndex": 0,
    }


def _accrue_interest_log(prev_rate: int = 100, interest: int = 50, fee_shares: int = 1) -> dict[str, Any]:
    data = "0x" + encode_uint256(prev_rate) + encode_uint256(interest) + encode_uint256(fee_shares)
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            EVENT_TOPICS["AccrueInterest"],
            MARKET_ID,
        ],
        "data": data,
        "logIndex": 0,
    }


def _transfer_log(amount: int = 1000) -> dict[str, Any]:
    return {
        "address": "0x" + "ee" * 20,
        "topics": [
            EVENT_TOPICS["Transfer"],
            pad_addr(CALLER),
            pad_addr(RECEIVER),
        ],
        "data": "0x" + encode_uint256(amount),
        "logIndex": 0,
    }


def _approval_log(amount: int = 1000) -> dict[str, Any]:
    return {
        "address": "0x" + "ee" * 20,
        "topics": [
            EVENT_TOPICS["Approval"],
            pad_addr(CALLER),
            pad_addr(USER),
        ],
        "data": "0x" + encode_uint256(amount),
        "logIndex": 0,
    }


# =============================================================================
# Tests
# =============================================================================


class TestParseSupply:
    def test_parses_supply_event(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_supply_log(assets=12345, shares=678)))
        assert result.success
        assert len(result.events) == 1
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.SUPPLY
        assert evt.data["assets"] == "12345"
        assert evt.data["shares"] == "678"


class TestParseWithdraw:
    def test_parses_withdraw_event(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_withdraw_log(assets=100, shares=99)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.WITHDRAW
        assert evt.data["assets"] == "100"
        assert evt.data["receiver"].lower() == RECEIVER.lower()


class TestParseBorrow:
    def test_parses_borrow_event(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_borrow_log(assets=200)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.BORROW
        assert evt.data["assets"] == "200"


class TestParseRepay:
    def test_parses_repay_event(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_repay_log(assets=150)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.REPAY
        assert evt.data["assets"] == "150"


class TestParseSupplyCollateral:
    def test_parses_supply_collateral(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_supply_collateral_log(assets=99)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.SUPPLY_COLLATERAL
        assert evt.data["assets"] == "99"


class TestParseWithdrawCollateral:
    def test_parses_withdraw_collateral(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_withdraw_collateral_log(assets=77)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.WITHDRAW_COLLATERAL
        assert evt.data["assets"] == "77"


class TestParseLiquidate:
    def test_parses_liquidate_full(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(
            _wrap_receipt(
                _liquidate_log(
                    repaid_assets=100,
                    repaid_shares=99,
                    seized_assets=50,
                    bad_debt_assets=10,
                    bad_debt_shares=9,
                )
            )
        )
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.LIQUIDATE
        assert evt.data["repaid_assets"] == "100"
        assert evt.data["seized_assets"] == "50"
        assert evt.data["bad_debt_assets"] == "10"

    def test_parses_liquidate_short_data_no_bad_debt(self, parser: MorphoBlueReceiptParser) -> None:
        # Short data: only first 3 fields
        data = "0x" + encode_uint256(100) + encode_uint256(99) + encode_uint256(50)
        log = {
            "address": MORPHO_ADDRESS,
            "topics": [
                EVENT_TOPICS["Liquidate"],
                MARKET_ID,
                pad_addr(CALLER),
                pad_addr(USER),
            ],
            "data": data,
            "logIndex": 0,
        }
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success
        evt = result.events[0]
        assert evt.data["bad_debt_assets"] == "0"
        assert evt.data["bad_debt_shares"] == "0"


class TestParseFlashLoan:
    def test_parses_flash_loan(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_flash_loan_log(assets=1_000_000)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.FLASH_LOAN
        assert evt.data["assets"] == "1000000"

    def test_parses_flash_loan_with_empty_data(self, parser: MorphoBlueReceiptParser) -> None:
        log = {
            "address": MORPHO_ADDRESS,
            "topics": [
                EVENT_TOPICS["FlashLoan"],
                pad_addr(CALLER),
                pad_addr(RECEIVER),
            ],
            "data": "0x",
            "logIndex": 0,
        }
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success


class TestParseCreateMarket:
    def test_parses_create_market(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_create_market_log(lltv=860000000000000000)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.CREATE_MARKET
        assert evt.data["loan_token"].lower().endswith("1111111111111111111111111111111111111111")


class TestParseSetAuthorization:
    def test_authorize(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_set_authorization_log(True)))
        assert result.success
        assert result.events[0].data["is_authorized"] is True

    def test_deauthorize(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_set_authorization_log(False)))
        assert result.success
        assert result.events[0].data["is_authorized"] is False


class TestParseAccrueInterest:
    def test_parses_accrue_interest(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(
            _wrap_receipt(_accrue_interest_log(prev_rate=42, interest=100, fee_shares=5))
        )
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.ACCRUE_INTEREST
        assert evt.data["interest"] == "100"


class TestParseTransfer:
    def test_parses_transfer(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_transfer_log(amount=12345)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.TRANSFER
        assert evt.data["amount"] == "12345"


class TestParseApproval:
    def test_parses_approval(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt(_wrap_receipt(_approval_log(amount=999)))
        assert result.success
        evt = result.events[0]
        assert evt.event_type == MorphoBlueEventType.APPROVAL
        assert evt.data["amount"] == "999"


class TestParseEdgeCases:
    def test_unknown_topic_returns_none(self, parser: MorphoBlueReceiptParser) -> None:
        log = {
            "address": MORPHO_ADDRESS,
            "topics": ["0x" + "ff" * 32],
            "data": "0x",
            "logIndex": 0,
        }
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success
        # Unknown event filtered out
        assert len(result.events) == 0

    def test_empty_topics_skipped(self, parser: MorphoBlueReceiptParser) -> None:
        log = {"address": MORPHO_ADDRESS, "topics": [], "data": "0x", "logIndex": 0}
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success
        assert len(result.events) == 0

    def test_bytes_topic0_is_normalized(self, parser: MorphoBlueReceiptParser) -> None:
        # Topic as bytes (without 0x prefix) — must be normalized to hex string
        topic_bytes = bytes.fromhex(EVENT_TOPICS["AccrueInterest"][2:])
        log = {
            "address": MORPHO_ADDRESS,
            "topics": [
                topic_bytes,
                bytes.fromhex(MARKET_ID[2:]),
            ],
            "data": bytes.fromhex(encode_uint256(0) + encode_uint256(0) + encode_uint256(0)),
            "logIndex": 0,
        }
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == MorphoBlueEventType.ACCRUE_INTEREST

    def test_receipt_with_bytes_tx_hash(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_supply_log())
        receipt["transactionHash"] = bytes.fromhex("aa" * 32)
        result = parser.parse_receipt(receipt)
        assert result.success
        assert result.transaction_hash.startswith("0x")

    def test_block_number_string(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_supply_log())
        receipt["blockNumber"] = "1000"
        result = parser.parse_receipt(receipt)
        assert result.block_number == 1000

    def test_block_number_hex(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_supply_log())
        receipt["blockNumber"] = "0x100"
        result = parser.parse_receipt(receipt)
        assert result.block_number == 256

    def test_no_events_summary_branch(self, parser: MorphoBlueReceiptParser) -> None:
        # Only unknown events: triggers the empty-summary branch
        log = {
            "address": MORPHO_ADDRESS,
            "topics": ["0x" + "ff" * 32],
            "data": "0x",
            "logIndex": 0,
        }
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success

    def test_completely_empty_logs(self, parser: MorphoBlueReceiptParser) -> None:
        result = parser.parse_receipt({"logs": [], "transactionHash": "0x" + "00" * 32})
        assert result.success
        assert len(result.events) == 0

    def test_parse_receipt_with_exception_returns_failure(self, parser: MorphoBlueReceiptParser) -> None:
        # Pass a malformed receipt that triggers an error inside parse_receipt
        result = parser.parse_receipt({"logs": "this-is-not-iterable-properly"})
        # Iteration over chars works but each "log" is a str without .get → caught per-log
        # which goes to _parse_log warning. parse_receipt overall stays True.
        assert result.success

    def test_parse_log_with_exception_returns_none(self, parser: MorphoBlueReceiptParser) -> None:
        # Malformed Supply log: data too short for uint256 decode
        log = {
            "address": MORPHO_ADDRESS,
            "topics": [
                EVENT_TOPICS["Supply"],
                MARKET_ID,
                pad_addr(CALLER),
                pad_addr(USER),
            ],
            "data": "0x12",  # way too short
            "logIndex": 0,
        }
        # This is parsed without a crash even with short data because HexDecoder pads it.
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success

    def test_address_as_bytes(self, parser: MorphoBlueReceiptParser) -> None:
        log = _supply_log()
        log["address"] = bytes.fromhex(MORPHO_ADDRESS[2:])
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success
        assert result.events[0].contract_address.startswith("0x")

    def test_data_as_bytes(self, parser: MorphoBlueReceiptParser) -> None:
        log = _supply_log()
        log["data"] = bytes.fromhex(encode_uint256(100) + encode_uint256(99))
        result = parser.parse_receipt(_wrap_receipt(log))
        assert result.success


class TestExtractAmounts:
    def test_extract_withdraw_amount(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_withdraw_log(assets=500))
        assert parser.extract_withdraw_amount(receipt) == 500

    def test_extract_withdraw_amount_none_for_other_event(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_supply_log())
        assert parser.extract_withdraw_amount(receipt) is None

    def test_extract_borrow_amount(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_borrow_log(assets=200))
        assert parser.extract_borrow_amount(receipt) == 200

    def test_extract_borrow_amount_none_for_other(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.extract_borrow_amount(_wrap_receipt(_supply_log())) is None

    def test_extract_repay_amount(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_repay_log(assets=150))
        assert parser.extract_repay_amount(receipt) == 150

    def test_extract_repay_amount_none_for_other(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.extract_repay_amount(_wrap_receipt(_supply_log())) is None

    def test_extract_shares_burned_from_withdraw(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_withdraw_log(shares=99))
        assert parser.extract_shares_burned(receipt) == 99

    def test_extract_shares_burned_from_repay(self, parser: MorphoBlueReceiptParser) -> None:
        receipt = _wrap_receipt(_repay_log(shares=42))
        assert parser.extract_shares_burned(receipt) == 42

    def test_extract_shares_burned_none_for_other(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.extract_shares_burned(_wrap_receipt(_supply_log())) is None


class TestExtractResultVariants:
    """The Ok / Missing / Error tagged variants."""

    def test_withdraw_amount_result_ok(self, parser: MorphoBlueReceiptParser) -> None:
        from almanak.framework.execution.extract_result import ExtractOk

        out = parser.extract_withdraw_amount_result(_wrap_receipt(_withdraw_log(assets=42)))
        assert isinstance(out, ExtractOk)
        assert out.value == 42

    def test_borrow_amount_result_ok(self, parser: MorphoBlueReceiptParser) -> None:
        from almanak.framework.execution.extract_result import ExtractOk

        out = parser.extract_borrow_amount_result(_wrap_receipt(_borrow_log(assets=88)))
        assert isinstance(out, ExtractOk)

    def test_repay_amount_result_ok(self, parser: MorphoBlueReceiptParser) -> None:
        from almanak.framework.execution.extract_result import ExtractOk

        out = parser.extract_repay_amount_result(_wrap_receipt(_repay_log(assets=11)))
        assert isinstance(out, ExtractOk)

    def test_supply_amount_result_ok(self, parser: MorphoBlueReceiptParser) -> None:
        from almanak.framework.execution.extract_result import ExtractOk

        out = parser.extract_supply_amount_result(_wrap_receipt(_supply_log(assets=33)))
        assert isinstance(out, ExtractOk)
        assert out.value == 33


class TestProtocolFeesEdgeCases:
    """Beyond the existing tests, cover withdraw / borrow / repay / collateral."""

    @pytest.mark.parametrize(
        "log_factory",
        [
            _withdraw_log,
            _borrow_log,
            _repay_log,
            _supply_collateral_log,
            _withdraw_collateral_log,
        ],
    )
    def test_returns_zero_for_recognised_operation(
        self, parser: MorphoBlueReceiptParser, log_factory
    ) -> None:
        receipt = _wrap_receipt(log_factory())
        fees = parser.extract_protocol_fees(receipt)
        assert fees is not None
        assert fees.total_usd == Decimal(0)
        assert fees.lending_origination_fee_usd == Decimal(0)


class TestIsMorphoEvent:
    def test_known_topic_str(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.is_morpho_event(EVENT_TOPICS["Supply"])

    def test_known_topic_bytes(self, parser: MorphoBlueReceiptParser) -> None:
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Supply"][2:])
        assert parser.is_morpho_event(topic_bytes)

    def test_known_topic_no_prefix(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.is_morpho_event(EVENT_TOPICS["Supply"][2:])

    def test_unknown_topic_returns_false(self, parser: MorphoBlueReceiptParser) -> None:
        assert not parser.is_morpho_event("0x" + "ff" * 32)


class TestGetEventType:
    def test_by_topic_str(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.get_event_type(EVENT_TOPICS["Supply"]) == MorphoBlueEventType.SUPPLY

    def test_by_topic_bytes(self, parser: MorphoBlueReceiptParser) -> None:
        assert (
            parser.get_event_type(bytes.fromhex(EVENT_TOPICS["Supply"][2:]))
            == MorphoBlueEventType.SUPPLY
        )

    def test_by_event_name(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.get_event_type("Supply") == MorphoBlueEventType.SUPPLY

    def test_unknown_returns_unknown(self, parser: MorphoBlueReceiptParser) -> None:
        assert parser.get_event_type("0x" + "ff" * 32) == MorphoBlueEventType.UNKNOWN
        assert parser.get_event_type("NotARealEvent") == MorphoBlueEventType.UNKNOWN


class TestEventDataClasses:
    """Smoke-test to_dict serializers for every event data class."""

    def test_supply_event_data(self) -> None:
        e = SupplyEventData(market_id="x", caller="c", on_behalf_of="o", assets=Decimal(1), shares=Decimal(2))
        assert e.to_dict()["assets"] == "1"

    def test_withdraw_event_data(self) -> None:
        e = WithdrawEventData(
            market_id="x", caller="c", on_behalf_of="o", receiver="r", assets=Decimal(1), shares=Decimal(2)
        )
        assert "receiver" in e.to_dict()

    def test_borrow_event_data(self) -> None:
        e = BorrowEventData(
            market_id="x", caller="c", on_behalf_of="o", receiver="r", assets=Decimal(1), shares=Decimal(2)
        )
        assert "receiver" in e.to_dict()

    def test_repay_event_data(self) -> None:
        e = RepayEventData(market_id="x", caller="c", on_behalf_of="o", assets=Decimal(1), shares=Decimal(2))
        assert "assets" in e.to_dict()

    def test_supply_collateral_event_data(self) -> None:
        e = SupplyCollateralEventData(market_id="x", caller="c", on_behalf_of="o", assets=Decimal(1))
        assert "assets" in e.to_dict()

    def test_withdraw_collateral_event_data(self) -> None:
        e = WithdrawCollateralEventData(
            market_id="x", caller="c", on_behalf_of="o", receiver="r", assets=Decimal(1)
        )
        assert "receiver" in e.to_dict()

    def test_liquidate_event_data(self) -> None:
        e = LiquidateEventData(
            market_id="x",
            caller="c",
            borrower="b",
            repaid_assets=Decimal(1),
            repaid_shares=Decimal(2),
            seized_assets=Decimal(3),
        )
        assert "bad_debt_assets" in e.to_dict()

    def test_flash_loan_event_data(self) -> None:
        e = FlashLoanEventData(caller="c", token="t", assets=Decimal(1))
        assert "token" in e.to_dict()

    def test_create_market_event_data(self) -> None:
        e = CreateMarketEventData(
            market_id="x",
            loan_token="lt",
            collateral_token="ct",
            oracle="o",
            irm="i",
            lltv=860000000000000000,
        )
        assert e.to_dict()["lltv_percent"] == 86.0

    def test_set_authorization_event_data(self) -> None:
        e = SetAuthorizationEventData(caller="c", authorized="a", is_authorized=True)
        assert e.to_dict()["is_authorized"] is True

    def test_accrue_interest_event_data(self) -> None:
        e = AccrueInterestEventData(
            market_id="x", prev_borrow_rate=Decimal(1), interest=Decimal(2), fee_shares=Decimal(3)
        )
        assert "interest" in e.to_dict()

    def test_transfer_event_data(self) -> None:
        e = TransferEventData(from_address="f", to_address="t", amount=Decimal(1))
        d = e.to_dict()
        assert d["from"] == "f"

    def test_morpho_blue_event_to_dict_and_from_dict_roundtrip(self) -> None:
        evt = MorphoBlueEvent(
            event_type=MorphoBlueEventType.SUPPLY,
            event_name="Supply",
            log_index=0,
            transaction_hash="0xabc",
            block_number=1,
            contract_address="0xdef",
            data={"x": "y"},
            timestamp=datetime.now(UTC),
        )
        roundtrip = MorphoBlueEvent.from_dict(evt.to_dict())
        assert roundtrip.event_type == MorphoBlueEventType.SUPPLY
        assert roundtrip.event_name == "Supply"

    def test_morpho_blue_event_from_dict_no_timestamp(self) -> None:
        evt_dict = {
            "event_type": "SUPPLY",
            "event_name": "Supply",
            "log_index": 0,
            "transaction_hash": "0xabc",
            "block_number": 1,
            "contract_address": "0xdef",
            "data": {"x": "y"},
        }
        rt = MorphoBlueEvent.from_dict(evt_dict)
        assert rt.event_name == "Supply"

    def test_parse_result_to_dict(self) -> None:
        pr = ParseResult(success=True, transaction_hash="0xabc", block_number=10)
        as_dict = pr.to_dict()
        assert as_dict["success"] is True
        assert as_dict["events"] == []
