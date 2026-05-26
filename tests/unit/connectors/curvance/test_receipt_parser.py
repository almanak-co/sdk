"""Unit tests for ``CurvanceReceiptParser``.

Exercises the four event branches (Deposit, Withdraw, Borrow, Repay) with
hand-built receipts so we cover layout assumptions without running on-chain.
The on-chain coverage lives in ``tests/intents/monad/test_curvance_lending.py``
(SUPPLY) — non-supply events are blocked from real execution by a documented
oracle CAUTION fork artefact, but the decoding logic is fully testable here.
"""

from __future__ import annotations

from almanak.connectors.curvance.receipt_parser import (
    CurvanceEventType,
    CurvanceReceiptParser,
)
from almanak.connectors.curvance.sdk import (
    EVENT_TOPIC_BORROW,
    EVENT_TOPIC_DEPOSIT,
    EVENT_TOPIC_REPAY,
    EVENT_TOPIC_WITHDRAW,
)

CTOKEN = "0x1e240E30E51491546deC3aF16B0b4EAC8Dd110D4"
BORROWABLE = "0x8EE9FC28B8Da872c38A496e9dDB9700bb7261774"
USER = "0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF"
RECEIVER = "0x000000000000000000000000000000000000DEAD"
PAYER = "0x000000000000000000000000000000000000beef"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").rjust(64, "0")


def _word(value: int) -> str:
    return f"{value:064x}"


def _make_receipt(logs: list[dict]) -> dict:
    return {
        "transactionHash": "0xdeadbeef",
        "status": 1,
        "logs": logs,
    }


def test_parse_deposit_event() -> None:
    parser = CurvanceReceiptParser()
    receipt = _make_receipt(
        [
            {
                "address": CTOKEN,
                "topics": [EVENT_TOPIC_DEPOSIT, _addr_topic(USER), _addr_topic(USER)],
                "data": "0x" + _word(1_000_000) + _word(990_000),
                "blockNumber": "0x10",
                "logIndex": "0x0",
            }
        ]
    )
    result = parser.parse_receipt(receipt)
    assert result.success
    assert len(result.supply_events) == 1
    ev = result.supply_events[0]
    assert ev.event_type == CurvanceEventType.DEPOSIT
    assert ev.contract.lower() == CTOKEN.lower()
    assert ev.data["assets"] == 1_000_000
    assert ev.data["shares"] == 990_000
    assert ev.data["from"].lower() == USER.lower()
    assert parser.extract_supply_amount(receipt) == 1_000_000


def test_parse_withdraw_event() -> None:
    parser = CurvanceReceiptParser()
    receipt = _make_receipt(
        [
            {
                "address": CTOKEN,
                "topics": [
                    EVENT_TOPIC_WITHDRAW,
                    _addr_topic(USER),
                    _addr_topic(RECEIVER),
                    _addr_topic(USER),
                ],
                "data": "0x" + _word(2_500_000) + _word(2_400_000),
                "blockNumber": "0x11",
                "logIndex": "0x1",
            }
        ]
    )
    result = parser.parse_receipt(receipt)
    assert result.success
    assert len(result.withdraw_events) == 1
    ev = result.withdraw_events[0]
    assert ev.event_type == CurvanceEventType.WITHDRAW
    assert ev.data["assets"] == 2_500_000
    assert ev.data["shares"] == 2_400_000
    assert ev.data["receiver"].lower() == RECEIVER.lower()
    assert parser.extract_withdraw_amount(receipt) == 2_500_000


def test_parse_borrow_event_three_words() -> None:
    """Borrow has 3 non-indexed words: assets, debtAssetsOwed, account."""
    parser = CurvanceReceiptParser()
    receipt = _make_receipt(
        [
            {
                "address": BORROWABLE,
                "topics": [EVENT_TOPIC_BORROW],
                "data": "0x" + _word(11_000_013) + _word(11_000_013) + _word(int(USER, 16)),
                "blockNumber": "0x12",
                "logIndex": "0x2",
            }
        ]
    )
    result = parser.parse_receipt(receipt)
    assert result.success
    assert len(result.borrow_events) == 1
    ev = result.borrow_events[0]
    assert ev.event_type == CurvanceEventType.BORROW
    assert ev.data["assets"] == 11_000_013
    assert ev.data["debt_assets_owed"] == 11_000_013
    assert ev.data["account"].lower() == USER.lower()
    assert parser.extract_borrow_amount(receipt) == 11_000_013


def test_parse_repay_event_four_words() -> None:
    """Repay has 4 non-indexed words: assets, debtAssetsOwed, payer, account."""
    parser = CurvanceReceiptParser()
    receipt = _make_receipt(
        [
            {
                "address": BORROWABLE,
                "topics": [EVENT_TOPIC_REPAY],
                "data": (
                    "0x"
                    + _word(11_000_025)
                    + _word(0)
                    + _word(int(PAYER, 16))
                    + _word(int(USER, 16))
                ),
                "blockNumber": "0x13",
                "logIndex": "0x3",
            }
        ]
    )
    result = parser.parse_receipt(receipt)
    assert result.success
    assert len(result.repay_events) == 1
    ev = result.repay_events[0]
    assert ev.event_type == CurvanceEventType.REPAY
    assert ev.data["assets"] == 11_000_025
    assert ev.data["debt_assets_owed"] == 0
    assert ev.data["payer"].lower() == PAYER.lower()
    assert ev.data["account"].lower() == USER.lower()
    assert parser.extract_repay_amount(receipt) == 11_000_025


def test_malformed_borrow_data_returns_none() -> None:
    """Too few data words must not crash and must not fabricate an event."""
    parser = CurvanceReceiptParser()
    receipt = _make_receipt(
        [
            {
                "address": BORROWABLE,
                "topics": [EVENT_TOPIC_BORROW],
                # Only 2 words instead of 3
                "data": "0x" + _word(11_000_013) + _word(11_000_013),
                "blockNumber": "0x14",
                "logIndex": "0x4",
            }
        ]
    )
    result = parser.parse_receipt(receipt)
    assert result.success
    assert result.borrow_events == []


def test_malformed_repay_data_returns_none() -> None:
    parser = CurvanceReceiptParser()
    receipt = _make_receipt(
        [
            {
                "address": BORROWABLE,
                "topics": [EVENT_TOPIC_REPAY],
                # Only 3 words instead of 4
                "data": "0x" + _word(11_000_025) + _word(0) + _word(int(PAYER, 16)),
                "blockNumber": "0x15",
                "logIndex": "0x5",
            }
        ]
    )
    result = parser.parse_receipt(receipt)
    assert result.success
    assert result.repay_events == []


def test_withdraw_missing_topics_returns_none() -> None:
    """Withdraw needs 4 topics (3 indexed + topic[0]); missing one must be skipped."""
    parser = CurvanceReceiptParser()
    receipt = _make_receipt(
        [
            {
                "address": CTOKEN,
                # Missing one indexed address
                "topics": [EVENT_TOPIC_WITHDRAW, _addr_topic(USER), _addr_topic(USER)],
                "data": "0x" + _word(2_500_000) + _word(2_400_000),
                "blockNumber": "0x16",
                "logIndex": "0x6",
            }
        ]
    )
    result = parser.parse_receipt(receipt)
    assert result.success
    assert result.withdraw_events == []


def test_no_op_extractors_for_lp_swap_hooks() -> None:
    """Curvance is a lending parser — LP/swap extractor hooks must return None."""
    parser = CurvanceReceiptParser()
    empty = _make_receipt([])
    assert parser.extract_position_id(empty) is None
    assert parser.extract_liquidity(empty) is None
    assert parser.extract_lp_close_data(empty) is None
    assert parser.extract_swap_amounts(empty) is None
