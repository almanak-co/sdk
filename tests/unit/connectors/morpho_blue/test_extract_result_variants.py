"""VIB-3159: Morpho Blue three-variant extract contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.morpho_blue.receipt_parser import EVENT_TOPICS, MorphoBlueReceiptParser
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
)


@pytest.fixture
def parser() -> MorphoBlueReceiptParser:
    return MorphoBlueReceiptParser()


def test_supply_amount_result_empty_is_missing(parser: MorphoBlueReceiptParser) -> None:
    assert isinstance(parser.extract_supply_amount_result({"logs": []}), ExtractMissing)


def test_withdraw_amount_result_empty_is_missing(parser: MorphoBlueReceiptParser) -> None:
    assert isinstance(parser.extract_withdraw_amount_result({"logs": []}), ExtractMissing)


def test_borrow_amount_result_empty_is_missing(parser: MorphoBlueReceiptParser) -> None:
    assert isinstance(parser.extract_borrow_amount_result({"logs": []}), ExtractMissing)


def test_repay_amount_result_empty_is_missing(parser: MorphoBlueReceiptParser) -> None:
    assert isinstance(parser.extract_repay_amount_result({"logs": []}), ExtractMissing)


def test_supply_amount_result_crash_is_error(parser: MorphoBlueReceiptParser) -> None:
    def boom(_receipt: dict[str, Any]) -> Any:
        raise RuntimeError("morpho parse failure")

    parser.extract_supply_amount = boom  # type: ignore[method-assign]
    out = parser.extract_supply_amount_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "morpho parse failure" in out.error


@pytest.mark.parametrize(
    ("event_name", "extractor_name"),
    [
        ("Supply", "extract_supply_amount_result"),
        ("Withdraw", "extract_withdraw_amount_result"),
        ("Borrow", "extract_borrow_amount_result"),
        ("Repay", "extract_repay_amount_result"),
        ("SupplyCollateral", "extract_supply_collateral_amount_result"),
        ("WithdrawCollateral", "extract_withdraw_collateral_amount_result"),
    ],
)
def test_malformed_recognized_money_event_is_error(
    parser: MorphoBlueReceiptParser,
    event_name: str,
    extractor_name: str,
) -> None:
    receipt = {"logs": [{"topics": [EVENT_TOPICS[event_name]], "data": "0x01"}]}

    parsed = parser.parse_receipt(receipt)
    extracted = getattr(parser, extractor_name)(receipt)

    assert parsed.success is False
    assert event_name in (parsed.error or "")
    assert isinstance(extracted, ExtractError)


def test_malformed_transfer_fails_closed(parser: MorphoBlueReceiptParser) -> None:
    address_topic = "0x" + "00" * 12 + "11" * 20
    receipt = {
        "logs": [
            {
                "topics": [EVENT_TOPICS["Transfer"], address_topic, address_topic],
                "data": "0x01",
            }
        ]
    }

    parsed = parser.parse_receipt(receipt)
    probed = parser._parse_receipt_result(receipt)

    assert parsed.success is False
    assert "Transfer" in (parsed.error or "")
    assert isinstance(probed, ExtractError)


def test_erc721_transfer_is_valid_but_not_fungible_value(parser: MorphoBlueReceiptParser) -> None:
    address_topic = "0x" + "00" * 12 + "11" * 20
    receipt = {
        "logs": [
            {
                "topics": [EVENT_TOPICS["Transfer"], address_topic, address_topic, "0x" + "22" * 32],
                "data": "0x",
            }
        ]
    }

    parsed = parser.parse_receipt(receipt)

    assert parsed.success is True
    assert len(parsed.events) == 1
    assert "amount" not in parsed.events[0].data
    assert parser.extract_primitive_money_legs(receipt) is None
