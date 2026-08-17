"""VIB-3159: Aave V3 three-variant extract contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.aave_v3.receipt_parser import EVENT_TOPICS, AaveV3ReceiptParser
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
)


@pytest.fixture
def parser() -> AaveV3ReceiptParser:
    return AaveV3ReceiptParser(chain="arbitrum")


def test_supply_amount_result_empty_is_missing(parser: AaveV3ReceiptParser) -> None:
    assert isinstance(parser.extract_supply_amount_result({"logs": []}), ExtractMissing)


def test_withdraw_amount_result_empty_is_missing(parser: AaveV3ReceiptParser) -> None:
    assert isinstance(parser.extract_withdraw_amount_result({"logs": []}), ExtractMissing)


def test_borrow_amount_result_empty_is_missing(parser: AaveV3ReceiptParser) -> None:
    assert isinstance(parser.extract_borrow_amount_result({"logs": []}), ExtractMissing)


def test_repay_amount_result_empty_is_missing(parser: AaveV3ReceiptParser) -> None:
    assert isinstance(parser.extract_repay_amount_result({"logs": []}), ExtractMissing)


def test_a_token_received_result_empty_is_missing(parser: AaveV3ReceiptParser) -> None:
    assert isinstance(parser.extract_a_token_received_result({"logs": []}), ExtractMissing)


def test_supply_amount_result_crash_is_error(parser: AaveV3ReceiptParser) -> None:
    def boom(_receipt: dict[str, Any]) -> Any:
        raise RuntimeError("aave parse failure")

    parser.extract_supply_amount = boom  # type: ignore[method-assign]
    out = parser.extract_supply_amount_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "aave parse failure" in out.error


@pytest.mark.parametrize(
    ("event_name", "extractor_name"),
    [
        ("Supply", "extract_supply_amount_result"),
        ("Withdraw", "extract_withdraw_amount_result"),
        ("Borrow", "extract_borrow_amount_result"),
        ("Repay", "extract_repay_amount_result"),
    ],
)
def test_malformed_recognized_money_event_is_error(
    parser: AaveV3ReceiptParser,
    event_name: str,
    extractor_name: str,
) -> None:
    receipt = {
        "logs": [{"topics": [EVENT_TOPICS[event_name]], "data": "0x01"}],
        "status": 1,
    }

    parsed = parser.parse_receipt(receipt)
    extracted = getattr(parser, extractor_name)(receipt)

    assert parsed.success is False
    assert event_name in (parsed.error or "")
    assert isinstance(extracted, ExtractError)


def test_malformed_transfer_is_error_for_atoken_result(parser: AaveV3ReceiptParser) -> None:
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
    extracted = parser.extract_a_token_received_result(receipt)

    assert parsed.success is False
    assert "Transfer" in (parsed.error or "")
    assert isinstance(extracted, ExtractError)


def test_erc721_transfer_is_valid_but_not_an_atoken_amount(parser: AaveV3ReceiptParser) -> None:
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
    extracted = parser.extract_a_token_received_result(receipt)

    assert parsed.success is True
    assert isinstance(extracted, ExtractMissing)
