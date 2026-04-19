"""VIB-3159: Aave V3 three-variant extract contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.connectors.aave_v3.receipt_parser import AaveV3ReceiptParser
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
