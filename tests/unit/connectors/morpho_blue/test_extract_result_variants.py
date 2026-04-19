"""VIB-3159: Morpho Blue three-variant extract contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.connectors.morpho_blue.receipt_parser import MorphoBlueReceiptParser
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
