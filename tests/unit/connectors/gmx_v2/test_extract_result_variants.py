"""VIB-3159: GMX v2 three-variant extract contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
)


@pytest.fixture
def parser() -> GMXv2ReceiptParser:
    return GMXv2ReceiptParser(chain="arbitrum")


def test_position_id_result_empty_is_missing(parser: GMXv2ReceiptParser) -> None:
    assert isinstance(parser.extract_position_id_result({"logs": []}), ExtractMissing)


def test_size_delta_result_empty_is_missing(parser: GMXv2ReceiptParser) -> None:
    assert isinstance(parser.extract_size_delta_result({"logs": []}), ExtractMissing)


def test_collateral_result_empty_is_missing(parser: GMXv2ReceiptParser) -> None:
    assert isinstance(parser.extract_collateral_result({"logs": []}), ExtractMissing)


def test_entry_price_result_empty_is_missing(parser: GMXv2ReceiptParser) -> None:
    assert isinstance(parser.extract_entry_price_result({"logs": []}), ExtractMissing)


def test_fees_paid_result_empty_is_missing(parser: GMXv2ReceiptParser) -> None:
    assert isinstance(parser.extract_fees_paid_result({"logs": []}), ExtractMissing)


def test_position_id_result_crash_is_error(parser: GMXv2ReceiptParser) -> None:
    def boom(_receipt: dict[str, Any]) -> Any:
        raise RuntimeError("gmx parse failure")

    parser.extract_position_id = boom  # type: ignore[method-assign]
    out = parser.extract_position_id_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "gmx parse failure" in out.error
