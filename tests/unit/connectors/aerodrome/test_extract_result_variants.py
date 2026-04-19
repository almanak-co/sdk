"""VIB-3159: Aerodrome three-variant extract contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
)


@pytest.fixture
def parser() -> AerodromeReceiptParser:
    return AerodromeReceiptParser(chain="base")


def test_swap_amounts_result_empty_is_missing(parser: AerodromeReceiptParser) -> None:
    assert isinstance(parser.extract_swap_amounts_result({"logs": []}), ExtractMissing)


def test_lp_close_data_result_empty_is_missing(parser: AerodromeReceiptParser) -> None:
    assert isinstance(parser.extract_lp_close_data_result({"logs": []}), ExtractMissing)


def test_position_id_result_empty_is_missing(parser: AerodromeReceiptParser) -> None:
    assert isinstance(parser.extract_position_id_result({"logs": []}), ExtractMissing)


def test_liquidity_result_empty_is_missing(parser: AerodromeReceiptParser) -> None:
    assert isinstance(parser.extract_liquidity_result({"logs": []}), ExtractMissing)


def test_swap_amounts_result_crash_is_error(parser: AerodromeReceiptParser) -> None:
    def boom(_receipt: dict[str, Any]) -> Any:
        raise RuntimeError("aerodrome parse failure")

    parser.extract_swap_amounts = boom  # type: ignore[method-assign]
    out = parser.extract_swap_amounts_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "aerodrome parse failure" in out.error


def test_lp_close_data_result_crash_is_error(parser: AerodromeReceiptParser) -> None:
    def boom(_receipt: dict[str, Any]) -> Any:
        raise RuntimeError("aerodrome lp close crash")

    parser.extract_lp_close_data = boom  # type: ignore[method-assign]
    out = parser.extract_lp_close_data_result({"logs": []})
    assert isinstance(out, ExtractError)
