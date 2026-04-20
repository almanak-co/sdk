"""VIB-3159: Uniswap V3 three-variant extract contract tests.

Each retrofitted extract_{field}_result method must dispatch to:
  - ExtractMissing on clean-but-empty receipts
  - ExtractOk on clean receipts with the expected event
  - ExtractError when the underlying parse raises
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.extract_result import (
    ExtractError,
    ExtractMissing,
    ExtractOk,
)


@pytest.fixture
def parser() -> UniswapV3ReceiptParser:
    return UniswapV3ReceiptParser(chain="arbitrum")


def test_extract_position_id_result_empty_logs_is_missing(parser: UniswapV3ReceiptParser) -> None:
    out = parser.extract_position_id_result({"logs": []})
    assert isinstance(out, ExtractMissing)


def test_extract_position_id_result_no_mint_transfer_is_missing(
    parser: UniswapV3ReceiptParser,
) -> None:
    # Unrelated Transfer (not from zero address → not a mint)
    bad_log = {
        "address": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x0000000000000000000000001111111111111111111111111111111111111111",
            "0x0000000000000000000000002222222222222222222222222222222222222222",
            "0x000000000000000000000000000000000000000000000000000000000000002a",
        ],
        "data": "0x",
    }
    out = parser.extract_position_id_result({"logs": [bad_log]})
    assert isinstance(out, ExtractMissing)


def test_extract_position_id_result_crash_is_error(parser: UniswapV3ReceiptParser) -> None:
    # Force the underlying impl to crash by monkeypatching it on the
    # instance — the _result wrapper must catch and wrap as ExtractError.
    def boom(_receipt: dict[str, Any]) -> int | None:
        raise RuntimeError("induced parser failure")

    parser.extract_position_id = boom  # type: ignore[method-assign]
    out = parser.extract_position_id_result({"logs": [{"topics": []}]})
    assert isinstance(out, ExtractError)
    assert "induced parser failure" in out.error


def test_extract_swap_amounts_result_empty_is_missing(parser: UniswapV3ReceiptParser) -> None:
    out = parser.extract_swap_amounts_result({"logs": []})
    assert isinstance(out, ExtractMissing)


def test_extract_swap_amounts_result_crash_is_error(parser: UniswapV3ReceiptParser) -> None:
    def boom(_receipt: dict[str, Any], **_kwargs: Any) -> Any:
        # Accept the VIB-3203 ``expected_out`` kwarg forwarded by the wrapper.
        raise ValueError("induced swap crash")

    parser.extract_swap_amounts = boom  # type: ignore[method-assign]
    out = parser.extract_swap_amounts_result({"logs": []})
    assert isinstance(out, ExtractError)
    assert "induced swap crash" in out.error


def test_extract_lp_close_data_result_empty_is_missing(parser: UniswapV3ReceiptParser) -> None:
    out = parser.extract_lp_close_data_result({"logs": []})
    assert isinstance(out, ExtractMissing)


def test_extract_liquidity_result_empty_is_missing(parser: UniswapV3ReceiptParser) -> None:
    out = parser.extract_liquidity_result({"logs": []})
    assert isinstance(out, ExtractMissing)


def test_extract_position_id_result_ok_on_real_mint(parser: UniswapV3ReceiptParser) -> None:
    # Genuine mint Transfer: from=zero, to=wallet, tokenId=42
    pos_mgr = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
    mint_log = {
        "address": pos_mgr,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "0x0000000000000000000000002222222222222222222222222222222222222222",
            "0x000000000000000000000000000000000000000000000000000000000000002a",
        ],
        "data": "0x",
    }
    out = parser.extract_position_id_result({"logs": [mint_log]})
    assert isinstance(out, ExtractOk)
    assert out.value == 0x2A
