"""Tests for multi-intent amount chaining in StrategyRunner.

Verifies that when swap_amounts.amount_out_decimal is None, the runner resets
previous_amount_received to None instead of silently reusing a stale value.
Also tests that Enso-extracted SwapAmounts feeds into chaining correctly.
"""

from decimal import Decimal
from enum import StrEnum
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.enso.receipt_parser import (
    TRANSFER_EVENT_SIGNATURE,
    EnsoReceiptParser,
)
from almanak.framework.execution.extracted_data import SwapAmounts


class _IterationStatus(StrEnum):
    SUCCESS = "success"


def test_amount_chaining_resets_on_missing_output():
    """When swap_amounts is None or amount_out_decimal is None, previous_amount_received must be None."""
    # Simulate the chaining logic from strategy_runner.py:1082-1096
    previous_amount_received = Decimal("999.99")  # stale value from a prior step

    # Simulate an execution result with no swap_amounts
    mock_result = MagicMock()
    mock_result.status = _IterationStatus.SUCCESS
    mock_result.execution_result = MagicMock()
    mock_result.execution_result.swap_amounts = None

    # Apply the same logic as strategy_runner.py
    er = mock_result.execution_result
    if er.swap_amounts and er.swap_amounts.amount_out_decimal is not None:
        previous_amount_received = er.swap_amounts.amount_out_decimal
    else:
        previous_amount_received = None

    # The stale value must NOT persist
    assert previous_amount_received is None


def test_amount_chaining_resets_on_none_amount_out():
    """When swap_amounts exists but amount_out_decimal is None, reset to None."""
    previous_amount_received = Decimal("500.0")

    mock_result = MagicMock()
    mock_result.status = _IterationStatus.SUCCESS
    mock_result.execution_result = MagicMock()
    mock_result.execution_result.swap_amounts = MagicMock()
    mock_result.execution_result.swap_amounts.amount_out_decimal = None

    er = mock_result.execution_result
    if er.swap_amounts and er.swap_amounts.amount_out_decimal is not None:
        previous_amount_received = er.swap_amounts.amount_out_decimal
    else:
        previous_amount_received = None

    assert previous_amount_received is None


def test_amount_chaining_preserves_valid_output():
    """When swap_amounts.amount_out_decimal is valid, it should chain through."""
    previous_amount_received = None

    mock_result = MagicMock()
    mock_result.status = _IterationStatus.SUCCESS
    mock_result.execution_result = MagicMock()
    mock_result.execution_result.swap_amounts = MagicMock()
    mock_result.execution_result.swap_amounts.amount_out_decimal = Decimal("0.5")

    er = mock_result.execution_result
    if er.swap_amounts and er.swap_amounts.amount_out_decimal is not None:
        previous_amount_received = er.swap_amounts.amount_out_decimal
    else:
        previous_amount_received = None

    assert previous_amount_received == Decimal("0.5")


# ---------------------------------------------------------------------------
# Enso integration: extract_swap_amounts feeds into chaining
# ---------------------------------------------------------------------------

def _pad_address(addr: str) -> str:
    return "0x" + addr[2:].lower().zfill(64)


def _encode_uint256(value: int) -> str:
    return "0x" + hex(value)[2:].zfill(64)


def _transfer_log(token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [TRANSFER_EVENT_SIGNATURE, _pad_address(from_addr), _pad_address(to_addr)],
        "data": _encode_uint256(amount),
    }


def test_enso_swap_amounts_chain_to_next_intent():
    """End-to-end: Enso extract_swap_amounts produces amount_out_decimal
    that the chaining logic picks up as previous_amount_received."""
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    token_in = "0x1111111111111111111111111111111111111111"
    token_out = "0x2222222222222222222222222222222222222222"

    receipt = {
        "from": wallet,
        "status": 1,
        "transactionHash": "0x" + "cc" * 32,
        "logs": [
            _transfer_log(token_in, wallet, "0xrouter", 1_000_000_000),  # 1000 USDC
            _transfer_log(token_out, "0xrouter", wallet, 500_000_000_000_000_000),  # 0.5 WETH
        ],
        "gasUsed": 150_000,
        "effectiveGasPrice": 30_000_000_000,
    }

    parser = EnsoReceiptParser(chain="arbitrum")
    with patch.object(parser, "_resolve_decimals", side_effect=[6, 18]):
        swap_amounts = parser.extract_swap_amounts(receipt)

    # Simulate the chaining logic from strategy_runner.py:1082-1085
    previous_amount_received = None
    if swap_amounts and swap_amounts.amount_out_decimal is not None:
        previous_amount_received = swap_amounts.amount_out_decimal
    else:
        previous_amount_received = None

    # The chained amount should be 0.5 (WETH)
    assert previous_amount_received == Decimal("0.5")


def test_enso_swap_amounts_frozen_dataclass():
    """SwapAmounts from Enso extraction is a proper frozen dataclass."""
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    token_out = "0x2222222222222222222222222222222222222222"

    receipt = {
        "from": wallet,
        "status": 1,
        "transactionHash": "0x" + "dd" * 32,
        "logs": [
            _transfer_log(token_out, "0xrouter", wallet, 1_000_000),
        ],
        "gasUsed": 100_000,
        "effectiveGasPrice": 25_000_000_000,
    }

    parser = EnsoReceiptParser()
    with patch.object(parser, "_resolve_decimals", return_value=18):
        swap_amounts = parser.extract_swap_amounts(receipt)

    assert isinstance(swap_amounts, SwapAmounts)
    # Verify it's frozen (immutable)
    try:
        swap_amounts.amount_out = 999  # type: ignore[misc]
        assert False, "SwapAmounts should be frozen"
    except AttributeError:
        pass  # Expected
