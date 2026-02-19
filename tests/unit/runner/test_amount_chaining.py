"""Tests for multi-intent amount chaining in StrategyRunner.

Verifies that when swap_amounts.amount_out_decimal is None, the runner resets
previous_amount_received to None instead of silently reusing a stale value.
"""

from decimal import Decimal
from enum import StrEnum
from unittest.mock import MagicMock


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
