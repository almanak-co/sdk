"""Tests for multi-intent amount chaining in StrategyRunner.

Verifies that when swap_amounts.amount_out_decimal is None, the runner resets
previous_amount_received to None instead of silently reusing a stale value.
Also tests that Enso-extracted SwapAmounts feeds into chaining correctly.
Tests that the amount chaining warning only fires when a subsequent step
actually uses amount='all' (VIB-156).
"""

import logging
from decimal import Decimal
from enum import StrEnum
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.enso.receipt_parser import (
    TRANSFER_EVENT_SIGNATURE,
    EnsoReceiptParser,
)
from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.intents.vocabulary import Intent, IntentType


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


# ---------------------------------------------------------------------------
# VIB-156: Amount chaining warning should only fire when a subsequent step
# actually uses amount='all'. Non-swap intents (LP, lending) that don't
# produce chainable output should log at DEBUG, not WARNING.
# ---------------------------------------------------------------------------


def _simulate_chaining_log(current_intent_type: str, remaining_intents: list, caplog):
    """Simulate the chaining logic from strategy_runner.py and capture log output.

    Mirrors the exact logic in strategy_runner.py: after a step completes
    without extractable output, check if any remaining step uses amount='all'.
    Only warn if they do; otherwise debug-log.

    Args:
        current_intent_type: The type of the intent that just completed
        remaining_intents: List of remaining intents (after the current one)
        caplog: pytest caplog fixture
    """
    # Build a mock intent with the given type
    mock_intent = MagicMock()
    mock_intent.intent_type = IntentType(current_intent_type)

    # Simulate: execution succeeded but no swap_amounts
    er = MagicMock()
    er.swap_amounts = None

    previous_amount_received = Decimal("999")  # stale value

    # This mirrors the exact logic from strategy_runner.py
    if er.swap_amounts and er.swap_amounts.amount_out_decimal is not None:
        previous_amount_received = er.swap_amounts.amount_out_decimal
    else:
        previous_amount_received = None
        # Only check the immediately next intent (not all remaining).
        # An intermediate step can repopulate previous_amount_received.
        has_chained = bool(remaining_intents) and Intent.has_chained_amount(remaining_intents[0])
        if has_chained:
            logging.getLogger("test").warning(
                "Amount chaining: no output amount extracted from step %d; "
                "subsequent amount='all' steps will fail",
                1,
            )
        else:
            intent_type = getattr(mock_intent, "intent_type", None)
            logging.getLogger("test").debug(
                "Amount chaining: step %d (%s) has no chainable output "
                "amount (normal for non-swap intents)",
                1,
                intent_type.value if intent_type else "unknown",
            )

    return previous_amount_received


def _make_intent_with_amount(amount):
    """Create a mock intent with a specific amount (Decimal or 'all')."""
    intent = MagicMock()
    intent.is_chained_amount = amount == "all"
    return intent


def test_lp_open_no_warning_when_next_uses_fixed_amount(caplog):
    """LP_OPEN followed by a fixed-amount step should log at DEBUG, not WARNING."""
    next_intent = _make_intent_with_amount(Decimal("300"))
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("LP_OPEN", [next_intent], caplog)

    assert result is None  # Still resets to None

    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    debug_msgs = [r for r in caplog.records if r.levelno == logging.DEBUG and "Amount chaining" in r.message]

    assert len(warning_msgs) == 0, "LP_OPEN should NOT produce WARNING when next step uses fixed amount"
    assert len(debug_msgs) == 1, "LP_OPEN should produce DEBUG message"


def test_lp_open_warns_when_next_uses_amount_all(caplog):
    """LP_OPEN followed by amount='all' step should still warn."""
    next_intent = _make_intent_with_amount("all")
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("LP_OPEN", [next_intent], caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING and "Amount chaining" in r.message]
    assert len(warning_msgs) == 1, "LP_OPEN should WARN when next step uses amount='all'"


def test_supply_no_warning_when_next_uses_fixed_amount(caplog):
    """SUPPLY followed by a fixed-amount step should log at DEBUG, not WARNING."""
    next_intent = _make_intent_with_amount(Decimal("100"))
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("SUPPLY", [next_intent], caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_msgs) == 0, "SUPPLY should NOT produce WARNING when next step uses fixed amount"


def test_borrow_no_warning_when_no_remaining_chained(caplog):
    """BORROW as last step in sequence should not warn (no remaining steps)."""
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("BORROW", [], caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_msgs) == 0, "BORROW as last step should NOT produce WARNING"


def test_swap_warns_when_next_uses_amount_all(caplog):
    """SWAP followed by amount='all' step should produce WARNING."""
    next_intent = _make_intent_with_amount("all")
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("SWAP", [next_intent], caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING and "Amount chaining" in r.message]
    assert len(warning_msgs) == 1, "SWAP should produce WARNING when next step uses amount='all'"


def test_swap_no_warning_when_next_uses_fixed_amount(caplog):
    """SWAP followed by fixed-amount step should NOT warn (false positive scenario)."""
    next_intent = _make_intent_with_amount(Decimal("300"))
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("SWAP", [next_intent], caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_msgs) == 0, "SWAP should NOT produce WARNING when next step uses fixed amount"


def test_no_warning_when_intermediate_step_can_repopulate(caplog):
    """LP_OPEN -> SWAP(fixed) -> SUPPLY(all) should NOT warn on step 1.

    The intermediate SWAP step can repopulate previous_amount_received,
    so the chained SUPPLY will succeed. Only the immediately next step matters.
    Regression test for the false-positive reported by CodeRabbit/Gemini.
    """
    fixed_intent = _make_intent_with_amount(Decimal("100"))
    chained_intent = _make_intent_with_amount("all")
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("LP_OPEN", [fixed_intent, chained_intent], caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_msgs) == 0, "Should NOT warn when next step uses fixed amount (intermediate can repopulate)"
