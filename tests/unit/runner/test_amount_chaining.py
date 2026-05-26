"""Tests for multi-intent amount chaining in StrategyRunner.

Verifies that when swap_amounts.amount_out_decimal is None, the runner resets
previous_amount_received to None instead of silently reusing a stale value.
Also tests that Enso-extracted SwapAmounts feeds into chaining correctly.
Tests that non-swap intents (LP, lending) don't produce spurious warnings.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors.enso.receipt_parser import (
    TRANSFER_EVENT_SIGNATURE,
    EnsoReceiptParser,
)
from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.intents.vocabulary import LPOpenIntent, SwapIntent
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)


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
# VIB-156: LP/lending intents should NOT produce amount-chaining WARNING
# ---------------------------------------------------------------------------


def _simulate_chaining_log(intent_type_value: str, caplog):
    """Simulate the chaining logic from strategy_runner.py and capture log output.

    Mirrors the exact logic in strategy_runner.py lines 1100-1122.
    """
    from almanak.framework.intents.vocabulary import IntentType

    # Build a mock intent with the given type
    mock_intent = MagicMock()
    mock_intent.intent_type = IntentType(intent_type_value)

    # Simulate: execution succeeded but no swap_amounts
    er = MagicMock()
    er.swap_amounts = None

    previous_amount_received = Decimal("999")  # stale value

    # This mirrors the exact logic from strategy_runner.py
    if er.swap_amounts and er.swap_amounts.amount_out_decimal is not None:
        previous_amount_received = er.swap_amounts.amount_out_decimal
    else:
        previous_amount_received = None
        intent_type_val = getattr(mock_intent, "intent_type", None)
        is_swap = intent_type_val == IntentType.SWAP
        if is_swap:
            logging.getLogger("test").warning(
                "Amount chaining: no output amount extracted from step %d; "
                "subsequent amount='all' steps will fail",
                1,
            )
        else:
            logging.getLogger("test").debug(
                "Amount chaining: step %d (%s) has no chainable output amount (normal for non-swap intents)",
                1,
                intent_type_val.value if intent_type_val else "unknown",
            )

    return previous_amount_received


def test_lp_open_no_warning(caplog):
    """LP_OPEN should log at DEBUG, not WARNING, when no output amount."""
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("LP_OPEN", caplog)

    assert result is None  # Still resets to None

    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    debug_msgs = [r for r in caplog.records if r.levelno == logging.DEBUG and "Amount chaining" in r.message]

    assert len(warning_msgs) == 0, "LP_OPEN should NOT produce WARNING"
    assert len(debug_msgs) == 1, "LP_OPEN should produce DEBUG message"


def test_lp_close_no_warning(caplog):
    """LP_CLOSE should log at DEBUG, not WARNING."""
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("LP_CLOSE", caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_msgs) == 0, "LP_CLOSE should NOT produce WARNING"


def test_supply_no_warning(caplog):
    """SUPPLY should log at DEBUG, not WARNING."""
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("SUPPLY", caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_msgs) == 0, "SUPPLY should NOT produce WARNING"


def test_borrow_no_warning(caplog):
    """BORROW should log at DEBUG, not WARNING."""
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("BORROW", caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_msgs) == 0, "BORROW should NOT produce WARNING"


def test_swap_still_warns(caplog):
    """SWAP should still produce WARNING when no output amount."""
    with caplog.at_level(logging.DEBUG, logger="test"):
        result = _simulate_chaining_log("SWAP", caplog)

    assert result is None
    warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING and "Amount chaining" in r.message]
    assert len(warning_msgs) == 1, "SWAP should still produce WARNING"


# ---------------------------------------------------------------------------
# VIB-2036: warning gate must consult subsequent intents, not just position
# ---------------------------------------------------------------------------
#
# The runner used to log a WARNING whenever a non-last intent in a multi-intent
# sequence produced no swap_amounts, regardless of whether any subsequent step
# actually used ``amount='all'``. The hosted ``UsdcWbtcDynamicLpStrategy`` -- a
# 2-step ``[LP_OPEN, LP_OPEN]`` sequence with explicit amounts on both legs --
# tripped this every iteration, polluting operator logs. The fix gates the
# warning on ``Intent.has_chained_amount(...)`` against ``intents[idx+1:]``.


def _build_runner_for_chain_test():
    """Build a real ``StrategyRunner`` with mocked collaborators.

    Mirrors ``test_run_iteration_steps_extended._make_runner`` but inline so
    this test file stays self-contained.
    """
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
        circuit_breaker=None,
    )


def _build_state_for_chain_test(intents, deployment_id="vib-2036-test"):
    """Build a ``RunIterationState`` populated with ``intents``."""
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x" + "ab" * 20

    state = RunIterationState(
        strategy=strategy,
        deployment_id=deployment_id,
        start_time=datetime.now(UTC),
    )
    state.intents = list(intents)
    state.market = MagicMock()
    return state


def _success_result_without_swap_amounts(intent, deployment_id):
    """An ``IterationResult`` whose ``execution_result`` has no swap_amounts."""
    return IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        deployment_id=deployment_id,
        duration_ms=1,
        execution_result=SimpleNamespace(swap_amounts=None),
    )


@pytest.mark.asyncio
async def test_vib_2036_no_warning_when_no_subsequent_chained_amount(caplog):
    """[LP_OPEN, LP_OPEN] both with explicit amounts -> no chaining warning.

    Reproduces the hosted ``UsdcWbtcDynamicLpStrategy`` log pollution from
    VIB-2036: neither step uses ``amount='all'`` and the warning must stay
    silent.
    """
    intent1 = LPOpenIntent(
        pool="USDC/WBTC",
        amount0=Decimal("100"),
        amount1=Decimal("0.001"),
        range_lower=Decimal("1000"),
        range_upper=Decimal("2000"),
        protocol="uniswap_v3",
    )
    intent2 = LPOpenIntent(
        pool="USDC/WBTC",
        amount0=Decimal("50"),
        amount1=Decimal("0.0005"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v3",
    )
    runner = _build_runner_for_chain_test()
    state = _build_state_for_chain_test([intent1, intent2])

    results = [
        _success_result_without_swap_amounts(intent1, state.deployment_id),
        _success_result_without_swap_amounts(intent2, state.deployment_id),
    ]
    with (
        patch.object(runner, "_execute_single_chain", new=AsyncMock(side_effect=results)),
        caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"),
    ):
        await runner._run_single_chain_intents(state)

    chaining_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "Amount chaining" in r.getMessage()
    ]
    assert chaining_warnings == [], (
        f"VIB-2036: expected zero chaining warnings for [LP_OPEN, LP_OPEN], "
        f"got {[r.getMessage() for r in chaining_warnings]}"
    )


@pytest.mark.asyncio
async def test_vib_2036_warning_still_fires_when_next_intent_uses_all(caplog):
    """[SWAP(missing swap_amounts), SWAP(amount='all')] -> warning fires.

    Inverse of the false-positive case: when downstream really does need a
    chained amount and we have nothing to give it, the warning must still
    surface so operators see the broken sequence.
    """
    intent1 = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"))
    intent2 = SwapIntent(from_token="WETH", to_token="DAI", amount="all")
    runner = _build_runner_for_chain_test()
    state = _build_state_for_chain_test([intent1, intent2])

    # Only one step actually reaches the orchestrator: step 1 succeeds without
    # swap_amounts (chain broken), then ``_resolve_chained_amount_for_intent``
    # short-circuits step 2 with COMPILATION_FAILED before it can dispatch.
    mock_execute = AsyncMock(
        side_effect=[_success_result_without_swap_amounts(intent1, state.deployment_id)]
    )
    with (
        patch.object(runner, "_execute_single_chain", new=mock_execute),
        caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"),
    ):
        await runner._run_single_chain_intents(state)

    assert mock_execute.await_count == 1
    chaining_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "Amount chaining" in r.getMessage()
    ]
    assert len(chaining_warnings) == 1, (
        f"VIB-2036: expected exactly one chaining warning when a downstream "
        f"amount='all' is unsatisfied, got {len(chaining_warnings)}: "
        f"{[r.getMessage() for r in chaining_warnings]}"
    )
