"""Tests for the pre-retry confirmation path in StrategyRunner.

When a previous execution attempt times out but the tx was actually
submitted and later confirmed on-chain, the retry loop detects this
via a pre-retry receipt check. This test verifies that the returned
IterationResult carries a *fresh* successful ExecutionResult rather
than the stale timeout-failure from the first attempt.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.orchestrator import (
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    StrategyRunner,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TX_HASH = "0xabc123"


def _make_runner(**overrides) -> StrategyRunner:
    defaults = dict(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )
    defaults.update(overrides)
    return StrategyRunner(**defaults)


def _make_swap_intent():
    """Return a mock SwapIntent-like object with attributes needed by the runner."""
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value="SWAP")
    intent.chain = "arbitrum"
    intent.intent_id = "test-intent-001"
    intent.from_token = "USDC"
    intent.to_token = "ETH"
    intent.amount = 100
    intent.amount_usd = None  # avoid Decimal conversion issues
    intent.max_slippage = None
    intent.protocol = None
    return intent


def _make_strategy(intent):
    """Build a minimal mock strategy that returns *intent* from decide()."""
    strategy = MagicMock()
    strategy.strategy_id = "test_strat"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xWALLET"
    strategy.decide.return_value = intent
    strategy.create_market_snapshot.return_value = MagicMock(
        get_price_oracle_dict=MagicMock(return_value={"ETH": 3000}),
    )
    return strategy


def _timeout_execution_result() -> ExecutionResult:
    """An ExecutionResult that simulates a timeout with a submitted tx."""
    return ExecutionResult(
        success=False,
        phase=ExecutionPhase.CONFIRMATION,
        transaction_results=[
            TransactionResult(tx_hash=TX_HASH, success=False),
        ],
        error="Transaction confirmation timeout after 60s",
    )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_retry_confirmed_updates_execution_result() -> None:
    """When pre-retry receipt check confirms a timed-out tx, the returned
    IterationResult.execution_result must reflect success -- not the stale
    timeout failure from the original attempt.
    """
    swap_intent = _make_swap_intent()
    strategy = _make_strategy(swap_intent)

    # Build an orchestrator mock whose execute() returns a timeout failure
    # on the first call.
    orch = MagicMock()
    orch.execute = AsyncMock(return_value=_timeout_execution_result())
    # The pre-retry check uses orchestrator.submitter.get_receipt()
    # Must include all fields accessed when building TransactionResult
    confirmed_receipt = SimpleNamespace(
        success=True,
        tx_hash=TX_HASH,
        gas_used=21000,
        gas_cost_wei=21000 * 100,
        logs=[],
    )
    orch.submitter = MagicMock()
    orch.submitter.get_receipt = AsyncMock(return_value=confirmed_receipt)
    # No tx_risk_config
    orch.tx_risk_config = None

    runner = _make_runner(execution_orchestrator=orch)
    # Allow 1 retry so the pre-retry check path is reached
    runner.config.max_retries = 1

    # We need to control the IntentStateMachine and IntentCompiler since
    # they are created inside _execute_single_chain.  Use a stateful mock
    # that simulates: step1(PREPARING)->needs_execution,
    # step2(SADFLOW)->retry_delay, step3(PREPARING)->needs_execution again,
    # then after pre-retry sets success receipt -> step4 complete.
    step_idx = 0
    mock_bundle = MagicMock()
    mock_bundle.transactions = [MagicMock()]

    class FakeStateMachine:
        def __init__(self, **kwargs):
            self._steps_done = 0
            self._retry = 0
            self._complete = False
            self._success = False
            self._error = None

        @property
        def is_complete(self):
            return self._complete

        @property
        def success(self):
            return self._success

        @property
        def error(self):
            return self._error

        @property
        def retry_count(self):
            return self._retry

        def step(self):
            self._steps_done += 1
            if self._steps_done == 1:
                # First: needs execution (initial attempt)
                return SimpleNamespace(
                    needs_execution=True,
                    action_bundle=mock_bundle,
                    retry_delay=None,
                    error=None,
                    is_complete=False,
                )
            elif self._steps_done == 2:
                # Sadflow: retry delay
                return SimpleNamespace(
                    needs_execution=False,
                    action_bundle=None,
                    retry_delay=0.001,  # negligible
                    error=None,
                    is_complete=False,
                )
            elif self._steps_done == 3:
                # Second attempt (retry_count > 0 triggers pre-retry check)
                self._retry = 1
                return SimpleNamespace(
                    needs_execution=True,
                    action_bundle=mock_bundle,
                    retry_delay=None,
                    error=None,
                    is_complete=False,
                )
            else:
                # After pre-retry confirms tx, state machine completes
                self._complete = True
                self._success = True
                return SimpleNamespace(
                    needs_execution=False,
                    action_bundle=None,
                    retry_delay=None,
                    error=None,
                    is_complete=True,
                )

        def set_receipt(self, receipt):
            # On the success receipt from pre-retry, mark complete
            if receipt.success:
                self._complete = True
                self._success = True

    # Patch IntentStateMachine, IntentCompiler, and
    # _is_strategy_paused / _check_teardown_requested to skip unrelated logic
    with (
        patch(
            "almanak.framework.runner.strategy_runner.IntentStateMachine",
            side_effect=lambda **kwargs: FakeStateMachine(**kwargs),
        ),
        patch(
            "almanak.framework.runner.strategy_runner.IntentCompiler",
            return_value=MagicMock(),
        ),
        patch.object(runner, "_is_strategy_paused", return_value=(False, None)),
        patch.object(runner, "_check_teardown_requested", return_value=None),
        patch.object(runner, "_emit_execution_timeline_event"),
    ):
        result: IterationResult = await runner.run_iteration(strategy)

    # Core assertion: the result should be SUCCESS with an updated
    # execution result that reflects the confirmed tx, NOT the stale
    # timeout failure.
    assert result.status == IterationStatus.SUCCESS
    assert result.execution_result is not None
    assert result.execution_result.success is True
    assert result.execution_result.phase == ExecutionPhase.COMPLETE
    assert len(result.execution_result.transaction_results) == 1
    assert result.execution_result.transaction_results[0].tx_hash == TX_HASH
    assert result.execution_result.transaction_results[0].success is True
    # Must NOT be the stale timeout error
    assert result.execution_result.error is None
