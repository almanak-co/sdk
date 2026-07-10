"""Driver-level smoke tests for ``StrategyRunner.run_iteration`` and
``StrategyRunner._execute_single_chain``.

Each test constructs a real ``StrategyRunner`` and drives one of the
public / package-level driver methods end-to-end. Mocks sit at the
edges (gateway, orchestrator, strategy callbacks, balance provider),
not inside the driver. The goal is to catch integration bugs where a
step helper's contract silently drifts from what the driver expects.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.chain_executor import TransactionExecutionResult
from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from almanak.framework.runner.runner_models import ExecutionProgress
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)

# =============================================================================
# Helpers
# =============================================================================


def _make_runner(*, dry_run: bool = False, max_retries: int = 2) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
        max_retries=max_retries,
    )
    balance_provider = MagicMock()
    balance_provider.invalidate_cache = MagicMock()

    async def _bp_get(token: str) -> SimpleNamespace:
        return SimpleNamespace(balance=Decimal("100"))

    balance_provider.get_balance = _bp_get
    execution_orchestrator = MagicMock()
    execution_orchestrator.tx_risk_config = None

    state_manager = MagicMock()
    state_manager.load_state = AsyncMock(return_value=None)
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=execution_orchestrator,
        state_manager=state_manager,
        config=config,
    )
    # VIB-5670 Stage 3: bridge-wait success paths run the real per-leg
    # accounting pipeline. Pin non-live (test_vib5670_stage1.py convention) so
    # MagicMock persistence backends degrade to logged errors instead of the
    # live-mode fail-closed AccountingPersistenceError.
    runner._is_live_mode = MagicMock(return_value=False)
    return runner


def _make_strategy(*, intent=None) -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "driver-test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890AbcdEF1234567890aBcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.create_market_snapshot.return_value.has_critical_data_failures.return_value = False
    strategy.decide.return_value = intent if intent is not None else HoldIntent(reason="default hold")
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    # Remove wallet activity provider so periodic hooks are no-op
    del strategy._wallet_activity_provider
    return strategy


# =============================================================================
# run_iteration: pause-flow happy path
# =============================================================================


class TestRunIterationPauseFlow:
    @pytest.mark.asyncio
    async def test_paused_strategy_returns_hold_without_decide(self) -> None:
        runner = _make_runner()
        runner.state_manager.load_state = AsyncMock(
            return_value=SimpleNamespace(state={"is_paused": True, "pause_reason": "scheduled maintenance"})
        )
        strategy = _make_strategy()

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert isinstance(result.intent, HoldIntent)
        assert "scheduled maintenance" in result.intent.reason
        # decide() must not have been called
        strategy.decide.assert_not_called()


# =============================================================================
# run_iteration: teardown-flow happy path
# =============================================================================


class TestRunIterationTeardownFlow:
    @pytest.mark.asyncio
    async def test_teardown_requested_routes_to_execute_teardown(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        teardown_result = MagicMock()

        with (
            patch.object(
                runner,
                "_is_strategy_paused",
                new=AsyncMock(return_value=(False, None)),
            ),
            patch.object(runner, "_check_teardown_requested", return_value="SOFT"),
            patch.object(runner, "_execute_teardown", new=AsyncMock(return_value=teardown_result)) as mock_teardown,
        ):
            result = await runner.run_iteration(strategy)

        assert result is teardown_result
        mock_teardown.assert_awaited_once()
        # decide() was skipped because teardown intercepted
        strategy.decide.assert_not_called()


# =============================================================================
# run_iteration: HOLD intent short-circuits
# =============================================================================


class TestRunIterationHoldIntent:
    @pytest.mark.asyncio
    async def test_decide_returns_hold_short_circuits_to_hold(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy(intent=HoldIntent(reason="market quiet"))

        with (
            patch.object(runner, "_is_strategy_paused", new=AsyncMock(return_value=(False, None))),
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_pre_warm_prices", new=AsyncMock()),
        ):
            result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert result.intent is not None
        assert result.intent.reason == "market quiet"


# =============================================================================
# run_iteration: strategy decide exception -> STRATEGY_ERROR
# =============================================================================


class TestRunIterationStrategyError:
    @pytest.mark.asyncio
    async def test_decide_raises_returns_strategy_error(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.decide.side_effect = RuntimeError("strategy implementation bug")

        with (
            patch.object(runner, "_is_strategy_paused", new=AsyncMock(return_value=(False, None))),
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_pre_warm_prices", new=AsyncMock()),
        ):
            result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "strategy implementation bug" in (result.error or "")


# =============================================================================
# run_iteration: single-chain SUCCESS flow
# =============================================================================


class TestRunIterationSingleChainSuccess:
    @pytest.mark.asyncio
    async def test_single_chain_swap_returns_success(self) -> None:
        runner = _make_runner()
        swap = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        strategy = _make_strategy(intent=swap)

        # Mock out _execute_single_chain at the edge of the driver — we're
        # testing the run_iteration orchestration, not the inner execution state
        # machine (which already has its own unit tests).
        success_result = MagicMock()
        success_result.status = IterationStatus.SUCCESS
        success_result.intent = swap
        success_result.success = True

        with (
            patch.object(runner, "_is_strategy_paused", new=AsyncMock(return_value=(False, None))),
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_pre_warm_prices", new=AsyncMock()),
            patch.object(runner, "_execute_single_chain", new=AsyncMock(return_value=success_result)),
        ):
            result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.SUCCESS


# =============================================================================
# run_iteration: single-chain FAILURE flow
# =============================================================================


class TestRunIterationSingleChainFailure:
    @pytest.mark.asyncio
    async def test_single_chain_failure_propagates_as_execution_failed(self) -> None:
        runner = _make_runner()
        swap = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        strategy = _make_strategy(intent=swap)

        failed_result = MagicMock()
        failed_result.status = IterationStatus.EXECUTION_FAILED
        failed_result.intent = swap
        failed_result.success = False
        failed_result.error = "reverted"

        with (
            patch.object(runner, "_is_strategy_paused", new=AsyncMock(return_value=(False, None))),
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_pre_warm_prices", new=AsyncMock()),
            patch.object(runner, "_execute_single_chain", new=AsyncMock(return_value=failed_result)),
        ):
            result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.EXECUTION_FAILED
        assert result.error == "reverted"


# =============================================================================
# _execute_with_bridge_waiting: cross-chain success path
# =============================================================================


class TestBridgeWaitingDriverSuccess:
    @pytest.mark.asyncio
    async def test_cross_chain_success_returns_success(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()

        orch = MagicMock()
        orch.wallet_address = strategy.wallet_address
        orch.primary_chain = "arbitrum"
        orch._config = SimpleNamespace(rpc_urls={"arbitrum": "https://arb"})

        _tx = TransactionExecutionResult(success=True, tx_hash="0xabc")
        _tx.actual_amount_received = Decimal("50")
        result_obj = SimpleNamespace(success=True, error=None, tx_result=_tx)
        orch.execute = AsyncMock(return_value=result_obj)

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("100"),
            chain="arbitrum",
            destination_chain="base",
        )

        # Gateway-only boundary: cross-chain bridge source-TX verification
        # requires a gateway client (direct Web3 fallback is forbidden -- see
        # docs/internal/blueprints/20-gateway-security-architecture.md). Provide a stub
        # gateway client; ``_bridge_wait_cross_chain`` is itself mocked, so
        # the gateway is never actually called.
        gateway_client = MagicMock()

        # Mock the cross-chain path so the bridge poll returns "completed"
        with (
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=MagicMock(),
            ),
            patch(
                "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
                return_value=True,
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_chain",
                return_value="base",
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_token",
                return_value="USDC",
            ),
            patch.object(runner, "_compute_intents_hash", return_value="h"),
            patch.object(runner, "_load_execution_progress", new=AsyncMock(return_value=None)),
            patch.object(runner, "_save_execution_progress", new=AsyncMock()),
            patch.object(runner, "_clear_execution_progress", new=AsyncMock()),
            patch.object(runner, "_get_gateway_client", return_value=gateway_client),
            patch.object(
                runner,
                "_bridge_wait_cross_chain",
                new=AsyncMock(return_value=False),  # don't break; treat as completed
            ),
        ):
            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orch,
                start_time=datetime.now(UTC),
            )

        assert result.status == IterationStatus.SUCCESS


# =============================================================================
# _execute_with_bridge_waiting: timeout path
# =============================================================================


class TestBridgeWaitingDriverTimeout:
    @pytest.mark.asyncio
    async def test_cross_chain_bridge_timeout_returns_execution_failed(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()

        orch = MagicMock()
        orch.wallet_address = strategy.wallet_address
        orch.primary_chain = "arbitrum"
        orch._config = SimpleNamespace(rpc_urls={"arbitrum": "https://arb"})
        orch.execute = AsyncMock(
            return_value=SimpleNamespace(
                success=True,
                error=None,
                tx_result=SimpleNamespace(tx_hash="0xabc"),
            )
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDC",
            amount=Decimal("100"),
            chain="arbitrum",
            destination_chain="base",
        )

        # Gateway-only boundary: cross-chain bridge processing requires a
        # gateway client (fail-fast on None -- see
        # docs/internal/blueprints/20-gateway-security-architecture.md). Provide a stub;
        # ``_bridge_wait_cross_chain`` is mocked so the client is unused.
        gateway_client = MagicMock()

        with (
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=MagicMock(),
            ),
            patch(
                "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
                return_value=True,
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_chain",
                return_value="base",
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_token",
                return_value="USDC",
            ),
            patch.object(runner, "_compute_intents_hash", return_value="h"),
            patch.object(runner, "_load_execution_progress", new=AsyncMock(return_value=None)),
            patch.object(runner, "_save_execution_progress", new=AsyncMock()),
            patch.object(runner, "_clear_execution_progress", new=AsyncMock()),
            patch.object(runner, "_get_gateway_client", return_value=gateway_client),
            # Simulate bridge timeout: the cross-chain step sets failed_step/error and returns True
            patch.object(
                runner,
                "_bridge_wait_cross_chain",
                new=AsyncMock(side_effect=_simulate_bridge_timeout),
            ),
        ):
            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orch,
                start_time=datetime.now(UTC),
            )

        assert result.status == IterationStatus.EXECUTION_FAILED
        assert "step-1-bridge" in (result.error or "")


async def _simulate_bridge_timeout(state, **kwargs):
    state.failed_step = "step-1-bridge"
    state.error_message = "Bridge transfer timed out after 5 minutes"
    state.callback_fired = True
    return True


# =============================================================================
# _execute_with_bridge_waiting: resume from stuck state
# =============================================================================


class TestBridgeWaitingResume:
    @pytest.mark.asyncio
    async def test_resume_from_stuck_progress_skips_completed_steps(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        orch = MagicMock()
        orch.wallet_address = strategy.wallet_address
        orch.primary_chain = "arbitrum"
        orch._config = SimpleNamespace(rpc_urls={"arbitrum": "https://arb"})

        executed_intents = []

        async def _exec(intent, **kwargs):
            executed_intents.append(intent)
            return SimpleNamespace(
                success=True,
                error=None,
                tx_result=TransactionExecutionResult(success=True, tx_hash="0xok"),
            )

        orch.execute = AsyncMock(side_effect=_exec)

        intent_a = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="arbitrum")
        intent_b = SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1"), chain="arbitrum")

        # Resume progress says step 0 was completed
        resume = ExecutionProgress(
            execution_id="eid",
            deployment_id=strategy.deployment_id,
            intents_hash="h",
            total_steps=2,
        )
        resume.completed_step_index = 0
        resume.previous_amount_received = Decimal("100")

        with (
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=MagicMock(),
            ),
            patch(
                "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
                return_value=False,
            ),
            patch.object(runner, "_compute_intents_hash", return_value="h"),
            patch.object(runner, "_save_execution_progress", new=AsyncMock()),
            patch.object(runner, "_clear_execution_progress", new=AsyncMock()),
            patch.object(runner, "_get_gateway_client", return_value=None),
        ):
            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent_a, intent_b],
                orchestrator=orch,
                start_time=datetime.now(UTC),
                resume_progress=resume,
            )

        assert result.status == IterationStatus.SUCCESS
        # Only step 1 (intent_b) was executed, step 0 was skipped
        assert len(executed_intents) == 1
