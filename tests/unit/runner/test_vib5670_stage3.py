"""VIB-5670 Stage 3 — bridge-wait lane accounting: persist-before-progress +
source-REQUEST-on-bridge-fail.

Covers the Stage-3 wiring of ``_persist_executed_leg`` into the bridge-wait
lane:

- a successful leg persists through the shared pipeline BEFORE
  ``completed_step_index`` advances, with per-leg chain/wallet and recon
  enabled only for same-chain legs (cross-chain BRIDGE legs keep degraded
  recon — async destination settlement, design v3 #3);
- a persistence failure after a CONFIRMED broadcast stamps
  ``ExecutionProgress.accounting_pending_step_index`` (never re-broadcast),
  persists it, and re-raises (design v3 #4 / v4 #1);
- a source-succeeded / bridge-failed step records the money that moved as a
  REQUEST-phase TRANSFER with ``settlement_status="degraded"`` — ledger +
  outbox only; a persistence failure there goes to the deferred-write log and
  never blocks the failure path (design v3 #3 / v4 #2);
- ``_bridge_wait_finalize`` attaches a summary-only aggregate
  ``ExecutionResult`` built from ``BridgeWaitState.leg_tx_results``;
- resuming with an accounting-pending step surfaces loudly (operator alert +
  deferred-write log) and does not re-broadcast.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.chain_executor import TransactionExecutionResult
from almanak.framework.execution.orchestrator import (
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.runner_models import ExecutionProgress, IterationStatus
from almanak.framework.runner.strategy_runner import (
    BridgeWaitState,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.state.exceptions import AccountingPersistenceError

# =============================================================================
# Helpers
# =============================================================================


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=MagicMock(),
        config=config,
    )
    runner._is_live_mode = MagicMock(return_value=False)  # type: ignore[method-assign]
    return runner


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "dep-bridge"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xstrategywallet"
    return strategy


def _make_state(intents: list, strategy: MagicMock | None = None) -> BridgeWaitState:
    strategy = strategy or _make_strategy()
    orch = MagicMock()
    orch.wallet_address = "0xstrategywallet"
    orch.primary_chain = "arbitrum"
    orch.execute = AsyncMock()
    return BridgeWaitState(
        strategy=strategy,
        intents=intents,
        orchestrator=orch,
        start_time=datetime.now(UTC),
        deployment_id=strategy.deployment_id,
        first_intent=intents[0] if intents else None,
    )


def _make_progress(*, total_steps: int = 1) -> ExecutionProgress:
    return ExecutionProgress(
        execution_id="e3",
        deployment_id="dep-bridge",
        intents_hash="h",
        total_steps=total_steps,
    )


def _success_leg(tx_hash: str = "0xabc") -> MagicMock:
    result = MagicMock()
    result.success = True
    result.error = None
    result.tx_result = TransactionExecutionResult(success=True, tx_hash=tx_hash, gas_used=7, gas_cost_wei=700)
    return result


# =============================================================================
# Persist-before-progress (success leg)
# =============================================================================


class TestBridgeLegPersistBeforeProgress:
    async def _run_step(self, runner, state, *, cross_chain: bool = False):
        with patch(
            "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
            return_value=cross_chain,
        ):
            return await runner._bridge_wait_process_intent(state, 0)

    @pytest.mark.asyncio
    async def test_same_chain_leg_persists_with_leg_chain_and_recon(self):
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
        runner._persist_executed_leg = AsyncMock(return_value="")  # type: ignore[method-assign]
        leg_provider = MagicMock(name="leg_provider")
        runner._balance_provider_for_chain = MagicMock(return_value=leg_provider)  # type: ignore[method-assign]
        runner._snapshot_balances_for_intent = AsyncMock(return_value={"USDC": Decimal("1")})  # type: ignore[method-assign]
        runner._multichain_wallet_for = MagicMock(return_value="0xlegwallet")  # type: ignore[method-assign]

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="arbitrum")
        state = _make_state([intent])
        state.progress = _make_progress()
        state.orchestrator.execute.return_value = _success_leg()

        should_break = await self._run_step(runner, state)

        assert should_break is False
        persist_kwargs = runner._persist_executed_leg.await_args.kwargs
        assert persist_kwargs["chain"] == "arbitrum"
        assert persist_kwargs["wallet_address"] == "0xlegwallet"
        assert persist_kwargs["run_recon"] is True
        assert persist_kwargs["record_metrics"] is False
        assert persist_kwargs["pre_snapshot"] == {"USDC": Decimal("1")}
        # Pre-snapshot read on the leg's own chain-scoped provider.
        snap_kwargs = runner._snapshot_balances_for_intent.await_args.kwargs
        assert snap_kwargs["balance_provider"] is leg_provider
        # Progress advanced only after persistence succeeded.
        assert state.progress.completed_step_index == 0
        assert state.progress.accounting_pending_step_index is None
        # Leg tx results collected for the finalize aggregate.
        assert len(state.leg_tx_results) == 1

    @pytest.mark.asyncio
    async def test_cross_chain_leg_skips_recon_and_pre_snapshot(self):
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
        runner._persist_executed_leg = AsyncMock(return_value="")  # type: ignore[method-assign]
        runner._balance_provider_for_chain = MagicMock()  # type: ignore[method-assign]
        runner._snapshot_balances_for_intent = AsyncMock()  # type: ignore[method-assign]
        runner._bridge_wait_cross_chain = AsyncMock(return_value=False)  # type: ignore[method-assign]

        intent = SwapIntent(
            from_token="USDC", to_token="USDC", amount=Decimal("1"), chain="arbitrum", destination_chain="base"
        )
        state = _make_state([intent])
        state.progress = _make_progress()
        state.gateway_client = MagicMock()
        state.orchestrator.execute.return_value = _success_leg()

        with (
            patch("almanak.framework.runner.strategy_runner.get_intent_destination_chain", return_value="base"),
            patch("almanak.framework.runner.strategy_runner.get_intent_destination_token", return_value="USDC"),
        ):
            should_break = await self._run_step(runner, state, cross_chain=True)

        assert should_break is False
        runner._snapshot_balances_for_intent.assert_not_awaited()
        persist_kwargs = runner._persist_executed_leg.await_args.kwargs
        assert persist_kwargs["run_recon"] is False
        assert persist_kwargs["pre_snapshot"] is None

    @pytest.mark.asyncio
    async def test_accounting_error_stamps_pending_marker_and_reraises(self):
        """Design v3 #4: never re-broadcast a confirmed step after an accounting halt."""
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
        runner._persist_executed_leg = AsyncMock(  # type: ignore[method-assign]
            side_effect=AccountingPersistenceError("ledger", deployment_id="dep-bridge")
        )
        runner._balance_provider_for_chain = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._multichain_wallet_for = MagicMock(return_value="0xlegwallet")  # type: ignore[method-assign]

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="arbitrum")
        state = _make_state([intent])
        state.progress = _make_progress()
        state.orchestrator.execute.return_value = _success_leg()

        with pytest.raises(AccountingPersistenceError):
            await self._run_step(runner, state)

        # The distinct accounting-pending marker was stamped and persisted;
        # completed_step_index did NOT advance.
        assert state.progress.accounting_pending_step_index == 0
        assert state.progress.completed_step_index == -1
        runner._save_execution_progress.assert_awaited()
        # Restart semantics: next step skips past the pending index — the
        # confirmed broadcast is never re-executed.
        assert state.progress.next_step_to_execute == 1
        assert state.leg_tx_results == []


# =============================================================================
# Degraded source-REQUEST persistence (bridge-fail)
# =============================================================================


class TestDegradedBridgeSourcePersistence:
    @pytest.mark.asyncio
    async def test_persists_degraded_transfer_and_marks_callback(self):
        runner = _make_runner()
        runner._persist_executed_leg = AsyncMock(return_value="")  # type: ignore[method-assign]
        runner._multichain_wallet_for = MagicMock(return_value="0xlegwallet")  # type: ignore[method-assign]

        intent = SwapIntent(
            from_token="USDC", to_token="USDC", amount=Decimal("1"), chain="arbitrum", destination_chain="base"
        )
        state = _make_state([intent])
        state.failed_step = "step-1-bridge"
        result = _success_leg()

        await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", result)

        persist_kwargs = runner._persist_executed_leg.await_args.kwargs
        assert persist_kwargs["settlement_status"] == "degraded"
        assert persist_kwargs["run_recon"] is False
        assert persist_kwargs["run_slippage_guard"] is False
        assert persist_kwargs["chain"] == "arbitrum"
        assert state.callback_fired is True

    @pytest.mark.asyncio
    async def test_skipped_when_failure_is_not_bridge_suffixed(self):
        """Source reverted / pre-broadcast failures move no money — nothing to record."""
        runner = _make_runner()
        runner._persist_executed_leg = AsyncMock(return_value="")  # type: ignore[method-assign]

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state([intent])
        state.failed_step = "step-1"  # no -bridge suffix: source tx did not confirm-and-strand

        await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", _success_leg())

        runner._persist_executed_leg.assert_not_awaited()
        assert state.callback_fired is False

    @pytest.mark.asyncio
    async def test_persistence_failure_goes_to_deferred_log_and_never_raises(self):
        runner = _make_runner()
        runner._persist_executed_leg = AsyncMock(  # type: ignore[method-assign]
            side_effect=AccountingPersistenceError("ledger", deployment_id="dep-bridge")
        )
        runner._multichain_wallet_for = MagicMock(return_value="0xlegwallet")  # type: ignore[method-assign]

        intent = SwapIntent(
            from_token="USDC", to_token="USDC", amount=Decimal("1"), chain="arbitrum", destination_chain="base"
        )
        state = _make_state([intent])
        state.failed_step = "step-1-bridge"

        with patch("almanak.framework.accounting.deferred_log.append_now", return_value=True) as deferred:
            # Must NOT raise — loud-but-never-block (blueprint 27 §14.1).
            await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", _success_leg())

        deferred.assert_called_once()
        assert deferred.call_args.kwargs["kind"] == "bridge_source_request"
        assert deferred.call_args.kwargs["tx_hash"] == "0xabc"
        # Callback NOT marked fired — finalize's fallback notify covers it.
        assert state.callback_fired is False


# =============================================================================
# Finalize aggregate
# =============================================================================


class TestBridgeFinalizeAggregate:
    @pytest.mark.asyncio
    async def test_success_attaches_summary_aggregate(self):
        runner = _make_runner()
        runner._clear_execution_progress = AsyncMock()  # type: ignore[method-assign]
        runner._record_success = MagicMock()  # type: ignore[method-assign]

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state([intent])
        state.leg_tx_results = [
            TransactionResult(tx_hash="0x1", success=True, gas_used=10, gas_cost_wei=100),
            TransactionResult(tx_hash="0x2", success=True, gas_used=20, gas_cost_wei=200),
        ]

        result = await runner._bridge_wait_finalize(state)

        assert result.status == IterationStatus.SUCCESS
        aggregate = result.execution_result
        assert isinstance(aggregate, ExecutionResult)
        assert aggregate.phase == ExecutionPhase.COMPLETE
        assert len(aggregate.transaction_results) == 2
        assert aggregate.total_gas_used == 30
        assert aggregate.total_gas_cost_wei == 300
        # Summary only — no enriched financial fields on the aggregate.
        assert aggregate.position_id is None
        runner._record_success.assert_called_once_with(execution_proved=True)

    @pytest.mark.asyncio
    async def test_success_with_no_executed_legs_keeps_none(self):
        """All-steps-skipped resume: nothing executed here, aggregate stays None."""
        runner = _make_runner()
        runner._clear_execution_progress = AsyncMock()  # type: ignore[method-assign]
        runner._record_success = MagicMock()  # type: ignore[method-assign]

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state([intent])

        result = await runner._bridge_wait_finalize(state)

        assert result.status == IterationStatus.SUCCESS
        assert result.execution_result is None


# =============================================================================
# Accounting-pending resume surfacing
# =============================================================================


class TestAccountingPendingResume:
    @pytest.mark.asyncio
    async def test_resume_surfaces_loudly_and_skips_pending_step(self):
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="h")  # type: ignore[method-assign]
        runner._get_gateway_client = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._alert_accounting_failure = AsyncMock()  # type: ignore[method-assign]

        saved = _make_progress(total_steps=2)
        saved.intents_hash = "h"
        saved.accounting_pending_step_index = 0

        runner._load_execution_progress = AsyncMock(return_value=saved)  # type: ignore[method-assign]
        runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]

        intents = [
            SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1")),
            SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1")),
        ]
        state = _make_state(intents)

        with (
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=MagicMock(),
            ),
            patch("almanak.framework.accounting.deferred_log.append_now", return_value=True) as deferred,
        ):
            await runner._init_bridge_wait_state(state)

        # The pending step's broadcast is confirmed — never re-broadcast:
        # resume starts AFTER it.
        assert state.start_step_index == 1
        # Loud surfacing: operator alert + deferred-write log.
        runner._alert_accounting_failure.assert_awaited_once()
        deferred.assert_called_once()
        assert deferred.call_args.kwargs["kind"] == "accounting_pending_replay"

    @pytest.mark.asyncio
    async def test_driver_resume_never_rebroadcasts_pending_step(self):
        """Audit fix (CodeRabbit): drive the FULL bridge-wait driver on resume —
        the accounting-pending step must never reach orchestrator.execute."""
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="h")  # type: ignore[method-assign]
        runner._get_gateway_client = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._alert_accounting_failure = AsyncMock()  # type: ignore[method-assign]
        runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
        runner._clear_execution_progress = AsyncMock()  # type: ignore[method-assign]
        runner._persist_executed_leg = AsyncMock(return_value="")  # type: ignore[method-assign]
        runner._balance_provider_for_chain = MagicMock(return_value=None)  # type: ignore[method-assign]
        runner._multichain_wallet_for = MagicMock(return_value="0xlegwallet")  # type: ignore[method-assign]
        runner._record_success = MagicMock()  # type: ignore[method-assign]

        saved = _make_progress(total_steps=2)
        saved.intents_hash = "h"
        saved.accounting_pending_step_index = 0  # step 0 broadcast-confirmed
        runner._load_execution_progress = AsyncMock(return_value=saved)  # type: ignore[method-assign]

        strategy = _make_strategy()
        intents = [
            SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="arbitrum"),
            SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1"), chain="arbitrum"),
        ]
        orch = MagicMock()
        orch.wallet_address = "0xstrategywallet"
        orch.primary_chain = "arbitrum"
        orch._config = None
        orch.execute = AsyncMock(return_value=_success_leg("0xstep2"))

        with (
            patch(
                "almanak.framework.runner.strategy_runner.EnsoStateProvider",
                return_value=MagicMock(),
            ),
            patch(
                "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
                return_value=False,
            ),
            patch("almanak.framework.accounting.deferred_log.append_now", return_value=True),
        ):
            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=intents,
                orchestrator=orch,
                start_time=datetime.now(UTC),
            )

        assert result.status == IterationStatus.SUCCESS
        # The pending step 0 was NEVER re-broadcast; only step 1 executed.
        assert orch.execute.await_count == 1
        executed_intent = orch.execute.await_args.args[0]
        assert executed_intent is intents[1]
