"""Pending swap recovery uses original state without compiling or submitting."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator
from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.execution.submission import SubmissionProvenance, SubmissionTransactionEvidence
from almanak.framework.intents.vocabulary import Intent
from almanak.framework.observability.context import get_cycle_id, set_cycle_id
from almanak.framework.runner.reconciliation import BalanceSnapshot
from almanak.framework.runner.recovery_context import ExecutionRecoveryContext
from almanak.framework.runner.runner_models import (
    ExecutionBarrierPhase,
    ExecutionLane,
    ExecutionProgress,
    IterationResult,
    IterationStatus,
)
from almanak.framework.runner.single_chain_recovery import recover_pending_swap
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig
from almanak.framework.state.strategy_state import replace_strategy_persistent_state
from almanak.framework.strategies.intent_strategy import IntentStrategy

TX = "0x" + "11" * 32
WALLET = "0x" + "22" * 20
TOKEN_IN = "0x" + "33" * 20
TOKEN_OUT = "0x" + "44" * 20
DEPLOYMENT = "deployment:recovery"


class RecoverableStrategy(IntentStrategy):
    def decide(self, market):
        raise AssertionError("Recovery must not call decide")

    def generate_teardown_intents(self, mode, market=None):
        return []

    def get_open_positions(self):
        return []

    def load_persistent_state(self, state):
        self.restored.append(state)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", ["confirmed", "pending", "repair_failure", "wrong_plan", "claim_failure", "reverted", "legacy"]
)
async def test_recovery_claim_precedes_original_state_finalization(tmp_path, outcome):
    manager = StateManager(
        StateManagerConfig(load_state_on_startup=False),
        warm_backend=SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "recovery.sqlite"))),
    )
    await manager.initialize()
    strategy = object.__new__(RecoverableStrategy)
    strategy._deployment_id, strategy._chain, strategy._wallet_address = DEPLOYMENT, "bsc", WALLET
    strategy._state_manager = manager
    strategy.restored = []
    intent = Intent.swap(from_token=TOKEN_IN, to_token=TOKEN_OUT, amount=Decimal("1"), chain="bsc")
    context = ExecutionRecoveryContext.capture(
        plan_hash="a" * 64,
        execution=ExecutionContext(
            deployment_id=DEPLOYMENT,
            intent_id=intent.intent_id,
            chain="bsc",
            wallet_address=WALLET,
            cycle_id="original-cycle",
        ),
        pre_snapshot=BalanceSnapshot(datetime.now(UTC), {TOKEN_IN: Decimal("10"), TOKEN_OUT: Decimal("0")}),
        prices={TOKEN_IN: Decimal("1"), TOKEN_OUT: Decimal("338.161")},
        bundle_metadata={"original_pool": "exact-pool"},
        failed_attempt_receipts={},
    ).with_strategy_checkpoint({"entered": False}, {})
    progress = ExecutionProgress(
        intent.intent_id,
        DEPLOYMENT,
        "pending",
        1,
        serialized_intents=[intent.serialize()],
        recovery_context=context,
        execution_lane=ExecutionLane.SINGLE_CHAIN,
        barrier_phase=ExecutionBarrierPhase.RECONCILIATION_REQUIRED,
    )
    progress.record_submission_evidence(
        step_index=0,
        chain="bsc",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        submitted_transaction_ids=[TX],
        execution_plan_hash="b" * 64 if outcome == "wrong_plan" else "a" * 64,
        submission_transactions=[SubmissionTransactionEvidence(TX, plan_indices=(0,), plan_transaction_count=1)],
    )
    await replace_strategy_persistent_state(
        manager,
        DEPLOYMENT,
        {"entered": False},
        runner_state={"execution_progress": progress.to_dict()},
    )
    client = MagicMock()
    orchestrator = GatewayExecutionOrchestrator(client, chain="bsc")
    receipt = TransactionReceipt(TX, 7, "0x" + "55" * 32, 21000, 6, 1)
    from almanak.framework.execution.interfaces import TransactionRevertedError

    if outcome == "legacy":
        progress.recovery_context = None
    orchestrator.get_completed_plan_receipts = AsyncMock(
        side_effect=(
            TransactionRevertedError(tx_hash=TX, receipt=receipt)
            if outcome == "reverted"
            else TimeoutError()
            if outcome == "pending"
            else None
        ),
        return_value=(receipt,),
    )

    async def finalize(state):
        durable = await manager.load_state(DEPLOYMENT)
        assert durable.state["execution_progress"]["barrier_phase"] == "landed_repair_pending"
        assert get_cycle_id() == "original-cycle"
        assert state.pre_snapshot.balances[TOKEN_IN] == Decimal("10")
        assert state.price_oracle[TOKEN_OUT] == Decimal("338.161")
        assert state.last_execution_context.intent_id == intent.intent_id
        assert state.last_bundle_metadata == {"original_pool": "exact-pool"}
        if outcome == "repair_failure":
            raise OSError("accounting unavailable")
        return IterationResult(status=IterationStatus.SUCCESS)

    runner = SimpleNamespace(
        state_manager=manager,
        _total_iterations=0,
        execution_orchestrator=orchestrator,
        _get_gateway_client=lambda: client,
        _flush_strategy_pending_save_strict=AsyncMock(),
        _single_chain_handle_success=AsyncMock(side_effect=finalize),
        _calculate_duration_ms=lambda _: 1,
        _last_cycle_id="current-observation-cycle",
    )
    if outcome == "claim_failure":
        manager.save_state = AsyncMock(side_effect=OSError("state persistence failed"))
    set_cycle_id("current-observation-cycle")
    try:
        result = await recover_pending_swap(runner, strategy, progress, datetime.now(UTC))
        if outcome in {"pending", "wrong_plan", "claim_failure", "reverted", "legacy"}:
            if outcome == "claim_failure":
                assert result.status is IterationStatus.ACCOUNTING_FAILED
            elif outcome in {"reverted", "legacy"}:
                assert result.status is IterationStatus.EXECUTION_PENDING
                assert result.execution_pending_reason
                assert result.execution_pending_since == progress.started_at
            else:
                assert result is None
            assert not strategy.restored
            assert runner._last_cycle_id == "current-observation-cycle"
            runner._single_chain_handle_success.assert_not_awaited()
        else:
            assert result.status is (
                IterationStatus.ACCOUNTING_FAILED if outcome == "repair_failure" else IterationStatus.SUCCESS
            )
            assert strategy.restored == [{"entered": False}]
            assert runner._last_cycle_id == "original-cycle"
            durable = await manager.load_state(DEPLOYMENT)
            assert (
                await recover_pending_swap(
                    runner,
                    strategy,
                    ExecutionProgress.from_dict(durable.state["execution_progress"]),
                    datetime.now(UTC),
                )
                is None
            )
            runner._single_chain_handle_success.assert_awaited_once()
        assert get_cycle_id() == "current-observation-cycle"
        client.execution.Execute.assert_not_called()
    finally:
        await manager.close()
