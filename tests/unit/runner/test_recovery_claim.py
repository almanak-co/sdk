"""Recovery ownership is exclusive and preserves durable state on races."""

from types import SimpleNamespace

import pytest

from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.execution.submission import SubmissionProvenance, SubmissionTransactionEvidence
from almanak.framework.runner.recovery_context import ExecutionRecoveryContext
from almanak.framework.runner.runner_models import ExecutionBarrierPhase, ExecutionLane, ExecutionProgress
from almanak.framework.runner.runner_recovery import claim_observed_single_chain_recovery
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig
from almanak.framework.state.strategy_state import (
    STRATEGY_USER_STATE_KEY,
    StateValuePreconditionError,
    replace_strategy_persistent_state,
)

TX = "0x" + "11" * 32
DEPLOYMENT = "deployment:claim"


def marker():
    context = ExecutionRecoveryContext.capture(
        plan_hash="a" * 64,
        execution=ExecutionContext(
            deployment_id=DEPLOYMENT, intent_id="intent", chain="bsc", wallet_address="0x" + "22" * 20
        ),
        pre_snapshot=None,
        prices=None,
        bundle_metadata={},
    ).with_strategy_checkpoint({"entered": False}, {})
    progress = ExecutionProgress(
        "intent",
        DEPLOYMENT,
        "reconciliation-required",
        1,
        recovery_context=context,
        execution_lane=ExecutionLane.SINGLE_CHAIN,
        barrier_phase=ExecutionBarrierPhase.RECONCILIATION_REQUIRED,
    )
    progress.record_submission_evidence(
        step_index=0,
        chain="bsc",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        submitted_transaction_ids=[TX],
        execution_plan_hash="a" * 64,
        submission_transactions=[SubmissionTransactionEvidence(TX, plan_indices=(0,), plan_transaction_count=1)],
    )
    return progress


def observation():
    return TransactionReceipt(TX, 7, "0x" + "33" * 32, 21000, 6, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "strategy", "marker"])
async def test_claim_requires_unchanged_checkpoint_and_can_only_succeed_once(tmp_path, changed):
    config = SQLiteConfig(db_path=str(tmp_path / "claim.sqlite"))
    manager = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=SQLiteStore(config))
    await manager.initialize()
    original = marker()
    await replace_strategy_persistent_state(
        manager,
        DEPLOYMENT,
        {"entered": False},
        runner_state={"execution_progress": original.to_dict()},
    )
    runner = SimpleNamespace(state_manager=manager)
    try:
        if changed:
            row = await manager.load_state(DEPLOYMENT)
            if changed == "strategy":
                row.state[STRATEGY_USER_STATE_KEY]["entered"] = True
            else:
                row.state["execution_progress"]["execution_id"] = "newer-execution"
            await manager.save_state(row, expected_version=row.version)
            with pytest.raises(StateValuePreconditionError):
                await claim_observed_single_chain_recovery(runner, original, [observation()])
        else:
            claimed, version = await claim_observed_single_chain_recovery(runner, original, [observation()])
            assert version == 2
            assert claimed.effective_barrier_phase is ExecutionBarrierPhase.LANDED_REPAIR_PENDING
            assert original.effective_barrier_phase is ExecutionBarrierPhase.RECONCILIATION_REQUIRED
            with pytest.raises(StateValuePreconditionError):
                await claim_observed_single_chain_recovery(runner, original, [observation()])
    finally:
        await manager.close()
    restarted = StateManager(StateManagerConfig(load_state_on_startup=False), warm_backend=SQLiteStore(config))
    await restarted.initialize()
    try:
        row = await restarted.load_state(DEPLOYMENT)
        durable = ExecutionProgress.from_dict(row.state["execution_progress"])
        if changed is None:
            assert durable.recovery_receipts == [observation().to_dict()]
            assert row.state[STRATEGY_USER_STATE_KEY] == {"entered": False}
        else:
            assert durable.recovery_receipts is None
    finally:
        await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["concurrent_write", "lost_ack"])
async def test_claim_write_must_be_exclusively_acknowledged(tmp_path, monkeypatch, race):
    from almanak.framework.state.state_manager import StateConflictError

    manager = StateManager(
        StateManagerConfig(load_state_on_startup=False),
        warm_backend=SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "race.sqlite"))),
    )
    await manager.initialize()
    original = marker()
    await replace_strategy_persistent_state(
        manager,
        DEPLOYMENT,
        {"entered": False},
        runner_state={"execution_progress": original.to_dict()},
    )
    real_save = manager.save_state

    async def interrupted_save(candidate, expected_version=None):
        if race == "concurrent_write":
            concurrent = await manager.load_state(DEPLOYMENT)
            concurrent.state[STRATEGY_USER_STATE_KEY]["entered"] = True
            await real_save(concurrent, expected_version=concurrent.version)
            return await real_save(candidate, expected_version=expected_version)
        await real_save(candidate, expected_version=expected_version)
        raise OSError("acknowledgment lost")

    monkeypatch.setattr(manager, "save_state", interrupted_save)
    try:
        with pytest.raises(StateConflictError if race == "concurrent_write" else OSError):
            await claim_observed_single_chain_recovery(
                SimpleNamespace(state_manager=manager), original, [observation()]
            )
        row = await manager.load_state(DEPLOYMENT)
        durable = ExecutionProgress.from_dict(row.state["execution_progress"])
        if race == "concurrent_write":
            assert row.state[STRATEGY_USER_STATE_KEY]["entered"] is True
            assert durable.effective_barrier_phase is ExecutionBarrierPhase.RECONCILIATION_REQUIRED
        else:
            assert durable.effective_barrier_phase is ExecutionBarrierPhase.LANDED_REPAIR_PENDING
            assert durable.recovery_receipts == [observation().to_dict()]
    finally:
        await manager.close()
