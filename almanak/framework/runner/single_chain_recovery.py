"""Finish a pending swap from its original checkpoint without redispatch."""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator, GatewayExecutionResult
from almanak.framework.execution.interfaces import TransactionRevertedError
from almanak.framework.execution.plan_completion import PlanCompletionUnproven
from almanak.framework.intents.vocabulary import Intent, SwapIntent
from almanak.framework.observability.context import clear_cycle_id, get_cycle_id, set_cycle_id
from almanak.framework.state.state_manager import StateConflictError
from almanak.framework.state.strategy_state import StateValuePreconditionError
from almanak.framework.strategies.intent_strategy import IntentStrategy

from .recovery_context import ExecutionRecoveryContext
from .runner_models import ExecutionBarrierPhase, ExecutionLane, ExecutionProgress, IterationResult, IterationStatus
from .runner_recovery import claim_observed_single_chain_recovery

logger = logging.getLogger(__name__)


def _has_original_swap_context(context: ExecutionRecoveryContext | None, progress: ExecutionProgress) -> bool:
    return not (
        context is None
        or context.strategy_checkpoint is None
        or context.pre_snapshot is None
        or not context.pre_snapshot.balances
        or not context.prices
        or not context.execution.cycle_id
        or not isinstance(context.failed_attempt_receipts, dict)
        or context.execution.dry_run
        or not progress.serialized_intents
        or len(progress.serialized_intents) != 1
    )


def _original_swap(strategy: Any, progress: ExecutionProgress) -> SwapIntent | None:
    context = progress.recovery_context
    if (
        not isinstance(strategy, IntentStrategy)
        or progress.execution_lane is not ExecutionLane.SINGLE_CHAIN
        or progress.effective_barrier_phase is not ExecutionBarrierPhase.RECONCILIATION_REQUIRED
        or progress.total_steps != 1
        or len(progress.submission_evidence) != 1
        or not _has_original_swap_context(context, progress)
    ):
        return None
    execution = cast(ExecutionRecoveryContext, context).execution
    if (
        execution.deployment_id != strategy.deployment_id
        or progress.deployment_id != strategy.deployment_id
        or execution.chain != strategy.chain
        or execution.wallet_address.lower() != strategy.wallet_address.lower()
    ):
        return None
    intent = Intent.deserialize(cast(list[dict[str, Any]], progress.serialized_intents)[0])
    if not isinstance(intent, SwapIntent) or intent.intent_id != execution.intent_id:
        return None
    if intent.chain is not None and intent.chain != execution.chain:
        return None
    return intent


def _operator_reconciliation(
    runner: Any, progress: ExecutionProgress, start_time: datetime, reason: str
) -> IterationResult:
    runner._total_iterations += 1
    return IterationResult(
        status=IterationStatus.EXECUTION_PENDING,
        error=progress.failure_error,
        execution_pending_since=progress.started_at,
        execution_pending_reason=reason,
        deployment_id=progress.deployment_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )


async def recover_pending_swap(
    runner: Any,
    strategy: Any,
    progress: ExecutionProgress,
    start_time: datetime,
) -> IterationResult | None:
    """Observe and exclusively claim recovery before restoring callback state."""
    if (
        progress.execution_lane is not ExecutionLane.SINGLE_CHAIN
        or progress.effective_barrier_phase is not ExecutionBarrierPhase.RECONCILIATION_REQUIRED
    ):
        return None
    orchestrator = runner.execution_orchestrator
    if not isinstance(orchestrator, GatewayExecutionOrchestrator):
        return None
    if getattr(strategy, "_state_manager", None) is not runner.state_manager:
        return None
    try:
        intent = _original_swap(strategy, progress)
        if intent is None:
            return _operator_reconciliation(
                runner, progress, start_time, "Checkpoint is not eligible for automatic SWAP recovery"
            )
        context = progress.recovery_context
        assert context is not None
        evidence = progress.submission_evidence[0]
        receipts = await orchestrator.get_completed_plan_receipts(
            expected_plan_hash=context.plan_hash,
            observed_plan_hash=evidence.execution_plan_hash,
            provenance=evidence.submission_provenance,
            submitted_tx_ids=evidence.submitted_transaction_ids,
            evidence=evidence.submission_transactions,
        )
    except TransactionRevertedError:
        return _operator_reconciliation(
            runner,
            progress,
            start_time,
            "Canonical receipt proves a submitted transaction reverted; reconcile the entire plan before release",
        )
    except PlanCompletionUnproven as exc:
        return _operator_reconciliation(runner, progress, start_time, str(exc))
    except Exception as exc:
        logger.info("Swap recovery remains pending (%s)", type(exc).__name__)
        return None
    try:
        await runner._flush_strategy_pending_save_strict(strategy)
        claimed, state_version = await claim_observed_single_chain_recovery(runner, progress, receipts)
    except (PlanCompletionUnproven, StateValuePreconditionError, StateConflictError) as exc:
        logger.info("Swap recovery ownership refused (%s)", type(exc).__name__)
        return None
    except Exception as exc:
        logger.exception("Swap recovery checkpoint persistence failed")
        return IterationResult(
            status=IterationStatus.ACCOUNTING_FAILED,
            intent=intent,
            error=f"Recovery checkpoint persistence failed: {type(exc).__name__}",
            deployment_id=progress.deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
    result = None
    previous_cycle = get_cycle_id()
    set_cycle_id(context.execution.cycle_id)
    # Post-iteration snapshots must follow the recovered trade's accounting cycle.
    runner._last_cycle_id = context.execution.cycle_id
    try:
        from .strategy_runner import SingleChainExecutionState

        assert claimed.recovery_receipts is not None
        assert context.failed_attempt_receipts is not None
        checkpoint = deepcopy(context.strategy_checkpoint)
        assert checkpoint is not None
        strategy._state_version = state_version
        IntentStrategy._restore_framework_state(strategy, checkpoint["framework_state"])
        strategy.load_persistent_state(checkpoint["user_state"])
        result = GatewayExecutionResult(
            success=True,
            tx_hashes=[receipt["tx_hash"] for receipt in claimed.recovery_receipts],
            total_gas_used=sum(receipt.gas_used for receipt in receipts),
            receipts=claimed.recovery_receipts,
            execution_id=context.execution.correlation_id,
            submission_provenance=evidence.submission_provenance,
            execution_plan_hash=context.plan_hash,
            submission_transactions=evidence.submission_transactions,
        )
        state = SingleChainExecutionState(
            strategy=strategy,
            intent=intent,
            start_time=start_time,
            deployment_id=progress.deployment_id,
            gateway_client=runner._get_gateway_client(),
            price_oracle=context.prices,
            pre_snapshot=context.pre_snapshot,
            last_execution_result=result,
            last_execution_context=context.execution,
            last_bundle_metadata=context.bundle_metadata,
            replay_barrier=claimed,
            state_machine=SimpleNamespace(retry_count=0),
            failed_attempt_receipts={key: tuple(value) for key, value in context.failed_attempt_receipts.items()},
        )
        return await runner._single_chain_handle_success(state)
    except Exception as exc:
        logger.exception("Recovered swap requires downstream accounting/state repair")
        return IterationResult(
            status=IterationStatus.ACCOUNTING_FAILED,
            intent=intent,
            error=f"Recovered swap completion failed: {type(exc).__name__}",
            execution_result=result,
            deployment_id=progress.deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
    finally:
        if previous_cycle is None:
            clear_cycle_id()
        else:
            set_cycle_id(previous_cycle)
