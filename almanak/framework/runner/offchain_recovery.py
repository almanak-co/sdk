"""Resolve an off-chain venue submission whose outcome was unknown, without re-sending it.

The off-chain lane holds the same pre-broadcast replay barrier as the on-chain
lane. When the venue did not confirm a submission (or the runner died during
it), the barrier stays sealed and every iteration asks the connector's handler
to reconcile it from the venue: a confirmed execution is delivered to the
strategy through the normal success path, a proven non-execution releases the
barrier, and anything else keeps the strategy held.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from almanak.framework.execution.offchain_venue import build_offchain_handler, offchain_execution_result
from almanak.framework.intents.vocabulary import Intent
from almanak.framework.observability.context import clear_cycle_id, get_cycle_id, set_cycle_id
from almanak.framework.state.state_manager import StateConflictError
from almanak.framework.state.strategy_state import StateValuePreconditionError
from almanak.framework.strategies.intent_strategy import IntentStrategy

from .runner_models import ExecutionBarrierPhase, ExecutionLane, ExecutionProgress, IterationResult, IterationStatus
from .runner_recovery import claim_observed_offchain_recovery

logger = logging.getLogger(__name__)

_OFFCHAIN_REQUEST_KEYS = ("order_request", "withdraw_request")


def offchain_metadata(progress: ExecutionProgress) -> dict[str, Any] | None:
    """The compiled off-chain bundle metadata a marker was taken for, or ``None``."""
    context = progress.recovery_context
    metadata = context.bundle_metadata if context is not None else None
    if not isinstance(metadata, dict) or not any(isinstance(metadata.get(k), dict) for k in _OFFCHAIN_REQUEST_KEYS):
        return None
    return metadata


def _held(runner: Any, progress: ExecutionProgress, start_time: datetime, reason: str) -> IterationResult:
    runner._total_iterations += 1
    return IterationResult(
        status=IterationStatus.EXECUTION_PENDING,
        error=progress.failure_error,
        execution_pending_since=progress.started_at,
        execution_pending_reason=reason,
        deployment_id=progress.deployment_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )


async def recover_pending_offchain(
    runner: Any, strategy: Any, progress: ExecutionProgress, start_time: datetime
) -> IterationResult | None:
    """Reconcile a held off-chain submission; ``None`` when this marker is not an off-chain one
    or the venue proved it never executed (the barrier is released and the iteration continues)."""
    metadata = offchain_metadata(progress)
    if (
        metadata is None
        or progress.execution_lane is not ExecutionLane.SINGLE_CHAIN
        or progress.effective_barrier_phase
        not in {ExecutionBarrierPhase.PRE_BROADCAST, ExecutionBarrierPhase.RECONCILIATION_REQUIRED}
    ):
        return None
    context = progress.recovery_context
    assert context is not None
    if (
        not isinstance(strategy, IntentStrategy)
        or getattr(strategy, "_state_manager", None) is not runner.state_manager
        or context.strategy_checkpoint is None
        or not progress.serialized_intents
        or len(progress.serialized_intents) != 1
        or context.execution.deployment_id != strategy.deployment_id
    ):
        return _held(
            runner, progress, start_time, "Off-chain checkpoint is incomplete; operator reconciliation required"
        )
    try:
        handler = build_offchain_handler(
            protocol=str(metadata.get("protocol") or ""),
            chain=context.execution.chain,
            gateway_client=runner._get_gateway_client(),
            wallet=strategy.wallet_address,
        )
    except Exception as exc:  # noqa: BLE001 — no handler means no answer: keep the barrier
        logger.warning("Off-chain handler unavailable for reconciliation: %s", exc)
        handler = None
    reconcile = getattr(handler, "reconcile", None)
    if not callable(reconcile):
        return _held(runner, progress, start_time, "This venue cannot reconcile; operator reconciliation required")
    try:
        outcome = await reconcile(metadata, since=progress.started_at)
    except Exception as exc:  # noqa: BLE001 — an unreadable venue keeps the barrier, never releases it
        logger.info("Off-chain reconciliation still pending (%s: %s)", type(exc).__name__, exc)
        outcome = None
    if outcome is None:
        return _held(runner, progress, start_time, "The venue has not confirmed the submission's outcome yet")
    if not outcome.success:
        if getattr(outcome, "filled_size", 0):
            # Failed short of its goal but partly executed: book what filled
            # before the barrier goes. Claim first so the booking happens once:
            # after a crash or a failed release the marker reads accounting-
            # pending (operator repair), never re-reconciled and re-booked.
            try:
                await runner._flush_strategy_pending_save_strict(strategy)
                await claim_observed_offchain_recovery(runner, progress)
            except (StateValuePreconditionError, StateConflictError) as exc:
                logger.warning("Off-chain partial-fill claim refused (%s); holding", type(exc).__name__)
                return _held(runner, progress, start_time, "Partial venue execution could not be claimed; retrying")
            await _book_failed_fill(runner, strategy, progress, outcome)
        logger.warning(
            "Off-chain submission %s did not complete (%s); releasing its replay barrier",
            progress.execution_id,
            outcome.error,
        )
        await runner._clear_execution_progress(progress.deployment_id)
        return None
    return await _deliver_success(runner, strategy, progress, metadata, outcome, start_time)


async def _book_failed_fill(runner: Any, strategy: Any, progress: ExecutionProgress, outcome: Any) -> None:
    context = progress.recovery_context
    assert context is not None
    intent = Intent.deserialize(progress.serialized_intents[0])  # type: ignore[index]
    result = offchain_execution_result(outcome)
    previous_cycle = get_cycle_id()
    if context.execution.cycle_id:
        set_cycle_id(context.execution.cycle_id)
    try:
        ledger_id = await runner._write_ledger_entry(
            strategy, intent, result=result, success=False, error=outcome.error or "", price_oracle=context.prices
        )
        if ledger_id:
            await runner._write_outbox_and_fire_processor(strategy, intent, ledger_id)
    finally:
        if previous_cycle is None:
            clear_cycle_id()
        else:
            set_cycle_id(previous_cycle)


async def _deliver_success(
    runner: Any,
    strategy: Any,
    progress: ExecutionProgress,
    metadata: dict[str, Any],
    outcome: Any,
    start_time: datetime,
) -> IterationResult | None:
    context = progress.recovery_context
    assert context is not None and context.strategy_checkpoint is not None
    intent = Intent.deserialize(progress.serialized_intents[0])  # type: ignore[index]
    try:
        await runner._flush_strategy_pending_save_strict(strategy)
        claimed, state_version = await claim_observed_offchain_recovery(runner, progress)
    except (StateValuePreconditionError, StateConflictError) as exc:
        # The venue confirmed the execution, so the barrier must hold: returning
        # nothing here would let the strategy decide again and send a new order.
        logger.warning("Off-chain recovery ownership refused (%s); holding", type(exc).__name__)
        return _held(runner, progress, start_time, "Venue-confirmed execution could not be claimed; retrying")
    except Exception as exc:
        logger.exception("Off-chain recovery checkpoint persistence failed")
        return IterationResult(
            status=IterationStatus.ACCOUNTING_FAILED,
            intent=intent,
            error=f"Recovery checkpoint persistence failed: {type(exc).__name__}",
            deployment_id=progress.deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
    result = offchain_execution_result(outcome)
    previous_cycle = get_cycle_id()
    if context.execution.cycle_id:
        set_cycle_id(context.execution.cycle_id)
        # Post-iteration snapshots must follow the recovered submission's accounting cycle.
        runner._last_cycle_id = context.execution.cycle_id
    try:
        from .strategy_runner import SingleChainExecutionState

        checkpoint = deepcopy(context.strategy_checkpoint)
        strategy._state_version = state_version
        IntentStrategy._restore_framework_state(strategy, checkpoint["framework_state"])
        strategy.load_persistent_state(checkpoint["user_state"])
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
            last_bundle_metadata=metadata,
            replay_barrier=claimed,
            state_machine=SimpleNamespace(retry_count=0),
        )
        logger.info("Off-chain submission %s confirmed by the venue; completing it", progress.execution_id)
        return await runner._single_chain_handle_success(state)
    except Exception as exc:
        logger.exception("Recovered off-chain execution requires downstream accounting/state repair")
        return IterationResult(
            status=IterationStatus.ACCOUNTING_FAILED,
            intent=intent,
            error=f"Recovered execution completion failed: {type(exc).__name__}",
            execution_result=result,
            deployment_id=progress.deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
    finally:
        if previous_cycle is None:
            clear_cycle_id()
        else:
            set_cycle_id(previous_cycle)


__all__ = ["offchain_metadata", "recover_pending_offchain"]
