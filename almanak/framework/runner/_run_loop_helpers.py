"""Phase helpers for :meth:`StrategyRunner.run_loop` (Phase 6A.2).

This module contains phase-level helpers extracted from the main loop body
of ``StrategyRunner.run_loop`` to reduce cyclomatic complexity and isolate
responsibilities. Every helper preserves the EXACT original behavior
captured by the characterization tests in
``tests/unit/runner/test_run_loop_characterization.py``.

Design notes
------------
* Helpers are module-level functions (not methods) that take the runner
  instance explicitly. This keeps them free of ``self.`` noise inside
  ``run_loop`` while still respecting the runner's private state.
* All ``_consecutive_errors`` / ``_first_error_at`` semantics, circuit
  breaker call sites, lifecycle write precedence, and log messages are
  reproduced byte-for-byte from the pre-extraction body.
* ``_run_loop_helpers`` does NOT import from ``strategy_runner`` at module
  load time — it uses ``TYPE_CHECKING`` to avoid a circular import while
  still offering typed signatures.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..state.exceptions import AccountingPersistenceError

if TYPE_CHECKING:
    from .runner_models import (
        IterationResult,
        StatefulActivityProviderProtocol,
        StrategyProtocol,
    )
    from .strategy_runner import StrategyRunner

logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# =============================================================================
# Pre-loop initialization
# =============================================================================


async def initialize_run_loop(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    interval: int,
) -> StatefulActivityProviderProtocol | None:
    """Run the one-shot setup before the ``while`` loop begins.

    Mirrors the original setup block from ``run_loop`` lines ~1411-1472:
    state manager init, incomplete session recovery, copy-trading cursor
    restore, shutdown flag reset, gateway wiring, lifecycle RUNNING write,
    and the STRATEGY_STARTED timeline event.

    Returns the resolved ``activity_provider`` (may be ``None``) so the
    caller can feed it to the success-branch copy-trading persist step.
    """
    # Initialize state if enabled
    state_manager_ready = False
    if runner.config.enable_state_persistence:
        try:
            await runner.state_manager.initialize()
            state_manager_ready = True
            logger.debug(f"State manager initialized for {strategy_id}")
        except Exception as e:
            if runner._is_live_mode():
                raise RuntimeError(f"Failed to initialize state manager for {strategy_id}: {e}") from e
            logger.error(f"Failed to initialize state manager: {e}")

    # Reconstruct FIFO basis store from durable accounting_events so REPAY and
    # PT_REDEEM attribution is correct after a runner restart (VIB-3484).
    # Gate on state_manager_ready: an uninitialized backend returns [] silently,
    # which would leave the FIFO store empty — the exact restart hole VIB-3484 fixes.
    if runner.config.enable_state_persistence and state_manager_ready:
        try:
            deployment_id = getattr(strategy, "deployment_id", "") or strategy_id
            events = runner.state_manager.get_accounting_events_sync(deployment_id)
            if events:
                replayed = runner._lending_basis_store.reconstruct_from_events(events)
                if replayed:
                    logger.info(
                        "Reconstructed %d FIFO lot operations for %s from accounting_events",
                        replayed,
                        deployment_id,
                    )
        except Exception as e:
            if runner._is_live_mode():
                raise RuntimeError(f"Failed to reconstruct FIFO basis store for {deployment_id}: {e}") from e
            logger.warning("Failed to reconstruct FIFO basis store on startup: %s", e)

    # Recover incomplete sessions from previous runs
    try:
        recovered = await runner._recover_incomplete_sessions()
        if recovered > 0:
            logger.info(f"Recovered {recovered} incomplete sessions on startup")
    except Exception as e:
        logger.error(f"Failed to recover incomplete sessions: {e}")

    # Restore copy trading cursor state if configured
    from .runner_models import StatefulActivityProviderProtocol

    activity_provider = cast(
        StatefulActivityProviderProtocol | None,
        getattr(strategy, "_wallet_activity_provider", None),
    )
    if activity_provider is not None and runner.config.enable_state_persistence:
        try:
            state = await runner.state_manager.load_state(strategy_id)
            if state is not None and "copy_trading_state" in state.state:
                activity_provider.set_state(state.state["copy_trading_state"])
                logger.info("Copy trading: cursor state restored from persistence")
        except Exception as e:
            logger.warning(f"Failed to restore copy trading state: {e}")

    runner._shutdown_requested = False
    runner._signal_received = False
    runner._terminal_lifecycle_state = None
    runner._terminal_lifecycle_error_message = None

    # Set up dual-write for timeline events (gateway persistence)
    gateway_client = runner._get_gateway_client()
    if gateway_client is not None:
        from ..api.timeline import set_event_gateway_client

        set_event_gateway_client(gateway_client)
        logger.debug("Enabled gateway dual-write for timeline events")

    # Register this strategy instance with the gateway
    runner._register_with_gateway(strategy)

    # Write RUNNING state to LifecycleStore
    runner._lifecycle_write_state(strategy_id, "RUNNING")

    # Emit strategy started event
    start_event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.STRATEGY_STARTED,
        description=f"Strategy {strategy_id} started with interval={interval}s",
        strategy_id=strategy_id,
        chain=getattr(runner.config, "chain", ""),
        details={
            "interval_seconds": interval,
            "enable_state_persistence": runner.config.enable_state_persistence,
        },
    )
    add_event(start_event)
    logger.debug(f"Emitted STRATEGY_STARTED event for {strategy_id}")

    return activity_provider


# =============================================================================
# Per-iteration helpers
# =============================================================================


def invoke_pre_iteration_callback(
    pre_iteration_callback: Callable[[], None] | None,
) -> None:
    """Invoke the user-supplied pre-iteration callback.

    Regular ``Exception`` subclasses are logged and swallowed so the loop
    continues with the iteration. ``CriticalCallbackError`` is re-raised
    by the caller's try/except so the loop exits (fail-closed).

    Note: ``CriticalCallbackError`` is NOT caught here — it is allowed to
    propagate. The caller's outer try/except handles it. We catch
    ``Exception`` (base class) and let ``CriticalCallbackError`` bypass
    this handler because ``CriticalCallbackError`` inherits from
    ``Exception``; the original code had a dedicated ``except
    CriticalCallbackError`` clause before the generic ``except Exception``.
    """
    if pre_iteration_callback is None:
        return

    # Local import to avoid circular dependency at module load time.
    from .runner_models import CriticalCallbackError

    try:
        pre_iteration_callback()
    except CriticalCallbackError:
        # Fail-closed: safety-critical callbacks stop the loop
        raise
    except Exception as e:
        logger.error(f"Pre-iteration callback error: {e}")


async def capture_snapshot_with_accounting(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    result: IterationResult,
    iteration_start_monotonic: float | None = None,
) -> IterationResult:
    """Capture portfolio snapshot after an iteration, applying the
    live-mode ACCOUNTING_FAILED escalation contract.

    Returns the (possibly rebuilt) ``IterationResult``. In non-live mode
    or when persistence succeeds, the input ``result`` is returned
    unchanged. In live mode, a raised ``AccountingPersistenceError`` is
    converted into a fresh ``IterationResult`` with
    ``IterationStatus.ACCOUNTING_FAILED``. The rebuilt result's
    ``duration_ms`` covers the FULL wall-clock window from iteration
    start through the snapshot phase that failed (issue #1782 -- the
    #1770 fix preserved the iteration-body duration but still excluded
    the snapshot-phase time, undercounting ``duration_ms`` by the
    wall-clock spent in the post-iteration snapshot that actually
    failed). The caller passes ``iteration_start_monotonic``, captured
    at the top of the iteration body via ``time.monotonic()``; we
    compute ``duration_ms`` from that anchor at the moment of failure.
    If the caller doesn't supply the anchor we fall back to
    ``result.duration_ms`` (iteration-body only) -- this preserves the
    post-#1770 behaviour for any external caller that hasn't been
    updated, but all in-tree callers pass the anchor. The rebuilt
    result also carries over ``result.intent``,
    ``result.execution_result``, and ``result.balance_reconciliation``
    so operators retain the on-chain tx hash, gas metrics, and
    reconciliation context that preceded the accounting failure. The
    helper bypasses ``_create_error_result`` to avoid a redundant
    ``_total_iterations`` bump -- ``run_iteration`` already counted
    this iteration via ``_record_success`` before the snapshot phase
    ran.
    """
    # Local import to avoid circular dependency at module load time.
    from .runner_models import IterationResult, IterationStatus

    if not runner.config.enable_state_persistence:
        return result

    try:
        await runner._capture_portfolio_snapshot(
            strategy=strategy,
            iteration_number=runner._total_iterations,
        )
    except AccountingPersistenceError as acc_err:
        # Mode-aware: only escalate to ACCOUNTING_FAILED in
        # live mode, matching the contract used by
        # _write_ledger_entry (live raises, paper/dry-run
        # logs). In non-live modes, swallow + ERROR-log so
        # pre-prod drift is visible without halting the loop.
        if runner._is_live_mode():
            # Capture the failure timestamp BEFORE any alert / logging
            # side effects so ``duration_ms`` reflects only the
            # iteration-body + snapshot-phase wall-clock. The alert
            # hook (``_alert_accounting_failure``) performs network
            # I/O for Slack / PagerDuty and can add noticeable
            # latency; including that latency in the reported
            # duration would skew operator dashboards and misrepresent
            # the cost of the iteration + snapshot work that actually
            # failed (Gemini / Codex review of PR #1786).
            #
            # Report the FULL wall-clock cost of the failed
            # iteration, including the snapshot phase that
            # actually failed. Issue #1770 fixed the obvious
            # undercount (snapshot-only duration); issue
            # #1782 finishes the job by also including the
            # snapshot-phase time that elapsed between
            # ``run_iteration`` returning and
            # ``AccountingPersistenceError`` firing. When the
            # caller passes ``iteration_start_monotonic``
            # (the anchor captured at the top of the
            # iteration body), we measure from that anchor
            # through ``time.monotonic()`` now; otherwise we
            # fall back to ``result.duration_ms`` so an
            # external caller that hasn't been updated still
            # gets the #1770 behaviour.
            if iteration_start_monotonic is not None:
                duration_ms = (time.monotonic() - iteration_start_monotonic) * 1000.0
            else:
                duration_ms = result.duration_ms
            logger.exception(
                "Accounting persistence failed in live mode for %s (write_kind=%s)",
                strategy_id,
                acc_err.write_kind,
            )
            await runner._alert_accounting_failure(strategy, acc_err)
            # Forensic metadata (``intent``,
            # ``execution_result``, ``balance_reconciliation``)
            # is carried across unchanged: the iteration
            # succeeded on-chain and operators need the tx
            # hash, gas metrics, and reconciliation context
            # to diagnose what preceded the accounting
            # failure.
            #
            # We still build the result directly rather
            # than via ``_create_error_result`` -- that
            # helper, post fix #1771, no longer mutates
            # ``_consecutive_errors``, but it DOES still
            # bump ``_total_iterations`` which would
            # double-count this iteration (``run_iteration``
            # already counted it via ``_record_success``
            # before the snapshot phase ran).
            return IterationResult(
                status=IterationStatus.ACCOUNTING_FAILED,
                error=f"Accounting persistence failed ({acc_err.write_kind}): {acc_err}",
                strategy_id=strategy_id,
                duration_ms=duration_ms,
                intent=result.intent,
                execution_result=result.execution_result,
                balance_reconciliation=result.balance_reconciliation,
            )
        logger.error(
            "Snapshot accounting persistence failed in non-live mode for %s "
            "(write_kind=%s, continuing; pre-prod drift): %s",
            strategy_id,
            acc_err.write_kind,
            acc_err,
        )
    return result


async def handle_iteration_failure(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    result: IterationResult,
) -> None:
    """Post-iteration bookkeeping for the failure branch.

    Increments ``_consecutive_errors``, records the first-error timestamp,
    records the failure on the circuit breaker (skipping statuses that
    were already recorded inline to avoid double-counting), maybe
    triggers emergency stop if the breaker just tripped OPEN, and emits
    the max-consecutive-errors alert + ERROR lifecycle write when the
    streak threshold is reached.

    Ownership of ``_consecutive_errors`` (fix for issue #1771): this
    helper is the SOLE owner of the consecutive-error streak counter for
    iteration results. ``StrategyRunner._create_error_result`` does NOT
    increment it. The one remaining increment outside this helper lives
    in ``run_loop``'s outer ``except Exception`` clause, which handles
    raised (as opposed to returned-as-result) errors -- a separate flow
    that never reaches ``run_iteration``'s result path and therefore
    never touches this helper.
    """
    from .runner_models import IterationStatus

    runner._consecutive_errors += 1
    if runner._first_error_at is None:
        runner._first_error_at = datetime.now(UTC)

    # Record failure in circuit breaker (skip statuses that already
    # recorded inline to avoid double-counting)
    if runner._circuit_breaker is not None and result.status not in (
        IterationStatus.CIRCUIT_BREAKER_OPEN,
        IterationStatus.STRATEGY_TIMEOUT,  # already recorded in decide() handler
        IterationStatus.STRATEGY_ERROR,  # already recorded in decide() handler
    ):
        runner._circuit_breaker.record_failure(
            error_message=result.error or f"Iteration failed: {result.status.value}",
        )

    # Auto-trigger emergency stop if breaker just tripped to OPEN
    # (checked after both inline and run_loop recording paths)
    if runner._circuit_breaker is not None:
        await runner._maybe_trigger_emergency(strategy, result)

    if runner._consecutive_errors >= runner.config.max_consecutive_errors:
        await runner._alert_consecutive_errors(strategy, result)
        runner._lifecycle_write_state(strategy_id, "ERROR", error_message=str(result.error) if result.error else None)


def handle_iteration_success(
    runner: StrategyRunner,
    strategy_id: str,
    was_in_error_streak: bool,
) -> None:
    """Post-iteration bookkeeping for the success branch.

    Recovers lifecycle state if the runner was previously in an error
    streak and is not about to shut down / transition to a terminal
    state. Then resets the consecutive-error counter, clears the
    first-error timestamp, and resets the emergency trigger guard when
    the circuit breaker is not OPEN.
    """
    # Recover lifecycle state if we were in an error streak before
    # this iteration succeeded. The counter has already been reset to
    # 0 inside `run_iteration` via `_record_success`, so we rely on the
    # pre-iteration snapshot captured above. Skip the recovery write
    # when the same iteration has already transitioned to a terminal
    # state (e.g., teardown writes TERMINATED and requests shutdown) --
    # otherwise we would clobber that terminal state with RUNNING.
    if was_in_error_streak and not runner._shutdown_requested and runner._terminal_lifecycle_state is None:
        runner._lifecycle_write_state(strategy_id, "RUNNING")
        logger.info(
            "Strategy %s recovered after error streak (max_consecutive_errors=%d) - lifecycle state reset to RUNNING",
            strategy_id,
            runner.config.max_consecutive_errors,
        )
    elif was_in_error_streak:
        logger.debug(
            "Skipping lifecycle recovery write for %s: shutdown/terminal state active",
            strategy_id,
        )
    runner._consecutive_errors = 0
    runner._first_error_at = None
    # Reset emergency guard so a future HALF_OPEN->OPEN relapse can re-fire
    if runner._circuit_breaker is not None:
        from ..execution.circuit_breaker import CircuitBreakerState

        if runner._circuit_breaker.state != CircuitBreakerState.OPEN:
            runner._emergency_triggered_for_open = False


async def handle_lifecycle_command(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
    command: str | None,
) -> None:
    """Route a polled lifecycle command (STOP / PAUSE / RESUME).

    ``STOP``: delegate to ``_lifecycle_handle_stop`` (which will call
    ``request_shutdown`` itself — the outer loop drains on the next
    iteration).

    ``PAUSE``: write PAUSED state, send a last position heartbeat, then
    enter an inner loop that sends heartbeats at
    ``config.lifecycle_poll_interval`` while waiting for ``RESUME``.
    ``STOP`` received during pause also calls ``_lifecycle_handle_stop``
    and exits the inner loop. ``_shutdown_requested`` (set externally by
    signal handler) also exits the inner loop.
    """
    if command == "STOP":
        logger.info("Received STOP command for %s", strategy_id)
        runner._lifecycle_handle_stop(strategy_id, strategy)
        return

    if command != "PAUSE":
        return

    logger.info("Received PAUSE command for %s", strategy_id)
    runner._lifecycle_write_state(strategy_id, "PAUSED")
    runner._gateway_update_status(strategy_id, "PAUSED")
    # Preserve position snapshot so the dashboard doesn't lose it during pause
    runner._gateway_heartbeat(strategy_id, positions=runner._collect_position_snapshot(strategy))
    # Wait for RESUME command (send heartbeats so operator sees liveness)
    while not runner._shutdown_requested:
        runner._lifecycle_heartbeat(strategy_id)
        resume_cmd = runner._lifecycle_poll_command(strategy_id)
        if resume_cmd == "RESUME":
            logger.info("Received RESUME command for %s", strategy_id)
            runner._lifecycle_write_state(strategy_id, "RUNNING")
            runner._gateway_update_status(strategy_id, "RUNNING")
            runner._gateway_heartbeat(strategy_id, positions=runner._collect_position_snapshot(strategy))
            break
        if resume_cmd == "STOP":
            logger.info("Received STOP command while paused for %s", strategy_id)
            runner._lifecycle_handle_stop(strategy_id, strategy)
            break
        await asyncio.sleep(runner.config.lifecycle_poll_interval)


# =============================================================================
# Post-loop finalization
# =============================================================================


async def finalize_run_loop(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    strategy_id: str,
) -> None:
    """Run the teardown block after the ``while`` loop exits.

    Mirrors the original teardown block at ~line 1681-1720: final
    lifecycle write (preserving any ERROR set by the circuit breaker),
    gateway deregistration, STRATEGY_STOPPED timeline event, strategy
    ``flush_pending_saves`` (if provided), and the state manager
    ``close``.
    """
    # Write final state to LifecycleStore (preserve ERROR if set by circuit breaker)
    runner._lifecycle_write_state(
        strategy_id,
        runner._terminal_lifecycle_state or "TERMINATED",
        error_message=runner._terminal_lifecycle_error_message,
    )

    # Deregister from gateway (mark as INACTIVE)
    runner._deregister_from_gateway(strategy_id)

    # Emit strategy stopped event
    stop_event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.STRATEGY_STOPPED,
        description=f"Strategy {strategy_id} stopped",
        strategy_id=strategy_id,
        chain=getattr(runner.config, "chain", ""),
        details={
            "shutdown_requested": runner._shutdown_requested,
            "consecutive_errors": runner._consecutive_errors,
        },
    )
    add_event(stop_event)
    logger.debug(f"Emitted STRATEGY_STOPPED event for {strategy_id}")

    logger.info(f"Run loop ended for strategy {strategy_id}")

    # Flush any pending state saves before cleanup
    if hasattr(strategy, "flush_pending_saves"):
        try:
            await strategy.flush_pending_saves()
        except Exception as e:
            logger.warning(f"Error flushing pending saves: {e}")

    # Cleanup
    if runner.config.enable_state_persistence:
        try:
            await runner.state_manager.close()
        except Exception as e:
            logger.error(f"Error closing state manager: {e}")
