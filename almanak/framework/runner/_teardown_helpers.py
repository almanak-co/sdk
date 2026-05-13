"""Phase helpers for :func:`execute_teardown_via_manager` (Phase 6A.4).

This module holds the phase-level helpers extracted from
``execute_teardown_via_manager`` to reduce cyclomatic complexity and isolate
responsibilities. Every helper preserves the EXACT original behavior captured
by the characterization tests in
``tests/unit/runner/test_teardown_flow.py::TestExecuteTeardownViaManagerCharacterization``.

Design notes
------------
* Helpers are module-level functions that take the runner instance explicitly
  so ``execute_teardown_via_manager`` reads as a clean sequencer without
  ``self.`` noise.
* The outer ``try/except`` in ``execute_teardown_via_manager`` is preserved in
  the caller because its ``except`` branch needs access to locals
  (``teardown_state``, ``teardown_state_adapter``) from the execute/verify
  phase. Moving the ``try`` into a helper would break that semantic.
* Log messages, error strings, and ``state_manager.mark_*`` ordering are
  reproduced byte-for-byte from the pre-extraction body, including the
  double ``mark_failed`` call on the verify-fail path (pinned by char tests).
* Module uses ``TYPE_CHECKING`` to avoid circular imports at load time.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path as _Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..teardown import TeardownMode
    from ..teardown.models import TeardownResult, TeardownState
    from .runner_models import IterationResult, StrategyProtocol

# Use the same logger as runner_teardown so existing log-capture tests keep
# working after extraction.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# =============================================================================
# Phase 1: Compiler + positions resolution (with fallback branches)
# =============================================================================


async def resolve_compiler_or_fallback(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> tuple[Any | None, IterationResult | None]:
    """Build the teardown compiler. If compiler build fails, either return an
    early STRATEGY_ERROR (when ``allow_unsafe_teardown_fallback=False``) or
    delegate to the inline fallback path.

    Returns
    -------
    (compiler, early_result):
        - ``compiler`` is truthy and ``early_result`` is ``None`` on success.
        - ``compiler`` is ``None`` when the caller should return
          ``early_result`` immediately.
    """
    from .runner_models import IterationStatus

    strategy_id = strategy.strategy_id

    # Call through runner method so instance-level mock patching in tests works.
    compiler = runner._build_teardown_compiler(strategy, teardown_market)
    if compiler is not None:
        return compiler, None

    if not runner.config.allow_unsafe_teardown_fallback:
        error_msg = (
            f"Cannot build TeardownManager compiler for {strategy_id}. "
            f"Inline fallback is disabled (allow_unsafe_teardown_fallback=False). "
            f"Fix compiler dependencies or enable fallback for local testing."
        )
        logger.error(error_msg)
        if request:
            from .runner_teardown import _safe_mark

            _safe_mark(state_manager, "mark_failed", strategy_id, error=error_msg)
        runner._request_teardown_failure_shutdown(error_msg)
        return None, runner._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, error_msg, start_time)

    logger.warning(
        f"Cannot build compiler for TeardownManager — falling back to inline teardown "
        f"for {strategy_id} (unsafe fallback enabled)"
    )
    fallback_result = await runner._execute_teardown_inline(
        strategy, teardown_intents, teardown_market, start_time, request, state_manager
    )
    return None, fallback_result


async def fetch_positions_or_fallback(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> tuple[Any | None, IterationResult | None]:
    """Get open positions for safety validation. If positions can't be
    fetched, either return an early STRATEGY_ERROR (when
    ``allow_unsafe_teardown_fallback=False``) or delegate to the inline
    fallback path. NOTE: passing an empty portfolio through safety validation
    is unsafe (loss cap of 3% of $0 = $0 passes trivially).

    Returns
    -------
    (positions, early_result):
        - ``positions`` is truthy and ``early_result`` is ``None`` on success.
        - ``positions`` is ``None`` when the caller should return
          ``early_result`` immediately.
    """
    from .runner_models import IterationStatus

    strategy_id = strategy.strategy_id
    try:
        positions = strategy.get_open_positions()
    except Exception as pos_err:
        if not runner.config.allow_unsafe_teardown_fallback:
            error_msg = (
                f"Cannot fetch positions for safety validation for {strategy_id}: {pos_err}. "
                f"Inline fallback is disabled (allow_unsafe_teardown_fallback=False)."
            )
            logger.error(error_msg)
            if request:
                from .runner_teardown import _safe_mark

                _safe_mark(state_manager, "mark_failed", strategy_id, error=error_msg)
            runner._request_teardown_failure_shutdown(error_msg)
            return None, runner._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, error_msg, start_time)
        logger.warning(
            f"Cannot fetch positions for safety validation — "
            f"falling back to inline teardown for {strategy_id} (unsafe fallback enabled): {pos_err}"
        )
        fallback_result = await runner._execute_teardown_inline(
            strategy, teardown_intents, teardown_market, start_time, request, state_manager
        )
        return None, fallback_result

    return positions, None


# =============================================================================
# Phase 2: TeardownManager construction
# =============================================================================


def build_teardown_manager(runner: Any, compiler: Any, state_manager: Any) -> tuple[Any, Any | None]:
    """Instantiate the teardown state adapter and ``TeardownManager``.

    Runtime persistence is SQLite in local mode and gateway-backed in hosted
    mode. Returns the pair ``(teardown_manager, teardown_state_adapter)``.

    Prefer an explicit DB path from the StateManager when it's a real
    filesystem path (SQLite only).

    VIB-3773: builds a :class:`TeardownRunnerHelpers` bag and threads it
    into the manager so per-intent commit + pre/post snapshot bracket
    fire on the runner's full accounting pipeline. Without this, the
    teardown lane bypasses every accounting writer (the original April-29
    silent-failure class).
    """
    from ..teardown import create_teardown_state_adapter_for_runtime
    from ..teardown.runner_helpers import build_runner_helpers
    from ..teardown.teardown_manager import TeardownManager

    _raw_db_path = getattr(state_manager, "db_path", None)
    _adapter_db_path = _raw_db_path if isinstance(_raw_db_path, str | _Path) else None
    teardown_state_adapter = create_teardown_state_adapter_for_runtime(
        gateway_client=runner._get_gateway_client(),
        sqlite_path=_adapter_db_path,
    )

    teardown_mgr = TeardownManager(
        orchestrator=runner.execution_orchestrator,
        compiler=compiler,
        alert_manager=runner.alert_manager,
        state_manager=teardown_state_adapter,
        runner_helpers=build_runner_helpers(runner),
    )
    return teardown_mgr, teardown_state_adapter


# =============================================================================
# Phase 3: Safety validation
# =============================================================================


def validate_safety_or_error(
    runner: Any,
    teardown_mgr: Any,
    strategy: StrategyProtocol,
    positions: Any,
    teardown_mode: TeardownMode,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> IterationResult | None:
    """Run safety validation. Returns ``None`` on pass, or a pre-built
    ``IterationResult(STRATEGY_ERROR)`` on fail. On fail, also emits the
    ``mark_failed`` row (if request present) and the teardown-failure
    shutdown side effect.
    """
    from .runner_models import IterationStatus

    strategy_id = strategy.strategy_id
    validation = teardown_mgr.safety_guard.validate_teardown_request(positions, teardown_mode)
    if validation.all_passed:
        return None

    logger.error(f"🛑 Teardown safety validation failed: {validation.blocked_reason}")
    if request:
        from .runner_teardown import _safe_mark

        _safe_mark(
            state_manager,
            "mark_failed",
            strategy_id,
            error=f"Safety validation failed: {validation.blocked_reason}",
        )
    runner._request_teardown_failure_shutdown(f"Teardown safety validation failed: {validation.blocked_reason}")
    return runner._create_error_result(
        strategy_id,
        IterationStatus.STRATEGY_ERROR,
        f"Teardown safety validation failed: {validation.blocked_reason}",
        start_time,
    )


# =============================================================================
# Phase 4: Cancel window + state persist
# =============================================================================


async def run_cancel_window_and_persist(
    runner: Any,
    teardown_mgr: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_mode: TeardownMode,
    is_auto_mode: bool,
    start_time: datetime,
) -> tuple[TeardownState | None, IterationResult | None]:
    """Persist state, run the cancel window, and — on non-cancelled flow —
    transition state to EXECUTING.

    Returns
    -------
    (teardown_state, cancel_result):
        - On non-cancelled: ``teardown_state`` is the EXECUTING state;
          ``cancel_result`` is ``None``.
        - On cancelled: ``teardown_state`` is ``None``; ``cancel_result`` is a
          pre-built TEARDOWN ``IterationResult`` the caller must return.
    """
    from ..teardown.models import TeardownStatus
    from .runner_models import IterationResult, IterationStatus

    strategy_id = strategy.strategy_id

    teardown_id = f"td_{uuid.uuid4().hex[:12]}"
    teardown_state = await teardown_mgr._persist_state(
        teardown_id=teardown_id,
        strategy=strategy,
        mode=teardown_mode,
        intents=teardown_intents,
    )

    # Run cancel window — gives operator time to abort
    cancel_result = await teardown_mgr.cancel_window.run_cancel_window(
        teardown_id=teardown_id,
        is_auto_mode=is_auto_mode,
    )
    if cancel_result.was_cancelled:
        logger.info(f"🛑 Teardown {teardown_id} cancelled during window")
        runner._record_success()
        short_circuit = IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
        return None, short_circuit

    # Update state to EXECUTING after cancel window
    teardown_state.status = TeardownStatus.EXECUTING
    if teardown_mgr.state_manager:
        await teardown_mgr.state_manager.save_teardown_state(teardown_state)
    return teardown_state, None


# =============================================================================
# Phase 5: Price oracle resolution (pure helper)
# =============================================================================


def resolve_price_oracle(teardown_market: Any | None) -> dict | None:
    """Extract a price oracle dict from the market snapshot, falling back to
    stablecoin defaults when the market is missing or returns an empty/None
    mapping. Note: an empty ``{}`` falls through to the fallback because
    ``if not price_oracle`` treats empty dicts as falsy. The fallback itself
    may return ``None`` if no stablecoins are resolvable — mirrors the
    pre-extraction behavior at the call site in ``execute_teardown_via_manager``.
    """
    from .runner_teardown import get_fallback_teardown_prices

    price_oracle: dict | None = None
    if teardown_market is not None and hasattr(teardown_market, "get_price_oracle_dict"):
        fetched = teardown_market.get_price_oracle_dict()
        price_oracle = fetched if fetched is not None else None
    if not price_oracle:
        price_oracle = get_fallback_teardown_prices(teardown_market)
    return price_oracle


# =============================================================================
# Phase 6: Execute intents + post-execution verify
# =============================================================================


async def execute_and_verify(
    runner: Any,
    teardown_mgr: Any,
    teardown_state_adapter: Any,
    teardown_state: TeardownState,
    strategy: StrategyProtocol,
    teardown_intents: list,
    positions: Any,
    teardown_mode: TeardownMode,
    teardown_market: Any | None,
    is_auto_mode: bool,
    price_oracle: dict | None,
    request: Any | None,
    state_manager: Any,
) -> TeardownResult:
    """Run ``_execute_intents`` and the post-execution closure verification.

    Fail-closed (VIB-2925): if execution succeeded but positions remain,
    mark the ``TeardownResult`` as failed and persist ``TeardownStatus.FAILED``
    so the SQLite row reflects reality. Skip verification when execution
    already failed — the original error is more actionable than
    "positions still open".

    Verify exceptions are caught here so they don't discard successful
    on-chain execution stats in the ``TeardownResult``.

    Returns the (possibly-replaced) ``TeardownResult``. Does NOT handle
    alerts or cleanup — those are pipelined after this helper.
    """
    from ..teardown.models import TeardownStatus
    from .runner_teardown import _make_approval_callback, _safe_mark

    strategy_id = strategy.strategy_id

    # Build approval callback for slippage escalation (VIB-2927).
    # Only wire for manual mode — auto mode uses hard slippage limits.
    # Both local SQLite and hosted Postgres adapters publish the
    # approval channel through the same Protocol (VIB-4049), so the
    # callback wires the same way in both modes.
    #
    # If a manual teardown reaches this point without an adapter, we must NOT
    # silently downgrade the request — slippage escalation has to be gated by
    # operator consent. Fail fast instead.
    approval_callback = None
    if not is_auto_mode:
        if teardown_state_adapter is None:
            raise RuntimeError(
                "Manual teardown requires a teardown state adapter for the operator "
                "approval channel — refusing to proceed without slippage-escalation gating. "
                "Check that the hosted gateway is reachable, or that the strategy folder "
                "is resolvable in local mode."
            )
        approval_callback = _make_approval_callback(runner, teardown_state_adapter)

    # Execute intents with escalating slippage
    teardown_result = await teardown_mgr._execute_intents(
        teardown_id=teardown_state.teardown_id,
        strategy=strategy,
        intents=teardown_intents,
        positions=positions,
        mode=teardown_mode,
        teardown_state=teardown_state,
        on_approval_needed=approval_callback,
        is_auto_mode=is_auto_mode,
        price_oracle=price_oracle,
        market=teardown_market,
    )

    # Post-execution verification: check positions are actually closed.
    if teardown_result.success:
        verify_error_msg: str | None = None
        try:
            # VIB-3742: thread the pre-execution ``positions`` snapshot so
            # protocol-specific on-chain post-condition checks can run for
            # each position the teardown should have closed.
            positions_closed = await teardown_mgr._verify_closure(
                strategy,
                pre_execution_positions=positions,
            )
        except Exception as verify_err:
            logger.exception(
                "Post-teardown verification raised for %s — treating as verify-fail",
                strategy_id,
            )
            positions_closed = False
            verify_error_msg = f"Post-teardown verification error: {verify_err}. Manual check required."

        if not positions_closed:
            if verify_error_msg is None:
                verify_error_msg = "Post-teardown verification failed: positions still open. Manual check required."
            logger.warning(f"Post-teardown verification: {strategy_id} incomplete. Marking as failed.")
            teardown_result = replace(
                teardown_result,
                success=False,
                error=verify_error_msg,
                recovery_options=["Verify positions on-chain", "Re-run teardown"],
            )
            # Persist the failure so the SQLite row reflects reality —
            # `_execute_intents` already set status=COMPLETED; flip it to
            # FAILED so a postmortem reader doesn't see a row claiming
            # success while the teardown actually failed. Hosted mode has
            # no SQLite adapter (build_teardown_manager returns None);
            # the platform owns teardown lifecycle tracking there.
            teardown_state.status = TeardownStatus.FAILED
            teardown_state.updated_at = datetime.now(UTC)
            if teardown_state_adapter is not None:
                try:
                    await teardown_state_adapter.save_teardown_state(teardown_state)
                except Exception:
                    logger.warning(
                        "Failed to persist FAILED status for teardown %s after verify-fail",
                        teardown_state.teardown_id,
                        exc_info=True,
                    )
            if request:
                _safe_mark(state_manager, "mark_failed", strategy_id, error=verify_error_msg)

    return teardown_result


async def send_alert_and_cleanup(teardown_mgr: Any, teardown_result: TeardownResult, teardown_id: str) -> None:
    """Send completion alert (on success) and clean up persisted state.

    Both operations are best-effort — exceptions are logged and swallowed.
    """
    if teardown_mgr.alert_manager and teardown_result.success:
        try:
            await teardown_mgr.alert_manager.send_teardown_complete(teardown_result)
        except Exception as alert_err:
            logger.warning(f"Failed to send teardown completion alert: {alert_err}")

    if teardown_mgr.state_manager and teardown_result.success:
        try:
            await teardown_mgr.state_manager.delete_teardown_state(teardown_id)
        except Exception as cleanup_err:
            logger.warning(f"Failed to clean up teardown state: {cleanup_err}")


# =============================================================================
# Phase 7: Exception-handler for the outer try
# =============================================================================


async def handle_executor_exception(
    runner: Any,
    strategy: StrategyProtocol,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
    teardown_state: TeardownState | None,
    teardown_state_adapter: Any | None,
    exc: Exception,
) -> IterationResult:
    """Side effects for the outer try/except in ``execute_teardown_via_manager``.

    Logs the error, marks the state-manager row as failed (if a request is
    present), reflects the failure in the ``TeardownStateAdapter`` row (best
    effort — the exception may have fired before the state row or adapter was
    initialized), and requests a teardown-failure shutdown. Returns a pre-
    built STRATEGY_ERROR ``IterationResult`` the caller must return.
    """
    from ..teardown.models import TeardownStatus as _TS
    from .runner_models import IterationStatus
    from .runner_teardown import _safe_mark

    strategy_id = strategy.strategy_id
    logger.error(f"🛑 TeardownManager execution failed for {strategy_id}: {exc}")
    if request:
        _safe_mark(state_manager, "mark_failed", strategy_id, error=str(exc))

    # Best effort: reflect the failure in the adapter's row so postmortem
    # readers don't see an EXECUTING teardown_execution_state row paired
    # with a FAILED teardown_requests row.
    try:
        if teardown_state is not None and teardown_state_adapter is not None:
            teardown_state.status = _TS.FAILED
            teardown_state.updated_at = datetime.now(UTC)
            await teardown_state_adapter.save_teardown_state(teardown_state)
    except Exception:
        logger.warning(
            "Failed to persist FAILED teardown_execution_state for %s after exception",
            strategy_id,
            exc_info=True,
        )
    runner._request_teardown_failure_shutdown(str(exc))
    return runner._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, str(exc), start_time)


# =============================================================================
# Phase 8: Final TeardownResult -> IterationResult mapping
# =============================================================================


def map_teardown_result(
    runner: Any,
    strategy: StrategyProtocol,
    start_time: datetime,
    teardown_result: TeardownResult,
    teardown_mode: TeardownMode,
    request: Any | None,
    state_manager: Any,
) -> IterationResult:
    """Map a TeardownResult to the runner's IterationResult, firing the
    terminal side effects (shutdown, lifecycle write, mark_completed /
    mark_failed + teardown-failure shutdown).
    """
    from ..teardown import TeardownMode
    from .runner_models import IterationResult, IterationStatus
    from .runner_teardown import _safe_mark

    strategy_id = strategy.strategy_id
    mode_str = "graceful" if teardown_mode == TeardownMode.SOFT else "emergency"

    if teardown_result.success:
        logger.info(
            f"🛑 {strategy_id} teardown complete via TeardownManager "
            f"({teardown_result.intents_succeeded}/{teardown_result.intents_total} intents, "
            f"{teardown_result.duration_seconds:.1f}s)"
        )
        runner.request_shutdown()
        runner._lifecycle_write_state(strategy_id, "TERMINATED")
        if request:
            _safe_mark(
                state_manager,
                "mark_completed",
                strategy_id,
                result={
                    "intents": teardown_result.intents_succeeded,
                    "mode": mode_str,
                    "duration_s": teardown_result.duration_seconds,
                },
            )
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    logger.warning(f"🛑 {strategy_id} teardown incomplete via TeardownManager: {teardown_result.error}")
    if request:
        _safe_mark(state_manager, "mark_failed", strategy_id, error=teardown_result.error or "teardown failed")
    runner._request_teardown_failure_shutdown(teardown_result.error or "teardown failed")
    return IterationResult(
        status=IterationStatus.STRATEGY_ERROR,
        error=teardown_result.error,
        strategy_id=strategy_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )
