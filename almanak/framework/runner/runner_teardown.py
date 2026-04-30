"""Teardown execution methods for StrategyRunner.

Extracted from strategy_runner.py for maintainability. Each function takes
``runner`` (a StrategyRunner instance) as its first argument and is called
via a thin delegation stub in StrategyRunner.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from ..intents.compiler import IntentCompiler, IntentCompilerConfig
from ..intents.vocabulary import Intent

if TYPE_CHECKING:
    from ..teardown import TeardownMode
    from .runner_models import IterationResult, StrategyProtocol

# Use the original strategy_runner logger so existing log-capture tests and
# log-filtering rules continue to work after the extraction.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# -------------------------------------------------------------------------
# Approval callback for slippage escalation (VIB-2927)
# -------------------------------------------------------------------------

# Valid approval actions. Defensive against typos or legacy payloads.
# "approve" and "continue" both mean "accept this level"; "wait_and_escalate"
# advances to the next slippage level after a pause; "cancel" aborts.
_VALID_APPROVAL_ACTIONS = {"approve", "continue", "wait_and_escalate", "cancel"}

# Teardown request sources that imply a human operator is actively watching.
# Everything else — including ``request=None`` and unknown future sources —
# is treated as auto mode. Fail-closed: adding a new source requires an
# explicit decision to put it on this list, so a new source cannot silently
# start blocking on approvals no one will give. Tests import this constant
# directly so the taxonomy cannot drift between runtime and test expectations.
_MANUAL_TEARDOWN_REQUESTERS: frozenset[str] = frozenset({"cli", "dashboard", "dashboard_api"})


def derive_teardown_auto_mode(request: Any) -> bool:
    """Return True when teardown should run in auto mode (no approval callback).

    Exposed as a standalone helper so tests can exercise the predicate directly
    instead of re-implementing it. The production call site in
    ``execute_teardown_via_manager`` uses this function as well.

    Rules:
    - ``request is None`` → auto (strategy self-signalled; no operator present)
    - ``requested_by`` in ``_MANUAL_TEARDOWN_REQUESTERS`` → manual
    - everything else (including unknown future sources) → auto (fail-closed)
    """
    if request is None:
        return True
    return getattr(request, "requested_by", None) not in _MANUAL_TEARDOWN_REQUESTERS


# Safe default when an approval payload is missing or malformed. Cancelling a
# teardown on malformed input would be destructive (operator loses the chance
# to approve). wait_and_escalate advances through the EscalatingSlippageManager
# under auto-approve rules, which is the safe fallback.
_SAFE_DEFAULT_APPROVAL_ACTION = "wait_and_escalate"

# Poll interval for the approval SQLite channel. Short enough to feel responsive
# to an operator click; long enough to avoid hammering SQLite.
_APPROVAL_POLL_INTERVAL_S = 5.0

# Fallback approval deadline when ApprovalRequest.expires_at is None. Matches
# the typical escalation-level window in EscalatingSlippageManager.
_APPROVAL_DEFAULT_TIMEOUT = timedelta(minutes=30)


def _parse_approval_response(response_json: str, teardown_id: str) -> Any:
    """Parse an approval response JSON string into an ApprovalResponse.

    Defensive against malformed payloads: JSON errors and unknown actions are
    logged and treated as wait_and_escalate (safe default) rather than cancel.
    """
    from ..teardown.models import ApprovalResponse

    try:
        data = json.loads(response_json)
    except json.JSONDecodeError as e:
        logger.error(
            "Malformed approval response JSON for teardown %s (%s); treating as %s",
            teardown_id,
            e,
            _SAFE_DEFAULT_APPROVAL_ACTION,
        )
        return ApprovalResponse(
            approved=False,
            teardown_id=teardown_id,
            action=_SAFE_DEFAULT_APPROVAL_ACTION,
        )

    if not isinstance(data, dict):
        logger.error(
            "Approval response for teardown %s is not a JSON object; treating as %s",
            teardown_id,
            _SAFE_DEFAULT_APPROVAL_ACTION,
        )
        return ApprovalResponse(
            approved=False,
            teardown_id=teardown_id,
            action=_SAFE_DEFAULT_APPROVAL_ACTION,
        )

    action = data.get("action")
    if action not in _VALID_APPROVAL_ACTIONS:
        logger.error(
            "Approval response for teardown %s has unknown action %r; treating as %s",
            teardown_id,
            action,
            _SAFE_DEFAULT_APPROVAL_ACTION,
        )
        action = _SAFE_DEFAULT_APPROVAL_ACTION

    # Parse `approved` explicitly so `{"approved": "false"}` does not collapse
    # to True via bool() on a non-empty string. Accept bool / canonical string
    # forms and reject everything else.
    approved_raw = data.get("approved", False)
    if isinstance(approved_raw, bool):
        approved = approved_raw
    elif isinstance(approved_raw, str):
        approved = approved_raw.strip().lower() in {"true", "1", "yes"}
    elif isinstance(approved_raw, int | float):
        approved = bool(approved_raw)
    else:
        approved = False

    # Parse approved_slippage defensively — an invalid Decimal string would
    # have crashed the callback and fallen into the outer try/except of the
    # approval loop. Fall back to safe-default action instead.
    approved_slippage: Decimal | None = None
    approved_slippage_raw = data.get("approved_slippage")
    if approved_slippage_raw is not None:
        try:
            approved_slippage = Decimal(str(approved_slippage_raw))
        except (InvalidOperation, TypeError, ValueError):
            logger.error(
                "Approval response for teardown %s has invalid approved_slippage %r; treating as %s",
                teardown_id,
                approved_slippage_raw,
                _SAFE_DEFAULT_APPROVAL_ACTION,
            )
            action = _SAFE_DEFAULT_APPROVAL_ACTION
            approved = False

    return ApprovalResponse(
        approved=approved,
        teardown_id=teardown_id,
        action=action,
        approved_slippage=approved_slippage,
    )


def _make_approval_callback(runner: Any, state_adapter: Any):
    """Create the approval callback wired to the shared SQLite channel.

    Flow:
    1. On escalation, write a row to ``teardown_approvals`` keyed by
       ``(teardown_id, level)``. Operator responds by writing to the same row
       via the teardown API or CLI (both go through the same adapter).
    2. Send an alert so the operator knows to look.
    3. Poll the row until a response arrives or the expiry deadline passes.
       Uses ``time.monotonic()`` for the deadline — wall-clock skew or NTP
       adjustments must not extend or truncate the window unexpectedly.
    4. Parse the response defensively; unknown or malformed actions fall back
       to ``wait_and_escalate`` (safe escalation) instead of ``cancel``.
    5. On timeout, auto-escalate via ``wait_and_escalate`` rather than cancelling.
    """

    async def on_approval_needed(request):
        # Expiry fallback: ApprovalRequest.expires_at is typed Optional, so don't
        # crash if a future caller omits it.
        expires_at = request.expires_at or datetime.now(UTC) + _APPROVAL_DEFAULT_TIMEOUT
        timeout_s = max(0.0, (expires_at - datetime.now(UTC)).total_seconds())
        monotonic_deadline = time.monotonic() + timeout_s

        # Persist the request. Include fields that let the operator make an
        # informed decision (age of request, next-level slippage if declined).
        await asyncio.to_thread(
            state_adapter.create_approval_request,
            teardown_id=request.teardown_id,
            strategy_id=request.strategy_id,
            level=request.current_level,
            request_json=json.dumps(
                {
                    "teardown_id": request.teardown_id,
                    "strategy_id": request.strategy_id,
                    "current_level": request.current_level.value
                    if hasattr(request.current_level, "value")
                    else str(request.current_level),
                    "current_slippage": str(request.current_slippage),
                    "estimated_loss_usd": str(request.estimated_loss_usd),
                    "position_value_usd": str(request.position_value_usd),
                    "reason": request.reason,
                    "options": request.options,
                    "requested_at": request.requested_at.isoformat()
                    if getattr(request, "requested_at", None)
                    else datetime.now(UTC).isoformat(),
                    "expires_at": expires_at.isoformat(),
                }
            ),
            expires_at=expires_at.isoformat(),
        )

        # Alert operator. Failure to alert is serious — without the alert, the
        # operator may never know approval is waiting. Log as error with stack.
        if runner.alert_manager:
            try:
                await runner.alert_manager.send_approval_needed(request)
            except Exception:
                logger.error(
                    "Failed to send approval alert for teardown %s — operator may be unaware",
                    request.teardown_id,
                    exc_info=True,
                )

        logger.info(
            "Approval required for teardown %s (level %s): %s. Polling every %.1fs up to %s...",
            request.teardown_id,
            request.current_level,
            request.reason,
            _APPROVAL_POLL_INTERVAL_S,
            expires_at.isoformat(),
        )

        # Poll the SQLite channel until response or monotonic timeout.
        while time.monotonic() < monotonic_deadline:
            response_json = await asyncio.to_thread(
                state_adapter.get_approval_response,
                request.teardown_id,
                request.current_level,
            )
            if response_json:
                return _parse_approval_response(response_json, request.teardown_id)
            await asyncio.sleep(_APPROVAL_POLL_INTERVAL_S)

        # Timeout — try to mark the row resolved so a late API response
        # doesn't land on this stale level. The UPDATE uses
        # `WHERE response_json IS NULL`, so if an operator responded in the
        # final sleep gap their response wins and our timeout write returns
        # False. In that case, read the row back and honour the real response
        # instead of auto-escalating.
        timeout_payload = json.dumps(
            {
                "approved": False,
                "action": _SAFE_DEFAULT_APPROVAL_ACTION,
                "timeout": True,
            }
        )
        wrote_timeout: bool | None = None
        try:
            wrote_timeout = await asyncio.to_thread(
                state_adapter.write_approval_response,
                request.teardown_id,
                request.current_level,
                timeout_payload,
            )
        except Exception:
            logger.warning(
                "Failed to mark approval row resolved on timeout for teardown %s (level %s); "
                "a late operator response may land on this stale row",
                request.teardown_id,
                request.current_level,
                exc_info=True,
            )

        if wrote_timeout is False:
            # Someone else wrote first — likely a just-in-time operator response.
            # Read it back and honour it.
            try:
                late_response = await asyncio.to_thread(
                    state_adapter.get_approval_response,
                    request.teardown_id,
                    request.current_level,
                )
            except Exception:
                late_response = None
                logger.warning(
                    "Failed to re-read approval row after timeout-write race for teardown %s",
                    request.teardown_id,
                    exc_info=True,
                )
            if late_response:
                logger.info(
                    "Late operator response beat timeout write for teardown %s (level %s); honouring it",
                    request.teardown_id,
                    request.current_level,
                )
                return _parse_approval_response(late_response, request.teardown_id)

        from ..teardown.models import ApprovalResponse

        logger.warning(
            "Approval timeout for teardown %s (level %s). Auto-escalating to next slippage level.",
            request.teardown_id,
            request.current_level,
        )
        return ApprovalResponse(
            approved=False,
            teardown_id=request.teardown_id,
            action=_SAFE_DEFAULT_APPROVAL_ACTION,
        )

    return on_approval_needed


def _safe_mark(state_manager: Any, method_name: str, strategy_id: str, **kwargs: Any) -> None:
    """Call a ``mark_*`` state-manager method, swallowing any persistence error.

    ``mark_completed`` / ``mark_failed`` / ``mark_cancelled`` touch SQLite and
    can fail transiently (lock contention, disk full). A failure here must NOT
    crash the runner — the teardown has already run to its terminal state in
    memory. Log and continue.
    """
    if state_manager is None:
        return
    method = getattr(state_manager, method_name, None)
    if method is None:
        return
    try:
        method(strategy_id, **kwargs)
    except Exception:
        logger.warning(
            "Failed to call %s for strategy %s (non-fatal)",
            method_name,
            strategy_id,
            exc_info=True,
        )


# -------------------------------------------------------------------------
# Main teardown entry point
# -------------------------------------------------------------------------


async def execute_teardown(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_mode: TeardownMode,
    start_time: datetime,
) -> IterationResult:
    """Execute teardown, routing through TeardownManager when possible.

    For single-chain strategies, delegates to TeardownManager which provides:
    - Position-aware loss caps (1-3% based on position size)
    - Escalating slippage tolerance (tight -> loose with approval gates)
    - Cancel window (configurable, default 10 seconds)
    - Post-execution verification (checks positions are actually closed)
    - State persistence for resumability

    For multi-chain strategies, uses the inline execution path (TeardownManager
    does not yet support multi-chain orchestration).

    Args:
        runner: StrategyRunner instance
        strategy: The strategy to teardown
        teardown_mode: SOFT (graceful) or HARD (emergency)
        start_time: When the iteration started

    Returns:
        IterationResult with teardown status
    """
    from ..deployment import is_hosted
    from ..local_paths import LocalPathError
    from ..teardown import get_teardown_state_manager
    from .runner_models import IterationResult, IterationStatus

    strategy_id = strategy.strategy_id
    # The local teardown state manager is the cross-process approval
    # channel between the SDK CLI/API and the runner — it lives in
    # SQLite next to the runner's local DB (VIB-3761). In hosted mode
    # the channel is owned by the gateway/Postgres, so the local
    # manager is not instantiated and this helper raises
    # ``LocalPathError``. Treat hosted mode as "no pending operator
    # request" — auto-mode below will derive ``is_auto_mode=True`` from
    # ``request is None`` and the rest of the unwind proceeds.
    #
    # In *local* mode, ``LocalPathError`` is genuinely unexpected — it
    # means a path-resolution helper rejected the call for a reason
    # other than "hosted mode" (e.g., a misconfigured strategy folder).
    # Re-raise so the operator sees the misconfiguration instead of
    # silently disabling the operator-approval flow.
    manager: Any = None
    request: Any = None
    try:
        manager = get_teardown_state_manager()
        request = manager.get_active_request(strategy_id)
    except LocalPathError:
        # Local mode: ``LocalPathError`` here means a path-helper rejected
        # the call for a reason other than "hosted mode" (typically a
        # misconfigured ``ALMANAK_STRATEGY_FOLDER``). Re-raise so the
        # operator sees the misconfiguration rather than silently
        # disabling the operator-approval flow.
        if not is_hosted():
            raise
        logger.debug(
            "execute_teardown[%s]: local teardown state manager unavailable "
            "(hosted mode); proceeding with auto-mode unwind",
            strategy_id,
        )

    # Step T1: Create market snapshot (SAME as normal decide() path)
    teardown_market = None
    try:
        teardown_market = strategy.create_market_snapshot()
        if hasattr(teardown_market, "get_price_oracle_dict"):
            logger.debug(
                f"Created market snapshot for teardown with prices: "
                f"{list(teardown_market.get_price_oracle_dict().keys())}"
            )
        else:
            logger.debug("Created multi-chain market snapshot for teardown")
    except Exception as e:
        logger.warning(f"Failed to create market snapshot for teardown: {e}. Continuing without market data.")

    # Step T2: Generate teardown intents WITH market (symmetric with decide(market))
    try:
        try:
            teardown_intents = strategy.generate_teardown_intents(teardown_mode, market=teardown_market)
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            # Backward compat: old-style signature def generate_teardown_intents(self, mode)
            logger.debug(f"Strategy {strategy_id} uses old teardown signature (no market param), falling back")
            teardown_intents = strategy.generate_teardown_intents(teardown_mode)
    except Exception as e:
        logger.error(f"Failed to generate teardown intents for {strategy_id}: {e}")
        if request:
            _safe_mark(manager, "mark_failed", strategy_id, error=str(e))
        runner._request_teardown_failure_shutdown(str(e))
        return runner._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, str(e), start_time)

    if not teardown_intents:
        logger.info(f"🛑 {strategy_id} teardown complete (no positions to close)")
        if request:
            _safe_mark(manager, "mark_completed", strategy_id, result={"reason": "no_positions"})
        runner.request_shutdown()
        # Match the adjacent all-balances-zero + TeardownManager-success paths —
        # the lifecycle supervisor must see TERMINATED so it doesn't treat a
        # teardown-with-no-positions as still running.
        runner._lifecycle_write_state(strategy_id, "TERMINATED")
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    logger.info(f"🛑 {strategy_id} entering TEARDOWN mode ({len(teardown_intents)} intents to execute)")
    if request:
        _safe_mark(manager, "mark_started", strategy_id, total_positions=len(teardown_intents))

    # Step T2.5: Pre-fetch prices for tokens in teardown intents
    if teardown_market is not None and hasattr(teardown_market, "price"):
        try:
            prefetch_teardown_prices(teardown_market, teardown_intents)
        except Exception as e:
            logger.warning(f"Failed to pre-fetch teardown prices: {e}")

    # Note: amount="all" resolution is handled lazily inside _execute_intents
    # (per-intent, just before execution) so staged exits work correctly
    # (e.g., withdraw then swap uses tokens produced by the earlier step).

    # Step T2.7: If all intents were resolved away, teardown is complete
    if not teardown_intents:
        logger.info(f"🛑 {strategy_id} teardown complete (all positions already closed)")
        if request:
            _safe_mark(manager, "mark_completed", strategy_id, result={"reason": "all_balances_zero"})
        runner.request_shutdown()
        runner._lifecycle_write_state(strategy_id, "TERMINATED")
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    # Step T3: Execute teardown intents
    if runner._is_multi_chain:
        # Multi-chain: use inline path (TeardownManager doesn't support multi-chain yet)
        result = await runner._execute_multi_chain(
            strategy=strategy,
            intents=teardown_intents,
            start_time=start_time,
            market=teardown_market,
        )
        if result.success:
            result.status = IterationStatus.TEARDOWN
            logger.info(f"🛑 {strategy_id} teardown complete - shutting down strategy runner")
            runner.request_shutdown()
            if request:
                _safe_mark(manager, "mark_completed", strategy_id, result={"intents": len(teardown_intents)})
        else:
            if request:
                _safe_mark(manager, "mark_failed", strategy_id, error=result.error or "execution failed")
            runner._request_teardown_failure_shutdown(result.error or "multi-chain teardown execution failed")
        return result
    else:
        # Single-chain: route through TeardownManager for safety guarantees
        # Call through runner method (not standalone function) so instance-level
        # mock patching in tests continues to work.
        return await runner._execute_teardown_via_manager(
            strategy=strategy,
            teardown_intents=teardown_intents,
            teardown_mode=teardown_mode,
            teardown_market=teardown_market,
            start_time=start_time,
            request=request,
            state_manager=manager,
        )


# -------------------------------------------------------------------------
# TeardownManager path (single-chain)
# -------------------------------------------------------------------------


async def execute_teardown_via_manager(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_mode: TeardownMode,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> IterationResult:
    """Execute single-chain teardown through TeardownManager for full safety.

    TeardownManager provides safety features that the inline path lacks:
    - Position-aware loss caps (1-3% based on portfolio size)
    - Escalating slippage tolerance with operator approval gates
    - Cancel window for operator intervention
    - Post-execution verification (checks positions are closed on-chain)
    - Resumable state persistence

    Falls back to inline sequential execution if TeardownManager cannot
    be initialized (e.g., incompatible orchestrator type).

    Args:
        runner: StrategyRunner instance
        strategy: The strategy to teardown
        teardown_intents: Pre-resolved teardown intents
        teardown_mode: SOFT (graceful) or HARD (emergency)
        teardown_market: Market snapshot (may be None)
        start_time: When the iteration started
        request: Active teardown request from state manager
        state_manager: Teardown state manager for lifecycle tracking
    """
    from ..teardown import TeardownMode
    from . import _teardown_helpers as _h

    strategy_id = strategy.strategy_id
    mode_str = "graceful" if teardown_mode == TeardownMode.SOFT else "emergency"

    # Derive auto mode from teardown request source (VIB-2923). See
    # ``derive_teardown_auto_mode`` at module level for the predicate —
    # exposed there so tests exercise the real logic.
    is_auto_mode = derive_teardown_auto_mode(request)

    # Phase 1: build compiler (or return early/fallback).
    compiler, early = await _h.resolve_compiler_or_fallback(
        runner, strategy, teardown_intents, teardown_market, start_time, request, state_manager
    )
    if compiler is None:
        return early  # type: ignore[return-value]

    # Phase 2: construct TeardownManager + state adapter.
    teardown_mgr, teardown_state_adapter = _h.build_teardown_manager(runner, compiler, state_manager)

    logger.info(
        f"🛑 Routing {strategy_id} teardown through TeardownManager (mode={mode_str}, intents={len(teardown_intents)})"
    )

    # Outer try preserves the original exception contract: any failure in
    # the execution/verify phases (including helpers) is caught here so we
    # can reflect FAILED into both `state_manager` (teardown_requests) and
    # the adapter row (teardown_execution_state). The helper handles both.
    teardown_state = None
    # VIB-3773: track cycle-id swap state so the ``finally`` clause restores
    # the runner's surfaces even on the exception path. ``None`` sentinels
    # mean "no swap performed yet" — we only restore what we set.
    saved_last_cycle_id: str | None = None
    saved_ctx_cycle_id: str | None = None
    cycle_id_swapped = False
    pre_bracket_outcome = None
    post_bracket_outcome = None

    try:
        # Phase 3: fetch positions (or return early/fallback).
        positions, early = await _h.fetch_positions_or_fallback(
            runner, strategy, teardown_intents, teardown_market, start_time, request, state_manager
        )
        if positions is None:
            return early  # type: ignore[return-value]

        # Phase 4: safety validation (loss caps).
        safety_error = _h.validate_safety_or_error(
            runner, teardown_mgr, strategy, positions, teardown_mode, start_time, request, state_manager
        )
        if safety_error is not None:
            return safety_error

        # Phase 5: persist state + cancel window (may short-circuit cancel).
        teardown_state, cancel_short_circuit = await _h.run_cancel_window_and_persist(
            runner, teardown_mgr, strategy, teardown_intents, teardown_mode, is_auto_mode, start_time
        )
        if cancel_short_circuit is not None:
            return cancel_short_circuit
        # Contract: when cancel_short_circuit is None, run_cancel_window_and_persist
        # returns a concrete TeardownState. The assertion narrows the type for
        # mypy so downstream helpers can treat it as non-optional.
        assert teardown_state is not None

        # VIB-3773: swap cycle id on BOTH surfaces (P1-4 — runner_state.py:486
        # reads ``runner._last_cycle_id`` first, then falls back to the
        # contextvar). Without updating ``_last_cycle_id`` the snapshot/metrics
        # rows would be stamped with the iteration's cycle id, not the
        # teardown's. Restored in the ``finally`` clause.
        teardown_cycle_id = f"teardown-{teardown_state.teardown_id}"
        from ..observability.context import (
            get_cycle_id,
            set_cycle_id,
        )

        saved_last_cycle_id = getattr(runner, "_last_cycle_id", "") or ""
        saved_ctx_cycle_id = get_cycle_id()
        runner._last_cycle_id = teardown_cycle_id
        set_cycle_id(teardown_cycle_id)
        cycle_id_swapped = True

        # Phase 6: price oracle resolution (pure helper).
        price_oracle = _h.resolve_price_oracle(teardown_market)

        # VIB-3773 Phase 6.5: pre-teardown snapshot bracket. Fires once,
        # before the first intent runs, so operators see "starting
        # balances at teardown_t0" in ``portfolio_snapshots`` /
        # ``portfolio_metrics``. Degraded-but-continue: a backend write
        # failure here logs ERROR + appends to deferred-write log; the
        # teardown still runs.
        if teardown_mgr.runner_helpers.has_snapshot:
            pre_bracket_outcome = await teardown_mgr.runner_helpers.capture_snapshot(
                strategy,
                teardown_cycle_id=teardown_cycle_id,
                pre_teardown=True,
            )
            if pre_bracket_outcome.accounting_degraded:
                logger.error(
                    "Pre-teardown snapshot accounting degraded for %s — %s",
                    strategy_id,
                    pre_bracket_outcome.degraded_reason or "unknown",
                )

        # Phase 7: execute intents + post-execution verify.
        teardown_result = await _h.execute_and_verify(
            runner,
            teardown_mgr,
            teardown_state_adapter,
            teardown_state,
            strategy,
            teardown_intents,
            positions,
            teardown_mode,
            teardown_market,
            is_auto_mode,
            price_oracle,
            request,
            state_manager,
        )

        # VIB-3773 Phase 7.5: post-teardown snapshot bracket. Fires after
        # the unwind (and the verifier) completes so the SDK records
        # final wallet state — the row that fund-NAV / dashboards
        # actually need post-shutdown. Same degraded-but-continue
        # contract.
        if teardown_mgr.runner_helpers.has_snapshot:
            post_bracket_outcome = await teardown_mgr.runner_helpers.capture_snapshot(
                strategy,
                teardown_cycle_id=teardown_cycle_id,
                pre_teardown=False,
            )
            if post_bracket_outcome.accounting_degraded:
                logger.error(
                    "Post-teardown snapshot accounting degraded for %s — %s",
                    strategy_id,
                    post_bracket_outcome.degraded_reason or "unknown",
                )

        # VIB-3773: fold the snapshot-bracket degraded signals into the
        # TeardownResult. Per-intent commits already wrote their flags
        # inside ``_execute_intents``; the brackets are additive.
        bracket_failures = sum(
            1 for o in (pre_bracket_outcome, post_bracket_outcome) if o is not None and o.accounting_degraded
        )
        if bracket_failures:
            teardown_result.accounting_degraded = True
            teardown_result.accounting_degraded_count += bracket_failures

        # Phase 8: alert + cleanup (best effort, swallow exceptions).
        await _h.send_alert_and_cleanup(teardown_mgr, teardown_result, teardown_state.teardown_id)

    except Exception as e:
        return await _h.handle_executor_exception(
            runner,
            strategy,
            start_time,
            request,
            state_manager,
            teardown_state,
            teardown_state_adapter,
            e,
        )
    finally:
        # VIB-3773: restore both cycle-id surfaces no matter how we exit
        # (success, return-early, or exception). Skipping this would leak
        # ``teardown-...`` cycle ids onto subsequent iteration rows.
        if cycle_id_swapped:
            from ..observability.context import (
                clear_cycle_id as _clear_cycle_id,
            )
            from ..observability.context import (
                set_cycle_id as _set_cycle_id,
            )

            runner._last_cycle_id = saved_last_cycle_id or ""
            if saved_ctx_cycle_id is None:
                _clear_cycle_id()
            else:
                _set_cycle_id(saved_ctx_cycle_id)

    # Phase 9: map TeardownResult -> IterationResult + terminal side effects.
    return _h.map_teardown_result(runner, strategy, start_time, teardown_result, teardown_mode, request, state_manager)


# -------------------------------------------------------------------------
# Inline teardown fallback
# -------------------------------------------------------------------------


async def execute_teardown_inline(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> IterationResult:
    """Fallback inline teardown execution (no TeardownManager safety features).

    Used when TeardownManager cannot be initialized (e.g., incompatible
    orchestrator type or missing compiler dependencies).

    Executes teardown intents sequentially via _execute_single_chain.

    VIB-3773: this lane reuses the runner's iteration-lane writers
    (``_execute_single_chain`` → ``_write_ledger_entry`` →
    ``_write_outbox_and_fire_processor`` → sidecar) so ledger / position
    event / outbox / sidecar all fire. Two gaps closed here:

    1. Snapshot/metrics weren't written — the iteration wrapper that
       drives ``capture_snapshot_with_accounting`` is at run_loop level,
       not inside ``_execute_single_chain``. We bracket the inline loop
       with the same teardown snapshot helper Lane B uses.
    2. Live-mode AccountingPersistenceError would have halted the
       unwind (iteration semantics). Teardown's degraded-but-continue
       contract (P0-2) requires we catch + log + record + continue.
    """
    import uuid

    from ..accounting.deferred_log import append_now as _deferred_append_now
    from ..observability.context import (
        clear_cycle_id,
        get_cycle_id,
        set_cycle_id,
    )
    from ..teardown.runner_helpers import build_runner_helpers

    strategy_id = strategy.strategy_id

    # VIB-3773: dual cycle-id swap so ledger/outbox/snapshot rows carry the
    # teardown's cycle id, not the iteration that triggered the inline path.
    # Per blueprint 27 §teardown-cycle-id contract, every teardown row must
    # carry ``cycle_id = f"teardown-{teardown_id}"`` (canonical format
    # consumed by reconciliation queries). When the runner is invoked
    # without a persisted ``TeardownRequest`` (no operator-initiated
    # request, e.g. strategy-self-signalled hosted teardown), synthesize a
    # UUID so the lane still produces a unique correlation id — but keep
    # the ``teardown-`` prefix and avoid the ``-inline-`` infix that the
    # earlier draft used; reconciliation walks ``cycle_id LIKE 'teardown-%'``
    # and any divergence breaks correlation across the 5 accounting tables.
    teardown_id = getattr(request, "teardown_id", None) or str(uuid.uuid4())
    teardown_cycle_id = f"teardown-{teardown_id}"
    saved_last_cycle_id = getattr(runner, "_last_cycle_id", "") or ""
    saved_ctx_cycle_id = get_cycle_id()
    runner._last_cycle_id = teardown_cycle_id
    set_cycle_id(teardown_cycle_id)

    # VIB-3773: snapshot bracket (degraded-but-continue, never raises).
    helpers = build_runner_helpers(runner)
    accounting_degraded_count = 0

    try:
        if helpers.has_snapshot:
            pre_outcome = await helpers.capture_snapshot(  # type: ignore[misc]
                strategy,
                teardown_cycle_id=teardown_cycle_id,
                pre_teardown=True,
            )
            if pre_outcome.accounting_degraded:
                accounting_degraded_count += 1
                logger.error(
                    "🛑 Pre-teardown (inline) snapshot accounting degraded for %s — %s",
                    strategy_id,
                    pre_outcome.degraded_reason or "unknown",
                )

        result, inline_degraded = await _execute_teardown_inline_body(
            runner,
            strategy,
            teardown_intents,
            teardown_market,
            start_time,
            request,
            state_manager,
            teardown_cycle_id=teardown_cycle_id,
            deferred_append=_deferred_append_now,
        )
        # ``_execute_teardown_inline_body`` returns its own IterationResult
        # alongside the per-intent inline_degraded_count; we only need to
        # fire the post-snapshot bracket and tally degraded counts.
        # Capture-failures in the body are accumulated through the same
        # channel as bracket failures.
        accounting_degraded_count += inline_degraded

        if helpers.has_snapshot:
            post_outcome = await helpers.capture_snapshot(  # type: ignore[misc]
                strategy,
                teardown_cycle_id=teardown_cycle_id,
                pre_teardown=False,
            )
            if post_outcome.accounting_degraded:
                accounting_degraded_count += 1
                logger.error(
                    "🛑 Post-teardown (inline) snapshot accounting degraded for %s — %s",
                    strategy_id,
                    post_outcome.degraded_reason or "unknown",
                )
        if accounting_degraded_count and getattr(result, "error", None) is None:
            # Annotate the IterationResult so operators see the degraded
            # signal without needing to grep the deferred log.
            result.error = (
                f"accounting_degraded={accounting_degraded_count} (chain-side OK; "
                "see accounting_deferred.jsonl for failed writes)"
            )
        return result
    finally:
        # Restore both cycle-id surfaces no matter how we exit.
        runner._last_cycle_id = saved_last_cycle_id
        if saved_ctx_cycle_id is None:
            clear_cycle_id()
        else:
            set_cycle_id(saved_ctx_cycle_id)


async def _execute_teardown_inline_body(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
    *,
    teardown_cycle_id: str,
    deferred_append: Any,
) -> tuple[IterationResult, int]:
    """Inner loop of the inline teardown — the body that the brackets wrap.

    Catches per-intent ``AccountingPersistenceError`` so the chain-side
    unwind continues even when the runner's iteration-lane writers raise
    in live mode. The deferred-write log is the durable backstop;
    operators reconcile via that + outbox tail (or a future
    ``almanak ax accounting reconcile``).

    Returns the iteration result paired with the per-intent
    ``inline_degraded_count`` accumulated while looping. The bracket
    caller adds this to the snapshot-bracket degraded count to produce
    the final ``accounting_degraded_count`` for the inline lane.
    """
    from ..state.exceptions import AccountingPersistenceError
    from .runner_models import IterationResult, IterationStatus

    strategy_id = strategy.strategy_id
    deployment_id = getattr(strategy, "deployment_id", "") or strategy_id

    inline_degraded_count = 0
    all_success = True
    last_result: IterationResult | None = None
    for i, intent in enumerate(teardown_intents):
        logger.info(f"🛑 Executing teardown intent {i + 1}/{len(teardown_intents)}: {intent.intent_type.value}")

        # Resolve amount="all" to actual wallet balance before execution.
        # Only resolve for intents with a token balance field (e.g., SwapIntent.from_token).
        # Intents like vault_redeem(shares="all") are handled natively by the compiler.
        intent_to_execute = intent
        if Intent.has_chained_amount(intent):
            balance_token = (
                getattr(intent, "from_token", None)
                or getattr(intent, "token", None)
                or getattr(intent, "token_in", None)
            )
            if balance_token and teardown_market is not None:
                # Resolve balance — pass chain for multi-chain market snapshots
                intent_chain = getattr(intent, "chain", None)
                try:
                    if intent_chain:
                        bal = teardown_market.balance(balance_token, intent_chain)
                    else:
                        bal = teardown_market.balance(balance_token)
                except TypeError:
                    # Single-chain MarketSnapshot doesn't accept chain param
                    bal = teardown_market.balance(balance_token)
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        f"🛑 Teardown intent {i + 1}: failed to resolve balance for {balance_token}: {e}. "
                        f"Token may be missing from the registry. Position may remain open."
                    )
                    all_success = False
                    last_result = IterationResult(
                        status=IterationStatus.COMPILATION_FAILED,
                        intent=intent,
                        error=f"Cannot resolve amount='all' for {balance_token}: {e}",
                        strategy_id=strategy_id,
                        duration_ms=runner._calculate_duration_ms(start_time),
                    )
                    break
                # MarketSnapshot.balance() returns Decimal; IntentStrategy.balance() returns TokenBalance
                balance_value = bal.balance if hasattr(bal, "balance") else bal
                if balance_value <= 0:
                    logger.info(f"🛑 Teardown intent {i + 1}: {balance_token} balance is 0, skipping (already closed)")
                    continue
                intent_to_execute = Intent.set_resolved_amount(intent, balance_value)
                logger.info(f"🛑 Resolved amount='all' for {balance_token}: {balance_value}")
            elif balance_token and teardown_market is None:
                # Have a token to resolve but no market — log warning, let compiler try
                logger.warning(
                    f"🛑 Teardown intent {i + 1}: amount='all' for {balance_token} but no market context. "
                    f"Passing to compiler as-is — compilation may fail."
                )
            else:
                # No token field — let compiler handle natively (e.g., shares="all")
                logger.debug(f"🛑 Teardown intent {i + 1}: no token field, passing to compiler as-is")

        try:
            result = await runner._execute_single_chain(
                strategy=strategy,
                intent=intent_to_execute,
                start_time=start_time,
                total_intents=1,
                market=teardown_market,
            )
        except AccountingPersistenceError as acc_err:
            # VIB-3773: iteration-lane writers raise on failure; teardown
            # MUST continue. Convert into a synthetic success-shaped result
            # (the chain-side TX already landed by the time the writer
            # raised) and record into the deferred-write log.
            logger.error(
                "🛑 Teardown intent %d/%d (inline) — accounting persistence failed but chain-side OK: %s",
                i + 1,
                len(teardown_intents),
                acc_err,
            )
            inline_degraded_count += 1
            # Deferred-log writes are the durable backstop, but the
            # backstop itself can fail (disk full, log rotation race).
            # Swallow that secondary failure too — halting the unwind
            # because the *log* of the original failure could not be
            # written would re-introduce the silent-failure shape we
            # eliminated. Operators see the chain-side OK + ERROR log.
            try:
                deferred_append(
                    kind=str(acc_err.write_kind) if acc_err.write_kind else "ledger",
                    strategy_id=strategy_id,
                    deployment_id=deployment_id,
                    cycle_id=teardown_cycle_id,
                    intent_type=getattr(intent_to_execute.intent_type, "value", str(intent_to_execute.intent_type)),
                    error=str(acc_err),
                    extra={"phase": "inline-per-intent"},
                )
            except Exception:  # noqa: BLE001 — never propagate
                logger.exception(
                    "🛑 Teardown intent %d/%d (inline) — deferred-write log append failed; "
                    "original error=%s; continuing teardown",
                    i + 1,
                    len(teardown_intents),
                    acc_err,
                )
            # Synthetic success — the unwind continues.
            result = IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent_to_execute,
                strategy_id=strategy_id,
                duration_ms=runner._calculate_duration_ms(start_time),
            )
        last_result = result
        if not result.success:
            all_success = False
            logger.error(f"🛑 Teardown intent {i + 1} failed: {result.error}")
            break  # Stop on first chain-side failure

    if last_result:
        if all_success:
            last_result.status = IterationStatus.TEARDOWN
            logger.info(f"🛑 {strategy_id} teardown complete - shutting down strategy runner")
            runner.request_shutdown()
            runner._lifecycle_write_state(strategy_id, "TERMINATED")
            runner._record_success()
            if request:
                _safe_mark(state_manager, "mark_completed", strategy_id, result={"intents": len(teardown_intents)})
        else:
            logger.warning(f"🛑 {strategy_id} teardown incomplete - manual intervention may be required")
            if request:
                _safe_mark(state_manager, "mark_failed", strategy_id, error=last_result.error or "execution failed")
            runner._request_teardown_failure_shutdown(last_result.error or "inline teardown execution failed")
        return last_result, inline_degraded_count

    # Edge case: no intents executed (all positions already closed)
    logger.info(f"🛑 {strategy_id} teardown: all positions already closed, shutting down")
    runner.request_shutdown()
    runner._lifecycle_write_state(strategy_id, "TERMINATED")
    runner._record_success()
    if request:
        _safe_mark(state_manager, "mark_completed", strategy_id, result={"reason": "all_positions_already_closed"})
    final_result = IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        strategy_id=strategy_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )
    return final_result, inline_degraded_count


# -------------------------------------------------------------------------
# Compiler / price helpers
# -------------------------------------------------------------------------


def build_teardown_compiler(
    runner: Any,
    strategy: StrategyProtocol,
    market: Any | None,
) -> IntentCompiler | None:
    """Build an IntentCompiler for TeardownManager teardown execution.

    Returns None if compiler cannot be built (e.g., missing RPC access).
    """
    from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

    gateway_client = None
    rpc_url = None

    if isinstance(runner.execution_orchestrator, GatewayExecutionOrchestrator):
        gateway_client = runner.execution_orchestrator._client
    else:
        rpc_url = getattr(runner.execution_orchestrator, "rpc_url", None)

    # Extract prices from market snapshot.
    # IMPORTANT: do NOT convert {} to None via `or None` — an empty dict
    # is distinct from None.  With None the compiler falls back to $1
    # placeholder prices, producing wildly wrong slippage calculations
    # and silent None action bundles on mainnet (VIB-1386..1391).
    fetched: dict[str, Decimal] | None = None
    if market is not None and hasattr(market, "get_price_oracle_dict"):
        fetched = market.get_price_oracle_dict()
    # Merge fallback prices (stablecoins + native/wrapped tokens) into the
    # fetched oracle.  This ensures partially-populated caches (e.g. only USDC)
    # still get WETH fallback prices instead of $1 placeholders.
    fallback = get_fallback_teardown_prices(market)
    merged = {**(fallback or {}), **(fetched if fetched is not None else {})}
    price_oracle = merged if merged else None

    has_prices = bool(price_oracle)
    if not has_prices:
        logger.warning(
            "No token prices available for teardown compiler — "
            "compilation will use placeholder prices ($1 for all tokens). "
            "This is likely a gateway connectivity issue."
        )

    try:
        compiler_config = IntentCompilerConfig(
            allow_placeholder_prices=not has_prices,
        )
        return IntentCompiler(
            chain=strategy.chain,
            wallet_address=strategy.wallet_address,
            rpc_url=rpc_url,
            price_oracle=price_oracle,
            config=compiler_config,
            gateway_client=gateway_client,
            chain_wallets=getattr(strategy, "_chain_wallets", None),
        )
    except Exception as e:
        logger.warning(f"Failed to build teardown compiler: {e}")
        return None


def prefetch_teardown_prices(market: Any, intents: list) -> None:
    """Eagerly fetch prices for tokens referenced in teardown intents.

    MarketSnapshot uses lazy loading — prices only populate when market.price()
    is called. During teardown, generate_teardown_intents() typically doesn't call
    market.price(), so get_price_oracle_dict() returns {} until this method
    pre-populates the cache with real prices for the teardown tokens.

    Teardown intents often reference tokens by address (e.g. 0xdefa1d...) rather
    than symbol. market.price() expects a symbol, so we resolve addresses to
    symbols first using the token resolver. Without this, tokens like ALMANAK
    (not in CoinGecko/Chainlink) fail price resolution during teardown.
    """
    token_attrs = ("from_token", "to_token", "token", "collateral_token", "borrow_token", "token_in")
    tokens: set[str] = set()
    for intent in intents:
        for attr in token_attrs:
            val = getattr(intent, attr, None)
            if val and isinstance(val, str):
                tokens.add(val)

    if not tokens:
        return

    # Resolve addresses to symbols so market.price() can look them up.
    # market.price() expects symbols (e.g. "ALMANAK"), not addresses.
    chain = getattr(market, "_chain", None) or getattr(market, "chain", None)
    address_to_symbol: dict[str, str] = {}
    if chain:
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            for token in tokens:
                if token.startswith("0x") and len(token) == 42:
                    try:
                        resolved = resolver.resolve(token, chain, log_errors=False, skip_gateway=True)
                        address_to_symbol[token] = resolved.symbol
                    except Exception as e:
                        logger.debug(f"Could not resolve teardown token address {token} to symbol: {e}")
        except Exception as e:
            logger.debug(f"Token resolver unavailable for teardown prefetch: {e}")

    fetched = []
    for token in sorted(tokens):
        # Try the symbol if we resolved the address, otherwise try the raw value
        symbol = address_to_symbol.get(token, token)
        try:
            market.price(symbol)
            fetched.append(symbol)
        except Exception:
            # If symbol lookup failed and we have the original address, try that too
            if symbol != token:
                try:
                    market.price(token)
                    fetched.append(token)
                except Exception:
                    logger.debug(f"Could not pre-fetch price for teardown token {token} (symbol={symbol})")
            else:
                logger.debug(f"Could not pre-fetch price for teardown token {token}")

    if fetched:
        logger.info(f"Pre-fetched {len(fetched)} teardown prices: {fetched}")


def get_fallback_teardown_prices(market: Any) -> dict[str, Decimal] | None:
    """Build a minimal fallback price oracle when the market snapshot has no cached prices.

    This prevents the compiler from using $1 placeholder prices for ALL tokens
    on mainnet, which causes wildly wrong slippage calculations and silent
    compilation failures (None action bundles).

    Returns a dict with at least stablecoin prices, or None if nothing can be
    determined.
    """
    # Start with stablecoin fallbacks (always ~$1, safe to assume)
    fallback: dict[str, Decimal] = {
        "USDC": Decimal("1"),
        "USDT": Decimal("1"),
        "DAI": Decimal("1"),
        "USDC.e": Decimal("1"),
        "USDbC": Decimal("1"),
    }

    # Derive native + wrapped token symbols from existing registry maps
    # so new chains are picked up automatically without code changes here.
    from almanak.framework.data.models import _NATIVE_TO_WRAPPED
    from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS

    chain = getattr(market, "_chain", None) or getattr(market, "chain", None)
    native = NATIVE_TOKEN_SYMBOLS.get(str(chain).lower(), "ETH") if chain else "ETH"
    wrapped = _NATIVE_TO_WRAPPED.get(native, f"W{native}")
    tokens_to_fetch = (native, wrapped)

    # Try to get real prices from the market one more time — the gateway
    # may have recovered since the prefetch attempt.
    if market is not None and hasattr(market, "price"):
        for symbol in tokens_to_fetch:
            try:
                price = market.price(symbol)
                if price and price > 0:
                    fallback[symbol] = price
            except Exception as exc:
                logger.warning("Could not fetch fallback teardown price for %s: %s", symbol, exc)

    # If we only have stablecoins, still return — it's better than $1 for everything
    return fallback if fallback else None


def inject_simulated_balances(runner: Any, market: Any, strategy: Any) -> None:
    """Inject simulated_balances from strategy config into the market snapshot.

    Called in dry-run mode (VIB-2329). When --dry-run --no-gateway is active,
    balance providers return 0 or error for chains where the wallet has no
    on-chain positions. simulated_balances in config.json lets strategy authors
    test logic without needing real funds on every chain.

    Injection is skipped when the market snapshot already has a real balance
    provider (gateway is active). This prevents simulated balances from
    silently overriding real on-chain data in normal dry-run simulations.

    Config format (config.json):
        {
            "simulated_balances": {
                "USDC": "10000",
                "WETH": "5"
            }
        }

    For MultiChainMarketSnapshot, balances are injected into every configured chain.

    balance_usd is computed by attempting market.price() lookup.  For tokens
    where the price is unavailable, balance_usd defaults to 0 (safe fallback —
    the strategy still sees a non-zero balance and can pass balance gates).
    """
    from decimal import InvalidOperation

    from almanak.framework.strategies.intent_strategy import MultiChainMarketSnapshot, TokenBalance

    # Skip injection when a real balance provider is active. MarketSnapshot.balance()
    # prefers pre-populated balances over the provider, so injecting with a live
    # gateway would silently override real on-chain data.
    if getattr(market, "_balance_provider", None) is not None:
        return

    simulated: dict | None = None
    try:
        simulated = strategy.get_config("simulated_balances")
    except AttributeError:
        # Strategy does not implement get_config — skip silently.
        return

    if not simulated or not isinstance(simulated, dict):
        if simulated is not None and not isinstance(simulated, dict):
            logger.warning("[dry-run] simulated_balances must be a dict, got %s — skipping", type(simulated).__name__)
        return

    is_multi_chain = isinstance(market, MultiChainMarketSnapshot)

    injected: list[str] = []
    for token, raw_amount in simulated.items():
        try:
            amount = Decimal(str(raw_amount))
        except InvalidOperation:
            logger.warning(f"[dry-run] simulated_balances: invalid amount for {token}: {raw_amount!r}")
            continue

        if not amount.is_finite() or amount <= 0:
            logger.warning(
                f"[dry-run] simulated_balances: amount must be a positive finite number for {token}: {raw_amount!r}"
            )
            continue

        tb = TokenBalance(symbol=token, balance=amount, balance_usd=Decimal("0"))
        try:
            if is_multi_chain:
                # MultiChainMarketSnapshot.set_balance and .price() both require an
                # explicit chain argument — inject and price each chain separately.
                for chain in market.chains:
                    balance_usd = Decimal("0")
                    try:
                        price = market.price(token, chain=chain)
                        balance_usd = amount * Decimal(str(price))
                    except Exception:
                        pass
                    chain_tb = TokenBalance(symbol=token, balance=amount, balance_usd=balance_usd)
                    market.set_balance(token, chain, chain_tb)
            else:
                # Best-effort USD valuation using the live price oracle.
                # Silently falls back to 0 if price is unavailable (strategy still
                # sees a non-zero balance, which is all that matters for gate checks).
                try:
                    price = market.price(token)
                    tb = TokenBalance(symbol=token, balance=amount, balance_usd=amount * Decimal(str(price)))
                except Exception:
                    pass
                market.set_balance(token, tb)
            injected.append(f"{token}={amount}")
        except Exception as e:
            logger.warning(f"[dry-run] simulated_balances: could not set {token}: {e}")

    if injected:
        logger.info(f"[dry-run] Injected simulated balances: {', '.join(injected)}")


def bridge_token_resolution_candidates(
    token_symbol: str | None,
    bridge_status: dict[str, Any],
) -> list[str]:
    """Collect token identifiers for bridge amount normalization."""
    candidates: list[str] = []
    keys = (
        "destination_token_address",
        "destinationTokenAddress",
        "token_address",
        "tokenAddress",
        "destination_token",
        "destinationToken",
        "token",
        "token_symbol",
    )

    def _append_candidate(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())

    for key in keys:
        _append_candidate(bridge_status.get(key))

    route_data = bridge_status.get("route_data")
    if isinstance(route_data, dict):
        for key in keys:
            _append_candidate(route_data.get(key))

    if token_symbol:
        candidates.append(token_symbol)

    # Preserve first-seen ordering while de-duplicating
    seen: set[str] = set()
    deduped: list[str] = []
    for candidate in candidates:
        candidate_key = candidate.lower()
        if candidate_key not in seen:
            seen.add(candidate_key)
            deduped.append(candidate)
    return deduped


def normalize_bridge_balance_increase(
    balance_increase_wei: int | str,
    destination_chain: str,
    token_symbol: str | None,
    bridge_status: dict[str, Any],
) -> tuple[Decimal | None, dict[str, Any]]:
    """Normalize bridge completion balance increase from wei to token units.

    Returns:
        (normalized_amount, metadata). If normalization fails, returns
        (None, metadata) with raw wei preserved for diagnostics.
    """
    try:
        raw_wei = int(balance_increase_wei)
    except (TypeError, ValueError):
        return None, {
            "raw_wei": balance_increase_wei,
            "destination_chain": destination_chain,
            "token_symbol": token_symbol,
            "error": "invalid_balance_increase_wei",
        }

    from ..data.tokens import get_token_resolver
    from ..data.tokens.exceptions import TokenNotFoundError

    resolver = get_token_resolver()
    candidates = bridge_token_resolution_candidates(token_symbol, bridge_status)
    for candidate in candidates:
        try:
            resolved = resolver.resolve(candidate, destination_chain)
            decimals = resolved.decimals
            normalized = Decimal(raw_wei) / Decimal(10**decimals)
            return normalized, {
                "raw_wei": raw_wei,
                "destination_chain": destination_chain,
                "token_symbol": token_symbol,
                "resolved_from": candidate,
                "resolved_address": resolved.address,
                "decimals": decimals,
            }
        except Exception:
            continue

    unresolved = token_symbol or (candidates[0] if candidates else "<unknown-token>")
    raise TokenNotFoundError(
        token=unresolved,
        chain=destination_chain,
        reason=(f"Unable to resolve token decimals for bridge balance normalization (candidates={candidates})"),
    )
