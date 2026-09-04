"""Teardown execution methods for StrategyRunner.

Extracted from strategy_runner.py for maintainability. Each function takes
``runner`` (a StrategyRunner instance) as its first argument and is called
via a thin delegation stub in StrategyRunner.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any

from almanak.core.chains._helpers import bridged_stablecoin_map
from almanak.core.lifecycle import LifecycleState

from ..execution.fork_signal import is_managed_fork_network
from ..intents.compiler import IntentCompiler, IntentCompilerConfig
from ..intents.vocabulary import Intent
from ..teardown.decision_log import TeardownDecisionPhase, log_teardown_decision
from ..teardown.sweep_warning import warn_if_sweep_non_strategy_balance

if TYPE_CHECKING:
    from ..teardown import TeardownMode
    from .runner_models import IterationResult, StrategyProtocol

# Keep the established logger name for log filters and capture tests.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


_VALID_APPROVAL_ACTIONS = {"approve", "continue", "wait_and_escalate", "cancel"}

# Unknown request sources stay automatic so teardown cannot block on an
# approval that no operator is present to provide.
_MANUAL_TEARDOWN_REQUESTERS: frozenset[str] = frozenset({"cli", "dashboard", "dashboard_api"})


_warn_if_sweep_non_strategy_balance = warn_if_sweep_non_strategy_balance


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


# Malformed approval data must not cancel teardown or fabricate approval.
_SAFE_DEFAULT_APPROVAL_ACTION = "wait_and_escalate"

_APPROVAL_POLL_INTERVAL_S = 5.0

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
    if not isinstance(action, str) or action not in _VALID_APPROVAL_ACTIONS:
        logger.error(
            "Approval response for teardown %s has unknown action %r; treating as %s",
            teardown_id,
            action,
            _SAFE_DEFAULT_APPROVAL_ACTION,
        )
        return ApprovalResponse(
            approved=False,
            teardown_id=teardown_id,
            action=_SAFE_DEFAULT_APPROVAL_ACTION,
        )

    # Explicit parsing prevents a non-empty string such as "false" from becoming true.
    approved_raw = data.get("approved", False)
    if isinstance(approved_raw, bool):
        approved = approved_raw
    elif isinstance(approved_raw, str):
        approved = approved_raw.strip().lower() in {"true", "1", "yes"}
    elif isinstance(approved_raw, int | float):
        approved = bool(approved_raw)
    else:
        approved = False

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
        expires_at = request.expires_at or datetime.now(UTC) + _APPROVAL_DEFAULT_TIMEOUT
        timeout_s = max(0.0, (expires_at - datetime.now(UTC)).total_seconds())
        monotonic_deadline = time.monotonic() + timeout_s

        await asyncio.to_thread(
            state_adapter.create_approval_request,
            teardown_id=request.teardown_id,
            deployment_id=request.deployment_id,
            level=request.current_level,
            request_json=json.dumps(
                {
                    "teardown_id": request.teardown_id,
                    "deployment_id": request.deployment_id,
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

        while time.monotonic() < monotonic_deadline:
            response_json = await asyncio.to_thread(
                state_adapter.get_approval_response,
                request.teardown_id,
                request.current_level,
            )
            if response_json:
                return _parse_approval_response(response_json, request.teardown_id)
            await asyncio.sleep(_APPROVAL_POLL_INTERVAL_S)

        # The conditional timeout write prevents a late response from being overwritten.
        # If the operator won the race, read and honor that response.
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


async def _count_open_positions(strategy: Any) -> int | None:
    """Best-effort pre-execution open-position count for teardown bookkeeping.

    VIB-5085: lifecycle ``positions_total`` / ``positions_closed`` must count
    *positions*, not teardown *intents* — one position can need several
    intents (REPAY + WITHDRAW + SWAP), so ``len(teardown_intents)``
    over-reports (the field-report symptom: 2 positions closed via 6 intents
    logged as ``positions_closed=6``).

    VIB-5459 / TD-01: the enumeration is reconciled against the
    ``position_registry`` WARM read path (additive union) so a restarted runner
    counts the cut-over LP positions the registry still remembers even when
    in-memory state was wiped. Non-LP / non-cut-over primitives fall through to
    the strategy's own ``get_open_positions`` unchanged, and the read degrades to
    the legacy enumeration on a backend without cutover storage.

    Returns ``None`` when the count can't be read. Callers MUST NOT substitute
    the intent count for ``positions_closed`` — that re-introduces the exact
    conflation this ticket fixes. On the unverified multi-chain / inline lanes
    they instead omit ``positions_closed`` from the result payload and let the
    persistence lift fall back to the legacy ``result_json["intents"]`` key.
    """
    from ..teardown.registry_enumeration import resolve_open_positions_with_registry

    try:
        positions = await resolve_open_positions_with_registry(strategy)
        return len(positions.positions)
    except Exception:
        logger.debug(
            "Teardown bookkeeping: could not read open-position count (positions_closed omitted)",
            exc_info=True,
        )
        return None


async def _failure_position_counts(strategy: Any) -> tuple[int | None, int | None]:
    """Best-effort ``(positions_closed, positions_failed)`` for a teardown that
    FAILED before execution (VIB-5778).

    Reuses the SAME registry-reconciled discovery the no-intent completeness gate
    uses (``_count_open_positions`` → ``resolve_open_positions_with_registry``) to
    learn how many positions were open when the failure struck. A generation /
    pre-execution failure ran NO closing intent, so ``positions_closed`` is a
    measured ``0`` and every enumerable open position is a ``positions_failed`` —
    honest position-level counts (blueprint 14 §Teardown; the columns count
    positions, VIB-4542 Item 6).

    Returns ``(0, count)`` when the open-position set is enumerable, or
    ``(None, None)`` when it is not. The ``None`` pair leaves ``mark_failed``'s
    counts at preserve-prior — NEVER a fabricated ``0/0`` — and the CLI's
    ``started_at``-NULL "unknown" inference then governs the render.

    ``_count_open_positions`` already never raises, but this call is defensively
    wrapped so the honesty step can NEVER mask the original teardown exception:
    per the teardown risk contract, extra bookkeeping on an already-failing path
    must not disturb it.
    """
    try:
        count = await _count_open_positions(strategy)
    except Exception:
        logger.debug(
            "VIB-5778: best-effort failure enumeration errored; leaving mark_failed counts unset",
            exc_info=True,
        )
        return None, None
    if count is None:
        return None, None
    return 0, count


async def _check_no_intent_completeness(strategy: Any, request: Any = None) -> Any:
    """Completeness report for the no-intents teardown gate (TD-11 / VIB-5469).

    Reads the KNOWN open-position set (registry-reconciled enumeration, TD-01)
    and checks it against an EMPTY intent list — so any enforceable tracked-open
    position is reported as uncovered. Returns the
    :class:`~almanak.framework.teardown.completeness.CompletenessReport`, or
    ``None`` when the known set **could not be read** because strategy
    enumeration raised. ``None`` is an UNREADABLE signal, not "no positions":
    the caller MUST fail loud on it (VIB-5469), because an unreadable set cannot
    certify a clean "no positions" exit and silently proceeding would strand any
    open position. The registry-unavailable case is NOT this path — it degrades
    cleanly inside ``resolve_open_positions_with_registry`` to a summary, so it
    returns a real report rather than ``None``.

    ``request`` (the active :class:`TeardownRequest`) supplies the consolidation
    target so a held STAKE/TOKEN position already denominated in the target — for
    which ``full_close`` emits no swap — is credited a no-op close instead of
    locking the deployment into a recurring failed-teardown loop on THIS gate
    (VIB-5494 Item 1). Absent / non-target-token policies keep strict behaviour.
    """
    from ..teardown.completeness import check_intent_coverage, resolve_consolidation_noop_target
    from ..teardown.registry_enumeration import resolve_open_positions_with_registry

    try:
        positions = await resolve_open_positions_with_registry(strategy)
    except Exception:
        logger.warning(
            "Teardown completeness: could not read known positions for the no-intents gate "
            "— failing loud rather than certifying a clean 'no positions' teardown",
            exc_info=True,
        )
        return None
    noop_target = resolve_consolidation_noop_target(
        getattr(request, "asset_policy", None),
        getattr(request, "target_token", None),
        # Resolve the no-preference target against the strategy chain before a plan exists.
        chain=getattr(strategy, "chain", None) or None,
    )
    return check_intent_coverage(positions, [], consolidation_target_token=noop_target)


async def reconcile_known_positions(runner: Any, strategy: Any, teardown_market: Any | None) -> Any:
    """Plan-A on-chain reconciliation CHECK over the KNOWN position set (TD-08 / VIB-5466).

    After teardown ledger enumeration, confirm each KNOWN position's live on-chain
    state and compare it to the WARM ledger's belief. Divergence (ledger believes
    open, chain reports closed) and unconfirmable positions are flagged LOUDLY with
    a structured :class:`~almanak.framework.teardown.plan_a_reconciliation.ReconciliationReport`
    that the TD-15 fail-closed verification consumes (and that composes with the
    TD-14 ``verification_status`` via ``report.apply_to_verification_status``).

    This is a CHECK, not an action: it closes/sweeps nothing, emits no intent, and
    is scoped strictly to the positions the framework already enumerated — NEVER a
    wallet-wide scan (that is Plan B / ``--discover``). It also never blocks the
    teardown's risk-reducing intents (blueprint 14 §Teardown — the check is loud
    but observational). Returns ``None`` only when the known set could not be
    enumerated at all; otherwise a report (possibly empty) is always returned.

    Never raises — reconciliation must never fault the teardown lane.
    """
    from ..teardown.plan_a_reconciliation import reconcile_known_positions_against_chain
    from ..teardown.registry_enumeration import resolve_open_positions_with_registry

    try:
        summary = await resolve_open_positions_with_registry(strategy)
    except Exception:
        logger.debug(
            "Teardown Plan-A reconciliation: could not enumerate the known position set — CHECK skipped",
            exc_info=True,
        )
        return None

    gateway_client = getattr(strategy, "_gateway_client", None)
    network = str(getattr(strategy, "_gateway_network", "") or "")
    try:
        return await reconcile_known_positions_against_chain(
            summary=summary,
            gateway_client=gateway_client,
            market=teardown_market,
            network=network,
            wallet_address=str(getattr(strategy, "wallet_address", "") or ""),
            wallet_for_chain=getattr(strategy, "get_wallet_for_chain", None),
        )
    except Exception:
        logger.debug(
            "Teardown Plan-A reconciliation: CHECK errored — continuing teardown (observational only)",
            exc_info=True,
        )
        return None


async def _recover_orphaned_lp_intents(
    runner: Any,
    strategy: Any,
    teardown_intents: list,
    teardown_mode: TeardownMode,
) -> tuple[list, bool, str | None]:
    """Auto-fallback to on-chain LP discovery when strategy state is lost (VIB-5138).

    Teardown emits ``LP_CLOSE`` only when the strategy's ``_position_id``
    survives. On state desync — the LP NFT is live on-chain but ``_position_id``
    was lost (often after an ``AccountingPersistenceError`` on LP open) —
    ``get_open_positions()`` returns no LP and ``generate_teardown_intents()``
    emits no LP_CLOSE, so the signal-driven runner lane would report "no
    positions" and strand the open NFT.

    **Deployment-ownership scoping (fund-safety, VIB-4976).** The on-chain scan
    is wallet-scoped, and a wallet may be shared across deployments. Recovery is
    scoped to ONLY the token ids THIS deployment opened, learned from its own
    durable accounting state (``position_registry`` OPEN rows + ``position_events``
    LP OPEN rows — both survive the LP-open ``AccountingPersistenceError`` desync,
    see ``runner_helpers._deployment_lp_ownership``). A sibling strategy's live LP
    on the same wallet is never in the set and is never closed.

    Ownership is read FIRST (cheap local-DB read). When this deployment has NO LP
    attribution on the chain (a non-LP strategy), the gateway scan is skipped
    entirely — so a non-LP teardown neither pays for a scan nor can be blocked by
    a transient blip on an unrelated NPM.

    Recovered ``LP_CLOSE`` intents are appended to ``teardown_intents`` and flow
    through the normal ``_execute_intents`` per-intent commit pipeline — so every
    recovered close lands in the teardown accounting lane (no bypass).

    Returns ``(augmented_intents, incomplete, warning)``:

    * ``augmented_intents`` — the input intents plus any deployment-owned
      recovered ``LP_CLOSE``.
    * ``incomplete`` — True ONLY when the scan was incomplete AND this deployment
      is known to have opened an LP on this chain (a deployment-owned orphan may
      remain). The caller MUST NOT report a clean success in that case.
    * ``warning`` — operator-facing reason when ``incomplete``.

    Never raises: discovery failure degrades the teardown loudly but must never
    block the next risk-reducing intent (teardown failure semantics are
    inverted vs the iteration lane).
    """
    from ..teardown.lp_recovery import merge_discovered_lp, strategy_reports_lp
    from ..teardown.runner_helpers import build_runner_helpers

    deployment_id = strategy.deployment_id
    chain = (getattr(strategy, "chain", "") or "").strip()
    try:
        positions = strategy.get_open_positions()
    except Exception:
        logger.debug(
            "Teardown LP recovery: get_open_positions failed for %s — skipping discovery fallback",
            deployment_id,
            exc_info=True,
        )
        return teardown_intents, False, None

    if strategy_reports_lp(positions):
        return teardown_intents, False, None

    helpers = build_runner_helpers(runner)
    if not helpers.has_lp_discovery:
        return teardown_intents, False, None

    get_ownership = helpers.get_deployment_lp_ownership
    discover = helpers.discover_lp_positions
    assert get_ownership is not None and discover is not None  # noqa: S101 — narrowed by has_lp_discovery

    # Prove deployment ownership before any wallet-scoped scan can authorize a close.
    try:
        ownership = await get_ownership(strategy, chain)
    except Exception:  # noqa: BLE001 — ownership read must never block risk reduction
        logger.exception(
            "Teardown LP recovery: ownership read raised for %s — skipping recovery (cannot prove ownership)",
            deployment_id,
        )
        return teardown_intents, False, None

    if not ownership.available:
        return teardown_intents, False, None

    # Avoid making an unrelated discovery outage relevant to a deployment with no LP attribution.
    if not ownership.token_ids and not ownership.had_lp_open:
        return teardown_intents, False, None

    try:
        # V4 is not enumerable, so recovery can verify only proven owned token IDs.
        discovery = await discover(strategy, ownership.token_ids)
    except Exception:  # noqa: BLE001 — discovery must never block risk reduction
        logger.exception(
            "Teardown LP recovery: discovery helper raised for %s — continuing without recovery",
            deployment_id,
        )
        # A failed scan for a deployment with LP attribution cannot certify closure.
        owns_lp_here = bool(ownership.token_ids) or ownership.had_lp_open
        if owns_lp_here:
            warning = (
                f"On-chain LP discovery raised during teardown recovery for {deployment_id}; "
                "this deployment is known to have opened an LP on this chain — manual on-chain "
                "verification required."
            )
            return teardown_intents, True, warning
        logger.warning(
            "Teardown LP recovery: discovery raised for %s but this deployment has no LP "
            "attribution on this chain — degrading to a warning, not a block.",
            deployment_id,
        )
        return teardown_intents, False, None

    outcome = merge_discovered_lp(
        positions=positions,
        intents=teardown_intents,
        discovery=discovery,
        ownership=ownership,
        mode=teardown_mode,
    )
    return outcome.intents, outcome.incomplete, outcome.warning


async def _prepare_deferred_pending_orders(strategy: Any, residuals: list[Any]) -> bool:
    """Ask owning connectors to progress measured non-cancellable orders."""
    deferred_by_protocol: dict[str, list[Any]] = {}
    for residual in residuals:
        details = residual.details or {}
        protocol = str(getattr(residual, "protocol", "") or "").lower()
        if details.get("kind") == "pending_order" and not details.get("cancellable") and protocol:
            deferred_by_protocol.setdefault(protocol, []).append(residual)
    if not deferred_by_protocol:
        return False

    from almanak.connectors._base.types import ProtocolName
    from almanak.connectors._strategy_runner_hook_registry import STRATEGY_RUNNER_HOOK_REGISTRY

    progressed = False
    for protocol, deferred_residuals in deferred_by_protocol.items():
        prepared = await asyncio.to_thread(
            STRATEGY_RUNNER_HOOK_REGISTRY.prepare_pending_orders_for_teardown,
            protocol=ProtocolName(protocol),
            gateway_client=getattr(strategy, "_gateway_client", None),
            chain=str(getattr(deferred_residuals[0], "chain", "") or getattr(strategy, "chain", "") or ""),
            wallet_address=str(getattr(strategy, "wallet_address", "") or ""),
            residuals=tuple(deferred_residuals),
            network=str(getattr(strategy, "_gateway_network", "") or ""),
        )
        progressed = progressed or prepared
    return progressed


def _record_teardown_pending_recovery_keys(runner: Any, keys: frozenset[str] | None) -> None:
    """Expose measured pending keys to the exact accepted-order resume lane."""
    if runner is not None:
        runner._teardown_pending_recovery_keys = keys


def _pending_order_keys(residuals: list[Any]) -> frozenset[str] | None:
    """Normalize pending-order keys while preserving unmeasured discovery."""
    if any((residual.details or {}).get("kind") == "residual_unverified" for residual in residuals):
        return None
    return frozenset(
        str((residual.details or {}).get("order_key") or "").lower()
        for residual in residuals
        if (residual.details or {}).get("kind") == "pending_order"
        and str((residual.details or {}).get("order_key") or "")
    )


async def _recover_pending_order_intents(
    runner: Any,
    strategy: Any,
    teardown_intents: list,
    teardown_mode: TeardownMode,  # noqa: ARG001 — a cancel is mode-agnostic (always full recovery)
) -> tuple[list, bool, str | None]:
    """Recover collateral from stranded pending (unfilled) perp orders (VIB-5568).

    The RECOVERY half of VIB-5116. Teardown's residual discovery
    (``discover_teardown_residuals``) reads the wallet's pending (unfilled) GMX V2
    orders directly from chain — collateral committed to the OrderVault that is NOT
    a position and is invisible to ``get_open_positions()`` /
    ``generate_teardown_intents()``. #3130 folds those residuals into the
    COMPLETENESS set so teardown fails LOUD on them, but nothing CANCELS them, so
    the collateral stays stranded. This lane turns each discovered pending-order
    residual into a ``PERP_CANCEL_ORDER`` intent (via ``full_close_intents``, whose
    PERP branch maps a ``kind="pending_order"`` residual to
    ``Intent.perp_cancel_order`` routed to the venue's compiler) and APPENDS it to
    ``teardown_intents`` so it flows through the normal ``_execute_intents``
    per-intent commit pipeline — no bypass. After the cancels execute, the venue's
    teardown post-condition re-reads the OrderVault
    (``getBytes32Count(ACCOUNT_ORDER_LIST) == 0``) and completeness passes.

    Returns ``(augmented_intents, incomplete, warning)`` — ``incomplete=True`` when
    the residual read was UNMEASURED (a ``residual_unverified`` sentinel: Empty ≠
    Zero, fail-closed) so teardown is NOT certified clean while a strand may remain.

    Never raises: discovery failure degrades loudly but never blocks the next
    risk-reducing intent (teardown's inverted failure semantics).
    """
    from ..teardown.full_close import full_close_intents
    from ..teardown.residual_discovery import discover_teardown_residuals, remeasure_teardown_residuals

    deployment_id = getattr(strategy, "deployment_id", "")

    # None is unmeasured; an empty set is a measured-empty pending-order book.
    _record_teardown_pending_recovery_keys(runner, None)
    try:
        residuals = discover_teardown_residuals(strategy)
    except Exception as exc:  # noqa: BLE001 — discovery must never block risk reduction
        # An unmeasured residual set cannot certify closure, but does not block risk reduction.
        logger.exception(
            "Teardown pending-order recovery: residual discovery raised for %s — failing closed (manual check)",
            deployment_id,
        )
        return (
            teardown_intents,
            True,
            f"Pending-order residual discovery failed for {deployment_id}: {exc} — "
            "manual on-chain verification required.",
        )

    if not residuals:
        _record_teardown_pending_recovery_keys(runner, frozenset())
        return teardown_intents, False, None

    # Once residuals are measured, later recovery faults fail closed through the result.
    try:
        if await _prepare_deferred_pending_orders(strategy, residuals):
            # Session preparation invalidates the prior measurement; cancel from one fresh read.
            residuals = remeasure_teardown_residuals(strategy, residuals)

        # None preserves an unmeasured read; an empty set is measured-empty.
        pending_order_keys = _pending_order_keys(residuals)
        unmeasured = pending_order_keys is None

        # GMX rejects cancellation before its age gate. Defer fresh or unmeasured orders
        # instead of exhausting the slippage ladder on a deterministic revert.
        pending = [p for p in residuals if (p.details or {}).get("kind") == "pending_order"]
        _record_teardown_pending_recovery_keys(runner, pending_order_keys)
        cancellable = [p for p in pending if (p.details or {}).get("cancellable")]
        deferred = [p for p in pending if not (p.details or {}).get("cancellable")]

        # The close planner also requires a valid bytes32 key before targeting a cancel.
        cancels = full_close_intents(cancellable) if cancellable else []
        if cancels:
            logger.warning(
                "🛑 Teardown recovering %d stranded pending-order(s) for %s → PERP_CANCEL_ORDER to "
                "return committed collateral to the wallet (VIB-5568)",
                len(cancels),
                deployment_id,
            )
            teardown_intents = list(teardown_intents) + list(cancels)

        deferred_warning = None
        if deferred:
            waits = [(p.details or {}).get("seconds_until_cancellable") for p in deferred]
            max_wait = max((w for w in waits if isinstance(w, int)), default=None)
            wait_txt = f"~{max_wait}s" if max_wait is not None else "up to ~300s"
            deferred_warning = (
                f"{len(deferred)} GMX pending order(s) not yet cancellable for {deployment_id} "
                f"(GMX ~300s cancel gate) — recoverable in {wait_txt}; re-run teardown after that."
            )
            logger.warning(
                "🛑 Teardown deferring %d not-yet-cancellable pending-order(s) for %s (recoverable in %s)",
                len(deferred),
                deployment_id,
                wait_txt,
            )

        unmeasured_warning = None
        if unmeasured:
            unmeasured_warning = (
                f"Pending-order residual read was UNMEASURED during teardown for {deployment_id}; "
                "a stranded order may remain — manual on-chain verification required."
            )
        incomplete = unmeasured or bool(deferred)
        warning = "; ".join(w for w in (unmeasured_warning, deferred_warning) if w) or None
        return teardown_intents, incomplete, warning
    except Exception:  # noqa: BLE001 — never block risk reduction; fail closed on the incomplete flag
        logger.exception(
            "Teardown pending-order recovery: building cancels raised for %s after residuals were "
            "measured — failing closed (teardown not certified clean)",
            deployment_id,
        )
        return (
            teardown_intents,
            True,
            (
                f"Pending-order recovery could not build cancels for {deployment_id} after discovering "
                "residuals — manual on-chain verification required."
            ),
        )


def _positions_completion_result(open_positions_count: int | None, intents_count: int) -> dict[str, Any]:
    """Build the ``mark_completed`` result_json for the unverified teardown lanes.

    VIB-5085: always carries the intent signal (``intents`` / ``intents_succeeded``
    / ``intents_total``); includes ``positions_closed`` / ``positions_total`` ONLY
    when the open-position count is known. The unverified multi-chain / inline
    lanes must NEVER fabricate ``positions_closed`` from the intent count — that
    is the conflation this ticket fixes; when the count is unknown the persistence
    lift falls back to the legacy ``intents`` key instead. Shared by both lanes.

    VIB-2932 / VIB-5472: these lanes never run the on-chain closure verifier, so
    a reported ``positions_closed`` here is counted closed-by-execution, never
    chain-confirmed — stamp ``verification_status=UNVERIFIED`` so the count is
    visibly optimistic. When no count is known the verifier simply did not run
    (``NOT_RUN``).
    """
    from ..teardown.models import VerificationStatus

    result: dict[str, Any] = {
        "intents": intents_count,
        "intents_succeeded": intents_count,
        "intents_total": intents_count,
    }
    if open_positions_count is not None:
        result["positions_closed"] = open_positions_count
        result["positions_total"] = open_positions_count
        result["verification_status"] = VerificationStatus.UNVERIFIED.value
    else:
        result["verification_status"] = VerificationStatus.NOT_RUN.value
    return result


def _safe_mark(state_manager: Any, method_name: str, deployment_id: str, **kwargs: Any) -> None:
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
        method(deployment_id, **kwargs)
    except Exception:
        logger.warning(
            "Failed to call %s for strategy %s (non-fatal)",
            method_name,
            deployment_id,
            exc_info=True,
        )


def _apply_lending_unwind_guard(
    teardown_intents: list,
    teardown_market: Any,
    deployment_id: str,
    mode: TeardownMode | None = None,
    teardown_id: str | None = None,
) -> list:
    """Sanitise strategy-emitted lending teardown intents against fresh state.

    Wraps the pure ``sanitize_lending_teardown_intents`` guard (VIB-5139 /
    VIB-4466) and logs any dropped / reordered / synthesised / degraded outcome.
    Returns the guarded intent list. A guard failure must never block teardown
    (its first job is removing on-chain risk), so any unexpected error falls back
    to the original intents with a loud WARNING.

    ``teardown_id`` is threaded through so the BLOCK / REPAIR decision-log
    entries emitted here correlate under the canonical ``teardown-{id}`` cycle
    id. This guard runs in ``execute_teardown`` BEFORE the runner swaps the
    observability contextvar to the teardown cycle id, so without an explicit
    ``teardown_id`` the decision-log entry would fall back to the ambient
    iteration cycle id and break correlation (VIB-5478).
    """
    from ..teardown.lending_unwind_guard import sanitize_lending_teardown_intents

    try:
        guarded = sanitize_lending_teardown_intents(teardown_intents, teardown_market, mode=mode)
    except Exception as e:  # pragma: no cover - defensive; guard is pure
        logger.warning(
            "Lending fresh-state guard errored for %s (%s); using original intents",
            deployment_id,
            e,
            exc_info=True,
        )
        return teardown_intents

    for reason in guarded.dropped:
        logger.info("🛑 %s lending guard dropped intent — %s", deployment_id, reason)
        log_teardown_decision(
            deployment_id=deployment_id,
            teardown_id=teardown_id,
            phase=TeardownDecisionPhase.BLOCK,
            outcome="lending_intent_dropped",
            description=f"lending guard dropped intent: {reason}",
            reason=reason,
        )
    if guarded.synthesized_positions:
        logger.info(
            "🛑 %s lending guard synthesised HF-safe unwind staircase (wallet cannot fully repay live "
            "debt — naive withdraw-all would revert) for: %s (VIB-4466)",
            deployment_id,
            ", ".join(guarded.synthesized_positions),
        )
        log_teardown_decision(
            deployment_id=deployment_id,
            teardown_id=teardown_id,
            phase=TeardownDecisionPhase.REPAIR,
            outcome="hf_safe_unwind_synthesized",
            description="lending guard synthesised HF-safe unwind staircase",
            position_count=len(guarded.synthesized_positions),
            reason=", ".join(guarded.synthesized_positions),
        )
    if guarded.no_op_positions:
        logger.info(
            "🛑 %s lending guard: positions already flat (no debt, no collateral): %s",
            deployment_id,
            ", ".join(guarded.no_op_positions),
        )
    if guarded.degraded:
        logger.warning(
            "🛑 %s lending guard degraded: a fresh exposure read was unmeasured — "
            "kept risk-reducing intents only, suppressed any unconfirmed withdraw_all (VIB-5139)",
            deployment_id,
        )
        log_teardown_decision(
            deployment_id=deployment_id,
            teardown_id=teardown_id,
            phase=TeardownDecisionPhase.BLOCK,
            outcome="lending_exposure_unmeasured",
            description="lending guard degraded: fresh exposure read unmeasured",
            reason="fresh_exposure_unmeasured",
            degraded=True,
        )
    return guarded.intents


@dataclass(frozen=True)
class _TeardownIntentGenerationOutcome:
    intents: Any = None
    failure_result: Any = None
    failed: bool = False


@dataclass(frozen=True)
class _TeardownRecoveryOutcome:
    intents: Any
    incomplete: bool
    warning: str | None


def _reset_teardown_verification_signals(runner: Any) -> None:
    """Reset the Blueprint 14a Stage 5 evidence owned by one teardown run."""
    runner._teardown_reconciliation = None
    runner._teardown_closure_verification = None


async def _run_pre_teardown_settlement_accounting(
    runner: Any,
    strategy: Any,
    deployment_id: str,
) -> str | None:
    """Run Blueprint 14's pre-enumeration settlement tick with Stage 6 semantics."""
    pre_gate_cycle_id = str(getattr(runner, "_last_cycle_id", None) or deployment_id)
    pre_gate_degraded: str | None = None
    try:
        from .perp_settlement_reconciler import reconcile_perp_settlements

        pre_gate_reconciliation = await reconcile_perp_settlements(
            runner,
            strategy,
            deployment_id=deployment_id,
            cycle_id=pre_gate_cycle_id,
            gateway_client=runner._get_gateway_client(),
        )
        if pre_gate_reconciliation.accounting_degraded:
            pre_gate_degraded = (
                "; ".join(pre_gate_reconciliation.degraded_reasons) or "pre-gate settlement reconciliation degraded"
            )
    except Exception as exc:  # noqa: BLE001 — settlement accounting must never block risk reduction
        pre_gate_degraded = f"pre-gate settlement reconciliation raised: {exc.__class__.__name__}: {exc}"
        logger.warning(
            "Teardown pre-enumeration perp settlement reconciliation failed (non-blocking)",
            exc_info=True,
        )
    if not pre_gate_degraded:
        return None

    logger.error(
        "🛑 Teardown pre-gate perp settlement reconciliation degraded for %s "
        "(teardown continues; the durable watch set retries on any later tick): %s",
        deployment_id,
        pre_gate_degraded,
    )
    from ..accounting.deferred_log import DeferredWrite
    from ..accounting.deferred_log import append as deferred_append

    try:
        deferred_append(
            DeferredWrite.now(
                kind="perp_settlement",
                deployment_id=deployment_id,
                cycle_id=pre_gate_cycle_id,
                intent_type="PERP_SETTLEMENT",
                error=pre_gate_degraded,
            )
        )
    except Exception:  # noqa: BLE001 - accounting failure must not block risk reduction
        logger.exception(
            "Could not persist teardown pre-gate settlement degradation for %s; teardown continues",
            deployment_id,
        )
    return pre_gate_degraded


def _create_teardown_market(strategy: Any) -> Any:
    """Create the market input consumed by Blueprint 14a Stage 2 planning."""
    try:
        teardown_market = strategy.create_market_snapshot()
        if hasattr(teardown_market, "get_price_oracle_dict"):
            logger.debug(
                f"Created market snapshot for teardown with prices: "
                f"{list(teardown_market.get_price_oracle_dict().keys())}"
            )
        else:
            logger.debug("Created multi-chain market snapshot for teardown")
        return teardown_market
    except Exception as exc:
        logger.warning(f"Failed to create market snapshot for teardown: {exc}. Continuing without market data.")
        return None


async def _generate_teardown_plan(
    runner: Any,
    strategy: Any,
    teardown_mode: TeardownMode,
    teardown_market: Any,
    request: Any,
    manager: Any,
    deployment_id: str,
    start_time: datetime,
) -> _TeardownIntentGenerationOutcome:
    """Generate the primitive-specific closing plan from Blueprint 14a Stage 2."""
    from .runner_models import IterationStatus

    try:
        try:
            teardown_intents = strategy.generate_teardown_intents(teardown_mode, market=teardown_market)
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            logger.debug(f"Strategy {deployment_id} uses old teardown signature (no market param), falling back")
            teardown_intents = strategy.generate_teardown_intents(teardown_mode)
    except Exception as exc:
        logger.error(f"Failed to generate teardown intents for {deployment_id}: {exc}")
        if request:
            # Nothing executed; enumerate honest failure counts without masking the
            # generation error. An unreadable count remains unset, never fabricated zero.
            closed, failed = await _failure_position_counts(strategy)
            _safe_mark(
                manager,
                "mark_failed",
                deployment_id,
                error=str(exc),
                positions_closed=closed,
                positions_failed=failed,
            )
        runner._request_teardown_failure_shutdown(str(exc))
        return _TeardownIntentGenerationOutcome(
            failure_result=runner._create_error_result(
                deployment_id,
                IterationStatus.STRATEGY_ERROR,
                str(exc),
                start_time,
            ),
            failed=True,
        )
    return _TeardownIntentGenerationOutcome(intents=teardown_intents)


async def _recover_teardown_positions(
    runner: Any,
    strategy: Any,
    teardown_intents: Any,
    teardown_mode: TeardownMode,
) -> _TeardownRecoveryOutcome:
    """Add Blueprint 14a Stage 1 LP and pending-order recovery discoveries."""
    teardown_intents, recovery_incomplete, recovery_warning = await _recover_orphaned_lp_intents(
        runner, strategy, teardown_intents, teardown_mode
    )
    lp_recovery_incomplete = recovery_incomplete
    lp_recovery_warning = recovery_warning
    teardown_intents, pending_incomplete, pending_warning = await _recover_pending_order_intents(
        runner, strategy, teardown_intents, teardown_mode
    )
    recovery_incomplete = recovery_incomplete or pending_incomplete
    # Preserve independent recovery failures instead of hiding one behind the other.
    recovery_warning = "; ".join(w for w in (recovery_warning, pending_warning) if w) or None
    # Execution still proceeds, but incomplete recovery must prevent clean certification.
    runner._teardown_recovery_incomplete = recovery_incomplete
    runner._teardown_recovery_warning = recovery_warning
    runner._teardown_lp_recovery_incomplete = lp_recovery_incomplete
    runner._teardown_lp_recovery_warning = lp_recovery_warning
    return _TeardownRecoveryOutcome(teardown_intents, recovery_incomplete, recovery_warning)


async def _restore_resumable_teardown_plan(
    runner: Any,
    manager: Any,
    deployment_id: str,
    teardown_intents: Any,
    start_time: datetime,
) -> tuple[Any, Any | None]:
    """Preserve accepted-order identity at Blueprint 14a's Stage 3 resume seam."""
    if teardown_intents:
        return teardown_intents, None

    accepted_lookup = await _load_runtime_resumable_accepted_async_state(runner, manager, deployment_id)
    if accepted_lookup.blocked_reason:
        return teardown_intents, _accepted_async_recovery_pending_result(
            runner, deployment_id, start_time, accepted_lookup.blocked_reason
        )
    accepted_state = accepted_lookup.state
    if accepted_state is not None:
        persisted_plan = json.loads(accepted_state.pending_intents_json)
        teardown_intents = persisted_plan if isinstance(persisted_plan, list) else []
        logger.warning(
            "🛑 %s generated no teardown intents but owns accepted async state; routing correlated resume",
            deployment_id,
        )
    return teardown_intents, None


def _fail_teardown_before_execution(
    runner: Any,
    manager: Any,
    request: Any,
    deployment_id: str,
    start_time: datetime,
    error: str,
) -> IterationResult:
    """Fail a pre-dispatch Stage 5 gate without certifying closure."""
    from .runner_models import IterationStatus

    logger.error("🛑 %s teardown blocked: %s", deployment_id, error)
    if request:
        _safe_mark(manager, "mark_failed", deployment_id, error=error)
    runner._request_teardown_failure_shutdown(error)
    return runner._create_error_result(deployment_id, IterationStatus.STRATEGY_ERROR, error, start_time)


async def _complete_teardown_without_intents(
    runner: Any,
    strategy: Any,
    manager: Any,
    request: Any,
    deployment_id: str,
    start_time: datetime,
    recovery_incomplete: bool,
    recovery_warning: str | None,
    pre_gate_degraded: str | None,
) -> IterationResult:
    """Apply Blueprint 14's Stage 5 no-intent completeness gate before success."""
    from .runner_models import IterationResult, IterationStatus

    completeness = await _check_no_intent_completeness(strategy, request)
    if completeness is None:
        error = (
            f"Teardown completeness: could not read the known open-position set for {deployment_id} "
            "(strategy enumeration failed); refusing to certify a clean 'no positions' teardown. "
            "Verify on-chain and re-run."
        )
        return _fail_teardown_before_execution(runner, manager, request, deployment_id, start_time, error)
    if not completeness.complete:
        return _fail_teardown_before_execution(
            runner,
            manager,
            request,
            deployment_id,
            start_time,
            completeness.error_message(),
        )
    if recovery_incomplete:
        error = recovery_warning or "On-chain LP discovery incomplete; manual check required."
        return _fail_teardown_before_execution(runner, manager, request, deployment_id, start_time, error)

    logger.info(f"🛑 {deployment_id} teardown complete (no positions to close)")
    if request:
        completion_result: dict[str, Any] = {"reason": "no_positions"}
        if pre_gate_degraded:
            completion_result["accounting_degraded"] = True
            completion_result["accounting_degraded_reason"] = pre_gate_degraded
        _safe_mark(manager, "mark_completed", deployment_id, result=completion_result)
    runner.request_shutdown()
    runner._lifecycle_write_state(deployment_id, LifecycleState.TERMINATED)
    runner._record_success()
    return IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        deployment_id=deployment_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )


async def _prepare_teardown_dispatch(
    runner: Any,
    strategy: Any,
    manager: Any,
    request: Any,
    deployment_id: str,
    teardown_intents: Any,
    teardown_market: Any,
) -> int | None:
    """Record Stage 1 enumeration and Stage 5 pre-dispatch reconciliation."""
    logger.info(f"🛑 {deployment_id} entering TEARDOWN mode ({len(teardown_intents)} intents to execute)")
    runner._lifecycle_write_state(deployment_id, LifecycleState.TEARING_DOWN)
    # Lifecycle accounting counts positions, not the several intents one position may require.
    # The intent count is only a start-time fallback when enumeration is unmeasured.
    open_positions_count = await _count_open_positions(strategy)
    total_positions = open_positions_count if open_positions_count is not None else len(teardown_intents)
    if request:
        _safe_mark(manager, "mark_started", deployment_id, total_positions=total_positions)

    log_teardown_decision(
        deployment_id=deployment_id,
        teardown_id=getattr(request, "teardown_id", None),
        phase=TeardownDecisionPhase.ENUMERATE,
        outcome="enumerated",
        description=f"enumerated {total_positions} open position(s); {len(teardown_intents)} closing intent(s)",
        position_count=open_positions_count,
        intent_count=len(teardown_intents),
    )
    runner._teardown_reconciliation = await reconcile_known_positions(runner, strategy, teardown_market)

    if teardown_market is not None and hasattr(teardown_market, "price"):
        try:
            prefetch_teardown_prices(teardown_market, teardown_intents)
        except Exception as exc:
            logger.warning(f"Failed to pre-fetch teardown prices: {exc}")
    return open_positions_count


def _complete_already_closed_teardown(
    runner: Any,
    manager: Any,
    request: Any,
    deployment_id: str,
    start_time: datetime,
) -> IterationResult:
    """Complete the retained Stage 2 all-balances-zero planning outcome."""
    from .runner_models import IterationResult, IterationStatus

    logger.info(f"🛑 {deployment_id} teardown complete (all positions already closed)")
    if request:
        _safe_mark(manager, "mark_completed", deployment_id, result={"reason": "all_balances_zero"})
    runner.request_shutdown()
    runner._lifecycle_write_state(deployment_id, LifecycleState.TERMINATED)
    runner._record_success()
    return IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        deployment_id=deployment_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )


async def _execute_multichain_teardown(
    runner: Any,
    strategy: Any,
    teardown_intents: Any,
    start_time: datetime,
    teardown_market: Any,
    manager: Any,
    request: Any,
    deployment_id: str,
    recovery_incomplete: bool,
    recovery_warning: str | None,
    open_positions_count: int | None,
) -> IterationResult:
    """Route the Blueprint 14a Stage 3 multi-chain compatibility lane."""
    from .runner_models import IterationStatus

    logger.warning(
        "🛑 %s multi-chain teardown lane performs NO token consolidation "
        "(VIB-5011 known gap — see blueprint 14 §Token Consolidation): "
        "residual non-target tokens stay in the wallet after closure.",
        deployment_id,
    )
    result = await runner._execute_multi_chain(
        strategy=strategy,
        intents=teardown_intents,
        start_time=start_time,
        market=teardown_market,
    )
    if result.success:
        result.status = IterationStatus.TEARDOWN
        logger.info(f"🛑 {deployment_id} teardown complete - shutting down strategy runner")
        runner.request_shutdown()
        if request:
            if recovery_incomplete:
                error = recovery_warning or "On-chain LP discovery incomplete; manual check required."
                logger.error("🛑 %s teardown degraded (recovery incomplete): %s", deployment_id, error)
                _safe_mark(manager, "mark_failed", deployment_id, error=error)
                runner._teardown_entry_blocked = True
                runner._teardown_entry_blocked_reason = f"teardown failed — {error}"
            else:
                _safe_mark(
                    manager,
                    "mark_completed",
                    deployment_id,
                    result=_positions_completion_result(open_positions_count, len(teardown_intents)),
                )
    else:
        from ..teardown.revert_hints import annotate_teardown_error

        failure_error = annotate_teardown_error(result.error) or "multi-chain teardown execution failed"
        if request:
            _safe_mark(manager, "mark_failed", deployment_id, error=failure_error)
        runner._request_teardown_failure_shutdown(failure_error)
    return result


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
    from ..teardown import get_teardown_state_manager_for_runtime

    deployment_id = strategy.deployment_id
    _reset_teardown_verification_signals(runner)
    manager: Any = get_teardown_state_manager_for_runtime(gateway_client=runner._get_gateway_client())
    request: Any = manager.get_active_request(deployment_id)

    pre_gate_degraded = await _run_pre_teardown_settlement_accounting(runner, strategy, deployment_id)
    teardown_market = _create_teardown_market(strategy)
    generation = await _generate_teardown_plan(
        runner,
        strategy,
        teardown_mode,
        teardown_market,
        request,
        manager,
        deployment_id,
        start_time,
    )
    if generation.failed:
        return generation.failure_result

    recovery = await _recover_teardown_positions(runner, strategy, generation.intents, teardown_mode)
    teardown_intents = _apply_lending_unwind_guard(
        recovery.intents,
        teardown_market,
        deployment_id,
        teardown_mode,
        teardown_id=getattr(request, "teardown_id", None),
    )
    teardown_intents, recovery_pending_result = await _restore_resumable_teardown_plan(
        runner, manager, deployment_id, teardown_intents, start_time
    )
    if recovery_pending_result is not None:
        return recovery_pending_result

    if not teardown_intents:
        return await _complete_teardown_without_intents(
            runner,
            strategy,
            manager,
            request,
            deployment_id,
            start_time,
            recovery.incomplete,
            recovery.warning,
            pre_gate_degraded,
        )
    open_positions_count = await _prepare_teardown_dispatch(
        runner,
        strategy,
        manager,
        request,
        deployment_id,
        teardown_intents,
        teardown_market,
    )
    if not teardown_intents:
        return _complete_already_closed_teardown(
            runner,
            manager,
            request,
            deployment_id,
            start_time,
        )

    if runner._is_multi_chain:
        return await _execute_multichain_teardown(
            runner,
            strategy,
            teardown_intents,
            start_time,
            teardown_market,
            manager,
            request,
            deployment_id,
            recovery.incomplete,
            recovery.warning,
            open_positions_count,
        )
    return await runner._execute_teardown_via_manager(
        strategy=strategy,
        teardown_intents=teardown_intents,
        teardown_mode=teardown_mode,
        teardown_market=teardown_market,
        start_time=start_time,
        request=request,
        state_manager=manager,
    )


def _load_pending_teardown_plan(state: Any) -> tuple[list[Any], int] | None:
    """Parse the persisted plan and its conservative resume floor."""
    try:
        plan = json.loads(state.pending_intents_json) if state.pending_intents_json else []
    except (TypeError, ValueError):
        return None
    if not isinstance(plan, list):
        return None
    floor = max(0, min(int(getattr(state, "current_intent_index", 0) or 0), len(plan)))
    return plan, floor


def _unique_pending_perp_close_index(plan: list[Any], floor: int, protocol: str) -> int | None:
    """Return one unambiguous same-protocol close index, never a guess."""
    candidate_indexes = [
        index
        for index in range(floor, len(plan))
        if isinstance(plan[index], dict)
        and str(plan[index].get("type", plan[index].get("intent_type", ""))).upper() == "PERP_CLOSE"
        and str(plan[index].get("protocol") or "").lower() == protocol
    ]
    return candidate_indexes[0] if len(candidate_indexes) == 1 else None


@dataclass(frozen=True)
class _AcceptedAsyncResumeLookup:
    """Tri-state accepted-order lookup: recovered, absent, or fail-closed."""

    state: Any | None = None
    blocked_reason: str | None = None


def _recovery_blocked(reason: str) -> _AcceptedAsyncResumeLookup:
    logger.error("Accepted async teardown recovery is unproven; deferring without dispatch: %s", reason)
    return _AcceptedAsyncResumeLookup(blocked_reason=reason)


async def _persist_recovered_async_marker(teardown_state_adapter: Any, state: Any, ledger_id: str) -> None:
    """Best-effort marker persistence; durable ledger recovery repeats next tick."""
    save_state = getattr(teardown_state_adapter, "save_teardown_state", None)
    if not callable(save_state):
        return
    try:
        maybe_saved = save_state(state)
        if inspect.isawaitable(maybe_saved):
            await maybe_saved
    except Exception:  # noqa: BLE001 — ledger reconstruction repeats every tick
        logger.exception(
            "Recovered accepted async marker from Phase-1 ledger %s but could not persist it; "
            "using the in-memory marker for this tick",
            ledger_id,
        )


async def _recover_accepted_async_marker_from_ledger(
    runner: Any, teardown_state_adapter: Any, state: Any
) -> _AcceptedAsyncResumeLookup:
    """Rebuild a missing marker from the exact unsettled Phase-1 ledger row."""
    from ..runner.perp_settlement_reconciler import _parse_async_orders, _read_phase1_close_inventory
    from ..teardown.teardown_manager import _mark_persisted_async_submission_accepted

    expected_cycle_id = f"teardown-{state.teardown_id}"
    inventory = await _read_phase1_close_inventory(runner, state.deployment_id, expected_cycle_id)
    if not inventory.measured:
        return _recovery_blocked(inventory.degraded_reason or "Phase-1 close ledger inventory was unmeasured")
    if not inventory.rows:
        return _AcceptedAsyncResumeLookup()
    if len(inventory.rows) != 1:
        return _recovery_blocked("multiple Phase-1 close ledgers match this teardown cycle")
    parsed_plan = _load_pending_teardown_plan(state)
    if parsed_plan is None:
        return _recovery_blocked("the persisted teardown plan could not be parsed")
    plan, floor = parsed_plan
    for ledger in inventory.rows:
        ledger_id = str(ledger.get("id") or "")
        protocol = str(ledger.get("protocol") or "").lower()
        # A ledger row does not identify its plan index; ambiguous same-protocol
        # matches must not suppress an arbitrary close.
        intent_index = _unique_pending_perp_close_index(plan, floor, protocol)
        if intent_index is None:
            return _recovery_blocked("the Phase-1 close ledger has no unique pending plan match")
        order_keys = tuple(
            key.lower() for key, _is_long in _parse_async_orders(ledger.get("extracted_data_json") or "")
        )
        if not order_keys:
            return _recovery_blocked("the Phase-1 close ledger has no exact order keys")
        if not _mark_persisted_async_submission_accepted(
            state,
            intent_index,
            order_keys=order_keys,
            ledger_entry_id=ledger_id,
        ):
            return _recovery_blocked("the accepted close could not be represented in the persisted plan")
        await _persist_recovered_async_marker(teardown_state_adapter, state, ledger_id)
        logger.warning(
            "Recovered missing accepted async marker from exact Phase-1 ledger %s (%d order key(s))",
            ledger_id,
            len(order_keys),
        )
        return _AcceptedAsyncResumeLookup(state=state)
    return _recovery_blocked("the Phase-1 close ledger could not be correlated")


async def _load_resumable_accepted_async_state(
    teardown_state_adapter: Any, deployment_id: str, *, runner: Any | None = None
) -> _AcceptedAsyncResumeLookup:
    """Load only a resumable state that owns an accepted async submission."""
    from ..teardown.teardown_manager import has_accepted_async_submission

    if teardown_state_adapter is None:
        return _recovery_blocked("the teardown execution-state adapter is unavailable")
    get_persisted_state = getattr(teardown_state_adapter, "get_teardown_state", None)
    if not callable(get_persisted_state):
        return _recovery_blocked("the teardown execution-state reader is unavailable")
    maybe_state = get_persisted_state(deployment_id)
    if not inspect.isawaitable(maybe_state):
        return _recovery_blocked("the teardown execution-state read was unmeasured")
    state = await maybe_state
    if state is None or not state.is_resumable:
        return _AcceptedAsyncResumeLookup()
    if has_accepted_async_submission(state):
        return _AcceptedAsyncResumeLookup(state=state)
    if runner is None:
        return _recovery_blocked("ledger recovery is unavailable for a resumable teardown")
    return await _recover_accepted_async_marker_from_ledger(runner, teardown_state_adapter, state)


async def _load_runtime_resumable_accepted_async_state(
    runner: Any, request_state_manager: Any, deployment_id: str
) -> _AcceptedAsyncResumeLookup:
    """Resolve the execution-state adapter before the no-intents fast path."""
    from ..teardown import create_teardown_state_adapter_for_runtime

    raw_db_path = getattr(request_state_manager, "db_path", None)
    sqlite_path = raw_db_path if isinstance(raw_db_path, str | Path) else None
    adapter = create_teardown_state_adapter_for_runtime(
        gateway_client=runner._get_gateway_client(),
        sqlite_path=sqlite_path,
    )
    return await _load_resumable_accepted_async_state(adapter, deployment_id, runner=runner)


def _accepted_async_recovery_pending_result(
    runner: Any, deployment_id: str, start_time: datetime, reason: str
) -> IterationResult:
    """Defer one teardown tick without dispatch while recovery is unproven."""
    from .runner_models import IterationResult, IterationStatus

    logger.error("🛑 %s accepted-order recovery deferred: %s", deployment_id, reason)
    return IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        error=reason,
        deployment_id=deployment_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )


async def _resolve_manager_execution_state(
    runner: Any,
    teardown_mgr: Any,
    strategy: Any,
    teardown_intents: list[Any],
    teardown_mode: Any,
    is_auto_mode: bool,
    start_time: datetime,
    accepted_lookup: _AcceptedAsyncResumeLookup,
) -> tuple[Any | None, IterationResult | None]:
    """Choose correlated resume, fail-closed deferral, or fresh persistence."""
    from . import _teardown_helpers as _h

    if accepted_lookup.blocked_reason:
        return None, _accepted_async_recovery_pending_result(
            runner,
            strategy.deployment_id,
            start_time,
            accepted_lookup.blocked_reason,
        )
    if accepted_lookup.state is not None:
        logger.warning(
            "🛑 Resuming accepted async teardown %s from its correlated persisted plan",
            accepted_lookup.state.teardown_id,
        )
        return accepted_lookup.state, None
    # Persist the deduplicated dispatch plan so a restart cannot resurrect a
    # duplicate perp close. Coverage still uses the original plan upstream.
    from ..teardown.single_close_guard import collapse_duplicate_perp_closes

    return await _h.run_cancel_window_and_persist(
        runner,
        teardown_mgr,
        strategy,
        collapse_duplicate_perp_closes(teardown_intents).dispatch,
        teardown_mode,
        is_auto_mode,
        start_time,
    )


def _clear_stale_pending_recovery_after_accepted_fill(
    runner: Any,
    resumable_state: Any,
    teardown_result: Any,
    *,
    resume_accepted_async: bool,
) -> None:
    """Drop only pre-resume pending evidence superseded by exact terminal proof."""
    if not resume_accepted_async or not teardown_result.success:
        return
    from ..teardown.teardown_manager import accepted_async_order_keys

    accepted_keys = accepted_async_order_keys(resumable_state)
    pending_recovery_keys = getattr(runner, "_teardown_pending_recovery_keys", None)
    if pending_recovery_keys is None or not pending_recovery_keys.issubset(accepted_keys):
        return
    # Exact terminal proof supersedes only the matching pending-order signal.
    runner._teardown_recovery_incomplete = bool(getattr(runner, "_teardown_lp_recovery_incomplete", False))
    runner._teardown_recovery_warning = getattr(runner, "_teardown_lp_recovery_warning", None)


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

    deployment_id = strategy.deployment_id
    mode_str = "graceful" if teardown_mode == TeardownMode.SOFT else "emergency"

    is_auto_mode = derive_teardown_auto_mode(request)

    compiler, early = await _h.resolve_compiler_or_fallback(
        runner, strategy, teardown_intents, teardown_market, start_time, request, state_manager
    )
    if compiler is None:
        return early  # type: ignore[return-value]

    teardown_mgr, teardown_state_adapter = _h.build_teardown_manager(
        runner, compiler, state_manager, request, strategy=strategy
    )
    accepted_lookup = await _load_resumable_accepted_async_state(teardown_state_adapter, deployment_id, runner=runner)
    resumable_state = accepted_lookup.state
    resume_accepted_async = resumable_state is not None

    logger.info(
        f"🛑 Routing {deployment_id} teardown through TeardownManager (mode={mode_str}, intents={len(teardown_intents)})"
    )

    # One catch boundary reflects execution failures into both persistence surfaces.
    teardown_state = None
    saved_last_cycle_id: str | None = None
    saved_ctx_cycle_id: str | None = None
    cycle_id_swapped = False
    pre_bracket_outcome = None
    post_bracket_outcome = None

    try:
        positions, early = await _h.fetch_positions_or_fallback(
            runner, strategy, teardown_intents, teardown_market, start_time, request, state_manager
        )
        if positions is None:
            return early  # type: ignore[return-value]

        safety_error = _h.validate_safety_or_error(
            runner, teardown_mgr, strategy, positions, teardown_mode, start_time, request, state_manager
        )
        if safety_error is not None:
            return safety_error

        # Resume only exact persisted order-key state; unproven recovery defers
        # without dispatch, and only measured absence creates fresh state.
        teardown_state, cancel_short_circuit = await _resolve_manager_execution_state(
            runner,
            teardown_mgr,
            strategy,
            teardown_intents,
            teardown_mode,
            is_auto_mode,
            start_time,
            accepted_lookup,
        )
        if cancel_short_circuit is not None:
            return cancel_short_circuit
        assert teardown_state is not None

        # Ledger, snapshot, and metrics writers consult both cycle-id surfaces.
        # Swap both for the bracket and restore them together in finally.
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

        price_oracle = _h.resolve_price_oracle(teardown_market)

        # Capture the accounting baseline before dispatch; write failures degrade
        # accounting but never block the unwind.
        if teardown_mgr.runner_helpers.has_snapshot:
            pre_bracket_outcome = await teardown_mgr.runner_helpers.capture_snapshot(
                strategy,
                teardown_cycle_id=teardown_cycle_id,
                pre_teardown=True,
            )
            if pre_bracket_outcome.accounting_degraded:
                logger.error(
                    "Pre-teardown snapshot accounting degraded for %s — %s",
                    deployment_id,
                    pre_bracket_outcome.degraded_reason or "unknown",
                )

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
            resume_accepted_async=resume_accepted_async,
        )

        _clear_stale_pending_recovery_after_accepted_fill(
            runner,
            resumable_state,
            teardown_result,
            resume_accepted_async=resume_accepted_async,
        )

        # Release only after closure and before the final snapshot so depositor
        # fund movements are included in terminal accounting.
        await _maybe_release_vault_after_teardown(
            runner, strategy, teardown_market, teardown_cycle_id, teardown_result, deployment_id
        )

        if teardown_mgr.runner_helpers.has_snapshot:
            post_bracket_outcome = await teardown_mgr.runner_helpers.capture_snapshot(
                strategy,
                teardown_cycle_id=teardown_cycle_id,
                pre_teardown=False,
            )
            if post_bracket_outcome.accounting_degraded:
                logger.error(
                    "Post-teardown snapshot accounting degraded for %s — %s",
                    deployment_id,
                    post_bracket_outcome.degraded_reason or "unknown",
                )

        # Snapshot degradation is additive to per-intent accounting degradation.
        bracket_failures = sum(
            1 for o in (pre_bracket_outcome, post_bracket_outcome) if o is not None and o.accounting_degraded
        )
        if bracket_failures:
            teardown_result.accounting_degraded = True
            teardown_result.accounting_degraded_count += bracket_failures

        # Apply all available risk reduction before refusing certification for an
        # incompletely measured recovery.
        if getattr(runner, "_teardown_recovery_incomplete", False) and teardown_result.success:
            warn = (
                getattr(runner, "_teardown_recovery_warning", None)
                or "Teardown recovery incomplete; manual check required."
            )
            logger.error(
                "🛑 %s teardown executed but recovery incomplete — marking manual-check: %s",
                deployment_id,
                warn,
            )
            teardown_result = replace(
                teardown_result,
                success=False,
                error=warn,
                recovery_options=[
                    "Verify residual positions / pending orders on-chain",
                    "Re-run teardown once the blocking condition clears (RPC health / GMX cancel window)",
                ],
            )

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
        runner._teardown_recovery_incomplete = False
        runner._teardown_recovery_warning = None
        runner._teardown_lp_recovery_incomplete = False
        runner._teardown_lp_recovery_warning = None
        runner._teardown_pending_recovery_keys = None

    return _h.map_teardown_result(runner, strategy, start_time, teardown_result, teardown_mode, request, state_manager)


class _VaultReleaseIntent:
    """Lightweight teardown-lane intent for a vault-release leg (VIB-5667).

    The vault-release legs (propose NAV / initiateClosing / close / redeem /
    approve) execute as raw ``ActionBundle``s, not framework ``Intent``s. This
    duck-typed adapter carries just the surface ``commit_teardown_intent`` reads
    off an intent (``intent_type`` with a ``.value``, ``protocol``, ``chain``) so
    each release leg drives the SAME teardown accounting/ledger pipeline as a
    normal closing intent — the loud-but-never-block commit path. The
    position-event builder returns ``None`` for these management action types
    (they are not positions), so no spurious CLOSE row is written.
    """

    __slots__ = ("intent_type", "protocol", "chain", "vault_address")

    def __init__(self, action_type: str, protocol: str, chain: str, vault_address: str) -> None:
        from types import SimpleNamespace

        self.intent_type = SimpleNamespace(value=action_type)
        self.protocol = protocol
        self.chain = chain
        self.vault_address = vault_address


async def execute_vault_release(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_market: Any | None,
    *,
    teardown_cycle_id: str,
) -> Any | None:
    """Release the vault (Open->Closing->Closed) after position closure (VIB-5667).

    Invoked from ``execute_teardown_via_manager`` AFTER the strategy's closing
    intents (LP_CLOSE + consolidation SWAP -> underlying) have run and the Safe
    holds the unwound capital in the vault's underlying token — so ``close()``'s
    ``transferFrom(safe, vault, totalAssets)`` is backed. Releasing transitions the
    vault to ``Closed`` so EVERY depositor (including a deposit-only user who never
    requested redemption) can synchronously redeem their capital.

    No-op for a plain (non-vault) strategy — ``runner._vault_lifecycle is None``.

    Every orchestrator execution inside ``release_on_teardown`` is paired with the
    ``commit`` callback bound here (``runner.commit_teardown_intent``) so ledger /
    accounting rows are written — the teardown anti-bypass invariant. A degraded
    release is loud but never blocks (teardown's inverted failure semantics); the
    caller folds ``ReleaseResult.degraded`` into the TeardownResult.
    """
    vault_lifecycle = getattr(runner, "_vault_lifecycle", None)
    if vault_lifecycle is None:
        return None

    from almanak.framework.vault.capability import default_vault_protocol

    vault_protocol = default_vault_protocol()
    chain = getattr(strategy, "chain", "") or ""

    async def _commit(*, action_type: str, bundle: Any, execution_result: Any, signer: str) -> None:  # noqa: ARG001
        # Every release execution uses the same teardown accounting commit boundary.
        intent = _VaultReleaseIntent(
            action_type=action_type,
            protocol=vault_protocol,
            chain=chain,
            vault_address=(
                getattr(_cfg, "vault_address", "") if (_cfg := getattr(vault_lifecycle, "_config", None)) else ""
            ),
        )
        await runner.commit_teardown_intent(
            strategy,
            intent,
            execution_result=execution_result,
            execution_context=None,
            bundle_metadata=getattr(bundle, "metadata", None),
            teardown_cycle_id=teardown_cycle_id,
        )

    logger.info(
        "🛑 %s vault-release: transitioning vault Open->Closing->Closed to free depositor capital",
        strategy.deployment_id,
    )
    release_result = await vault_lifecycle.release_on_teardown(strategy, teardown_market, commit=_commit)

    if release_result.skipped:
        logger.info("🛑 %s vault-release skipped: %s", strategy.deployment_id, release_result.reason)
    elif release_result.degraded:
        logger.error(
            "🛑 %s vault-release DEGRADED — depositors may be stranded: %s",
            strategy.deployment_id,
            release_result.reason,
        )
    elif release_result.released:
        logger.info(
            "🛑 %s vault-release complete: vault %s, final_nav=%d, manager_shares_redeemed=%d — "
            "all depositor capital is now claimable",
            strategy.deployment_id,
            release_result.final_state,
            release_result.final_nav,
            release_result.manager_shares_redeemed,
        )
    return release_result


async def _maybe_release_vault_after_teardown(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_market: Any | None,
    teardown_cycle_id: str,
    teardown_result: Any,
    deployment_id: str,
) -> None:
    """Fold vault-release into the manager-lane result (VIB-5667).

    No-op for a non-vault strategy (``_vault_lifecycle is None``) or when position
    closure did not succeed (a failed unwind means the Safe may not hold the
    underlying that ``close()``'s ``transferFrom`` requires — releasing then would
    just revert). A degraded release is loud but never blocks — teardown's first
    job (removing on-chain risk) is already done; it only flags accounting-degraded
    so the operator knows depositors may still need a manual release. Never raises
    into the teardown lane.

    Releasing the vault is IRREVERSIBLE (Open->Closed is one-way). So it must NOT
    run while an on-chain position may still be open (audit #1): if LP-orphan
    discovery / pending-order recovery came back INCOMPLETE
    (``runner._teardown_recovery_incomplete`` — the same signal that refuses to
    certify a clean teardown just below the call site), a deployment-owned position
    could still be live even though every executed intent succeeded. Closing the
    vault then would lock depositors into a Closed vault around an un-unwound
    position. Skip the release, flag degraded, and leave the vault Open — the
    operator re-runs teardown once the orphan is resolved (which then releases).
    """
    if not teardown_result.success or getattr(runner, "_vault_lifecycle", None) is None:
        return
    if getattr(runner, "_teardown_recovery_incomplete", False):
        logger.error(
            "🛑 %s vault-release SKIPPED — teardown recovery incomplete (a deployment-owned LP orphan "
            "or pending order may still be open); refusing the IRREVERSIBLE vault close until positions "
            "are certified gone. Vault left OPEN; re-run teardown after the orphan is resolved to release "
            "depositor capital.",
            deployment_id,
        )
        teardown_result.accounting_degraded = True
        teardown_result.accounting_degraded_count += 1
        return
    try:
        release_result = await runner._execute_vault_release(
            strategy, teardown_market, teardown_cycle_id=teardown_cycle_id
        )
        if release_result is not None and release_result.degraded:
            teardown_result.accounting_degraded = True
            teardown_result.accounting_degraded_count += 1
    except Exception:  # noqa: BLE001 — release must never fault the teardown lane
        logger.error(
            "🛑 %s vault-release raised unexpectedly — teardown risk reduction already complete; "
            "depositors may need manual release",
            deployment_id,
            exc_info=True,
        )
        teardown_result.accounting_degraded = True
        teardown_result.accounting_degraded_count += 1


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
    from ..teardown.single_close_guard import collapse_duplicate_perp_closes

    deployment_id = strategy.deployment_id

    # Deduplicate at dispatch so the manager's completeness check can still inspect
    # the original plan while this fallback never submits two closes for one perp.
    teardown_intents = collapse_duplicate_perp_closes(teardown_intents).dispatch

    # All accounting surfaces require the canonical teardown cycle ID. Synthesize
    # only the ID component when no persisted request exists.
    logger.warning(
        "🛑 %s inline-fallback teardown lane performs NO token consolidation "
        "(VIB-5011 known gap — see blueprint 14 §Token Consolidation): "
        "residual non-target tokens stay in the wallet after closure.",
        deployment_id,
    )

    teardown_id = getattr(request, "teardown_id", None) or str(uuid.uuid4())
    teardown_cycle_id = f"teardown-{teardown_id}"
    saved_last_cycle_id = getattr(runner, "_last_cycle_id", "") or ""
    saved_ctx_cycle_id = get_cycle_id()
    runner._last_cycle_id = teardown_cycle_id
    set_cycle_id(teardown_cycle_id)

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
                    deployment_id,
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
                    deployment_id,
                    post_outcome.degraded_reason or "unknown",
                )
        if accounting_degraded_count and getattr(result, "error", None) is None:
            result.error = (
                f"accounting_degraded={accounting_degraded_count} (chain-side OK; "
                "see accounting_deferred.jsonl for failed writes)"
            )
        return result
    finally:
        runner._last_cycle_id = saved_last_cycle_id
        if saved_ctx_cycle_id is None:
            clear_cycle_id()
        else:
            set_cycle_id(saved_ctx_cycle_id)


def _apply_inline_swap_clamp(
    runner: Any,
    intent: Any,
    balance_token: str,
    balance_value: Any,
    deployment_id: str,
    chain: str = "",
    wallet_address: str = "",
) -> tuple[bool, bool, Any]:
    """ALM-2766 inline-lane swap-back clamp — delegates to ``decide_swap_clamp``.

    Clamps an ``amount='all'`` swap-back to the strategy's TRACKED quantity so a
    default teardown never sweeps commingled wallet funds. The inline lane never
    runs token consolidation (blueprint 14 §4.5 "Known gaps"), so there is no
    consent opt-out — every swap-back is clamped to ``min(tracked, live)``.

    The decision is computed by the SAME shared pure helper the manager lane uses
    (``swap_clamp.decide_swap_clamp``), so the two lanes cannot drift. Extracted
    out of ``_execute_teardown_inline_body`` to keep that function's branches —
    and its CRAP score — bounded.

    Returns ``(skip, degraded, resolved_balance)``:
      * non-SWAP intents → ``(False, False, balance_value)`` (no clamp applies).
      * a fail-closed skip → fires the VIB-4587 sweep WARNING and returns
        ``(True, decision.degraded, None)``.
      * a proceeding clamp → ``(False, False, min(tracked, live))``.
    """
    from ..teardown.swap_clamp import SwapClampDecision, decide_swap_clamp, read_tracked_swap_inventory

    itype = getattr(getattr(intent, "intent_type", None), "value", getattr(intent, "intent_type", None))
    if not (isinstance(itype, str) and itype.rsplit(".", 1)[-1].upper() == "SWAP"):
        return False, False, balance_value

    try:
        live = Decimal(str(balance_value))
    except (InvalidOperation, TypeError, ValueError):
        live = None

    # The intent chain must key both the live balance and tracked inventory.
    effective_chain = getattr(intent, "chain", None) or chain

    if live is None:
        decision = SwapClampDecision(None, True, True, "live_balance_unmeasured")
    else:
        decision = decide_swap_clamp(
            live_balance=live,
            tracked_map=read_tracked_swap_inventory(
                state_manager=getattr(runner, "state_manager", None),
                deployment_id=deployment_id,
                chain=effective_chain,
                wallet_address=wallet_address,
            ),
            from_token=balance_token,
            chain=effective_chain,
        )

    if not decision.skip:
        return False, False, decision.amount

    warn_if_sweep_non_strategy_balance(
        state_manager=getattr(runner, "state_manager", None),
        deployment_id=deployment_id,
        intent=intent,
        balance_token=balance_token,
        balance_value=balance_value,
    )
    logger.warning(
        "🛑 ALM-2766 inline teardown swap-back clamp: SKIPPING %s swap "
        "(reason=%s, degraded=%s) — not sweeping commingled wallet funds.",
        balance_token,
        decision.reason,
        decision.degraded,
    )
    return True, decision.degraded, None


@dataclass(frozen=True)
class _InlinePreparedIntent:
    intent: Any
    skipped: bool = False
    failure_result: Any = None
    accounting_degraded: bool = False


@dataclass(frozen=True)
class _InlineDispatchOutcome:
    last_result: Any = None
    all_success: bool = True
    accounting_degraded_count: int = 0


def _read_inline_teardown_balance(
    teardown_market: Any,
    balance_token: str,
    intent_chain: str | None,
) -> tuple[Any, Exception | None]:
    """Read the live Stage 2 amount while retaining single-chain fallback semantics."""
    try:
        if intent_chain:
            return teardown_market.balance(balance_token, intent_chain), None
        return teardown_market.balance(balance_token), None
    except TypeError:
        # A single-chain MarketSnapshot does not accept the chain argument.
        try:
            return teardown_market.balance(balance_token), None
        except Exception as exc:  # noqa: BLE001
            return None, exc
    except Exception as exc:  # noqa: BLE001
        return None, exc


def _prepare_inline_teardown_intent(
    runner: Any,
    strategy: Any,
    intent: Any,
    teardown_market: Any | None,
    start_time: datetime,
    intent_index: int,
) -> _InlinePreparedIntent:
    """Resolve one Stage 2 live amount and apply the tracked-inventory clamp."""
    from .runner_models import IterationResult, IterationStatus

    if not Intent.has_chained_amount(intent):
        return _InlinePreparedIntent(intent)

    deployment_id = strategy.deployment_id
    balance_token = (
        getattr(intent, "from_token", None) or getattr(intent, "token", None) or getattr(intent, "token_in", None)
    )
    if balance_token and teardown_market is not None:
        invalidate = getattr(teardown_market, "invalidate_balance", None)
        if callable(invalidate):
            try:
                invalidate(balance_token)
            except Exception:  # noqa: BLE001
                logger.debug(
                    "invalidate_balance(%s) failed in inline lane; using cached balance",
                    balance_token,
                    exc_info=True,
                )

        balance, balance_error = _read_inline_teardown_balance(
            teardown_market,
            balance_token,
            getattr(intent, "chain", None),
        )
        if balance_error is not None:
            logger.error(
                f"🛑 Teardown intent {intent_index + 1}: failed to resolve balance for {balance_token}: "
                f"{balance_error}. Token may be missing from the registry. Position may remain open."
            )
            return _InlinePreparedIntent(
                intent,
                failure_result=IterationResult(
                    status=IterationStatus.COMPILATION_FAILED,
                    intent=intent,
                    error=f"Cannot resolve amount='all' for {balance_token}: {balance_error}",
                    deployment_id=deployment_id,
                    duration_ms=runner._calculate_duration_ms(start_time),
                ),
            )

        balance_value = balance.balance if hasattr(balance, "balance") else balance
        if balance_value <= 0:
            logger.info(
                f"🛑 Teardown intent {intent_index + 1}: {balance_token} balance is 0, skipping (already closed)"
            )
            return _InlinePreparedIntent(intent, skipped=True)

        skip, degraded, balance_value = _apply_inline_swap_clamp(
            runner,
            intent,
            balance_token,
            balance_value,
            deployment_id,
            chain=getattr(strategy, "chain", "") or "",
            wallet_address=getattr(strategy, "wallet_address", "") or "",
        )
        if skip:
            return _InlinePreparedIntent(intent, skipped=True, accounting_degraded=degraded)

        warn_if_sweep_non_strategy_balance(
            state_manager=getattr(runner, "state_manager", None),
            deployment_id=deployment_id,
            intent=intent,
            balance_token=balance_token,
            balance_value=balance_value,
        )
        resolved_intent = Intent.set_resolved_amount(intent, balance_value)
        logger.info(f"🛑 Resolved amount='all' for {balance_token}: {balance_value}")
        return _InlinePreparedIntent(resolved_intent)

    if balance_token:
        logger.warning(
            f"🛑 Teardown intent {intent_index + 1}: amount='all' for {balance_token} but no market context. "
            f"Passing to compiler as-is — compilation may fail."
        )
    else:
        logger.debug(f"🛑 Teardown intent {intent_index + 1}: no token field, passing to compiler as-is")
    return _InlinePreparedIntent(intent)


async def _execute_inline_teardown_intent(
    runner: Any,
    strategy: Any,
    intent: Any,
    teardown_market: Any | None,
    start_time: datetime,
    *,
    intent_index: int,
    intent_count: int,
    teardown_cycle_id: str,
    deferred_append: Any,
) -> tuple[Any, int]:
    """Dispatch one Stage 3 intent and preserve Stage 4 inverted accounting semantics."""
    from ..state.exceptions import AccountingPersistenceError
    from .runner_models import IterationResult, IterationStatus

    deployment_id = strategy.deployment_id
    try:
        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent,
            start_time=start_time,
            total_intents=1,
            market=teardown_market,
        )
    except AccountingPersistenceError as acc_err:
        logger.error(
            "🛑 Teardown intent %d/%d (inline) — accounting persistence failed but chain-side OK: %s",
            intent_index + 1,
            intent_count,
            acc_err,
        )
        try:
            deferred_append(
                kind=str(acc_err.write_kind) if acc_err.write_kind else "ledger",
                deployment_id=deployment_id,
                cycle_id=teardown_cycle_id,
                intent_type=getattr(intent.intent_type, "value", str(intent.intent_type)),
                error=str(acc_err),
                extra={"phase": "inline-per-intent"},
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "🛑 Teardown intent %d/%d (inline) — deferred-write log append failed; "
                "original error=%s; continuing teardown",
                intent_index + 1,
                intent_count,
                acc_err,
            )
        result = IterationResult(
            status=IterationStatus.SUCCESS,
            intent=intent,
            deployment_id=deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
        return result, 1
    return result, 0


async def _dispatch_inline_teardown_intents(
    runner: Any,
    strategy: Any,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    *,
    teardown_cycle_id: str,
    deferred_append: Any,
) -> _InlineDispatchOutcome:
    """Run Blueprint 14a Stage 3 sequentially, stopping only on chain-side failure."""
    accounting_degraded_count = 0
    last_result = None
    for intent_index, intent in enumerate(teardown_intents):
        logger.info(
            f"🛑 Executing teardown intent {intent_index + 1}/{len(teardown_intents)}: {intent.intent_type.value}"
        )
        prepared = _prepare_inline_teardown_intent(
            runner,
            strategy,
            intent,
            teardown_market,
            start_time,
            intent_index,
        )
        accounting_degraded_count += int(prepared.accounting_degraded)
        if prepared.failure_result is not None:
            return _InlineDispatchOutcome(prepared.failure_result, False, accounting_degraded_count)
        if prepared.skipped:
            continue

        result, degraded_count = await _execute_inline_teardown_intent(
            runner,
            strategy,
            prepared.intent,
            teardown_market,
            start_time,
            intent_index=intent_index,
            intent_count=len(teardown_intents),
            teardown_cycle_id=teardown_cycle_id,
            deferred_append=deferred_append,
        )
        accounting_degraded_count += degraded_count
        last_result = result
        if not result.success:
            logger.error(f"🛑 Teardown intent {intent_index + 1} failed: {result.error}")
            return _InlineDispatchOutcome(last_result, False, accounting_degraded_count)

    return _InlineDispatchOutcome(last_result, True, accounting_degraded_count)


def _finalize_inline_teardown(
    runner: Any,
    strategy: Any,
    teardown_intents: list,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
    pre_exec_positions_total: int | None,
    dispatch: _InlineDispatchOutcome,
) -> IterationResult:
    """Apply the inline lane's terminal persistence and lifecycle semantics."""
    from .runner_models import IterationResult, IterationStatus

    deployment_id = strategy.deployment_id
    last_result = dispatch.last_result
    all_success = dispatch.all_success

    # The inline lane cannot perform irreversible vault release. Never certify it
    # for a vault strategy, even when every position-closing intent succeeded.
    if all_success and last_result is not None and getattr(runner, "_vault_lifecycle", None) is not None:
        all_success = False
        last_result.error = (
            "vault strategy reached the inline teardown fallback, which cannot release the vault; "
            "positions closed but the vault is still OPEN (depositors would be stranded). Restart the "
            "runner and re-run teardown so the manager lane releases depositor capital."
        )
        logger.error(
            "🛑 %s inline teardown cannot release vault — failing closed: %s", deployment_id, last_result.error
        )

    if last_result:
        if all_success:
            last_result.status = IterationStatus.TEARDOWN
            logger.info(f"🛑 {deployment_id} teardown complete - shutting down strategy runner")
            runner.request_shutdown()
            runner._lifecycle_write_state(deployment_id, LifecycleState.TERMINATED)
            runner._record_success()
            if request:
                _safe_mark(
                    state_manager,
                    "mark_completed",
                    deployment_id,
                    result=_positions_completion_result(pre_exec_positions_total, len(teardown_intents)),
                )
        else:
            logger.warning(f"🛑 {deployment_id} teardown incomplete - manual intervention may be required")
            if request:
                _safe_mark(state_manager, "mark_failed", deployment_id, error=last_result.error or "execution failed")
            runner._request_teardown_failure_shutdown(last_result.error or "inline teardown execution failed")
        return last_result

    # The same vault-release refusal applies when no intent reached execution.
    if getattr(runner, "_vault_lifecycle", None) is not None:
        error = (
            "vault strategy reached the inline teardown fallback with no executed intents; the vault "
            "is still OPEN (depositors would be stranded). Restart the runner and re-run teardown so "
            "the manager lane releases depositor capital."
        )
        logger.error(
            "🛑 %s inline teardown (no intents) cannot release vault — failing closed: %s", deployment_id, error
        )
        if request:
            _safe_mark(state_manager, "mark_failed", deployment_id, error=error)
        runner._request_teardown_failure_shutdown(error)
        return IterationResult(
            status=IterationStatus.EXECUTION_FAILED,
            intent=None,
            error=error,
            deployment_id=deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    logger.info(f"🛑 {deployment_id} teardown: all positions already closed, shutting down")
    runner.request_shutdown()
    runner._lifecycle_write_state(deployment_id, LifecycleState.TERMINATED)
    runner._record_success()
    if request:
        _safe_mark(state_manager, "mark_completed", deployment_id, result={"reason": "all_positions_already_closed"})
    return IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        deployment_id=deployment_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )


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
    pre_exec_positions_total = await _count_open_positions(strategy)
    dispatch = await _dispatch_inline_teardown_intents(
        runner,
        strategy,
        teardown_intents,
        teardown_market,
        start_time,
        teardown_cycle_id=teardown_cycle_id,
        deferred_append=deferred_append,
    )
    result = _finalize_inline_teardown(
        runner,
        strategy,
        teardown_intents,
        start_time,
        request,
        state_manager,
        pre_exec_positions_total,
        dispatch,
    )
    return result, dispatch.accounting_degraded_count


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

    # Preserve the empty-dict measurement: None enables unsafe $1 placeholder prices.
    fetched: dict[str, Decimal] | None = None
    if market is not None and hasattr(market, "get_price_oracle_dict"):
        fetched = market.get_price_oracle_dict()
    fallback = get_fallback_teardown_prices(market)
    merged = {**(fallback or {}), **(fetched if fetched is not None else {})}
    price_oracle = merged if merged else None

    has_prices = bool(price_oracle)
    if not has_prices:
        # Refuse placeholder prices: they can mis-size expected output and slippage.
        logger.error(
            "🛑 Teardown HARD STOP (VIB-2928): no real token prices available "
            "for %s — refusing to compile teardown on placeholder ($1) prices "
            "(likely a gateway/oracle connectivity issue). Teardown fails "
            "loudly and must be retried once prices resolve.",
            strategy.deployment_id,
        )
        return None

    try:
        compiler_config = IntentCompilerConfig(
            allow_placeholder_prices=False,
            managed_fork=is_managed_fork_network(getattr(strategy, "_gateway_network", None)),
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


def _extract_perp_index_chains(intents: list, market: Any = None) -> dict[str, str | None]:
    """Return shared teardown index-symbol extraction without blocking unwind.

    ``market`` is optional so the symbol-shaped path keeps working without a
    gateway; when supplied, address-first perp markets are additionally
    resolved to their index symbol through the gateway's verified market
    record — the shape every GMX demo actually emits (ALM-3217; see
    ``resolve_perp_index_chains_via_gateway``).
    """
    try:
        from ..teardown.oracle_warmup import (
            extract_warmable_token_chains,
            resolve_perp_index_chains_via_gateway,
        )

        chains = extract_warmable_token_chains(intents, None)
        fallback = getattr(market, "chain", None) or getattr(market, "_chain", None)
        for symbol, symbol_chain in resolve_perp_index_chains_via_gateway(market, intents, fallback).items():
            chains.setdefault(symbol, symbol_chain)
        return chains
    except Exception as e:  # noqa: BLE001 - price warming is best-effort
        logger.warning("Could not extract perp index symbols for teardown price prefetch: %s", e)
        return {}


def _prefetch_teardown_price(
    market: Any,
    token: str,
    symbol: str,
    token_chain: str | None,
) -> str | None:
    """Warm one measured price, returning the identifier that succeeded."""
    try:
        if token_chain:
            market.price(symbol, chain=token_chain)
        else:
            market.price(symbol)
        return symbol
    except Exception:  # noqa: BLE001 - price warming is best-effort
        pass

    if symbol != token:
        try:
            market.price(token)
            return token
        except Exception:  # noqa: BLE001 - price warming is best-effort
            logger.debug("Could not pre-fetch price for teardown token %s (symbol=%s)", token, symbol)
            return None

    logger.debug("Could not pre-fetch price for teardown token %s", token)
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

    Perp index symbols are sourced from the shared teardown extractor. A perp's
    priceable index lives in ``intent.market`` (for example ``ETH/USD``), not in
    the token attributes below. Keeping that rule in one place prevents the
    live-runner and manager teardown lanes from drifting (VIB-6254).
    """
    token_attrs = ("from_token", "to_token", "token", "collateral_token", "borrow_token", "token_in")
    tokens: set[str] = set()
    for intent in intents:
        for attr in token_attrs:
            val = getattr(intent, attr, None)
            if val and isinstance(val, str):
                tokens.add(val)

    index_chains = _extract_perp_index_chains(intents, market)
    tokens.update(index_chains)

    if not tokens:
        return

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
        symbol = address_to_symbol.get(token, token)
        fetched_token = _prefetch_teardown_price(market, token, symbol, index_chains.get(token))
        if fetched_token is not None:
            fetched.append(fetched_token)

    if fetched:
        logger.info(f"Pre-fetched {len(fetched)} teardown prices: {fetched}")


# A bridged stable receives a $1 fallback only on chains that declare it;
# advertising phantom variants triggers slow resolver probes and unsafe routing inputs.
_CHAIN_BRIDGED_STABLECOINS: Mapping[str, tuple[str, ...]] = bridged_stablecoin_map()


def get_fallback_teardown_prices(market: Any) -> dict[str, Decimal] | None:
    """Build a minimal fallback price oracle when the market snapshot has no cached prices.

    This prevents the compiler from using $1 placeholder prices for ALL tokens
    on mainnet, which causes wildly wrong slippage calculations and silent
    compilation failures (None action bundles).

    The stablecoin set is chain-aware: the universal {USDC, USDT, DAI} is
    always seeded, with bridged variants (USDC.e, USDbC, …) added only on
    chains that actually deploy them (per ``_CHAIN_BRIDGED_STABLECOINS`` —
    grounded in ``almanak/framework/data/tokens/data/symbol_aliases.json``).
    Chains absent from that table (BSC, Linea, Mantle, …) get no bridged-USDC
    fallback because no such token is registered for them — the previous
    behaviour leaked phantom symbols into the merged ``price_oracle`` and
    downstream resolvers timed out probing them (VIB-3814).

    Returns a dict with at least stablecoin prices, or None if nothing can be
    determined.
    """
    from almanak.core.chains import ChainRegistry
    from almanak.framework.data.models import _NATIVE_TO_WRAPPED

    chain = getattr(market, "_chain", None) or getattr(market, "chain", None)
    chain_key = str(chain).lower() if chain else ""

    fallback: dict[str, Decimal] = {
        "USDC": Decimal("1"),
        "USDT": Decimal("1"),
        "DAI": Decimal("1"),
    }
    for symbol in _CHAIN_BRIDGED_STABLECOINS.get(chain_key, ()):
        fallback[symbol] = Decimal("1")

    descriptor = ChainRegistry.try_resolve(chain_key) if chain else None
    native = descriptor.native.symbol if descriptor is not None else "ETH"
    # Wrapped-native symbols are declared data; deriving ``W{native}`` creates
    # phantom assets on chains whose naming does not follow that convention.
    wrapped = _NATIVE_TO_WRAPPED.get(native)
    if wrapped is None:
        logger.warning(
            "Wrapped native unknown for chain %s (native=%s); skipping wrapped "
            "fallback price fetch. Declare NativeToken.wrapped_symbol on the "
            "chain descriptor.",
            chain_key,
            native,
        )
        tokens_to_fetch: tuple[str, ...] = (native,)
    else:
        tokens_to_fetch = (native, wrapped)

    if market is not None and hasattr(market, "price"):
        for symbol in tokens_to_fetch:
            try:
                price = market.price(symbol)
                if price and price > 0:
                    fallback[symbol] = price
            except Exception as exc:
                logger.warning("Could not fetch fallback teardown price for %s: %s", symbol, exc)

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

    from almanak.framework.market import MultiChainMarketSnapshot, TokenBalance

    # Pre-populated balances outrank the provider, so never inject over live reads.
    if getattr(market, "_balance_provider", None) is not None:
        return

    simulated: dict | None = None
    try:
        simulated = strategy.get_config("simulated_balances")
    except AttributeError:
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
