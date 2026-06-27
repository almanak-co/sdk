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
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from almanak.core.chains._helpers import bridged_stablecoin_map

from ..intents.compiler import IntentCompiler, IntentCompilerConfig
from ..intents.vocabulary import Intent
from ..teardown.sweep_warning import warn_if_sweep_non_strategy_balance

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


# -------------------------------------------------------------------------
# VIB-4587 / F5 — teardown sweep DX warning
# -------------------------------------------------------------------------
#
# Logic lives in ``almanak/framework/teardown/sweep_warning.py`` so the
# manager-driven teardown path (``teardown_manager.py``) and the inline
# fallback (this module) can both invoke it. We re-export the public
# name under the historical private name so existing unit tests keep
# importing from this module unchanged.
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
        # Can't read positions → can't decide. Leave intents untouched; the
        # downstream lane has its own positions/verify path.
        logger.debug(
            "Teardown LP recovery: get_open_positions failed for %s — skipping discovery fallback",
            deployment_id,
            exc_info=True,
        )
        return teardown_intents, False, None

    # Strategy still tracks its LP → its own close is authoritative; no scan.
    if strategy_reports_lp(positions):
        return teardown_intents, False, None

    helpers = build_runner_helpers(runner)
    if not helpers.has_lp_discovery:
        return teardown_intents, False, None

    # ``has_lp_discovery`` guarantees both callables are wired; bind them to
    # non-Optional locals so the type checker sees them as callable (mypy can't
    # narrow Optional through the property).
    get_ownership = helpers.get_deployment_lp_ownership
    discover = helpers.discover_lp_positions
    assert get_ownership is not None and discover is not None  # noqa: S101 — narrowed by has_lp_discovery

    # Ownership FIRST (cheap local read). Determines (a) which discovered NFTs we
    # may close and (b) whether an incomplete scan is even relevant to us.
    try:
        ownership = await get_ownership(strategy, chain)
    except Exception:  # noqa: BLE001 — ownership read must never block risk reduction
        logger.exception(
            "Teardown LP recovery: ownership read raised for %s — skipping recovery (cannot prove ownership)",
            deployment_id,
        )
        return teardown_intents, False, None

    # No attribution source readable → ownership unprovable → never close.
    if not ownership.available:
        return teardown_intents, False, None

    # This deployment owns no LP on this chain → nothing to recover, and an
    # unrelated NPM blip during a scan must not block this benign teardown.
    # Skip the gateway scan entirely (resolves the spurious-FAILED concern).
    if not ownership.token_ids and not ownership.had_lp_open:
        return teardown_intents, False, None

    try:
        discovery = await discover(strategy)
    except Exception:  # noqa: BLE001 — discovery must never block risk reduction
        logger.exception(
            "Teardown LP recovery: discovery helper raised for %s — continuing without recovery",
            deployment_id,
        )
        # Honesty axis (Gemini HIGH): a RAISED scan is the same information loss
        # as ``incomplete=True`` — we could not enumerate the wallet's NFTs. We
        # only reach here past the guard above, so this deployment IS known to
        # have an LP on this chain (token_ids and/or had_lp_open). Mirror the F2
        # logic: degrade to incomplete=True (manual-check / loud) for an
        # owned-LP deployment so teardown is NOT certified clean; a deployment
        # with no LP attribution here would have skipped the scan already.
        owns_lp_here = bool(ownership.token_ids) or ownership.had_lp_open
        if owns_lp_here:
            warning = (
                f"On-chain LP discovery raised during teardown recovery for {deployment_id}; "
                "this deployment is known to have opened an LP on this chain — manual on-chain "
                "verification required."
            )
            return teardown_intents, True, warning
        # Defensive: no LP attribution → a raised scan is benign (WARNING only).
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


# -------------------------------------------------------------------------
# Main teardown entry point
# -------------------------------------------------------------------------


def _apply_lending_unwind_guard(
    teardown_intents: list, teardown_market: Any, deployment_id: str, mode: TeardownMode | None = None
) -> list:
    """Sanitise strategy-emitted lending teardown intents against fresh state.

    Wraps the pure ``sanitize_lending_teardown_intents`` guard (VIB-5139 /
    VIB-4466) and logs any dropped / reordered / synthesised / degraded outcome.
    Returns the guarded intent list. A guard failure must never block teardown
    (its first job is removing on-chain risk), so any unexpected error falls back
    to the original intents with a loud WARNING.
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
    if guarded.synthesized_positions:
        logger.info(
            "🛑 %s lending guard synthesised HF-safe unwind staircase (wallet cannot fully repay live "
            "debt — naive withdraw-all would revert) for: %s (VIB-4466)",
            deployment_id,
            ", ".join(guarded.synthesized_positions),
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
    return guarded.intents


# crap-allowlist: VIB-4049 — pre-existing cc=21 teardown coordinator; PR touches a single line (``_lifecycle_write_state("TEARING_DOWN")``), zero new branches.
# Function is the canonical sequencer (market snapshot → intents → routing → manager/inline/fallback);
# decomposing it requires the four-step SDK crap-refactor protocol (blueprint 14 + Plan agent +
# test baseline) and is out of scope for a regression fix. The C901 exemption is already
# in place for the same complexity. Refactor tracked separately.
async def execute_teardown(  # noqa: C901
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
    from .runner_models import IterationResult, IterationStatus

    deployment_id = strategy.deployment_id
    # TD-08 (VIB-5466): reset the Plan-A reconciliation signal at the very start
    # so an early-exit path (no positions / all balances zero / generation
    # failure) on a REUSED runner instance can never let a prior teardown's
    # divergence report leak into this one. The post-enumeration CHECK below
    # overwrites it on the lanes that reach it.
    runner._teardown_reconciliation = None
    # Both modes have a real cross-process teardown channel: SQLite locally,
    # gateway-backed in hosted mode. Any error here is a genuine
    # misconfiguration and should propagate.
    manager: Any = get_teardown_state_manager_for_runtime(gateway_client=runner._get_gateway_client())
    request: Any = manager.get_active_request(deployment_id)

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
            logger.debug(f"Strategy {deployment_id} uses old teardown signature (no market param), falling back")
            teardown_intents = strategy.generate_teardown_intents(teardown_mode)
    except Exception as e:
        logger.error(f"Failed to generate teardown intents for {deployment_id}: {e}")
        if request:
            _safe_mark(manager, "mark_failed", deployment_id, error=str(e))
        runner._request_teardown_failure_shutdown(str(e))
        return runner._create_error_result(deployment_id, IterationStatus.STRATEGY_ERROR, str(e), start_time)

    # Step T2.4 (VIB-5138): auto-fallback to on-chain LP discovery when the
    # strategy's state desynced (LP NFT live on-chain but ``_position_id``
    # lost). Appends recovered LP_CLOSE intents and surfaces an incomplete flag
    # when the bounded scan could not enumerate every position. Runs on the
    # signal-driven runner lane only — the CLI ``teardown execute`` lane has the
    # explicit ``--discover`` flag for the same recovery.
    teardown_intents, recovery_incomplete, recovery_warning = await _recover_orphaned_lp_intents(
        runner, strategy, teardown_intents, teardown_mode
    )
    # F3 (VIB-5138): stash the incomplete signal on the runner so the
    # intents-present path (multi-chain + TeardownManager lanes) can degrade the
    # teardown lifecycle to manual-check after execution, instead of letting an
    # unenumerated orphan sit inside a clean COMPLETED. Consumed + cleared by the
    # completion-mark sites below / in execute_teardown_via_manager.
    runner._teardown_recovery_incomplete = recovery_incomplete
    runner._teardown_recovery_warning = recovery_warning

    # Step T2.5 (VIB-5139): universal fresh-state guard for lending unwind.
    # Strategies hand-roll REPAY/WITHDRAW teardown intents from cached exposure;
    # stale state emits a REPAY 0, a withdraw_all when already flat, or a
    # collateral withdraw before the debt repay — all of which revert
    # (e.g. Aave HealthFactorLowerThanLiquidationThreshold) and strand the
    # position. The guard does a FRESH on-chain exposure read (gateway-backed
    # ``market.position_health``) and drops measured-zero actions, enforces
    # repay-first, and degrades conservatively on an unmeasured read (Empty ≠
    # Zero — never acts on a None as if it were zero). Pure list transform: the
    # sanitised intents flow through the same dispatch funnel, so the per-intent
    # commit pairing / anti-bypass guards are untouched.
    teardown_intents = _apply_lending_unwind_guard(teardown_intents, teardown_market, deployment_id, teardown_mode)

    if not teardown_intents:
        if recovery_incomplete:
            # Discovery could not confirm the wallet holds no LP — refusing to
            # report a clean "no positions" success that might strand an orphan.
            err = recovery_warning or "On-chain LP discovery incomplete; manual check required."
            logger.error("🛑 %s teardown blocked: %s", deployment_id, err)
            if request:
                _safe_mark(manager, "mark_failed", deployment_id, error=err)
            runner._request_teardown_failure_shutdown(err)
            return runner._create_error_result(deployment_id, IterationStatus.STRATEGY_ERROR, err, start_time)
        logger.info(f"🛑 {deployment_id} teardown complete (no positions to close)")
        if request:
            _safe_mark(manager, "mark_completed", deployment_id, result={"reason": "no_positions"})
        runner.request_shutdown()
        # Match the adjacent all-balances-zero + TeardownManager-success paths —
        # the lifecycle supervisor must see TERMINATED so it doesn't treat a
        # teardown-with-no-positions as still running.
        runner._lifecycle_write_state(deployment_id, "TERMINATED")
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            deployment_id=deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    logger.info(f"🛑 {deployment_id} entering TEARDOWN mode ({len(teardown_intents)} intents to execute)")
    # VIB-4049: switch lifecycle state to TEARING_DOWN now that unwind work is
    # actually starting. Platform maps this to live_agent_status.TEARDOWN_IN_PROGRESS
    # so the dashboard / reconciler can distinguish "stopping cleanly" (SLA
    # 5min) from "actively unwinding positions" (SLA 45min). The terminal
    # TERMINATED / ERROR writes downstream are unchanged — they take over once
    # the unwind completes (or fails).
    runner._lifecycle_write_state(deployment_id, "TEARING_DOWN")
    # VIB-5085: record the open-position count (not the intent count) as the
    # teardown's positions_total. ``None`` when unreadable — the mark_started
    # denominator then degrades to the intent count (cosmetic; the completion
    # mark is authoritative), but the completed ``positions_closed`` is NEVER
    # fabricated from intents (see the multi-chain completion mark below).
    open_positions_count = await _count_open_positions(strategy)
    total_positions = open_positions_count if open_positions_count is not None else len(teardown_intents)
    if request:
        _safe_mark(manager, "mark_started", deployment_id, total_positions=total_positions)

    # TD-08 (VIB-5466): Plan-A on-chain reconciliation CHECK. After ledger
    # enumeration, a protocol-scoped chain read confirms each KNOWN position's
    # live state and flags any divergence from the WARM ledger LOUDLY with a
    # structured signal stashed on the runner for the TD-15 fail-closed verifier
    # to consume (it composes with TD-14's verification_status). CHECK only:
    # closes/sweeps nothing, emits no intent, and is position-scoped — never a
    # wallet-wide sweep (that is Plan B). The attribute was reset to None at the
    # top of this function, so the early-exit lanes leave no stale report behind.
    runner._teardown_reconciliation = await reconcile_known_positions(runner, strategy, teardown_market)

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
        logger.info(f"🛑 {deployment_id} teardown complete (all positions already closed)")
        if request:
            _safe_mark(manager, "mark_completed", deployment_id, result={"reason": "all_balances_zero"})
        runner.request_shutdown()
        runner._lifecycle_write_state(deployment_id, "TERMINATED")
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            deployment_id=deployment_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    # Step T3: Execute teardown intents
    if runner._is_multi_chain:
        # Multi-chain: use inline path (TeardownManager doesn't support multi-chain yet)
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
                    # F3 (VIB-5138): execution succeeded but on-chain LP discovery
                    # was incomplete for a deployment KNOWN to hold an LP here —
                    # an orphan may remain. Mark FAILED (manual-check) rather than
                    # a clean COMPLETED so the operator verifies on-chain.
                    err = recovery_warning or "On-chain LP discovery incomplete; manual check required."
                    logger.error("🛑 %s teardown degraded (recovery incomplete): %s", deployment_id, err)
                    _safe_mark(manager, "mark_failed", deployment_id, error=err)
                else:
                    # VIB-5085: the multi-chain lane has no position verifier; a
                    # fully-successful teardown closed every open position, so report
                    # the pre-execution count when known (else omit positions_closed
                    # so the lift falls back to the legacy ``intents`` key).
                    _safe_mark(
                        manager,
                        "mark_completed",
                        deployment_id,
                        result=_positions_completion_result(open_positions_count, len(teardown_intents)),
                    )
        else:
            # VIB-5470 (subsumes VIB-5152): decode lending / Safe-Roles revert
            # selectors into an operator-clear message before persisting +
            # surfacing the failure (the single-chain manager path annotates at
            # its own source). No-op when no known selector is embedded.
            from ..teardown.revert_hints import annotate_teardown_error

            failure_error = annotate_teardown_error(result.error) or "multi-chain teardown execution failed"
            if request:
                _safe_mark(manager, "mark_failed", deployment_id, error=failure_error)
            runner._request_teardown_failure_shutdown(failure_error)
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

    deployment_id = strategy.deployment_id
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

    # Phase 2: construct TeardownManager + state adapter. The request threads
    # asset_policy / target_token into the manager's TeardownConfig so the
    # token-consolidation phase honours the operator's choice (VIB-5011).
    teardown_mgr, teardown_state_adapter = _h.build_teardown_manager(runner, compiler, state_manager, request)

    logger.info(
        f"🛑 Routing {deployment_id} teardown through TeardownManager (mode={mode_str}, intents={len(teardown_intents)})"
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
                    deployment_id,
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
                    deployment_id,
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

        # F3 (VIB-5138): if on-chain LP discovery was incomplete for a deployment
        # KNOWN to hold an LP on this chain, a deployment-owned orphan may STILL
        # be open even though every executed intent succeeded. Refuse to certify
        # the teardown complete — degrade to a manual-check result regardless of
        # whether intents were present. This runs AFTER execution + verify (we did
        # all the risk reduction we could; we just don't certify it). Read off the
        # runner attribute the recovery step stashed (cleared in finally).
        if getattr(runner, "_teardown_recovery_incomplete", False) and teardown_result.success:
            warn = (
                getattr(runner, "_teardown_recovery_warning", None)
                or "On-chain LP discovery incomplete; manual check required."
            )
            logger.error(
                "🛑 %s teardown executed but LP discovery incomplete — marking manual-check: %s",
                deployment_id,
                warn,
            )
            teardown_result = replace(
                teardown_result,
                success=False,
                error=warn,
                recovery_options=[
                    "Verify LP positions on-chain (NPM balanceOf)",
                    "Re-run teardown once RPC is healthy",
                ],
            )

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
        # VIB-5138: clear the recovery-incomplete signal so it cannot leak into a
        # subsequent teardown on the same runner.
        runner._teardown_recovery_incomplete = False
        runner._teardown_recovery_warning = None

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

    deployment_id = strategy.deployment_id

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
                    deployment_id,
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

    if live is None:
        decision = SwapClampDecision(None, True, True, "live_balance_unmeasured")
    else:
        decision = decide_swap_clamp(
            live_balance=live,
            tracked_map=read_tracked_swap_inventory(
                state_manager=getattr(runner, "state_manager", None),
                deployment_id=deployment_id,
                chain=chain,
                wallet_address=wallet_address,
            ),
            from_token=balance_token,
        )

    if not decision.skip:
        return False, False, decision.amount

    # Keep the VIB-4587 sweep WARNING firing as the operator signal (esp. the
    # untracked-token / commingled case).
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


# crap-allowlist: VIB-5416 — pre-existing cc=28 inline-teardown coordinator; PR only threads two kwargs (chain/wallet_address) into the existing `_apply_inline_swap_clamp(...)` call so the NO_ACCOUNTING ledger lane keys correctly, adding ZERO new branches. Decomposition deferred to the standing inline-lane refactor; covering the fallback path needs a full inline-teardown integration harness.
async def _execute_teardown_inline_body(  # noqa: C901
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

    deployment_id = strategy.deployment_id

    # VIB-5085: capture the open-position count BEFORE the loop so a
    # fully-successful inline teardown reports positions closed, not intents
    # (this lane has no position verifier). ``None`` when unreadable — the
    # completion mark then omits ``positions_closed`` rather than fabricate it.
    pre_exec_positions_total = await _count_open_positions(strategy)

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
                # VIB-5465: evict the plan-build balance memo before this LIVE
                # read so the ``amount="all"`` exit resolves against current
                # on-chain state. The teardown snapshot was built BEFORE these
                # closing intents ran; an earlier intent (e.g. a REPAY/WITHDRAW
                # staircase that moved the wallet) would otherwise leave a stale
                # memoized balance and over-resolve this swap by exactly the
                # amount the earlier intent consumed. Mirrors the manager lane
                # (TeardownManager._execute_intents) and VIB-5074. No-op on
                # paper/dry-run (no balance provider); best-effort otherwise.
                _invalidate = getattr(teardown_market, "invalidate_balance", None)
                if callable(_invalidate):
                    try:
                        _invalidate(balance_token)
                    except Exception:  # noqa: BLE001
                        logger.debug(
                            "invalidate_balance(%s) failed in inline lane; using cached balance",
                            balance_token,
                            exc_info=True,
                        )
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
                        deployment_id=deployment_id,
                        duration_ms=runner._calculate_duration_ms(start_time),
                    )
                    break
                # MarketSnapshot.balance() returns Decimal; IntentStrategy.balance() returns TokenBalance
                balance_value = bal.balance if hasattr(bal, "balance") else bal
                if balance_value <= 0:
                    logger.info(f"🛑 Teardown intent {i + 1}: {balance_token} balance is 0, skipping (already closed)")
                    continue
                # ALM-2766: clamp an amount='all' swap-back to the strategy's
                # TRACKED quantity so a default teardown never sweeps commingled
                # wallet funds. The decision is delegated to the SHARED
                # ``decide_swap_clamp`` helper (same source as the manager lane,
                # so the two lanes cannot drift) via ``_apply_inline_swap_clamp``;
                # on a fail-closed skip the swap is bypassed (loud, degraded
                # counted), else ``balance_value`` is set to ``min(tracked, live)``.
                _skip, _degraded, balance_value = _apply_inline_swap_clamp(
                    runner,
                    intent,
                    balance_token,
                    balance_value,
                    deployment_id,
                    chain=getattr(strategy, "chain", "") or "",
                    wallet_address=getattr(strategy, "wallet_address", "") or "",
                )
                if _skip:
                    inline_degraded_count += int(_degraded)
                    continue
                # VIB-4587 / F5 — emit DX warning before sweeping when the
                # from-token wasn't seen in this strategy's accounting history.
                # NOTE: ``state_manager`` in this body is the teardown lifecycle
                # SM (``TeardownStateManager``) — it tracks teardown requests,
                # not accounting events. Hand the helper the runner's accounting
                # ``StateManager`` instead, which exposes
                # ``get_accounting_events_sync``.
                warn_if_sweep_non_strategy_balance(
                    state_manager=getattr(runner, "state_manager", None),
                    deployment_id=deployment_id,
                    intent=intent,
                    balance_token=balance_token,
                    balance_value=balance_value,
                )
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
                deployment_id=deployment_id,
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
            logger.info(f"🛑 {deployment_id} teardown complete - shutting down strategy runner")
            runner.request_shutdown()
            runner._lifecycle_write_state(deployment_id, "TERMINATED")
            runner._record_success()
            if request:
                # VIB-5085: report positions closed (= pre-execution count on a
                # full success) when known, keeping the intent signal alongside;
                # omit positions_closed when unknown (lift falls back to intents).
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
        return last_result, inline_degraded_count

    # Edge case: no intents executed (all positions already closed)
    logger.info(f"🛑 {deployment_id} teardown: all positions already closed, shutting down")
    runner.request_shutdown()
    runner._lifecycle_write_state(deployment_id, "TERMINATED")
    runner._record_success()
    if request:
        _safe_mark(state_manager, "mark_completed", deployment_id, result={"reason": "all_positions_already_closed"})
    final_result = IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        deployment_id=deployment_id,
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


# Per-chain bridged-stablecoin variants beyond the universal {USDC, USDT, DAI}
# set. Adding a symbol here means the framework will treat it as a $1 fallback
# *only* on the listed chains. Leaving it absent — as for ``bsc`` — keeps the
# fallback dict from advertising tokens that don't exist on that chain, which
# previously caused the swap-fee-tier heuristic and other downstream consumers
# to probe the resolver for ``USDC.e`` on BSC and burn the 240s harness window
# (VIB-3814 / BUG-30 residual).
# Derived from ``ChainDescriptor.bridged_stablecoin_variants`` (VIB-4851
# CS-6); membership and tuple order preserved verbatim, and absence stays
# load-bearing exactly as described above.
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

    # Universal stablecoins — present on every chain we support.
    fallback: dict[str, Decimal] = {
        "USDC": Decimal("1"),
        "USDT": Decimal("1"),
        "DAI": Decimal("1"),
    }
    # Chain-specific bridged variants (added only where they actually exist).
    for symbol in _CHAIN_BRIDGED_STABLECOINS.get(chain_key, ()):
        fallback[symbol] = Decimal("1")

    # Derive native + wrapped token symbols from the ChainRegistry
    # so new chains are picked up automatically without code changes here.
    descriptor = ChainRegistry.try_resolve(chain_key) if chain else None
    native = descriptor.native.symbol if descriptor is not None else "ETH"
    # No string-prefix fallback: chains like 0G break the ``W{native}`` rule
    # (A0GI -> W0G, not WA0GI) and a phantom symbol burns a 15s gateway
    # timeout per probe before silently returning None (VIB-3970).
    wrapped = _NATIVE_TO_WRAPPED.get(native)
    if wrapped is None:
        logger.warning(
            "Wrapped native unknown for chain %s (native=%s); skipping wrapped "
            "fallback price fetch. Add an entry to _NATIVE_TO_WRAPPED in "
            "almanak/framework/data/models.py.",
            chain_key,
            native,
        )
        tokens_to_fetch: tuple[str, ...] = (native,)
    else:
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

    from almanak.framework.market import MultiChainMarketSnapshot, TokenBalance

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
