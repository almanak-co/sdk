"""Structured decision log for teardown (VIB-5478 / TD-20, subsumes VIB-2933).

Teardown makes a sequence of irreversible, money-moving decisions —
*what was enumerated, sized, blocked/repaired, and verified* — and until now
those decisions were scattered across plain ``logger`` lines with no single,
queryable audit trail. This module emits ONE structured, auditable entry per
decision so a post-mortem reader can reconstruct exactly why a teardown closed
the positions it closed (and skipped the swaps it skipped).

Design decisions (deliberate, see blueprint 14 §Observability + AGENTS.md):

- **Reuse the existing observability sink — do not invent a parallel one.**
  Decisions are emitted as timeline events through
  :func:`almanak.framework.api.timeline.add_event`, exactly like every other
  forensic breadcrumb (lifecycle markers, teardown progress). They flow through
  the standard dual-write path (local ``.dashboard_events.json`` + gateway
  ``RecordTimelineEvent``) with zero new infrastructure and **no new Postgres
  DDL** (the gateway-owned schema is external — AGENTS.md §Database schema
  ownership). Correlation rides the canonical ``cycle_id = teardown-{id}``.

- **The timeline is a UX/audit channel, NOT an accounting record**
  (``almanak/framework/api/timeline.py`` module docstring, PRD-TimelineEvents
  §6.1). The money trail — amounts, prices, slippage, gas, USD totals — lives in
  ``transaction_ledger`` / ``accounting_events`` and is FORBIDDEN here. The
  decision log therefore records *the shape of the decision* (phase, outcome,
  position/intent counts, token SYMBOL, reason, verification status, degraded
  flag) and never a money-shaped value. The producer-side static guard
  (``tests/static/test_timeline_payload_keys.py``) enforces this.

- **Logging must NEVER block a risk-reducing intent** (teardown's inverted
  failure semantics — AGENTS.md §Teardown). Every public function here is
  best-effort: any exception is swallowed with a debug log. Observability that
  could halt an unwind would be strictly worse than no observability.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.observability.context import get_cycle_id

logger = logging.getLogger(__name__)

# Timeline ``phase`` prefix so decision entries are trivially filterable
# (``phase LIKE 'TEARDOWN_%'``) and never collide with the StrategyPhase
# breadcrumbs the iteration lane emits.
_PHASE_PREFIX = "TEARDOWN_"

# Marker key stamped into every decision payload so a consumer can select
# the teardown decision log out of the broader timeline stream with a single
# predicate, independent of the (free-form) event description.
_DECISION_MARKER = "teardown_decision"


class TeardownDecisionPhase(StrEnum):
    """The auditable phases of a teardown decision (VIB-5478).

    Mirrors the teardown pipeline's decision points:

    - ``ENUMERATE`` — which KNOWN positions were discovered (registry / TD-01).
    - ``SIZE`` — live amount resolution (TD-07): how a swap-back / sweep was
      sized against tracked inventory.
    - ``BLOCK`` — a risk decision REFUSED an action (swap-clamp skip on
      untracked/unmeasured funds, lending intent dropped, exposure unmeasured).
    - ``REPAIR`` — a guard REPLACED a naive plan with a safe one (HF-safe unwind
      staircase, TD-09/TD-10 lending fresh-state guard).
    - ``VERIFY`` — post-execution closure verification (TD-14 count + TD-15
      fail-closed on-chain verify, ``verification_status``).
    """

    ENUMERATE = "ENUMERATE"
    SIZE = "SIZE"
    BLOCK = "BLOCK"
    REPAIR = "REPAIR"
    VERIFY = "VERIFY"


def _resolve_cycle_id(teardown_id: str | None) -> str:
    """Resolve the correlation cycle id for a decision entry.

    Prefers an explicit ``teardown_id`` (some decisions — e.g. ENUMERATE — are
    made BEFORE the runner swaps the contextvar to the teardown cycle id), then
    falls back to the ambient cycle id from the observability context.
    """
    if teardown_id:
        # Canonical format consumed by reconciliation: ``teardown-{id}``.
        return teardown_id if teardown_id.startswith("teardown-") else f"teardown-{teardown_id}"
    return get_cycle_id() or ""


def build_decision_details(
    *,
    phase: TeardownDecisionPhase,
    outcome: str,
    teardown_id: str | None = None,
    position_count: int | None = None,
    intent_count: int | None = None,
    positions_closed: int | None = None,
    token: str | None = None,
    reason: str | None = None,
    verification_status: str | None = None,
    degraded: bool | None = None,
) -> dict[str, Any]:
    """Build the structured ``details`` payload for a teardown decision.

    Split out (pure, no I/O) so tests can assert the exact record shape and the
    money-key-safety invariant without exercising the timeline sink. ``None``
    fields are omitted (Empty ≠ Zero — an unmeasured count is absent, not 0).

    Every key here is intentionally NOT a money-shaped key
    (``tests/static/test_timeline_payload_keys.py:FORBIDDEN_KEYS``): the money
    trail belongs in ``transaction_ledger`` / ``accounting_events``.
    """
    details: dict[str, Any] = {
        _DECISION_MARKER: True,
        "decision_phase": phase.value,
        "outcome": outcome,
    }
    if teardown_id:
        details["teardown_id"] = teardown_id
    if position_count is not None:
        details["position_count"] = position_count
    if intent_count is not None:
        details["intent_count"] = intent_count
    if positions_closed is not None:
        details["positions_closed"] = positions_closed
    if token:
        details["token"] = token
    if reason:
        details["reason"] = reason
    if verification_status:
        details["verification_status"] = verification_status
    if degraded is not None:
        details["degraded"] = degraded
    return details


def log_teardown_decision(
    *,
    deployment_id: str,
    phase: TeardownDecisionPhase,
    outcome: str,
    description: str | None = None,
    teardown_id: str | None = None,
    chain: str = "",
    position_count: int | None = None,
    intent_count: int | None = None,
    positions_closed: int | None = None,
    token: str | None = None,
    reason: str | None = None,
    verification_status: str | None = None,
    degraded: bool | None = None,
) -> None:
    """Emit ONE structured teardown decision entry to the observability sink.

    Best-effort and non-blocking: any failure is swallowed (debug log) so a
    decision-log write can never halt a risk-reducing teardown intent
    (AGENTS.md §Teardown — inverted failure semantics).

    Args:
        deployment_id: The deployment whose teardown made this decision.
        phase: Which decision phase (enumerate / size / block / repair / verify).
        outcome: Short machine-readable outcome tag (e.g. ``"enumerated"``,
            ``"swap_clamp_skipped"``, ``"hf_safe_unwind_synthesized"``,
            ``"verified"``, ``"verify_failed"``).
        description: Human-readable one-liner for the timeline UX. Defaulted
            from ``phase``/``outcome`` when omitted.
        teardown_id: Teardown id for ``cycle_id`` correlation (preferred over
            the ambient context cycle id).
        chain: Optional chain for explorer linking / filtering.
        position_count / intent_count / positions_closed: Decision counts
            (Empty ≠ Zero — omit when unmeasured).
        token: Token SYMBOL the decision concerns (never an amount).
        reason: Why the decision was made (the clamp/guard reason string).
        verification_status: ``VerificationStatus`` value for VERIFY entries.
        degraded: Accounting-degraded flag for fail-closed / unmeasured reads.
    """
    try:
        details = build_decision_details(
            phase=phase,
            outcome=outcome,
            teardown_id=teardown_id,
            position_count=position_count,
            intent_count=intent_count,
            positions_closed=positions_closed,
            token=token,
            reason=reason,
            verification_status=verification_status,
            degraded=degraded,
        )
        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.CUSTOM,
            description=description or f"teardown {phase.value.lower()}: {outcome}",
            deployment_id=deployment_id,
            chain=chain or "",
            details=details,
            cycle_id=_resolve_cycle_id(teardown_id),
            phase=f"{_PHASE_PREFIX}{phase.value}",
        )
        add_event(event)
    except Exception:  # noqa: BLE001 — observability must never block teardown
        logger.debug(
            "teardown decision-log emit failed for %s (phase=%s, outcome=%s) — non-fatal",
            deployment_id,
            getattr(phase, "value", phase),
            outcome,
            exc_info=True,
        )
