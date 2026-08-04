"""Perp keeper-settlement commit lane — VIB-3872 (WI-3).

Runner-owned twin of :mod:`almanak.framework.runner.settlement_commit` (the
VIB-5666 vault-settlement precedent), for the perp keeper-settlement lane. Once
the WI-2 reconciler correlates a terminal keeper verdict for a submitted perp
order, this lane books the Phase-2 ``PERP_SETTLEMENT`` accounting event.

Two-phase write model (design §3 D2)
------------------------------------
Phase 1 (unchanged): the submission-time ``PERP_OPEN`` / ``PERP_CLOSE`` ledger row
+ typed event, honestly ESTIMATED with intent-known fields. Phase 2 (this lane):
an APPEND-ONLY ``PERP_SETTLEMENT`` event carrying the measured keeper economics.

Mechanics that make this safe (each a review-blocking requirement, design §3 D2):

* **Drain-first ordering guard (HARD precondition).** ``processor.drain_one(
  submission_ledger_id)`` runs BEFORE the settlement write so the submission event
  exists first — otherwise a settlement row keyed to the ledger id would make a later
  ``drain_one`` skip the submission event as "already written", permanently
  suppressing Phase-1. If the drain **raises**, the write is **aborted** this tick
  (non-booked + degraded, entry left non-terminal → re-derived + retried next tick);
  the drain must succeed before the settlement event is written.
* **No keeper-tx ledger row.** The keeper pays gas, not us — the event is written
  DIRECTLY through ``AccountingWriter`` (``processor._writer``); its
  ``identity.ledger_entry_id`` points at OUR submission ledger row. A keeper-tx
  ledger row would poison ``gas_usd`` + the capital-flow classifier (VIB-5952).
* **Deterministic id.** Keyed ONLY on STABLE settlement identity —
  ``(deployment_id, "PERP_SETTLEMENT", keeper_tx or order_key, position_key)``, NOT
  the per-tick ``cycle_id`` — so a re-book on a later tick / restart mints the SAME
  id (append-only idempotency backstop; ``cycle_id`` is identity metadata only).
* **Mode-aware, catch boundary in the RECONCILER.** The settlement write uses the
  mode-aware writer (live → ``AccountingPersistenceError``, paper/dry-run → ERROR
  + ``False``); this lane lets the live error PROPAGATE to the reconciler (the
  catch boundary, OUTSIDE the decide/execute risk loop), which leaves the entry
  non-terminal for an idempotent retry. Registry completion + position_events
  backfill are BEST-EFFORT (wrapped, never block the settlement write).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..accounting.perp_settlement_accounting import build_perp_settlement_event
from ..observability.context import clear_cycle_id, get_cycle_id, set_cycle_id

if TYPE_CHECKING:  # pragma: no cover
    from ..observability.ledger import LedgerEntry
    from .runner_models import StrategyProtocol
    from .strategy_runner import StrategyRunner

logger = logging.getLogger(__name__)

_EXECUTED = "EXECUTED"


@dataclass(frozen=True)
class PerpSettlementCommitOutcome:
    """Outcome of committing one terminal perp settlement verdict.

    Attributes:
        booked: ``True`` iff the ``PERP_SETTLEMENT`` accounting event was persisted
            (``writer.write`` returned ``True``). Only a booked entry drops out of
            the reconciler's re-derived watch set (a row now exists for it).
        event_id: The deterministic accounting-event id (for logging / dedupe).
        accounting_degraded: ``True`` iff a non-live write returned ``False`` or an
            enrichment step failed. NEVER blocks the next risk-reducing action.
        degraded_reason: Compact summary of what degraded, or ``None``.
    """

    booked: bool
    event_id: str
    accounting_degraded: bool
    degraded_reason: str | None = None


def _submission_price_oracle(submission_ledger: Any) -> dict[str, Any] | None:
    """The submission row's own ``price_inputs_json``, decoded (VIB-6061).

    Prices the keeper execution fee at the moment the ORDER was submitted rather
    than at the moment the keeper settled. That is the deliberate choice: the same
    dict already priced that row's ``gas_usd``, so the two native-denominated costs
    of one order are converted at one price. The native-lane reconciliation
    invariant compares their sum against a wallet balance delta, and mixing two
    price vintages inside that sum would show up as a reconciliation gap that no
    money actually caused.

    Returns ``None`` for a missing / malformed / non-object payload, which leaves
    the fee's USD unmeasured rather than fabricating a zero.
    """
    raw = getattr(submission_ledger, "price_inputs_json", None)
    if not raw:
        return None
    try:
        decoded = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


async def commit_perp_settlement(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    verdict: Any,
    submission_ledger: LedgerEntry,
    is_open: bool | None,
    settlement_cycle_id: str,
    chain: str,
    protocol: str,
    wallet_address: str,
) -> PerpSettlementCommitOutcome:
    """Book one terminal perp settlement verdict (drain-first → write → enrich).

    ``verdict`` is a terminal ``PerpSettlementVerdict`` (WI-2). ``submission_ledger``
    is the PERP_OPEN/PERP_CLOSE ledger row whose ``async_orders`` carried this order
    key. Raises ``AccountingPersistenceError`` (live mode only) up to the reconciler
    catch boundary; otherwise never raises.
    """
    deployment_id = strategy.deployment_id
    submission_ledger_entry_id = str(getattr(submission_ledger, "id", "") or "")
    execution_mode = str(getattr(submission_ledger, "execution_mode", "") or "") or _runner_mode(runner)

    event = build_perp_settlement_event(
        verdict=verdict,
        submission_ledger_entry_id=submission_ledger_entry_id,
        deployment_id=deployment_id,
        cycle_id=settlement_cycle_id,
        execution_mode=execution_mode,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        is_open=is_open,
        native_price_oracle=_submission_price_oracle(submission_ledger),
    )

    degraded_reasons: list[str] = []
    booked = False
    saved_cycle_id = get_cycle_id()
    set_cycle_id(settlement_cycle_id)
    try:
        # ── Ordering guard: drain the submission event FIRST (HARD precondition) ──
        # If the drain raises, the submission PERP_OPEN/CLOSE event may not be written
        # yet — writing the settlement event now (keyed to the same ledger id) would
        # make a LATER drain_one skip the submission event as "already written",
        # permanently suppressing Phase-1. So a drain failure ABORTS the write this
        # tick: return non-booked + degraded, leave the entry non-terminal (no row
        # written → re-derived + retried next tick, when the drain may succeed).
        drained = True
        try:
            await runner._accounting_processor.drain_one(submission_ledger_entry_id)
        except Exception as exc:  # noqa: BLE001 — drain failure aborts the write (ordering guard)
            drained = False
            logger.error(
                "perp settlement: drain_one(%s) failed — ABORTING settlement write this tick (retried next tick): %s",
                submission_ledger_entry_id,
                exc,
            )
            degraded_reasons.append(f"drain_guard aborted the write: {exc.__class__.__name__}: {exc}")

        # ── Settlement write (the money write) — mode-aware; live raises up ──
        # to the reconciler catch boundary. Only after a successful drain.
        if drained:
            booked = await runner._accounting_processor._writer.write(event)
            if not booked:
                degraded_reasons.append("writer.write returned False (non-live persist failure)")

        # ── Best-effort enrichment (never blocks the settlement write) ───────
        if booked and event.settlement_state == _EXECUTED:
            reg_reason = await _complete_registry(
                runner,
                strategy,
                submission_ledger=submission_ledger,
                event=event,
                chain=chain,
                protocol=protocol,
                is_open=is_open,
            )
            if reg_reason:
                degraded_reasons.append(reg_reason)
            bf_reason = await _backfill_position_events(runner, event=event, deployment_id=deployment_id)
            if bf_reason:
                degraded_reasons.append(bf_reason)
    finally:
        if saved_cycle_id is None:
            clear_cycle_id()
        else:
            set_cycle_id(saved_cycle_id)

    reason = "; ".join(degraded_reasons) if degraded_reasons else None
    return PerpSettlementCommitOutcome(
        booked=bool(booked),
        event_id=event.identity.id,
        accounting_degraded=bool(degraded_reasons),
        degraded_reason=reason,
    )


def _runner_mode(runner: StrategyRunner) -> str:
    try:
        return "live" if runner._is_live_mode() else "paper"
    except Exception:  # noqa: BLE001 — mode probe is best-effort
        return "paper"


async def _complete_registry(
    runner: StrategyRunner,
    strategy: StrategyProtocol,
    *,
    submission_ledger: LedgerEntry,
    event: Any,
    chain: str,
    protocol: str,
    is_open: bool | None,
) -> str | None:
    """Perform the perp registry write the submission path skipped (design §3 D2).

    On an EXECUTED open fill the venue ``positionKey`` is now measured, so a
    ``physical_identity_hash_perp`` anchor can finally be written (``status='open'``,
    ``opened_tx`` = OUR createOrder tx). On a close fill → ``status='closed'``.
    Uses ``mode='registry_reconciliation'`` so the submission ledger row is NOT
    re-touched. Best-effort: returns a degraded-reason string on failure, never
    raises. (KNOWN LIMITATION, mirrors the existing perp registry path: a PARTIAL
    close still flips the row to ``closed``; a side-aware full-exit gate is deferred
    to VIB-5946.)
    """
    position_key = str(getattr(event, "position_key", "") or "")
    if not position_key:
        return None
    try:
        from ..accounting.commit import RegistryRow, save_ledger_and_registry
        from ..accounting.policy import MatchingPolicy
        from ..migration.backfill import (
            _PERP_GROUPING_POLICY_VERSION,
            perp_direction_label,
            physical_identity_hash_perp,
            semantic_grouping_key_perp,
        )
        from ..primitives.types import AccountingCategory, Primitive

        opened = bool(is_open)
        tx_hash = str(getattr(submission_ledger, "tx_hash", "") or "") or None
        settlement_block = getattr(event, "block_number", None)
        if settlement_block is not None:
            settlement_block = int(settlement_block)
        payload: dict[str, Any] = {
            "protocol": protocol,
            "position_id": position_key.lower(),
            "market": getattr(event, "market", None),
            "collateral_token": getattr(event, "collateral_token", None),
            "direction": perp_direction_label(getattr(event, "is_long", None)),
            "source": "settlement_reconciler",
            "keeper_tx_hash": getattr(event, "keeper_tx_hash", None),
        }
        registry = RegistryRow(
            deployment_id=strategy.deployment_id,
            chain=chain,
            primitive=Primitive.PERP,
            accounting_category=AccountingCategory.PERP,
            physical_identity_hash=physical_identity_hash_perp(
                chain=chain, protocol=protocol, position_key=position_key
            ),
            semantic_grouping_key=semantic_grouping_key_perp(chain=chain, protocol=protocol, position_key=position_key),
            grouping_policy_version=_PERP_GROUPING_POLICY_VERSION,
            status="open" if opened else "closed",
            payload=payload,
            matching_policy_version=MatchingPolicy.for_primitive(Primitive.PERP),
            opened_at_block=settlement_block if opened else None,
            opened_tx=tx_hash if opened else None,
            closed_at_block=None if opened else settlement_block,
            closed_tx=None if opened else tx_hash,
            last_reconciled_at_block=settlement_block,
        )
        await save_ledger_and_registry(
            runner.state_manager,
            ledger=submission_ledger,
            registry=registry,
            mode="registry_reconciliation",
        )
        return None
    except Exception as exc:  # noqa: BLE001 — registry completion is best-effort (design §3 D2 "degrades, never blocks")
        logger.error(
            "perp settlement: registry completion failed for position_key=%s (continuing): %s",
            position_key,
            exc,
            exc_info=True,
        )
        return f"registry: {exc.__class__.__name__}"


async def _backfill_position_events(runner: StrategyRunner, *, event: Any, deployment_id: str) -> str | None:
    """Best-effort backfill of entry/exit price + funding into ``position_events``
    (design §3 D2, mirrors ``processor._backfill_lending_position_pnl``).

    Locates the Layer-3 position event for the submission ledger row and merges the
    measured settlement attribution into ``attribution_json`` — preserving every
    other key. Never blocks the books.
    """
    try:
        sm = runner.state_manager
        target = await _resolve_backfill_target(sm, event, deployment_id)
        if target is None:
            return None
        attribution = _load_attribution(target)
        if attribution is None:
            return None
        merged = _merge_measured_attribution(attribution, event)
        await sm.update_position_attribution(
            _field(target, "id"), json.dumps(merged), _attribution_version(target), deployment_id
        )
        return None
    except Exception as exc:  # noqa: BLE001 — attribution backfill is best-effort (never blocks books)
        logger.debug("perp settlement: position_events backfill failed (non-blocking): %s", exc, exc_info=True)
        return None


async def _resolve_backfill_target(sm: Any, event: Any, deployment_id: str) -> Any | None:
    """Locate the Layer-3 ``position_events`` row to backfill, or ``None`` when it is
    not resolvable (no keys, no history reader, no match, or no update method)."""
    position_key = str(getattr(event, "position_key", "") or "")
    ledger_entry_id = str(getattr(event.identity, "ledger_entry_id", "") or "")
    if not position_key or not ledger_entry_id or not hasattr(sm, "get_position_history"):
        return None
    history = await sm.get_position_history(deployment_id, position_key)
    target = _find_position_event_for_ledger(history or [], ledger_entry_id)
    if target is None or not _field(target, "id") or not hasattr(sm, "update_position_attribution"):
        return None
    return target


# Measured settlement fields backfilled into position_events.attribution_json.
_BACKFILL_ATTRIBUTION_FIELDS = ("entry_price", "exit_price", "funding_fee_usd", "realized_pnl_usd", "position_fee_usd")


def _load_attribution(target: Any) -> dict[str, Any] | None:
    """Load a position event's ``attribution_json`` as a dict, or ``None`` on any
    parse failure (a non-string / non-object payload is never clobbered)."""
    raw_attr = _field(target, "attribution_json") or "{}"
    try:
        attribution = json.loads(raw_attr) if isinstance(raw_attr, str) else dict(raw_attr)
    except (TypeError, ValueError):
        return None
    return attribution if isinstance(attribution, dict) else None


def _merge_measured_attribution(attribution: dict[str, Any], event: Any) -> dict[str, Any]:
    """Return ``attribution`` with the MEASURED settlement fields stamped on top.

    Empty ≠ Zero: a ``None`` field is left absent (never fabricated); a measured
    ``Decimal("0")`` is stamped. Every pre-existing attribution key is preserved.
    """
    merged = dict(attribution)
    for key in _BACKFILL_ATTRIBUTION_FIELDS:
        value = getattr(event, key, None)
        if value is not None:
            merged[key] = str(value)
    return merged


def _attribution_version(target: Any) -> int:
    """Parse the position event's attribution version, defaulting to 1."""
    version = _field(target, "attribution_version") or 1
    try:
        return int(version)
    except (TypeError, ValueError):
        return 1


def _field(row: Any, name: str) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name, None)


def _find_position_event_for_ledger(history: list[Any], ledger_entry_id: str) -> Any | None:
    for row in history:
        if str(_field(row, "ledger_entry_id") or "") == ledger_entry_id:
            return row
    return None


__all__ = ["PerpSettlementCommitOutcome", "commit_perp_settlement"]
