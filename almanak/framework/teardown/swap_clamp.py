"""ALM-2766 — clamp a teardown swap-back to the strategy's TRACKED quantity.

Default (automatic) teardown resolves an ``amount='all'`` swap-back against the
FULL live wallet balance. On a wallet shared across deployments that sweeps
commingled funds this strategy never owned. This module computes the clamp:

    swap_qty = min(Σ tracked_lot_remaining, live_balance)

and decides — fail-closed — whether the swap proceeds (clamped), is skipped, or
is degraded.

TERMINOLOGY: ``qty_idle = live − lot_held`` is the UNTRACKED/commingled
remainder; ``lot_held`` (Σ open wallet-basis lot remaining) is the TRACKED
quantity. We swap the TRACKED quantity, never ``qty_idle``.

Operator-initiated (MANUAL) consolidation opts OUT of the clamp — that lane
intentionally does a full-wallet sweep with the operator present. The VIB-5011
consolidation phase ALSO runs on AUTOMATIC teardowns (risk-guard / auto-protect
/ config-reload, blueprint 14 §4.5), and those keep the clamp ON (no operator to
consent to sweeping commingled / sibling-deployment balances). The CALLER gates
on ``consolidation_consent = not is_auto_mode``; this module only computes the
decision and reads inventory.

Read-only and best-effort: the inventory read NEVER raises (returns the
UNMEASURED sentinel ``None``) and a degraded decision flags
``TeardownResult.accounting_degraded`` WITHOUT blocking the teardown loop
(inverted-failure semantics, AGENTS.md §Teardown). A swap-back is never the
risk-reducing intent, so skipping it strands no on-chain risk.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.basis import canonical_pt_symbol, sum_open_wallet_basis_by_token

logger = logging.getLogger(__name__)


def _is_finite_decimal(value: Any) -> bool:
    """True only for a finite ``Decimal`` (rejects NaN / ±Infinity / non-Decimal).

    A non-finite quantity makes ``min(...)`` / ``<= 0`` behave undefined-ly, so
    the clamp treats it as UNMEASURED and fails closed (CodeRabbit, ALM-2766).
    """
    return isinstance(value, Decimal) and value.is_finite()


@dataclass(frozen=True)
class SwapClampDecision:
    """Outcome of :func:`decide_swap_clamp`.

    * ``amount`` — the clamped swap amount when proceeding; ``None`` on skip.
    * ``skip`` — ``True`` => do NOT execute the swap (no commingled-fund sweep).
    * ``degraded`` — ``True`` => flag ``TeardownResult.accounting_degraded``
      (the tracked quantity was unprovable, so we fail closed).
    * ``reason`` — short code for logging / telemetry.
    """

    amount: Decimal | None
    skip: bool
    degraded: bool
    reason: str


def decide_swap_clamp(
    *,
    live_balance: Decimal,
    tracked_map: dict[str, Decimal | None] | None,
    from_token: str,
) -> SwapClampDecision:
    """Decide how to resolve an ``amount='all'`` teardown swap-back (ALM-2766).

    ``tracked_map`` is the deployment-scoped ``{canonical_symbol: Decimal | None}``
    from :func:`read_tracked_swap_inventory`, or the UNMEASURED sentinel ``None``.
    A per-token ``None`` value means that symbol's total is UNPROVABLE — since
    VIB-5865 that is how a token touched by a ``WalletDeltaLane.UNMEASURED``
    primitive (LP / vault / perp / bridge / settlement) is presented. The decision
    table below is UNCHANGED; those tokens simply now reach the
    ``tracked_qty_unmeasured`` branch (visible, degraded) instead of the
    ``untracked_token`` branch (silent).

    Fail-closed decision table:

    * ``tracked_map is None`` (FIFO replay failed / empty deployment id /
      accessor errored) → skip + degraded (``tracked_inventory_unmeasured``).
    * ``from_token`` not in the map → UNTRACKED asset → skip, NOT degraded
      (``untracked_token``): never touch commingled funds.
    * map value ``None`` (Empty ≠ Zero — unmeasured for THIS token) → skip +
      degraded (``tracked_qty_unmeasured``). Do NOT coerce to 0.
    * ``min(tracked, live) <= 0`` (measured zero — nothing of ours) → skip, NOT
      degraded (``zero_tracked``).
    * otherwise → proceed with ``amount = min(tracked, live)`` (``clamped``).
    """
    # VIB-5353: ``canonical_pt_symbol`` is maturity-insensitive for PT symbols so
    # the strategy's maturity-less swap-back ``from_token`` (e.g. ``PT-wstETH``)
    # matches the maturity-bearing FIFO/tracked-inventory key (``PT-wstETH-…``);
    # it is identical to ``canonical_symbol`` for every non-PT token. The tracked
    # map is keyed the same way in ``sum_open_wallet_basis_by_token``.
    key = canonical_pt_symbol(from_token)
    if tracked_map is None:
        return SwapClampDecision(None, True, True, "tracked_inventory_unmeasured")
    # A non-finite (NaN / ±Inf) or non-Decimal live balance is UNMEASURED —
    # ``min`` / ``<= 0`` are undefined on it, so fail closed (CodeRabbit CR#1).
    if not _is_finite_decimal(live_balance):
        return SwapClampDecision(None, True, True, "live_balance_unmeasured")
    if key not in tracked_map:
        return SwapClampDecision(None, True, False, "untracked_token")
    tracked_qty = tracked_map[key]
    # Empty ≠ Zero (None) AND non-finite both fail closed for THIS token.
    if tracked_qty is None or not _is_finite_decimal(tracked_qty):
        return SwapClampDecision(None, True, True, "tracked_qty_unmeasured")
    swap_qty = min(tracked_qty, live_balance)
    if swap_qty <= 0:
        return SwapClampDecision(None, True, False, "zero_tracked")
    return SwapClampDecision(swap_qty, False, False, "clamped")


def read_no_accounting_ledger_rows(state_manager: Any, deployment_id: str) -> list[dict] | None:
    """Measured transaction_ledger rows for the NO_ACCOUNTING teardown lanes (VIB-5416 / VIB-5471), or ``None``.

    Returns the deployment's ledger rows ONLY when the gateway reports a MEASURED
    read (``ACCOUNTING_BACKEND_STATUS_AVAILABLE``). Returns ``None`` — *drop the
    NO_ACCOUNTING lane*, so STAKE/WRAP/MINT tokens strand (the safe under-sweep
    direction, == pre-VIB-5416 behaviour) — when the reader is absent (old gateway
    / wrong-flavour manager) or the read is UNMEASURED.

    Shared measured-gating seam for BOTH teardown fund-safety lanes that fold
    NO_ACCOUNTING acquisitions: the swap-back clamp's tracked-inventory read
    (:func:`read_tracked_swap_inventory`, VIB-5416) and the token-consolidation
    universe (``consolidation.derive_strategy_token_universe``, VIB-5471). Keeping
    the gateway read + Empty≠Zero ``measured`` gate in one place means the clamp
    and consolidation can never disagree on whether the ledger is trustworthy.

    Crucially this degrades ONLY the NO_ACCOUNTING lane: it is additive, never a
    reason to fail the whole tracked read. The accounting-event lane (the primary
    fail-closed signal) is independent, so an unmeasured / absent ledger backend
    must NOT strand accounted (SWAP/BORROW/WITHDRAW/PT) swap-backs — those keep
    their existing clamp behaviour. Never raises.
    """
    reader = getattr(state_manager, "read_ledger_entries_measured", None)
    if not callable(reader):
        return None
    try:
        rows, measured = reader(deployment_id)
    except Exception:  # noqa: BLE001 — read-only DX guard; never block the unwind.
        logger.warning(
            "VIB-5416 ledger read raised for %s — NO_ACCOUNTING tokens will not be clamp-tracked "
            "(strand, safe); accounted swap-backs unaffected",
            deployment_id,
            exc_info=True,
        )
        return None
    if not measured:
        logger.warning(
            "VIB-5416 ledger read for %s UNMEASURED — NO_ACCOUNTING tokens will not be clamp-tracked "
            "(strand, safe); accounted swap-backs unaffected",
            deployment_id,
        )
        return None
    return rows


def read_tracked_swap_inventory(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str = "",
    wallet_address: str = "",
) -> dict[str, Decimal | None] | None:
    """Deployment-scoped tracked wallet inventory, or the UNMEASURED sentinel.

    Returns ``None`` (unmeasured) when the deployment id is empty, the state
    manager cannot supply accounting events (no accounting backend wired), or
    any read / replay fails. Never raises — fail-closed handling lives in
    :func:`decide_swap_clamp`. Only the accounting ``StateManager``
    (``runner.state_manager``) exposes ``get_accounting_events_sync``; the
    teardown lifecycle state manager does not, so a wrong-flavour manager
    simply yields the sentinel.

    EMPTY ≠ ZERO at the backend boundary (VIB-5173). ``StateManager``
    collapses THREE cases inside ``get_accounting_events_sync`` into an empty
    list — backend structurally absent (e.g. hosted before the metrics-database
    migration, or no warm store), backend raised, genuinely-no-events — because
    that shared contract serves PortfolioValuer and others and must NOT change.
    Reading the empty list directly would feed ``sum_open_wallet_basis_by_token``
    a ``{}`` (measured-zero) on the absent-backend case, so a deployment WITH
    real tracked inventory but an absent backend would skip every swap-back as
    ``untracked_token`` WITHOUT flagging ``accounting_degraded`` — Empty wrongly
    treated as Zero. To fix this WITHOUT touching the shared ``[]`` contract we
    probe ``StateManager.has_accounting_event_backend()`` (the SAME structural
    guard the read runs internally) BEFORE reading: a ``False`` probe means the
    backend is absent, so we return the UNMEASURED sentinel ``None`` and the
    clamp fails closed + flags ``accounting_degraded`` instead of silently
    under-sweeping.

    PRODUCTION PATH (VIB-5185). The runner's ``state_manager`` is always
    ``GatewayStateManager`` for a real ``strat run`` (local AND hosted), so the
    structural probe above never fires in production — the absent-vs-empty
    distinction has to cross the gateway. ``GatewayStateManager`` exposes
    ``read_accounting_events_measured`` which returns ``(events, measured)`` in a
    SINGLE read: ``measured`` is the gateway's ``backend_status`` proto signal,
    ``True`` only when the backend is present AND the read succeeded. A
    ``measured=False`` (structurally absent — e.g. hosted before the
    metrics-database migration — OR a present-but-errored read, both previously
    collapsed into ``[]``) returns the UNMEASURED sentinel ``None`` here, so the
    clamp fails closed + flags ``accounting_degraded``. One read also means no
    structural-probe / read TOCTOU and no extra round-trip.
    """
    if not deployment_id or state_manager is None:
        return None
    # VIB-5185 preferred path: a backend that reports MEASURED vs UNMEASURED in
    # the SAME read it returns events from (GatewayStateManager over the gateway
    # ``backend_status`` proto signal). This is the only path that fires in
    # production, and unlike a pre-read structural probe it also catches a
    # present-but-errored read (Empty ≠ Zero for BOTH absent and errored).
    measured_reader = getattr(state_manager, "read_accounting_events_measured", None)
    if callable(measured_reader):
        try:
            events, measured = measured_reader(deployment_id)
        except Exception:  # noqa: BLE001 — read-only DX guard; never block the unwind.
            logger.warning(
                "ALM-2766 tracked-inventory measured-read failed for %s — swap-back clamp will fail closed",
                deployment_id,
                exc_info=True,
            )
            return None
        if not measured:
            # Backend absent or read errored → an empty read is unmeasured, not
            # zero. Fail closed so the clamp flags accounting_degraded.
            logger.warning(
                "ALM-2766 tracked-inventory read for %s: accounting backend UNMEASURED "
                "(absent or errored) — swap-back clamp will fail closed",
                deployment_id,
            )
            return None
        # VIB-5416: additively fold the deployment's NO_ACCOUNTING ledger rows
        # (STAKE/WRAP/MINT) into the tracked map so their wallet inventory is
        # clamp-visible. A None ledger read drops ONLY that lane (strand, safe).
        ledger_rows = read_no_accounting_ledger_rows(state_manager, deployment_id)
        return sum_open_wallet_basis_by_token(
            events, deployment_id, ledger_rows=ledger_rows, chain=chain, wallet_address=wallet_address
        )
    # VIB-5173 fallback (local ``StateManager``): no per-read measured signal,
    # but a cheap structural probe distinguishes a structurally-absent backend
    # (UNMEASURED) from a genuinely-empty event set (measured zero). Require the
    # read method up front so the probe branch can never fall through to an
    # AttributeError on the read call (``state_manager`` is non-None here — the
    # ``is None`` guard ran at the top alongside the deployment-id check).
    if not hasattr(state_manager, "get_accounting_events_sync"):
        return None
    probe = getattr(state_manager, "has_accounting_event_backend", None)
    if callable(probe):
        try:
            backend_present = probe()
        except Exception:  # noqa: BLE001 — read-only DX guard; never block the unwind.
            return None
        if not backend_present:
            # Backend absent → an empty read is unmeasured, not zero. Fail
            # closed so the clamp flags accounting_degraded (Empty ≠ Zero).
            logger.warning(
                "ALM-2766 tracked-inventory read for %s: accounting backend absent "
                "(has_accounting_event_backend=False) — swap-back clamp will fail closed (unmeasured)",
                deployment_id,
            )
            return None
    try:
        events = state_manager.get_accounting_events_sync(deployment_id)
        # VIB-5416: same additive NO_ACCOUNTING ledger fold as the measured path.
        ledger_rows = read_no_accounting_ledger_rows(state_manager, deployment_id)
        return sum_open_wallet_basis_by_token(
            events, deployment_id, ledger_rows=ledger_rows, chain=chain, wallet_address=wallet_address
        )
    except Exception:  # noqa: BLE001 — read-only DX guard; never block the unwind.
        logger.warning(
            "ALM-2766 tracked-inventory read failed for %s — swap-back clamp will fail closed",
            deployment_id,
            exc_info=True,
        )
        return None


__all__ = [
    "SwapClampDecision",
    "decide_swap_clamp",
    "read_no_accounting_ledger_rows",
    "read_tracked_swap_inventory",
]
