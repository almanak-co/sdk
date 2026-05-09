"""Atomic 3-change alignment guard (VIB-4164, T4).

T4 of the Primitives Refactor reclassifies BRIDGE → TRANSFER in three coupled
places:

1. ``primitives.taxonomy.classify("BRIDGE")`` returns
   ``AccountingCategory.TRANSFER`` (was ``NO_ACCOUNTING``).
2. ``primitives.taxonomy.record_for("TRANSFER")`` returns a
   ``PrimitiveRecord`` whose ``primitive`` is ``Primitive.BRIDGE`` (the
   payload-only TAXONOMY row added by T4 — without it the writer's augment
   chokepoint raises ``UnknownIntentTypeError`` on every live BRIDGE write).
3. ``"TRANSFER"`` is in ``accounting.models.ALL_ACCOUNTING_EVENT_TYPES`` so
   the gateway whitelist (``state_service.py:1396``) accepts the typed event.

The three changes MUST land together: any partial revert leaves production in
a silently-broken state (whitelist accepts but writer rejects, OR writer
accepts but gateway rejects, OR classifier silently routes BRIDGE to
``NO_ACCOUNTING`` while the new event_type lingers in the whitelist). This
test is conjunctive — it fails as soon as any one leg is reverted, so a
future "tidy" PR cannot silently drop one piece.

This test is the F6 (atomicity / silent-error guard) bullet of the UAT card.
"""

from __future__ import annotations

from almanak.framework.accounting.models import ALL_ACCOUNTING_EVENT_TYPES
from almanak.framework.primitives.taxonomy import (
    UnknownIntentTypeError,
    classify,
    record_for,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive


def test_atomic_bridge_transfer_alignment() -> None:
    """All three legs of T4 must hold simultaneously."""
    # Leg 1 — classifier flip.
    assert classify("BRIDGE") == AccountingCategory.TRANSFER, (
        "Leg 1 reverted: classify('BRIDGE') no longer returns "
        "AccountingCategory.TRANSFER. The TAXONOMY BRIDGE row must keep "
        "accounting_category=AccountingCategory.TRANSFER."
    )

    # Leg 1b — BRIDGE row's primitive is still Primitive.BRIDGE (defensive
    # guard against a partial revert that flips the row's primitive to
    # UTILITY while keeping accounting_category=TRANSFER, which would silently
    # break MATCHING_POLICY_VERSIONS lookup for BRIDGE-keyed intents).
    bridge_row = record_for("BRIDGE")
    assert bridge_row.primitive is Primitive.BRIDGE, (
        f"Leg 1b weakened: TAXONOMY['BRIDGE'].primitive must be "
        f"Primitive.BRIDGE so the writer's augment chokepoint stamps "
        f"MATCHING_POLICY_VERSIONS[Primitive.BRIDGE]; got {bridge_row.primitive!r}."
    )

    # Leg 2 — TAXONOMY row for the event_type "TRANSFER".
    try:
        record = record_for("TRANSFER")
    except UnknownIntentTypeError as exc:  # pragma: no cover — caught and re-raised informatively
        raise AssertionError(
            "Leg 2 reverted: TAXONOMY has no row for event_type 'TRANSFER'. "
            "Without it, accounting/writer.augment_accounting_payload raises "
            "UnknownIntentTypeError on every live BRIDGE write."
        ) from exc
    assert record.primitive is Primitive.BRIDGE, (
        f"Leg 2 weakened: TAXONOMY['TRANSFER'].primitive must be "
        f"Primitive.BRIDGE so MATCHING_POLICY_VERSIONS resolves correctly; "
        f"got {record.primitive!r}."
    )
    assert record.accounting_category is AccountingCategory.TRANSFER, (
        f"Leg 2 weakened: TAXONOMY['TRANSFER'].accounting_category must be "
        f"AccountingCategory.TRANSFER; got {record.accounting_category!r}."
    )

    # Leg 3 — gateway whitelist contains TRANSFER.
    assert "TRANSFER" in ALL_ACCOUNTING_EVENT_TYPES, (
        "Leg 3 reverted: ALL_ACCOUNTING_EVENT_TYPES no longer contains "
        "'TRANSFER'. The gateway will reject every TransferAccountingEvent at "
        "state_service.py:1396 with INVALID_ARGUMENT, even though the writer "
        "successfully augments the payload."
    )
