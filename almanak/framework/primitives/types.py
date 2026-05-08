"""Neutral enum tier for the primitives taxonomy.

This module is intentionally thin and dependency-free. It declares the
enums and the :class:`PrimitiveRecord` dataclass that the taxonomy table
uses, and **must not grow beyond that**.

Hard Ratification Condition #3 (VIB-4159, 2026-05-08): this module forbids
growing beyond enums + ``PrimitiveRecord``. Adding behaviour, mappings, or
any code that imports from ``accounting/``, ``intents/``, ``observability/``,
or any other framework subpackage re-introduces the import cycle that the
taxonomy module exists to break. If new behaviour is required, place it in
``primitives/taxonomy.py`` (the consumer-facing API layer) or in the
appropriate downstream module — never here.

The forbidden imports are enforced by a static AST test in
``tests/unit/primitives/test_types.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Primitive(StrEnum):
    """Top-level DeFi primitive families.

    A primitive describes the *kind* of action a strategy is taking. It is
    coarser than ``IntentType`` (e.g. ``LP_OPEN``, ``LP_CLOSE``,
    ``LP_COLLECT_FEES`` all roll up to ``Primitive.LP``).
    """

    SWAP = "swap"
    LP = "lp"
    LENDING = "lending"
    PERP = "perp"
    VAULT = "vault"
    STAKING = "staking"
    BRIDGE = "bridge"
    PREDICTION = "prediction"
    FLASH_LOAN = "flash_loan"
    UTILITY = "utility"


class AccountingCategory(StrEnum):
    """Accounting-handler routing key.

    Determines which ``category_handlers/<name>_handler.py`` processes the
    accounting event for a given intent. This enum is consumed by both the
    accounting processor and the gateway whitelist.

    The values are kept stable (the string form is persisted in
    ``accounting_events.category``) — never rename a value without a
    coordinated migration.
    """

    LENDING = "lending"
    PENDLE_LP = "pendle_lp"
    PENDLE_PT = "pendle_pt"
    LP = "lp"
    PERP = "perp"
    VAULT = "vault"
    SWAP = "swap"
    PREDICTION = "prediction"
    TRANSFER = "transfer"
    NO_ACCOUNTING = "no_accounting"


class PositionKind(StrEnum):
    """Position-tracking categories.

    Mirrors ``observability.position_events.PositionType`` but lives in the
    neutral tier so the taxonomy can declare it without depending on the
    observability package. T2 migrates ``PositionType`` to delegate here.
    """

    LP = "LP"
    PERP = "PERP"
    LENDING_COLLATERAL = "LENDING_COLLATERAL"
    LENDING_DEBT = "LENDING_DEBT"
    VAULT = "VAULT"
    STAKING = "STAKING"


class LifecyclePhase(StrEnum):
    """Where a step sits inside its primitive's lifecycle.

    ``ATOMIC`` — the step settles in one transaction (most current intents).
    ``REQUEST`` / ``CLAIM`` / ``SETTLE`` — phases of an async lifecycle
    (Vault async withdrawals, Pendle redemptions, future Bridge settlement).

    The phase is per-primitive: a Vault async withdraw is REQUEST, the matched
    claim transaction is SETTLE; both share a primitive but differ on phase.
    """

    ATOMIC = "atomic"
    REQUEST = "request"
    CLAIM = "claim"
    SETTLE = "settle"


class EventKind(StrEnum):
    """The shape of the accounting / position event the intent emits.

    ``OPEN`` / ``CLOSE`` — open or close a position leg.
    ``ADJUST`` — change size of an existing leg (perp INCREASE, lending
        BORROW after a SUPPLY).
    ``COLLECT`` — collect rewards / fees without changing principal
        (LP_COLLECT_FEES, vault harvest).
    ``TRANSFER`` — move value across wallets / chains without a position
        change (BRIDGE).
    ``NONE`` — utility intents that do not produce position or accounting
        rows (HOLD, ENSURE_BALANCE, WRAP_NATIVE).
    """

    OPEN = "open"
    CLOSE = "close"
    ADJUST = "adjust"
    COLLECT = "collect"
    TRANSFER = "transfer"
    NONE = "none"


@dataclass(frozen=True)
class PrimitiveRecord:
    """One row of the primitives taxonomy.

    A ``PrimitiveRecord`` describes how the framework should treat a single
    canonical intent string: which primitive it belongs to, which accounting
    handler runs, which position bucket it lands in, whether it carries an
    async settlement gap, and which lifecycle steps a fixture must exercise
    to count as a complete primitive test.

    Records are frozen so the table is hashable and shareable across modules
    without defensive copying. The taxonomy is initialised once at import
    time.

    Attributes:
        intent_type: Canonical intent string (must match
            ``IntentType.value`` for declared types). Aliases are resolved
            before lookup via :data:`ALIASES`.
        primitive: Top-level primitive family.
        accounting_category: Routing key for accounting handlers.
        position_type: Position bucket; ``None`` when the intent does not
            create or modify a tracked position (e.g. SWAP, BRIDGE, HOLD).
        event_kind: Shape of the position / accounting event emitted.
        is_async: ``True`` when the on-chain action does not settle
            atomically and a separate claim/settle step is required.
        lifecycle_phase: Where this step sits inside the primitive's
            lifecycle.
        required_lifecycle: Tuple of intent strings that a complete
            Accountant Test fixture must exercise for this primitive to be
            considered fully-covered. Empty tuple = atomic; the fixture only
            needs this one step. Used by the fixture-lifecycle harness in T2.
    """

    intent_type: str
    primitive: Primitive
    accounting_category: AccountingCategory
    position_type: PositionKind | None
    event_kind: EventKind
    is_async: bool
    lifecycle_phase: LifecyclePhase
    required_lifecycle: tuple[str, ...]
