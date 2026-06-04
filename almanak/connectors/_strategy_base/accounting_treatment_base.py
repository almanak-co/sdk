"""Strategy-side base types for connector-owned accounting treatments (VIB-4931).

The framework's accounting dispatcher (``processor._dispatch`` → the
``category_handlers`` registry) routes each ledger event to a handler keyed on a
generic :class:`~almanak.framework.primitives.types.AccountingCategory`. A few
venues need accounting that differs from the generic primitive handler for the
same category — Pendle's LP open/close and PT (principal-token) swaps, for
example, decode PT/YT/SY mechanics a vanilla Uniswap-V3 LP or ERC-20 swap does
not. Historically that difference leaked into the generic taxonomy as
protocol-named ``AccountingCategory`` members (``PENDLE_LP`` / ``PENDLE_PT``) and
``if "pendle" in protocol`` string branches scattered across the framework
(VIB-4931, part of the VIB-4851 self-containment epic).

This module owns the venue-neutral half of the seam that lets a connector fold
that knowledge back into its own folder, mirroring the read seams
(``LendingReadSpec`` / ``AccountStateReadSpec`` / ``PerpsReadSpec``):

* :class:`AccountingCategoryDecision` — "categorize *one* of my events as a
  *generic* category, tagged with an opaque connector treatment key".
* :class:`AccountingTreatmentSpec` — the descriptor a connector publishes (as a
  module-level ``ACCOUNTING_TREATMENT_SPEC``): how to categorize its events, the
  connector-owned pure treatment functions reached via the treatment key, and —
  optionally — how to derive its outbox position key.

The treatment functions are **pure**: they read the already-persisted
ledger/outbox rows (the same ``HandlerContext`` the generic category handlers
receive) and return a typed accounting event. They make **no** gateway egress
and **no** live chain calls (Gateway-boundary rule; the contract the
``category_handlers`` already obey). The category a decision carries is always a
*generic* :class:`AccountingCategory` (``LP`` / ``SWAP`` / ``VAULT`` / …), never
a protocol-named one — the whole point of VIB-4931 is that protocol names leave
the generic enum and travel as opaque payload metadata instead.

Gateway-boundary note: this module is ``_strategy_base`` (the broker tier). It
performs no network egress and does not import ``almanak.framework`` at runtime —
``AccountingCategory`` is referenced only in annotations (kept as strings by
``from __future__ import annotations``) under ``TYPE_CHECKING``, avoiding a
connectors→framework import cycle.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # Annotation-only: a runtime import of ``AccountingCategory`` would pull
    # ``almanak.framework.primitives`` into ``_strategy_base`` (a
    # connectors→framework cycle). Nothing here touches the enum at runtime — the
    # registry handles a decision's ``category`` opaquely — so the
    # type-checking-only import is sufficient.
    from almanak.framework.primitives.types import AccountingCategory

__all__ = [
    "AccountingCategoryDecision",
    "AccountingTreatmentSpec",
    "CategorizeFn",
    "PositionKeyFn",
    "TreatmentFn",
]

# A connector treatment: receives the same ``HandlerContext`` the generic
# category handlers get and returns a typed accounting event (or ``None``). Typed
# loosely (``Any``) so this ``_strategy_base`` module need not import the
# framework ``HandlerContext`` / accounting-event types.
TreatmentFn = Callable[..., Any]

# Categorizer: maps one ``(intent_type, protocol, token_out)`` to a
# generic-category decision, or ``None`` to decline ("this connector does not
# claim this event").
CategorizeFn = Callable[[str, str, str], "AccountingCategoryDecision | None"]

# Outbox position-key deriver: returns the connector's ``(position_key, market_id)``
# for an event it owns, or ``None`` when it does not own ``(protocol, intent_type)``.
# Called keyword-only at the registry boundary.
PositionKeyFn = Callable[..., "tuple[str, str] | None"]


@dataclass(frozen=True)
class AccountingCategoryDecision:
    """How a connector wants one of its events categorized into the GENERIC taxonomy.

    Attributes:
        category: A *generic* :class:`AccountingCategory` member (``LP`` /
            ``SWAP`` / ``VAULT`` / …) — never a protocol-named one. This is the
            value the persisted taxonomy and lot-matching key on.
        treatment_key: The connector-owned routing token the generic dispatcher
            sub-dispatches on (e.g. ``"pendle_lp"`` / ``"pendle_pt"``). Opaque to
            the framework; it lands in the event payload as
            ``payload['treatment']`` so persisted rows are self-describing. Must
            be a non-empty string.
    """

    category: AccountingCategory
    treatment_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.treatment_key, str) or not self.treatment_key:
            raise TypeError(f"treatment_key must be a non-empty string, got {self.treatment_key!r}.")


@dataclass(frozen=True)
class AccountingTreatmentSpec:
    """Connector-published descriptor: how to categorize + how to treat.

    The accounting analogue of the read seams. Pure: it *describes* a
    categorization and *owns* the treatment functions; it performs no gateway
    egress and makes no live chain calls.

    Attributes:
        categorize: ``(intent_type, protocol, token_out) -> AccountingCategoryDecision
            | None``. Returns ``None`` for events this connector does not claim
            (the registry then falls through to the next connector / the generic
            path).
        treatments: ``treatment_key -> treatment fn``. Each fn takes the same
            ``HandlerContext`` the generic category handlers receive and returns a
            typed accounting event (or ``None``). The connector-owned logic the
            generic dispatcher reaches via the decision's ``treatment_key``.
        claims_event_types: The generic ``intent_type`` / event-type strings this
            connector may re-treat (e.g. ``{"LP_OPEN", "LP_CLOSE", "SWAP"}``).
            Lets tooling build a cheap pre-filter and assert no two connectors
            claim contradictory categories for the same ``(event_type, protocol)``.
        position_key: Optional ``(*, protocol, intent_type, chain, wallet, intent)
            -> tuple[str, str] | None`` outbox position-key deriver, returning the
            ``(position_key, market_id)`` pair. Returns ``None`` when the connector
            does not own ``(protocol, intent_type)``; the registry then tries the
            next connector / the generic key derivation. ``None`` (the attribute
            itself) means this connector contributes no custom position key.
    """

    categorize: CategorizeFn
    treatments: Mapping[str, TreatmentFn]
    claims_event_types: frozenset[str]
    position_key: PositionKeyFn | None = None

    def __post_init__(self) -> None:
        # Reject a bare str/bytes for ``claims_event_types``: a str is iterable, so
        # it would silently register each *character* as a claimed event type — the
        # same hard-to-debug footgun ``PrimitiveDeclaration`` guards against. Coerce
        # any other iterable of non-empty strings to a frozenset (the dataclass is
        # frozen, so set via ``object.__setattr__``).
        claims = self.claims_event_types
        if isinstance(claims, str | bytes):
            raise TypeError(
                "claims_event_types must be a frozenset[str], not a bare "
                f"{type(claims).__name__} (a bare string would be iterated "
                "character-by-character); pass e.g. frozenset({'LP_OPEN'})."
            )
        coerced = frozenset(claims)
        for member in coerced:
            if not isinstance(member, str) or not member:
                raise TypeError(f"claims_event_types members must be non-empty strings, got {member!r}.")
        object.__setattr__(self, "claims_event_types", coerced)
