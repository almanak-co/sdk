"""One physical perp position must be closed ONCE per teardown plan (VIB-6341).

Teardown enumeration is **additive and never subtractive** by design: a
strategy-reported row is never dropped, because the OPEN-rows read cannot tell
"genuinely closed" from "registry write was skipped at open", and a false drop
strands a live position silently (``registry_enumeration.reconcile_lp_with_registry``,
VIB-6287). The enumeration is therefore *guaranteed* to hand duplicates
downstream in exactly the situations where identity could not be resolved — and
that is the correct, loud direction for an ENUMERATION.

It is not the correct direction for the MONEY LANE. Every plan builder maps one
enumerated row to one closing intent (``full_close.full_close_intents``), so an
over-split enumeration becomes **two full closes submitted against one physical
perp**. A close of delta ``d`` against size ``b`` leaves ``c = b - d``; issuing
it twice closes ``d`` again. On a managed Anvil fork no keeper fills, so the
second close acts on nothing and the hazard is invisible. On mainnet it is a
real position-reducing transaction.

**Why the guard lives here and not in the enumeration.** Collapsing rows in the
enumeration would move it in the silent direction — a wrong merge suppresses a
row, nothing builds a closing intent for it, and teardown reports success over
stranded funds. Collapsing *intents* is strictly safe in comparison: the rows
stay enumerated, counted, coverage-checked (``completeness.check_intent_coverage``
credits one intent to every position it covers — it does not consume intents) and
on-chain closure-verified. Only the duplicate ECONOMIC ACTION is removed, and a
second full close of a position that is already fully closed has no correct
outcome to lose.

**Why it is not conditional on registry hydration.** VIB-6341 was found with an
empty ``position_registry`` (GMX ``PERP_OPEN`` cannot write a venue position key
— the key does not exist until the keeper fills — so hydration is done later by
``perp_settlement_reconciler``, and on a fork it never runs). That made
VIB-6287's alias union early-return before it executed. But the additive contract
means the union would not have dropped the strategy row anyway, so the duplicate
is reachable on BOTH paths. This guard sits after every plan is built and takes
no registry input at all.

Identity is the venue's, never the framework's
==============================================

Two full closes are collapsed **iff the position's own venue says they name one
position** — via the same connector-published identity hook the enumeration and
the completeness check use (``perp_identity.venue_identity_tokens``). Nothing
here compares raw ``market`` / ``collateral_token`` strings: the producers of one
row write different VALUE SPACES under the same key names (``"ETH/USD"`` vs
``0x70d9…``), which no framework-side string comparison can reconcile — that is
the whole VIB-6287 result.

The probe is built from the three fields a ``PerpCloseIntent`` definitively
*states* — ``market``, ``collateral_token``, ``is_long`` — mirroring
``completeness._perp_intent_venue_aliases``. The intent's own ``position_id`` is
deliberately not read: it is venue-specific, documented as ignored for GMX, and
adopting an id an intent merely carries could collapse two intents that close
different positions.

Polarity, stated so a future change cannot quietly invert it
============================================================

* **Empty ≠ Zero.** An intent whose venue emits no token has UNMEASURED identity.
  It is never collapsed and never used to collapse another — two such intents
  stay two closes. Over-split, loud, recoverable.
* **FULL closes only.** Collapse applies to ``size_usd is None`` (close
  everything). Two sized closes against one position can both be intended (a
  staged exit), so they are left alone.
* **First wins.** Within a component the FIRST intent in plan order survives, so
  the surviving plan is a prefix-preserving subsequence of the original and the
  builder's risk-priority ordering (PERP -> BORROW -> SUPPLY -> LP -> TOKEN) is
  untouched.
* **Pure.** This never executes, signs, or commits. It returns new lists; the
  result flows through the same ``_execute_intents`` funnel, so the per-intent
  ``runner_helpers.commit`` pairing and the VIB-3773 anti-bypass guards are
  unaffected and no new execute site is introduced.

The collapse decides DISPATCH; coverage must see the PRE-COLLAPSE plan
=====================================================================

**This is the load-bearing rule of the module, and getting it wrong turns a
working teardown into a false FAILED (#3574 audit).** The guard returns a
:class:`SingleClosePlan`: ``dispatch`` is what may be submitted, and
``for_coverage`` is the plan exactly as it was built. Every
``check_intent_coverage`` call MUST be given ``for_coverage``.

The reason is that the two predicates are **not the same predicate**, and an
earlier version of this docstring claimed they were:

* This guard compares **intent to intent**. Both probes are wallet-free with
  ``position_id=""``, so ADOPT and DERIVE can never fire and every measured
  intent emits exactly one ``<slug>:sem:…`` token. Two closes therefore collapse
  on ``sem`` agreement.
* Coverage compares **intent to position**, and a POSITION can emit a
  ``key``-only token with **no** ``sem`` token at all. ``gmx_v2/perp_identity.py``
  does exactly that in two live branches: when the row's adopted key disagrees
  with the derived key (``:291``), and when no wallet is available so DERIVE
  cannot run (``:328``). Neither branch is a misconfiguration — the second is
  reachable through three deliberate fallbacks, and the first is the shape any
  keeper-tx mis-attribution produces.

A ``key``-only position cannot intersect a ``sem``-only intent probe, and the raw
``market`` fallback also fails across symbol-vs-address. So the intent this guard
drops can be **the only one covering that row**. Measured before the fix, both
shapes went ``2 closes / complete=True`` -> ``1 close / complete=False``.

The consequence was never a strand — the position still closes exactly once — but
the teardown got stamped ``VerificationStatus.FAILED`` with ``positions_failed >
0``, destroying the operator's only "are funds stranded?" signal on precisely the
runs where the enumeration is already misbehaving. ``completeness.py``'s own
warning names it: *"a coverage check that starts rejecting matches turns working
teardowns into FAILED, a regression that would read as an improvement."*

Feeding ``for_coverage`` to the gate is not a workaround: the dropped intent IS
the proof that the plan accounted for that row. The plan was complete; only the
submission is de-duplicated.

Wiring is therefore **dispatch-adjacent only** — every call site sits in the same
function as the dispatch it guards, so no lane can collapse a plan that a LATER
function still has to coverage-check. ``full_close_intents`` deliberately does
NOT collapse for this reason: it is a builder, its output crosses a function
boundary, and a builder that hides an intent from a downstream gate reintroduces
exactly this defect.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.teardown.completeness import _field, _intent_type
from almanak.framework.teardown.models import PositionInfo, PositionType

logger = logging.getLogger(__name__)

_PERP_CLOSE = "PERP_CLOSE"


def _full_close_venue_aliases(intent: Any) -> frozenset[str]:
    """Venue alias tokens for the position a FULL perp close targets.

    Empty means "not a full perp close" or "the venue could not name it" — both
    UNMEASURED for this guard's purposes, and both must leave the intent alone.
    """
    if _intent_type(intent) != _PERP_CLOSE:
        return frozenset()
    # ``size_usd`` is the full-close marker: ``None`` == close everything.
    if _field(intent, "size_usd") is not None:
        return frozenset()
    protocol = str(_field(intent, "protocol") or "").strip()
    chain = str(_field(intent, "chain") or "").strip().lower()
    market = _field(intent, "market")
    collateral = _field(intent, "collateral_token")
    is_long = _field(intent, "is_long")
    # A side that is not a measured bool is UNMEASURED — a long and a short in
    # one market are different venue keys, so guessing here would merge two live
    # positions.
    if not protocol or not chain or market is None or collateral is None or not isinstance(is_long, bool):
        return frozenset()

    from almanak.framework.teardown.perp_identity import venue_identity_tokens

    probe = PositionInfo(
        position_type=PositionType.PERP,
        position_id="",
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details={"market": market, "collateral_token": collateral, "is_long": is_long},
    )
    # Wallet-free on purpose (see ``completeness._perp_venue_alias_match``): a
    # derived-key token from a probe is corroborated by nothing. The ``sem``
    # token — which IS where the symbol-vs-address polysemy is resolved — needs
    # no wallet, so the shape this guard exists for is fully covered without one.
    return venue_identity_tokens(probe)


def _describe(intent: Any) -> str:
    return (
        f"{_field(intent, 'protocol')}/{_field(intent, 'chain')} "
        f"market={_field(intent, 'market')} collateral={_field(intent, 'collateral_token')} "
        f"is_long={_field(intent, 'is_long')}"
    )


@dataclass(frozen=True)
class SingleClosePlan:
    """What to SUBMIT, and what the coverage gate must still be shown.

    The two lists are deliberately separate fields rather than one return value,
    because the whole defect this class exists to prevent is a caller handing the
    collapsed list to ``check_intent_coverage`` (see the module docstring —
    "The collapse decides DISPATCH").
    """

    dispatch: list[Any]
    """The plan to execute: at most one full close per physical perp position."""

    dropped: list[Any]
    """Duplicate full closes removed from dispatch. Never executed."""

    for_coverage: list[Any]
    """The plan EXACTLY as built, pre-collapse.

    Hand this — never :attr:`dispatch` — to ``check_intent_coverage``. A dropped
    intent is the proof that the plan accounted for its position; the plan was
    complete, only the submission is de-duplicated. Coverage's intent<->position
    predicate is strictly weaker than this guard's intent<->intent one, so the
    dropped intent can be the ONLY one that covers a ``key``-only position row.
    """

    @property
    def collapsed(self) -> bool:
        """Whether anything was actually dropped (cheap check for logging)."""
        return bool(self.dropped)


def collapse_duplicate_perp_closes(intents: list[Any] | None) -> SingleClosePlan:
    """Split a teardown plan into what to dispatch and what coverage must see.

    Pure. Idempotent — it is applied on more than one lane, and applying it twice
    must be a no-op. Never raises: a guard that faults the teardown lane is worse
    than the duplicate it prevents, so any unexpected failure returns the plan
    unchanged (the pre-VIB-6341 behaviour).

    Returns:
        A :class:`SingleClosePlan`. Execute ``.dispatch``; pass ``.for_coverage``
        to every ``check_intent_coverage`` call in the same lane.
    """
    plan = list(intents or [])
    if len(plan) < 2:
        return SingleClosePlan(dispatch=plan, dropped=[], for_coverage=list(plan))
    try:
        return _collapse(plan)
    except Exception:  # noqa: BLE001 — a guard must never fault the teardown lane
        # The docstring promises "never raises", and teardown's first job is to
        # REMOVE on-chain risk: a guard that faults the lane strands the position
        # it was protecting, which is strictly worse than the duplicate close it
        # prevents. The identity read was already guarded; the collapse itself was
        # not, so an unexpected fault in the union-find or in a connector-published
        # identity hook could still take the lane down (#3574 audit).
        logger.exception("perp single-close guard faulted; leaving the plan unchanged")
        return SingleClosePlan(dispatch=plan, dropped=[], for_coverage=list(plan))


def _collapse(plan: list[Any]) -> SingleClosePlan:
    """Collapse duplicates. Callers must route through the guarded public wrapper."""
    aliases = [_full_close_venue_aliases(i) for i in plan]
    if sum(1 for a in aliases if a) < 2:
        return SingleClosePlan(dispatch=plan, dropped=[], for_coverage=list(plan))

    # "Same position iff the alias sets intersect", closed transitively — the
    # equivalence relation ``_strategy_base/perp_identity.py`` defines. Today an
    # intent probe can only emit one family (``sem``), so the closure is trivial;
    # it is written properly anyway so a hook that later emits a bridging row
    # cannot silently degrade this to an order-dependent greedy pass (VIB-6287).
    parent: dict[str, str] = {}

    def _find(token: str) -> str:
        parent.setdefault(token, token)
        while parent[token] != token:
            parent[token] = parent[parent[token]]
            token = parent[token]
        return token

    def _link(tokens: frozenset[str]) -> None:
        ordered = sorted(tokens)
        for other in ordered[1:]:
            root_a, root_b = _find(ordered[0]), _find(other)
            if root_a != root_b:
                parent[root_a] = root_b

    for token_set in aliases:
        _link(token_set)

    kept: list[Any] = []
    dropped: list[Any] = []
    claimed: set[str] = set()
    representative: dict[str, Any] = {}
    for intent, token_set in zip(plan, aliases, strict=True):
        if not token_set:
            kept.append(intent)
            continue
        roots = {_find(t) for t in token_set}
        already = roots & claimed
        if already:
            first = representative[next(iter(sorted(already)))]
            logger.error(
                "🛑 Teardown plan named ONE physical perp position twice — NOT submitting the "
                "duplicate full close (VIB-6341). kept=[%s] withheld=[%s]. The enumeration "
                "over-split this position (commonly an unhydrated position_registry row); "
                "submitting both would close it twice on-chain. The withheld intent is still "
                "shown to the completeness gate, so this cannot report a covered position as "
                "uncovered.",
                _describe(first),
                _describe(intent),
            )
            dropped.append(intent)
            continue
        claimed |= roots
        for root in roots:
            representative[root] = intent
        kept.append(intent)
    return SingleClosePlan(dispatch=kept, dropped=dropped, for_coverage=plan)


__all__ = ["SingleClosePlan", "collapse_duplicate_perp_closes"]
