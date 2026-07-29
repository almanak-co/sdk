"""Write-time Empty != Zero guard for ``transaction_ledger`` (VIB-6043 leg 2).

Why this exists
===============

VIB-6043's parser fix stopped the *cause* of unmeasured swaps under Safe /
Zodiac execution. This module guards the *blast radius*, independently of what
any parser does: **a ledger row must never claim a clean success while its
measured money columns are empty.**

That is exactly what PancakeSwap V3 produced on the Safe lane — a
``success = 1`` row carrying a tx hash, gas, and ``amount_in = ''`` /
``amount_out = ''`` for a swap that really moved $50. Downstream, the accounting
swap handler correctly refuses to fabricate USD or FIFO lots from empty amounts,
so the *only* lie is the success flag on the row itself. The scorecard already
encodes the invariant (Accountant Test cell G1 fails a ``success`` SWAP row with
a blank amount) — but it runs *after the fact*, against a fixture DB. This
module moves the same rule to write time.

Empty != Zero, precisely
========================

By the time a value reaches the row it has passed through
``ledger._measured_amount_to_row`` / ``_leg_amount_to_row``, which collapse the
trichotomy to a clean binary:

* ``""``          → **unmeasured** (absent, out-of-domain, or the parser emitted
  nothing). This is what the guard fires on.
* ``"0"`` / ``"0.000000"`` → **measured zero**, a legitimate result. Legal, never
  touched. A guard that rejected zero would be the mirror-image bug.

What the guard does (and deliberately does not do)
==================================================

It **downgrades and marks**; it does not reject. A rejected write would
manufacture the *other* half of VIB-6043 — the Curve blackout, where money moved
on-chain and the books recorded nothing at all. Between "a row that overclaims"
and "no row", the honest answer is "a row that says what it does not know".

So a violating row is stamped with the shape this repo already uses for
chain-landed-but-books-degraded rows (the slippage-breach, reconciliation-
incident and settlement paths all do this today):

* ``success = False`` — the *framework verdict*, not a claim about the chain.
  ``tx_hash``, ``gas_used``, ``gas_usd``, ``intent_type``, ``chain``,
  ``protocol`` and the state snapshots all stay populated, and position events
  still emit off chain success, so on-chain reality is not erased.
* ``error = "accounting_degraded:<reason>: <detail>"`` — greppable and
  SQL-``LIKE``-able.
* ``extracted_data_json["accounting_degradation"]`` — a structured sibling for
  programmatic consumers.
* The amounts stay ``""``. **Never** a fabricated zero.

No new columns: the deployed Postgres schema is owned outside this repo
(AGENTS.md §Database schema ownership), so the marker rides in existing fields.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from almanak.framework.observability.ledger import LedgerEntry

#: Stable prefix on ``transaction_ledger.error`` marking a degraded row.
#: Stable because operators and the Accountant Test both match on it.
DEGRADED_PREFIX: Final[str] = "accounting_degraded:"

#: Truncation ceiling for the detail text. The hosted ``error`` column's width
#: is owned out-of-repo, so keep the marker comfortably short (mirrors
#: ``accounting/deferred_log`` truncation).
_DETAIL_MAX: Final[int] = 800


# ---------------------------------------------------------------------------
# the LANDED predicate — one home, deliberately (VIB-6043 leg 2)
# ---------------------------------------------------------------------------
#
# ``transaction_ledger.success`` is the FRAMEWORK VERDICT, not chain reality:
# this guard writes ``success=0`` for transactions that really executed. So
# every reader asking "did this transaction happen?" needs the same widened
# predicate, and those readers span three layers — the Accountant Test in this
# package, the operator ship-gate in ``scripts/ci/``, and the demo scorer plus
# the matrix harness in ``scripts/qa/``.
#
# An earlier revision of this leg duplicated the prefix as a literal in each of
# them "to stay pure-stdlib", pinned by a test. That is a mirror-drift design:
# it needs a test to be safe, the test was written claiming to pin every mirror
# while missing one (``run_accounting_matrix``'s readiness poll, where drift
# HANGS the harness rather than answering wrong), and it invites a fifth copy.
# The definition therefore lives HERE, next to the writer that produces the
# marker, and every reader imports it. Scripts importing from ``almanak`` is
# the correct seam: they already run under ``uv run`` inside this repo.


def degraded(error: Any) -> bool:
    """Whether an ``error`` value carries the degradation marker.

    Separate from :func:`landed` because it answers a different question — "are
    the books degraded?" rather than "did this transaction happen?" — and
    callers must not reach for the wrong one. Both share this arm so the marker
    is matched identically everywhere.
    """
    return str(error or "").startswith(DEGRADED_PREFIX)


def _has_tx_hash(tx_hash: Any) -> bool:
    """Whether ``tx_hash`` is a real hash rather than "no transaction".

    Whitespace-only counts as absent: the writers spell "no transaction" as
    ``""``, but a hash that survived a strip/format round trip as ``" "`` makes
    the same statement and must not read as chain evidence.
    """
    return bool(str(tx_hash or "").strip())


def landed(success: Any, error: Any, tx_hash: Any = None) -> bool:
    """**The** landed rule, on raw values. Every other form delegates here.

    Two arms, and only the second consults ``tx_hash``.

    **The clean arm — ``success == 1``.** Unchanged, and deliberately blind to
    ``tx_hash``: callers whose SELECT has no hash column in scope must keep
    working, and a row the framework booked as a clean success is not made
    more or less true by which column the reader happened to project.

    **The degraded arm — the marker AND a non-empty ``tx_hash``** (VIB-6043).
    The marker alone is NOT on-chain evidence. :func:`apply_degradation` stamps
    ``chain_success: True`` **by construction — it never consults a receipt** —
    so it states "the framework believed this executed", not "a receipt says
    so". A NO-OP bundle satisfies every condition with **zero transactions
    submitted**: an ``LP_CLOSE`` against an already-empty position compiles to
    ``transactions=[]`` with ``metadata["no_op"]=True`` (the uniswap_v3 / curve
    / aerodrome compilers all emit this shape), the orchestrator sets
    ``result.success=True``, the runner writes a ledger row with
    ``tx_hash=""``, and the absent receipt leaves the money slots unmeasured —
    so :func:`classify_ledger_row` stamps the marker. Without the hash conjunct
    this function answers **True** for a routine idempotent teardown where
    nothing happened, and the readers turn that into a phantom executed row
    they can never complete.

    The lane the guard exists for — a Safe / bundle execution whose parser
    yielded no amounts — always carries a hash, so the conjunct costs it
    nothing. Omitting ``tx_hash`` fails CLOSED (the degraded arm cannot fire):
    a caller that cannot show a hash has not shown that anything landed.

    **A hash is NECESSARY, not SUFFICIENT — stated once here so no reader
    re-derives it and gets it wrong.** It excludes the no-op, which is what it
    is for. It does NOT prove confirmation, because the marker has **two
    independent producers** and only one of them looks at a receipt:

    * **The in-process commit path.** ``orchestrator`` returns early when any
      ``transaction_results`` entry is unsuccessful and sets
      ``result.success = True`` only after that check, and
      :func:`classify_ledger_row` short-circuits on ``success`` already False —
      so a bundle with a reverted leg never receives the marker here. Under
      THIS producer a hash does imply every leg confirmed.
    * **The gateway RPC boundary.** ``SaveLedgerEntry`` /
      ``SaveLedgerAndRegistry`` (``gateway/services/state_service.py``) take the
      caller's ``success``, amounts and ``tx_hash`` **verbatim** and run
      :func:`degrade_row_fields` without validating any receipt — deliberately,
      because the gateway is a trust boundary that must not assume its caller
      ran the guard. A differently-versioned strategy container or a replay tool
      can therefore submit ``success=True`` with blank amounts and a
      **submitted-but-reverted or never-confirmed** hash, and the marker is
      stamped. Under THIS producer a hash implies only "a transaction was
      broadcast".

    ``apply_degradation`` writes ``chain_success: True`` unconditionally,
    regardless of producer, so it corroborates nothing. Consequence for
    readers: this arm means **"not a no-op"**, not "confirmed on-chain". Where a
    surface needs actual confirmation it must consult per-leg receipt evidence
    (``extracted_data_json.sub_transactions[*].status``), and where only measured
    per-leg data will do it must NOT fall back to this arm. Closing the gateway
    hole properly needs receipt evidence at that boundary, not a stronger
    predicate here.

    ``success == 1``, deliberately — not ``bool(success)`` and not
    ``success is True``. All three were in the tree at once and they disagree:

    ======== ======== ============ ============
    value    ``== 1`` ``bool(...)`` ``is True``
    ======== ======== ============ ============
    ``True``  True     True         True
    ``1``     True     True         **False**
    ``'0'``   False    **True**     False
    ``'1'``   False    **True**     False
    ``2``     False    **True**     False
    ======== ======== ============ ============

    ``bool(...)`` treats the malformed string ``'0'`` as **landed** — an
    Empty != Zero violation, and the worst of the three. ``is True`` rejects
    the integer ``1``, which is exactly what ``sqlite3`` returns for a
    NUMERIC-affinity ``BOOLEAN`` column, so it silently drops genuine rows read
    straight from the DB.

    ``== 1`` is the only rule that accepts both real spellings of true
    (``True`` from an ORM/object, ``1`` from SQLite) while refusing to guess at
    a malformed value. Never widen it to truthiness.
    """
    return success == 1 or (degraded(error) and _has_tx_hash(tx_hash))


def row_landed(row: Any) -> bool:
    """:func:`landed` for anything indexable by column name.

    Accepts ``dict``, ``sqlite3.Row``, or a ledger view.

    ``row["error"]`` and ``row["tx_hash"]`` are BOTH read without ``.get`` and
    without a try/except, on purpose, and they fail in opposite directions —
    which is precisely why neither may be softened:

    * a missing ``error`` degrades this to ``row["success"] == 1``, the exact
      pre-guard predicate, reinstating the defect this function exists to
      detect;
    * a missing ``tx_hash`` degrades the degraded arm to always-False, so every
      genuinely landed-but-degraded row — the Safe / bundle lane this whole
      module exists for — silently drops out of the reader's population.

    Either way the failure is invisible: no error, and a green test suite. A
    loud ``IndexError``/``KeyError`` naming the column is the correct failure,
    and it tells the caller exactly which column to add to its SELECT.

    **Scope.** Matches the Empty != Zero marker only. The slippage
    circuit-breaker and the reconciliation finalizer also write ``success=0``
    on transactions that reached the chain, but with free-form ``error`` prose
    carrying no machine-readable marker, so they are NOT matched. Closing that
    gap is a PRODUCER-side contract — give those paths a stable marker — not a
    reader-side regex over prose, which would be a worse bug than the one being
    fixed. Tracked in VIB-6147.
    """
    return landed(row["success"], row["error"], row["tx_hash"])


#: The characters :meth:`str.strip` removes, spelled for SQLite's ``trim(X, Y)``
#: (which strips only literal spaces in its one-argument form). Written with
#: ``char(...)`` because a tab or newline embedded in a SQL string literal is
#: invisible in a diff — and an invisible character in a predicate that decides
#: whether money moved is not reviewable. Keeps :func:`landed_sql` and
#: :func:`_has_tx_hash` agreeing on every ASCII-whitespace-only hash.
#: EVERY character ``str.isspace()`` accepts — the set ``str.strip()`` removes.
#: The previous value covered only 6 of them, so 23 unicode whitespace
#: characters (NBSP, the EN/EM quad family, U+2028/9, ideographic space, the
#: C1 separators) were LANDED to SQL and NOT LANDED to Python — a divergence
#: in the one invariant this module exists to hold, invisible to a test that
#: only tried ASCII whitespace. (VIB-6043 review.)
_SQL_STRIP_CHARS: Final[str] = (
    "char(9)||char(10)||char(11)||char(12)||char(13)||char(28)||char(29)||char(30)||char(31)||char(32)||char(133)||char(160)||char(5760)||char(8192)||char(8193)||char(8194)||char(8195)||char(8196)||char(8197)||char(8198)||char(8199)||char(8200)||char(8201)||char(8202)||char(8232)||char(8233)||char(8239)||char(8287)||char(12288)"
)


def landed_sql(alias: str = "") -> str:
    """SQL form of :func:`row_landed`, for readers that filter in the database.

    **The two forms MUST agree.** A SQL reader and a Python reader disagreeing
    about what landed is the exact drift this module exists to prevent, so the
    degraded arm carries the same ``tx_hash`` conjunct :func:`landed` does — see
    there for why the marker alone is not on-chain evidence. ``tests/unit/
    accounting/test_ledger_guard.py`` runs both over one table and diffs them.

    Consequence for callers: the degraded arm now needs ``tx_hash`` **in the
    table being queried**. Every in-repo caller filters ``transaction_ledger``
    (or an alias of it) where the column exists; a subquery that projects the
    column away will raise ``no such column`` — loudly, which is the right
    failure, and the same discipline :func:`row_landed` applies in Python.

    Prefix match uses ``substr``, NOT ``LIKE``: ``_`` is a single-character
    LIKE wildcard and the prefix contains one, so ``LIKE 'accounting_degraded:%'``
    would also match ``accounting-degraded:`` and ``accountingXdegraded:``.
    ``substr`` sidesteps wildcard and ESCAPE-clause quoting entirely. A NULL
    ``error`` yields NULL, which is not true, so those rows are excluded.

    Uses a NAMED parameter. An earlier revision emitted two positional ``?``
    placeholders that each caller spliced into a tuple: swapping them makes
    SQLite coerce the text to integer ``0``, ``substr(error, 1, 0)`` returns
    ``''``, and the predicate collapses to ``success=1`` — the pre-guard bug,
    restored, **with no exception raised**. Named binding makes ordering
    irrelevant and lets the fragment repeat within one statement for free.

    Callers pass :data:`LANDED_PARAMS`, merging it into their own dict when
    they have other parameters.
    """
    p = f"{alias}." if alias else ""
    return (
        f"({p}success=1 OR (substr({p}error, 1, length(:degraded_prefix)) = :degraded_prefix"
        f" AND {p}tx_hash IS NOT NULL AND trim({p}tx_hash, {_SQL_STRIP_CHARS}) <> ''))"
    )


#: Named parameters for :func:`landed_sql`, repeatable within one statement.
LANDED_PARAMS: Final[dict] = {"degraded_prefix": DEGRADED_PREFIX}


class LedgerDegradationReason(StrEnum):
    """Why a row could not be booked as a clean success."""

    #: Required measured money columns are empty on a would-be success row.
    AMOUNTS_UNMEASURED = "amounts_unmeasured"


class SlotPolicy(StrEnum):
    """How many of an intent's declared money slots must be measured."""

    #: EVERY listed slot must be measured. A swap that reports what it spent
    #: but not what it received is half-booked, and half-booked is wrong.
    ALL = "all"

    #: AT LEAST ONE listed slot must be measured. Used where a *single-sided*
    #: leg is legitimate — a one-sided LP add, or a close that returns only
    #: one token — so requiring both would degrade correct rows. It still
    #: catches the shape that matters: a row where **nothing** was measured.
    ANY = "any"


#: Per-intent contract: which money columns must carry a measurement on a
#: success row, and how many of them.
#:
#: * ``SWAP`` — ALL. Exactly the set Accountant Test cell G1 enforces
#:   (``intent_type == "SWAP" and not (amount_in and amount_out)``); a test
#:   pins the two together so they cannot drift.
#: * ``LP_OPEN`` / ``LP_CLOSE`` — ANY. VIB-6051: Curve ``LP_CLOSE`` rows
#:   persisted with **all four** money columns empty while ``position_events``
#:   booked the value correctly — the books disagreed with themselves about a
#:   position that really closed. ANY (not ALL) because single-sided LP legs
#:   are legitimate and must not be degraded.
#:
#: This table stays SMALL on purpose. Per-primitive leg semantics belong to the
#: ``PrimitiveMoneyLegs`` seam (connector-declared legs); this is the
#: minimum-viable backstop for the two shapes that have actually bitten us in
#: production, not a growing framework-side registry of every primitive.
#:
#: .. warning::
#:
#:    **Before adding an intent type here, check its wallet-delta lane.**
#:    Degrading a row flips ``success`` to ``False``, and two fund-safety
#:    readers in ``accounting/basis.py`` — ``_is_ledger_projected_row``
#:    (``basis.py:1859``) and ``_unmeasured_ledger_token_footprint``
#:    (``basis.py:2146``) — reject a non-success row **first**, before they
#:    ever look at the lane, and they feed the teardown consolidation token
#:    universe (``teardown/consolidation.py:249``).
#:
#:    What makes this safe today is the lane **value**, not the check order:
#:    every intent in this table (``SWAP`` / ``LP_OPEN`` / ``LP_CLOSE``)
#:    declares ``wallet_delta=event_replay``, which matches neither
#:    ``LEDGER_PROJECTION`` nor ``UNMEASURED``, so those readers would skip
#:    these rows regardless of ``success``. Verified by running ``record_for``
#:    — the docstrings describe both helpers as operating on "SUCCESSFUL" rows
#:    and will lead you to the wrong conclusion.
#:
#:    Add a ``LEDGER_PROJECTION`` intent (STAKE / WRAP / CDP-mint) or an
#:    ``UNMEASURED`` one (LP / vault / perp / bridge / settlement) and the
#:    success gate DOES bite: a degraded row of that type silently drops out of
#:    the teardown token universe and the token strands at teardown. Fix those
#:    readers first (key them on chain success, as the reporting path now does).
REQUIRED_MONEY_SLOTS: Final[dict[str, tuple[tuple[str, ...], SlotPolicy]]] = {
    "SWAP": (("amount_in", "amount_out"), SlotPolicy.ALL),
    "LP_OPEN": (("amount_in", "amount_out"), SlotPolicy.ANY),
    "LP_CLOSE": (("amount_in", "amount_out"), SlotPolicy.ANY),
}


@dataclass(frozen=True)
class LedgerDegradation:
    """A verdict that a row cannot be booked as a clean success."""

    reason: LedgerDegradationReason
    columns: tuple[str, ...]
    detail: str

    def marker(self) -> str:
        """The ``error``-column marker for this degradation."""
        return f"{DEGRADED_PREFIX}{self.reason.value}: {self.detail[:_DETAIL_MAX]}"


def _is_unmeasured(value: Any) -> bool:
    """True when a money column carries no measurement.

    Unmeasured: ``None``, the empty / whitespace-only string, and any value
    whose type is not a legal money value (fail closed — see below).

    **A measured zero is a measurement.** ``"0"``, ``"0.000000"``,
    ``Decimal("0")`` and ``0`` are all MEASURED and must never be degraded —
    that is the mirror-image of the bug this module exists to prevent, and it
    is the repo-wide *Empty != Zero* rule (AGENTS.md §Accounting).

    Note the deliberate avoidance of ``value or ""``: every numeric zero is
    falsy, so that idiom collapses ``Decimal("0")`` / ``0`` / ``0.0`` to ``""``
    and reports a measured zero as unmeasured. Callers currently hand us
    ``str``-typed money columns, but the type hints are unenforced and
    ``build_ledger_entry`` produces ``Decimal | str | None``, so the predicate
    must be correct for the numeric case rather than rely on a stringifying
    caller (VIB-6043 leg 2 review).
    """
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    # ``bool`` is a subclass of ``int``, so it must be rejected BEFORE the
    # numeric check or ``False`` would read as a measured zero.
    if isinstance(value, bool):
        return True
    if isinstance(value, int | float | Decimal):
        # A number is a measurement — including zero. This is the whole point.
        return False
    # Unknown type in a money column: fail CLOSED. Degrading is the
    # conservative direction — it never fabricates a number, it only marks the
    # row as not-cleanly-successful, which is exactly what an unreadable money
    # column deserves. Treating it as measured would let a malformed value
    # ride through as a clean success.
    return True


def is_degraded(entry: LedgerEntry) -> bool:
    """Whether ``entry`` already carries a degradation marker."""
    return degraded(getattr(entry, "error", ""))


def classify_ledger_row(entry: LedgerEntry) -> LedgerDegradation | None:
    """Return a degradation verdict for ``entry``, or ``None`` when it is clean.

    Short-circuits (in order):

    * ``success`` is already False — the row makes no clean-success claim, so
      there is nothing to downgrade. This is also what makes the guard compose
      with the *other* degraded-row producers (slippage, reconciliation,
      settlement) instead of double-marking them.
    * the row is already marked — keeps the guard idempotent under retries /
      ``INSERT OR REPLACE``.
    * the intent type declares no money-slot contract — ``SETTLE_*`` rows,
      TRANSFER, perp/lending intents and anything else not in
      :data:`REQUIRED_MONEY_SLOTS` are untouched.
    * the contract is satisfied — under :attr:`SlotPolicy.ANY`, one measured
      slot is enough (a single-sided LP leg is legitimate); under
      :attr:`SlotPolicy.ALL`, every slot must be measured.
    """
    if not getattr(entry, "success", False):
        return None
    if is_degraded(entry):
        return None

    contract = REQUIRED_MONEY_SLOTS.get(str(getattr(entry, "intent_type", "") or "").upper())
    if not contract:
        return None
    slots, policy = contract

    unmeasured = tuple(slot for slot in slots if _is_unmeasured(getattr(entry, slot, "")))
    if not unmeasured:
        return None
    if policy is SlotPolicy.ANY and len(unmeasured) < len(slots):
        # At least one slot carries a measurement — a legitimate single-sided
        # leg, not a books failure.
        return None

    return LedgerDegradation(
        reason=LedgerDegradationReason.AMOUNTS_UNMEASURED,
        columns=unmeasured,
        detail=(
            f"intent={getattr(entry, 'intent_type', '')} "
            f"protocol={getattr(entry, 'protocol', '') or '?'} "
            f"chain={getattr(entry, 'chain', '') or '?'} "
            f"unmeasured={','.join(unmeasured)} "
            f"tx={getattr(entry, 'tx_hash', '') or '?'}"
        ),
    )


def apply_degradation(entry: LedgerEntry, degradation: LedgerDegradation) -> None:
    """Stamp ``degradation`` onto ``entry`` in place, before it is written.

    Mutates ``success``, ``error`` and ``extracted_data_json`` only. The money
    columns are left exactly as they are — unmeasured stays unmeasured; this
    function never writes a number it was not given.
    """
    entry.success = False
    entry.error = degradation.marker() if not entry.error else f"{degradation.marker()} | {entry.error}"

    payload: dict[str, Any] = {}
    raw = getattr(entry, "extracted_data_json", "") or ""
    if raw:
        try:
            loaded = json.loads(raw)
        except (ValueError, TypeError):
            # A non-JSON blob is preserved under a side key rather than dropped:
            # losing forensic data to make room for a marker would be its own
            # accounting bug.
            payload = {"_unparsed_extracted_data": raw[:_DETAIL_MAX]}
        else:
            if isinstance(loaded, dict):
                payload = loaded
            else:
                # VALID JSON that is not an object — an array, string, or number.
                # This used to fall through with `payload` still `{}`, silently
                # DESTROYING the blob: `json.loads` raises nothing, so the
                # except arm above never fired and the preservation promise it
                # documents did not apply. A top-level array carrying
                # `sub_transactions` therefore lost the whole audit trail that
                # CI gate G8 reads.
                #
                # Reachable exactly where it matters: the gateway RPCs
                # (`state_service.SaveLedgerEntry` / `SaveLedgerAndRegistry`),
                # the boundary added on the stated premise that the caller may
                # be untrusted or differently versioned — i.e. the one producer
                # most likely to send a shape this function did not expect.
                payload = {"_unparsed_extracted_data": raw[:_DETAIL_MAX]}

    payload["accounting_degradation"] = {
        "v": 1,
        "reason": degradation.reason.value,
        "columns": list(degradation.columns),
        "detail": degradation.detail[:_DETAIL_MAX],
        "chain_success": True,
    }
    entry.extracted_data_json = json.dumps(payload)


@dataclass
class _RowView:
    """Minimal mutable stand-in so non-``LedgerEntry`` callers reuse the guard.

    The gateway's ``SaveLedgerEntry`` servicer holds a protobuf request, not a
    ``LedgerEntry``. Rather than reimplement the rule there — where it would
    drift from this module the first time either side changed — the request's
    fields are projected onto this view, run through the SAME
    ``classify_ledger_row`` / ``apply_degradation`` pair, and read back.
    """

    intent_type: str = ""
    success: bool = True
    amount_in: str = ""
    amount_out: str = ""
    error: str = ""
    extracted_data_json: str = ""
    tx_hash: str = ""
    chain: str = ""
    protocol: str = ""


def degrade_row_fields(
    *,
    intent_type: str,
    success: bool,
    amount_in: str,
    amount_out: str,
    error: str = "",
    extracted_data_json: str = "",
    tx_hash: str = "",
    chain: str = "",
    protocol: str = "",
) -> tuple[bool, str, str] | None:
    """Guard verdict for a row held as loose fields rather than a ``LedgerEntry``.

    Returns ``(success, error, extracted_data_json)`` to persist INSTEAD of the
    supplied values when the row violates the Empty != Zero rule, or ``None``
    when the row is clean and should be written unchanged.

    Exists for **write boundaries that are not the commit primitive** — most
    importantly the gateway's ``SaveLedgerEntry`` RPC, which is the hosted
    write path and, being a trust boundary, must not assume its caller already
    applied the guard. The money columns are never returned: they are never
    altered.
    """
    view = _RowView(
        intent_type=intent_type,
        success=success,
        amount_in=amount_in,
        amount_out=amount_out,
        error=error,
        extracted_data_json=extracted_data_json,
        tx_hash=tx_hash,
        chain=chain,
        protocol=protocol,
    )
    degradation = classify_ledger_row(view)  # type: ignore[arg-type]
    if degradation is None:
        return None
    apply_degradation(view, degradation)  # type: ignore[arg-type]
    return view.success, view.error, view.extracted_data_json


__all__ = [
    "DEGRADED_PREFIX",
    "LANDED_PARAMS",
    "degraded",
    "landed",
    "landed_sql",
    "row_landed",
    "REQUIRED_MONEY_SLOTS",
    "SlotPolicy",
    "LedgerDegradation",
    "LedgerDegradationReason",
    "apply_degradation",
    "classify_ledger_row",
    "degrade_row_fields",
    "is_degraded",
]
