"""Detection: stale-post-teardown snapshot under the SWAP-class fallback (VIB-4907).

Background — the cascade
------------------------
Under VIB-3614 ``portfolio_snapshots.total_value_usd`` is **position-scoped**
(positive position values only).  For SWAP-class strategies whose only
"positions" are wallet pseudo-positions (TOKEN wrappers around wallet
balances), post-teardown both the pseudo-positions and the on-chain wallet
appear to be in motion at once, but the post-teardown ``portfolio_snapshots``
row currently sees neither.  Two compounding bugs make the headline number
misleading:

* **F2 / VIB-4906** — ``MarketSnapshot``'s in-memory balance cache is not
  invalidated before the post-teardown bracket runs, so the post-teardown
  snapshot reads pre-teardown wallet state.
* **F3 / cascade tail** — the headline PnL formula at
  ``portfolio/models.py:386`` is ``total_value_usd − initial_value_usd − …``;
  under the VIB-3614 position-scope, post-teardown that becomes
  ``0 − initial_value_usd`` (or worse, ``stale_pseudo_position −
  short_circuited_baseline``), even though the wallet recovered its capital.

Until VIB-4906 ships *and* the F3 schema/formula cascade is resolved
(blocked on VIB-4909 + ``metrics-database`` coordination), ``strat pnl``
should refuse to render the headline number for the affected pattern rather
than display an arithmetically valid but semantically wrong figure.

What this module does
---------------------
Read-only detection: given the recent ``portfolio_snapshots`` window and the
``transaction_ledger`` entries, decide whether the pattern fired.  Returns a
plain dataclass; the renderer applies the verdict.

Three conjunctive rules (all must hold):

1. The two most-recent snapshots are **byte-identical** on
   ``wallet_balances_json`` + ``positions_json`` + ``token_prices_json``,
   compared after canonicalisation (``sort_keys=True``) so insertion order
   doesn't gate the equality.
2. The later snapshot's ``cycle_id`` starts with ``teardown-`` — i.e. the
   second of the pair is the post-teardown bracket snapshot.
3. At least one ``LedgerEntry`` with ``intent_type == "SWAP"`` and
   ``success is True`` executed between the two snapshot timestamps.

If all three hold, on-chain state moved (a SWAP succeeded) but the recorded
snapshot did not — exactly the cache-staleness fingerprint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def _naive_utc(dt: datetime) -> datetime:
    """Normalise ``dt`` to naive UTC for tz-safe comparisons.

    Gemini audit (VIB-4907): the framework writes tz-aware UTC datetimes
    everywhere (``datetime.now(UTC)``), but SQLite roundtrip on a custom
    deserialiser COULD return naive datetimes — and Python raises
    ``TypeError`` on aware-vs-naive ``<=`` / ``>`` comparisons.  Strip the
    tz from aware values; assume UTC for already-naive values.  After
    normalisation every datetime is naive UTC and the comparison is safe.
    """
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


# Snapshot fields that make up the identity-comparison payload.  We
# deliberately do NOT include the derived numeric fields
# (``total_value_usd``, ``available_cash_usd``, ``wallet_total_value_usd``)
# — those can match across two genuinely distinct on-chain states (rare but
# possible with offsetting position moves).  The three structural fields
# below are the **raw inputs** the valuer reads from; identity on them is
# the strongest "no on-chain delta was observed" signal we have.
#
# The loader hands us live ``PortfolioSnapshot`` dataclasses (typed
# ``wallet_balances: list[TokenBalance]``, ``positions: list[PositionValue]``,
# ``token_prices: dict``), NOT the raw ``*_json`` text columns the SQLite
# row carries.  The canonicalisation path below serialises the typed fields
# back to a deterministic JSON shape so callers can compare for byte
# identity without worrying about dict-iteration order.
_IDENTITY_FIELDS: tuple[str, ...] = (
    "wallet_balances",
    "positions",
    "token_prices",
)


@dataclass(frozen=True)
class SwapClassFallbackDetection:
    """Verdict from :func:`detect_stale_post_teardown_snapshot`.

    Attributes:
        suppressed:
            ``True`` when all three rules hold and ``strat pnl`` should
            suppress the headline number.  ``False`` otherwise (including
            the "insufficient data" case — fewer than two snapshots or
            missing ledger).
        reason:
            Plain-English single-line explanation suitable for rendering
            next to ``Headline PnL: unavailable``.  Empty string when
            ``suppressed`` is ``False``.
    """

    suppressed: bool
    reason: str


def _to_jsonable(obj: Any) -> Any:
    """Recursively convert ``obj`` to a JSON-friendly shape.

    The detection compares values pulled from a live ``PortfolioSnapshot``
    dataclass — its fields contain ``Decimal`` instances and nested
    dataclasses (``TokenBalance`` / ``PositionValue``) that ``json.dumps``
    refuses by default.  This helper walks the structure and:

    * Calls ``to_dict()`` on nested dataclasses that expose one (the project
      convention for ``PortfolioSnapshot``-adjacent types).
    * Falls back to ``dataclasses.asdict`` for plain dataclass instances.
    * Stringifies ``Decimal`` so two snapshots producing equal logical
      amounts compare equal regardless of trailing-zero representation.
    """
    from dataclasses import asdict, is_dataclass
    from decimal import Decimal

    if obj is None or isinstance(obj, str | int | float | bool):
        return obj
    if isinstance(obj, Decimal):
        # ``str(Decimal)`` preserves exact representation; ``float()`` would
        # introduce rounding.  Two snapshots that wrote the same Decimal
        # must produce the same string here.
        return str(obj)
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, set | frozenset):
        # Gemini audit (VIB-4907): sets/frozensets are unordered so iteration
        # order varies across Python processes via hash randomisation.  Sort
        # before recursing so the canonical-JSON dump is deterministic.
        # ``key=str`` handles mixed-type elements without comparing them
        # directly (mixed-type comparisons raise on Python 3).
        return [_to_jsonable(v) for v in sorted(obj, key=str)]
    if isinstance(obj, list | tuple):
        return [_to_jsonable(v) for v in obj]
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        try:
            return _to_jsonable(obj.to_dict())
        except Exception:
            pass
    if is_dataclass(obj) and not isinstance(obj, type):
        try:
            return _to_jsonable(asdict(obj))
        except Exception:
            pass
    # Last-resort: stringify.  Better than raising — the comparison just
    # falls back to opaque text identity for anything weird.
    return str(obj)


def _canonical_json(value: Any) -> str:
    """Canonicalise ``value`` to a stable JSON string for byte identity.

    Two snapshots that wrote the same logical state always produce the
    same string here, regardless of dict-iteration order or trailing-zero
    Decimal formatting.

    Returns:
        Canonical JSON string.  ``""`` for ``None`` / empty inputs.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        # Already a string (e.g. a legacy raw-JSON column in a test double).
        # Round-trip through json.loads so insertion-order differences
        # cancel; if it doesn't parse, return it verbatim.
        if value == "":
            return ""
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return value
        return json.dumps(decoded, sort_keys=True, separators=(",", ":"))
    jsonable = _to_jsonable(value)
    try:
        return json.dumps(jsonable, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(value)


def _snapshot_attr(snapshot: Any, name: str) -> Any:
    """Read a ``PortfolioSnapshot`` attribute defensively.

    The detection takes ``Any`` rather than the concrete dataclass so the
    helper stays trivially mockable in tests and resilient to future
    additions to the dataclass.  Returns ``None`` if the attribute is
    absent.
    """
    return getattr(snapshot, name, None)


def _has_intervening_successful_swap(
    ledger_entries: list[Any],
    prev_ts: datetime,
    latest_ts: datetime,
) -> bool:
    """Return ``True`` iff some successful SWAP ran between the two timestamps.

    "Successful" is strict identity: ``entry.success is True``.  A truthy
    non-bool (e.g. legacy string ``"1"``) is rejected so we never silently
    upgrade a malformed row into a suppression signal.  Empty≠Zero
    discipline at the read site.
    """
    for entry in ledger_entries:
        if _snapshot_attr(entry, "success") is not True:
            continue
        intent_type = _snapshot_attr(entry, "intent_type") or ""
        if intent_type.upper() != "SWAP":
            continue
        entry_ts = _snapshot_attr(entry, "timestamp")
        if not isinstance(entry_ts, datetime):
            continue
        # Gemini audit — tz-safe comparison.  Both bounds were already
        # normalised in the caller (``detect_stale_post_teardown_snapshot``);
        # normalise the per-entry timestamp here so a malformed-from-SQLite
        # naive value can never trip a ``TypeError`` against aware bounds.
        if _naive_utc(prev_ts) <= _naive_utc(entry_ts) <= _naive_utc(latest_ts):
            return True
    return False


def detect_stale_post_teardown_snapshot(
    recent_snapshots: list[Any],
    ledger_entries: list[Any],
) -> SwapClassFallbackDetection:
    """Apply the three-rule SWAP-class fallback detection.

    Args:
        recent_snapshots:
            Latest portfolio snapshots **oldest-first** within the recent
            window.  The detection compares the last two entries; if there
            are fewer than two, the verdict is ``False`` (no signal,
            not a positive detection).
        ledger_entries:
            ``LedgerEntry`` rows for the same deployment.  Order is not
            required — the helper filters and timestamp-compares.

    Returns:
        :class:`SwapClassFallbackDetection`.
    """
    if len(recent_snapshots) < 2:
        return SwapClassFallbackDetection(False, "")

    prev_snap = recent_snapshots[-2]
    latest_snap = recent_snapshots[-1]

    # Rule 2 first — cheap and most-discriminating.
    cycle_id = _snapshot_attr(latest_snap, "cycle_id") or ""
    if not cycle_id.startswith("teardown-"):
        return SwapClassFallbackDetection(False, "")

    # Rule 1 — byte-equal on the three identity fields.  We read the typed
    # attributes (``wallet_balances``, ``positions``, ``token_prices``) and
    # canonicalise; legacy ``*_json`` text columns are also accepted as a
    # fallback for callers that materialise the SQLite row directly.
    for typed_field in _IDENTITY_FIELDS:
        prev_val_raw = _snapshot_attr(prev_snap, typed_field)
        latest_val_raw = _snapshot_attr(latest_snap, typed_field)
        # Legacy fallback: ``getattr(snapshot, "wallet_balances_json", ...)``
        # — accepted so test doubles or raw SQLite rows still work.
        if prev_val_raw is None and latest_val_raw is None:
            prev_val_raw = _snapshot_attr(prev_snap, f"{typed_field}_json")
            latest_val_raw = _snapshot_attr(latest_snap, f"{typed_field}_json")
        prev_val = _canonical_json(prev_val_raw)
        latest_val = _canonical_json(latest_val_raw)
        if prev_val != latest_val:
            return SwapClassFallbackDetection(False, "")

    # Rule 3 — successful SWAP between the two timestamps.
    prev_ts = _snapshot_attr(prev_snap, "timestamp")
    latest_ts = _snapshot_attr(latest_snap, "timestamp")
    if not isinstance(prev_ts, datetime) or not isinstance(latest_ts, datetime):
        return SwapClassFallbackDetection(False, "")
    # Gemini audit — tz-safe ordering check.  Strip tz so an aware-vs-naive
    # pair (possible if a future SQLite deserialiser flips one side) can't
    # raise ``TypeError`` mid-detection.
    if _naive_utc(prev_ts) > _naive_utc(latest_ts):
        # Caller passed wrong ordering; refuse to fire rather than guess.
        return SwapClassFallbackDetection(False, "")
    if not _has_intervening_successful_swap(ledger_entries, prev_ts, latest_ts):
        return SwapClassFallbackDetection(False, "")

    reason = (
        "post-teardown snapshot is byte-identical to the pre-teardown snapshot, "
        "but a successful SWAP ran between them — the MarketSnapshot cache "
        "was not invalidated before the post-teardown bracket (VIB-4906) and "
        "the headline number would compare stale state against itself (VIB-4907)."
    )
    return SwapClassFallbackDetection(True, reason)
