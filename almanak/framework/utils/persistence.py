"""Defensive helpers for round-tripping persisted strategy state.

Strategy ``load_persistent_state`` runs at startup against whatever the
state backend returns. The backend is permissive: it round-trips JSON
that another process / a previous version / a corrupted write may have
left in storage. A single malformed entry under, say, ``position_bin_ids``
must NOT block recovery — but a fundamentally wrong-shaped field (e.g.
the whole field is a stray string) MUST fail loudly so the strategy does
not silently lose track of an open on-chain position.

Contract for ``safe_int_list``:

- ``None`` / missing field → ``[]`` (legitimate "no state yet")
- iterable of mixed-quality entries → keep coercible ints, drop the
  rest with a warning per dropped entry
- str / bytes / dict / non-iterable scalar → raise ``ValueError`` — the
  schema is broken at the field level; the outer ``try/except`` in
  ``load_persistent_state`` should catch this and surface it as a
  recovery failure rather than silently zero-ing a position-tracking
  field.

VIB-3757 introduced this module after CodeRabbit flagged the per-strategy
pattern::

    self._position_bin_ids = [int(b) for b in state.get("position_bin_ids", [])]

which raises ``TypeError`` / ``ValueError`` on the first non-coercible
element and aborts ``load_persistent_state`` entirely.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

logger = logging.getLogger(__name__)


def safe_int_list(values: Any, *, name: str = "") -> list[int]:
    """Coerce a persisted value into a ``list[int]``, discarding bad entries.

    Restoration helper for strategy ``load_persistent_state`` paths that
    previously did ``[int(b) for b in raw or []]``. That pattern aborts
    on the first malformed entry inside an otherwise-valid list, blocking
    strategy recovery on restart even though most of the list is fine.

    Args:
        values: The value the persistence layer returned for an
            "expected list of ints" field. Common shapes: ``list[int]``,
            ``list[str]``, ``None``, missing key, iterable with mixed
            types.
        name: Field name used in warning / error messages so operators
            can pinpoint which restored field was malformed. Defaults
            to a generic message when omitted.

    Returns:
        A ``list[int]`` containing entries that successfully coerce to
        ``int``. Empty list when ``values`` is ``None``.

    Raises:
        ValueError: When ``values`` is fundamentally the wrong shape —
            a string, bytes, mapping, or non-iterable scalar — i.e. the
            persisted schema is corrupt at the field level. Raising here
            (rather than returning ``[]``) is deliberate: silently
            returning an empty list would let a strategy restart
            believing it has no open position when in fact the on-chain
            position is intact and tracking has been lost. Callers
            invoke this from inside ``load_persistent_state`` whose
            outer ``try/except`` is expected to surface the failure.

    Behaviour on partially-malformed inputs:
        - ``None`` → ``[]`` (no log; legitimate "no state yet")
        - iterable with bad entries → entries that coerce are kept;
          each bad entry logs a warning naming the field
        - str / bytes / dict / Mapping / non-iterable scalar → raises
          ``ValueError`` (NOT a silent ``[]``). See VIB-3757 audit:
          dropping the field whole-cloth would mask schema corruption
          and could orphan an open on-chain position.

    Use this for fields where partial recovery is preferable to a hard
    failure on a single bad entry, but where TOTAL field absence /
    corruption should still be a loud failure. Money-critical scalar
    fields (share counts, principal amounts) should fail loudly through
    the type system, not through this helper.
    """
    if values is None:
        return []

    label = f" for {name!r}" if name else ""

    # Whole-field-shape errors: raise rather than silently returning [].
    # str/bytes iterate as characters; dicts iterate as keys; both
    # produce confusing partial results that are NEVER what a persisted
    # "list of bin ids" was supposed to be. Treat as schema corruption.
    if isinstance(values, str | bytes | dict):
        raise ValueError(
            f"safe_int_list{label}: expected an iterable of ints, got "
            f"{type(values).__name__}. This indicates corrupt persisted "
            f"state; refusing to silently return an empty list because "
            f"that would orphan any tracked on-chain position."
        )

    if not isinstance(values, Iterable):
        raise ValueError(
            f"safe_int_list{label}: expected an iterable of ints, got "
            f"non-iterable {type(values).__name__}. This indicates "
            f"corrupt persisted state; refusing to silently return an "
            f"empty list."
        )

    out: list[int] = []
    for raw in values:
        try:
            out.append(int(raw))
        # OverflowError catches float("inf"), float("nan"), and very large
        # floats that overflow when truncated. (TypeError, ValueError) catch
        # the common cases (None, non-numeric strings).
        except (TypeError, ValueError, OverflowError):
            logger.warning(
                "safe_int_list%s: dropping malformed entry %r (%s); continuing recovery on remaining entries.",
                label,
                raw,
                type(raw).__name__,
            )
    return out
