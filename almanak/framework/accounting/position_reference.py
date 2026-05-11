"""Position-reference shape on ``accounting_events`` (VIB-4196 / T10).

Every ``accounting_events`` OPEN/CLOSE row carries a ``position_reference``
JSON pointer that lets a future auditor join the accounting event to a
``position_registry`` row by ``physical_identity_hash`` — without rebasing
the accounting schema when registry mode flips on per primitive.

The PRD (`docs/internal/prds/multi-position-tracking.md` §"The
``position_reference`` shape (forward-compat from day 1)") and blueprint 28
§3 ratify the shape:

.. code-block:: json

    {
      "source": "receipt" | "registry" | "legacy",
      "primitive": "lp",                       # canonical Primitive value
      "accounting_category": "lp",             # canonical AccountingCategory value
      "physical_identity_hash": "0x…" | null,  # null on Day-1 legacy
      "semantic_grouping_key": "0xpool…" | null,
      "registry_handle": "leg_a" | null,
      "grouping_policy_version": "univ3_lp@v1" | null,
      "matching_policy_version": 3 | null,
    }

Day-1 source semantics
----------------------

T10 lands ``source="legacy"`` for ALL primitives. Cutover tickets — T12
(UniV3 LP), T16 (Aave looping), T23 (GMX V2 perp), T28 (Pendle LP) — flip
their primitive's writes to ``source="receipt"`` (or ``source="registry"``
once registry mode is on for that primitive), gated by the per-primitive
parser audit (T02). T10 does NOT make any "this primitive is parser-clean"
claim; it is a structural placeholder so the cutover PRs are byte-additive
on the writer-side and not a schema rebase.

Why the helper takes a ``PrimitiveRecord`` (not raw strings)
-------------------------------------------------------------

The canonical taxonomy is the only allowed source of ``primitive`` /
``accounting_category`` strings (see `blueprints/28-position-registry.md`
§Forbidden patterns #11). Constructing the helper input from a
``PrimitiveRecord`` enforces that at the type level — callers that
fabricate strings cannot satisfy the function signature without going
through ``record_for(intent_type)`` first. The output is a frozen
dataclass; every field is locked at construction time and there is no
in-place mutation surface.

Augmentation chokepoint
-----------------------

The single permitted construction site is
:func:`almanak.framework.accounting.writer.augment_accounting_payload`.
Connectors, category handlers, and the runner cannot call
:func:`build_legacy_position_reference` directly — the AST guard at
``tests/unit/accounting/test_position_reference_no_writers.py`` enforces
that statically.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal

from almanak.framework.primitives.types import EventKind

if TYPE_CHECKING:
    from almanak.framework.primitives.types import PrimitiveRecord


# The three documented `source` values. Pinned as a tuple-literal so a typo
# in a future cutover (e.g. ``"recipt"``) fails at construction time.
POSITION_REFERENCE_SOURCES: tuple[Literal["receipt", "legacy", "registry"], ...] = (
    "receipt",
    "legacy",
    "registry",
)

# The two `event_kind` values that mandate a `position_reference`. ADJUST,
# COLLECT, TRANSFER, and NONE rows are explicitly NOT carriers — the
# accounting column stays NULL for them and the helper raises if asked to
# construct one.
_POSITION_BEARING_EVENT_KINDS: frozenset[EventKind] = frozenset({EventKind.OPEN, EventKind.CLOSE})


@dataclass(frozen=True)
class PositionReference:
    """Frozen ``position_reference`` payload for an ``accounting_events`` OPEN/CLOSE row.

    Every field is locked at construction time. The dataclass is consumed
    only by the accounting writer chokepoint; it is NEVER mutated after
    construction. The :meth:`to_dict` method emits a JSON-serializable dict
    whose key ordering is stable (sorted on serialization at the writer)
    so two equal references serialize byte-identical.

    Attributes
    ----------
    source
        One of ``"receipt"``, ``"legacy"``, or ``"registry"`` per the
        forward-compat spec. Day-1 implementations stamp ``"legacy"`` for
        all primitives; cutover PRs (T12+) flip per primitive.
    primitive
        Canonical :class:`almanak.framework.primitives.types.Primitive`
        StrEnum value (e.g. ``"lp"``, ``"perp"``). The constructor only
        accepts strings produced by the canonical taxonomy via
        :func:`almanak.framework.primitives.taxonomy.record_for` — see
        :func:`build_legacy_position_reference`.
    accounting_category
        Canonical
        :class:`almanak.framework.primitives.types.AccountingCategory`
        StrEnum value (e.g. ``"lp"``, ``"pendle_lp"``, ``"perp"``). Same
        provenance rule as ``primitive``.
    physical_identity_hash
        Receipt-derived stable identity. ``None`` on Day-1 legacy rows
        (no parser audit yet); cutover PRs populate it. Per
        ``CLAUDE.md`` "Empty ≠ zero": ``None`` means "unmeasured", NEVER
        the empty string.
    semantic_grouping_key
        Auto-mode collision predicate. UniV3: ``chain:pool_address``;
        Pendle: ``chain:market_addr:expiry_ts``. ``None`` on Day-1.
    registry_handle
        Optional author-supplied alias when registry mode is on for the
        primitive (e.g. ``"leg_a"`` on a delta-neutral LP). ``None`` until
        T12+ wires the registry write.
    grouping_policy_version
        Versioned grouping rule (e.g. ``"univ3_lp@v1"``). ``None`` until
        registry adoption per primitive.
    matching_policy_version
        Per-primitive lot-matching policy version sourced from
        :func:`almanak.framework.accounting.policy.MatchingPolicy.for_primitive`.
        Stamped at registry adoption (cutover PRs); ``None`` on Day-1.
    """

    source: Literal["receipt", "legacy", "registry"]
    primitive: str
    accounting_category: str
    physical_identity_hash: str | None
    semantic_grouping_key: str | None
    registry_handle: str | None
    grouping_policy_version: str | None
    matching_policy_version: int | None

    def __post_init__(self) -> None:
        if self.source not in POSITION_REFERENCE_SOURCES:
            raise ValueError(
                f"PositionReference.source must be one of {POSITION_REFERENCE_SOURCES!r}, got {self.source!r}"
            )
        if not isinstance(self.primitive, str) or not self.primitive:
            raise ValueError(f"PositionReference.primitive must be a non-empty string, got {self.primitive!r}")
        if not isinstance(self.accounting_category, str) or not self.accounting_category:
            raise ValueError(
                f"PositionReference.accounting_category must be a non-empty string, got {self.accounting_category!r}"
            )
        # Empty ≠ zero. None is the only permitted "unmeasured" sentinel.
        # An empty / whitespace-only physical_identity_hash is a parser bug
        # masquerading as a value; reject it loudly so a cutover PR cannot
        # silently land "" rows in production.
        for name, value in (
            ("physical_identity_hash", self.physical_identity_hash),
            ("semantic_grouping_key", self.semantic_grouping_key),
            ("registry_handle", self.registry_handle),
            ("grouping_policy_version", self.grouping_policy_version),
        ):
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"PositionReference.{name} must be None or a non-empty, "
                    f"non-whitespace string; got {value!r}. Per CLAUDE.md "
                    f"'Empty ≠ zero', use None for unmeasured fields."
                )
        if self.matching_policy_version is not None and not isinstance(self.matching_policy_version, int):
            raise ValueError(
                f"PositionReference.matching_policy_version must be None or an int, "
                f"got {self.matching_policy_version!r}"
            )

    def to_dict(self) -> dict[str, str | int | None]:
        """Return the JSON-serializable dict form.

        Field ordering follows the dataclass declaration. The writer
        chokepoint serializes via :func:`json.dumps` with ``sort_keys=True``
        so two equal references produce byte-identical persistence.
        """
        return asdict(self)


def build_registry_position_reference(
    record: PrimitiveRecord,
    *,
    registry_row: dict,
) -> PositionReference:
    """Construct a ``source="registry"`` reference from a ``position_registry`` row.

    Used by the augment chokepoint (VIB-4278) when a registry lookup finds the
    row corresponding to an OPEN/CLOSE event. The reference fields are pulled
    directly from the registry row (the registry is the source of truth at
    write time):

    * ``physical_identity_hash`` ← ``registry_row["physical_identity_hash"]``
    * ``semantic_grouping_key``  ← ``registry_row["semantic_grouping_key"]``
    * ``registry_handle``        ← ``registry_row["handle"]`` (may be ``None``)
    * ``grouping_policy_version``← ``registry_row["grouping_policy_version"]``
    * ``matching_policy_version``← ``registry_row["matching_policy_version"]``

    The ``primitive`` / ``accounting_category`` fields stay sourced from the
    ``PrimitiveRecord`` so they're guaranteed to use canonical enum values
    (matches the legacy helper's contract).

    Per CLAUDE.md "Empty ≠ Zero", the registry row's ``physical_identity_hash``
    MUST be a non-empty, non-whitespace string. An empty value here is a
    parser bug masquerading as a value; the dataclass ``__post_init__``
    raises ``ValueError`` and the caller (augment chokepoint) decides
    fail-loud vs fall-through per its mode-aware contract.

    Parameters
    ----------
    record
        Canonical taxonomy row obtained via ``record_for(event_type)``.
    registry_row
        A ``position_registry`` row dict with at least the five identity
        fields named above. The full schema is documented in
        :file:`blueprints/28-position-registry.md` §3.

    Returns
    -------
    PositionReference
        Frozen reference with ``source="registry"`` and identity fields
        populated from the row. ``registry_handle`` may be ``None`` when the
        row was written in auto-mode without an explicit handle.

    Raises
    ------
    ValueError
        ``record.event_kind`` is not OPEN or CLOSE, OR the registry row's
        identity fields fail the ``Empty ≠ zero`` shape check (empty string,
        whitespace-only string, non-string non-None value), OR
        ``matching_policy_version`` is present but is not an integer.
    """
    if record.event_kind not in _POSITION_BEARING_EVENT_KINDS:
        raise ValueError(
            f"build_registry_position_reference: event_kind "
            f"{record.event_kind!r} (intent_type={record.intent_type!r}) is "
            f"not OPEN or CLOSE; only OPEN/CLOSE rows carry a "
            f"position_reference per blueprint 28 §3."
        )
    # Fail loud on missing / empty required registry-identity fields. The
    # registry schema declares ``physical_identity_hash``,
    # ``semantic_grouping_key``, and ``grouping_policy_version`` as
    # ``TEXT NOT NULL``; a row reaching this helper with any of them
    # missing / empty / whitespace-only / non-string is a corruption
    # signal — emitting ``source="registry"`` with a ``None`` /
    # ``""`` hash would violate the L5_22 invariant (the row must carry
    # a non-null join key) and surface as broken accounting later. Per
    # CLAUDE.md "Empty ≠ Zero", an empty value is a parser bug, not a
    # value. The PositionReference ``__post_init__`` already enforces
    # this for non-None strings, but it would let ``None`` through (None
    # is legal on legacy rows); we tighten it here for the registry
    # construction path specifically. CodeRabbit PR #2236 round 2.
    required_registry_fields = {
        "physical_identity_hash": registry_row.get("physical_identity_hash"),
        "semantic_grouping_key": registry_row.get("semantic_grouping_key"),
        "grouping_policy_version": registry_row.get("grouping_policy_version"),
    }
    for field_name, value in required_registry_fields.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"build_registry_position_reference: registry row missing "
                f"required identity field {field_name!r} — got {value!r}. "
                f"Per CLAUDE.md 'Empty ≠ Zero', None / empty string / "
                f"non-string here is a registry-write bug; refusing to "
                f"emit source='registry' with a null join key."
            )
    raw_matching_version = registry_row.get("matching_policy_version")
    matching_policy_version: int | None
    if raw_matching_version is None:
        matching_policy_version = None
    elif isinstance(raw_matching_version, int) and not isinstance(raw_matching_version, bool):
        matching_policy_version = raw_matching_version
    else:
        # Reject bool (int subclass) and str / float / other — the
        # registry schema column is INTEGER NOT NULL, so a non-int here
        # is a corruption signal. ``type_name`` calls out the bool case
        # explicitly because ``isinstance(True, int)`` would otherwise
        # quietly pass the int branch above; surfacing the type in the
        # error is what makes the silent-bool footgun visible to the
        # operator.
        type_name = "bool" if isinstance(raw_matching_version, bool) else type(raw_matching_version).__name__
        raise ValueError(
            f"build_registry_position_reference: matching_policy_version "
            f"must be None or an int, got {type_name}: {raw_matching_version!r}"
        )
    return PositionReference(
        source="registry",
        primitive=record.primitive.value,
        accounting_category=record.accounting_category.value,
        physical_identity_hash=required_registry_fields["physical_identity_hash"],
        semantic_grouping_key=required_registry_fields["semantic_grouping_key"],
        registry_handle=registry_row.get("handle"),
        grouping_policy_version=required_registry_fields["grouping_policy_version"],
        matching_policy_version=matching_policy_version,
    )


def build_legacy_position_reference(record: PrimitiveRecord) -> PositionReference:
    """Construct a Day-1 ``source="legacy"`` reference from a canonical taxonomy row.

    Forces callers through the canonical taxonomy: the input is a
    ``PrimitiveRecord`` (returned by
    :func:`almanak.framework.primitives.taxonomy.record_for`), so a caller
    cannot fabricate a ``primitive`` / ``accounting_category`` string
    without first looking up the canonical row.

    Only OPEN/CLOSE rows carry a ``position_reference`` per blueprint 28
    §3. ADJUST, COLLECT, TRANSFER, and NONE rows do NOT — calling this
    helper for any of those raises :class:`ValueError` so a future caller
    cannot silently emit a position pointer on a row that doesn't have a
    position lifecycle.

    Parameters
    ----------
    record
        Canonical taxonomy row, typically obtained via
        ``record_for(event_type)``.

    Returns
    -------
    PositionReference
        Frozen reference with ``source="legacy"``, canonical primitive +
        accounting_category strings, and every post-registry field set to
        ``None``. Cutover PRs (T12+) construct a different shape with
        ``source="receipt"`` / ``"registry"`` and populated identity
        fields.

    Raises
    ------
    ValueError
        ``record.event_kind`` is not OPEN or CLOSE.
    """
    if record.event_kind not in _POSITION_BEARING_EVENT_KINDS:
        raise ValueError(
            f"build_legacy_position_reference: event_kind "
            f"{record.event_kind!r} (intent_type={record.intent_type!r}) is "
            f"not OPEN or CLOSE; only OPEN/CLOSE rows carry a "
            f"position_reference per blueprint 28 §3."
        )
    return PositionReference(
        source="legacy",
        primitive=record.primitive.value,
        accounting_category=record.accounting_category.value,
        physical_identity_hash=None,
        semantic_grouping_key=None,
        registry_handle=None,
        grouping_policy_version=None,
        matching_policy_version=None,
    )


__all__ = [
    "POSITION_REFERENCE_SOURCES",
    "PositionReference",
    "build_legacy_position_reference",
    "build_registry_position_reference",
]
