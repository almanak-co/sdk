"""MeasuredMoney — a money value that enforces the Empty≠Zero invariant *by construction*.

VIB-5205 (US-004), foundation of Workstream 1 (the accounting invariant / contract
layer). This is the typed root that later stories (US-005..008) build on.

The canonical accounting discipline (`docs/internal/blueprints/27-accounting.md`
§10.10 "The empty ≠ zero discipline") distinguishes three states that must NEVER be
substituted for one another:

| Raw value     | Meaning                                                |
|---------------|--------------------------------------------------------|
| ``Decimal("0")`` | **measured zero** — a value was looked up and it is zero |
| ``None``         | **unmeasured** — a value was not / could not be measured |
| ``""``           | **absent** — the parser / source did not emit the field  |

Historically these three live in loosely-typed ``Decimal | None`` fields plus bare
``str`` fields, and the discipline is enforced only by convention and a downstream
persistence-invariant test. Every accidental ``None`` → ``Decimal("0")`` or
``""`` → ``Decimal("0")`` coercion silently corrupts the books.

``MeasuredMoney`` makes the three states a single closed type so the invariant is
guaranteed at the point of construction rather than re-checked everywhere:

* It is **impossible** to build a measured value out of ``None`` or ``""`` — the
  state and the payload are validated together in ``__post_init__``.
* ``from_raw`` is the one *total* mapping from the legacy raw forms; it maps
  ``Decimal`` → measured, ``None`` → unmeasured, ``""`` → absent, and NEVER collapses
  ``""``/``None`` into ``Decimal("0")``.
* Arithmetic *propagates* unmeasured / absent: any non-measured operand makes the
  result non-measured, so a missing input can never masquerade as a measured zero in
  a sum.

Pure value type — no gateway calls, no I/O. Frozen + hashable, mirroring the immutable
style of the typed event models in this package (``models.py``).

Absent-vs-unmeasured arithmetic rule
------------------------------------
The three states form an information lattice, ordered by how much we know:

    MEASURED  (most information — a real number)
        <  UNMEASURED  (we know it was not measured)
        <  ABSENT      (we don't even have the field)

Combining two values takes the **join** — the *least*-informative state of the two
operands. So ``measured + measured = measured`` (sum), ``measured + unmeasured =
unmeasured``, ``unmeasured + absent = absent``, ``measured + absent = absent``.

Rationale: ``absent`` is a strictly stronger "no data" signal than ``unmeasured``
(the field was never emitted vs. emitted-but-null), so it dominates — propagating the
most diagnostic state upward is the conservative choice for an aggregate. We do NOT
collapse ``absent`` → ``unmeasured`` because that would discard the distinction this
type exists to preserve.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import IntEnum
from typing import Final


class MeasuredState(IntEnum):
    """The three mutually-exclusive states of a money value.

    ``IntEnum`` so the values are ordered by information content
    (MEASURED < UNMEASURED < ABSENT). Arithmetic combines two values by taking the
    ``max`` state — the least-informative (most-unknown) of the operands.
    """

    MEASURED = 0
    UNMEASURED = 1
    ABSENT = 2


class UnmeasuredValueError(ValueError):
    """Raised when the measured ``Decimal`` is read from a non-measured value."""


@dataclass(frozen=True, slots=True)
class MeasuredMoney:
    """An immutable money value carrying one of three Empty≠Zero states.

    Do not construct via the raw ``MeasuredMoney(state, amount)`` form in normal
    code — use the explicit, intention-revealing classmethods
    (:meth:`measured`, :meth:`unmeasured`, :meth:`absent`, :meth:`from_raw`).
    The raw constructor still validates its invariants so an inconsistent
    ``(state, amount)`` pair is impossible to build (it raises), but the
    classmethods document intent and are the supported surface.
    """

    state: MeasuredState
    amount: Decimal | None

    def __post_init__(self) -> None:
        # The (state, amount) pair is validated together so the three states can
        # never be conflated, and a non-measured value can never secretly carry a
        # number (which is exactly how "" / None silently become Decimal("0")).
        if self.state is MeasuredState.MEASURED:
            if self.amount is None:
                raise ValueError(
                    "MeasuredMoney.MEASURED requires a Decimal amount, got None. "
                    "Use MeasuredMoney.unmeasured() for an unmeasured value."
                )
            if not isinstance(self.amount, Decimal):
                raise TypeError(
                    "MeasuredMoney amount must be a Decimal (never float / int / str), "
                    f"got {type(self.amount).__name__}."
                )
            if not self.amount.is_finite():
                raise ValueError(f"MeasuredMoney amount must be finite, got {self.amount!r}.")
        else:
            # UNMEASURED and ABSENT carry no number — enforce it so a sentinel
            # Decimal can never hide behind a non-measured state.
            if self.amount is not None:
                raise ValueError(f"MeasuredMoney.{self.state.name} must not carry an amount, got {self.amount!r}.")

    # -- explicit constructors -------------------------------------------------

    @classmethod
    def measured(cls, amount: Decimal) -> MeasuredMoney:
        """A real measured value, INCLUDING ``Decimal("0")`` (measured zero is a value).

        Rejects ``None`` and non-``Decimal`` inputs (``""``, ``str``, ``float``,
        ``int``) so a measured value can only ever be built from a true ``Decimal``.
        """
        if amount is None:
            raise ValueError("MeasuredMoney.measured() requires a Decimal, got None. Use MeasuredMoney.unmeasured().")
        if not isinstance(amount, Decimal):
            raise TypeError(
                "MeasuredMoney.measured() requires a Decimal (never float / int / str), "
                f"got {type(amount).__name__}. Empty≠Zero: parse to Decimal explicitly."
            )
        return cls(MeasuredState.MEASURED, amount)

    @classmethod
    def unmeasured(cls) -> MeasuredMoney:
        """A value that was not / could not be measured (the ``None`` equivalent)."""
        return cls(MeasuredState.UNMEASURED, None)

    @classmethod
    def absent(cls) -> MeasuredMoney:
        """The source did not emit this field at all (the ``""`` equivalent)."""
        return cls(MeasuredState.ABSENT, None)

    @classmethod
    def from_raw(cls, value: Decimal | str | None) -> MeasuredMoney:
        """Total mapping from a legacy raw money form to a :class:`MeasuredMoney`.

        The one bridge from loosely-typed accounting fields. Mapping:

        * ``Decimal``                → measured (``Decimal("0")`` → measured zero)
        * ``None``                   → unmeasured
        * ``""`` / whitespace-only   → absent
        * other non-empty ``str``    → parsed to ``Decimal`` → measured

        It is total over the documented input domain and NEVER coerces ``""`` or
        ``None`` into ``Decimal("0")``. Unsupported types (``float``, ``int``,
        ``bool``, …) raise ``TypeError`` — money must be ``Decimal``-typed, so an
        ambiguous numeric input is rejected rather than silently accepted.
        """
        if value is None:
            return cls.unmeasured()
        if isinstance(value, Decimal):
            return cls.measured(value)
        if isinstance(value, str):
            # "" — and, defensively, whitespace-only — is the canonical "parser did
            # not emit" marker. It maps to absent, NEVER to measured zero.
            if value.strip() == "":
                return cls.absent()
            try:
                return cls.measured(Decimal(value))
            except Exception as exc:  # noqa: BLE001 - re-raise as a typed, total error
                raise ValueError(
                    f"MeasuredMoney.from_raw() could not parse non-empty string {value!r} as a Decimal."
                ) from exc
        raise TypeError(
            "MeasuredMoney.from_raw() accepts Decimal | str | None only "
            f"(money must be Decimal-typed), got {type(value).__name__}."
        )

    # -- state predicates ------------------------------------------------------

    @property
    def is_measured(self) -> bool:
        """True iff this carries a real measured value (including measured zero)."""
        return self.state is MeasuredState.MEASURED

    @property
    def is_unmeasured(self) -> bool:
        """True iff this value was not / could not be measured."""
        return self.state is MeasuredState.UNMEASURED

    @property
    def is_absent(self) -> bool:
        """True iff the source did not emit this field at all."""
        return self.state is MeasuredState.ABSENT

    # -- value access ----------------------------------------------------------

    @property
    def value(self) -> Decimal:
        """The measured ``Decimal``; raises :class:`UnmeasuredValueError` otherwise.

        Forces the caller to handle the non-measured states explicitly instead of
        reading a silent ``Decimal("0")`` sentinel.
        """
        if self.state is not MeasuredState.MEASURED:
            raise UnmeasuredValueError(
                f"Cannot read .value of a {self.state.name} MeasuredMoney. "
                "Check .is_measured or use .value_or(default)."
            )
        # __post_init__ guarantees a finite Decimal for the MEASURED state.
        assert self.amount is not None
        return self.amount

    def value_or(self, default: Decimal) -> Decimal:
        """The measured ``Decimal`` if measured, else *default* (no raise)."""
        # `amount is not None` ⟺ MEASURED (enforced by __post_init__); checking the
        # amount directly lets the type checker narrow `Decimal | None` to `Decimal`.
        return self.amount if self.amount is not None else default

    # -- arithmetic (propagates non-measured states) ---------------------------

    def __add__(self, other: MeasuredMoney) -> MeasuredMoney:
        if not isinstance(other, MeasuredMoney):
            return NotImplemented
        result_state = MeasuredState(max(self.state, other.state))
        if result_state is MeasuredState.MEASURED:
            # Both operands are measured here, so both amounts are non-None.
            assert self.amount is not None and other.amount is not None
            return MeasuredMoney.measured(self.amount + other.amount)
        # Any non-measured operand wins: propagate the least-informative state.
        return MeasuredMoney(result_state, None)

    def __neg__(self) -> MeasuredMoney:
        if self.state is MeasuredState.MEASURED:
            assert self.amount is not None
            return MeasuredMoney.measured(-self.amount)
        return self

    def __sub__(self, other: MeasuredMoney) -> MeasuredMoney:
        if not isinstance(other, MeasuredMoney):
            return NotImplemented
        return self + (-other)

    # -- payload boundary codec (VIB-5213) -------------------------------------

    def to_payload(self) -> str | None:
        """Serialize to the legacy persisted accounting-payload money form.

        The inverse of :meth:`from_payload` / :meth:`from_raw` and the write
        half of the accounting-payload boundary codec (VIB-5213 / US-007). It
        maps the three states onto the EXISTING on-the-wire column semantics so
        persistence stays byte-compatible — the richer type is in-memory only:

        * measured   → ``str(amount)`` (``Decimal("0")`` → ``"0"``, a value)
        * unmeasured → ``None``  (JSON ``null`` — "not measured")
        * absent     → ``""``    (the parser-didn't-emit marker)

        It NEVER fabricates a ``"0"`` for a non-measured value: an unmeasured
        value stays ``None`` and an absent value stays ``""``, so the Empty≠Zero
        distinction survives the round-trip into a typed accounting event.
        """
        if self.state is MeasuredState.MEASURED:
            # __post_init__ guarantees a finite Decimal for the MEASURED state.
            assert self.amount is not None
            return str(self.amount)
        if self.state is MeasuredState.ABSENT:
            return ""
        return None

    @classmethod
    def from_payload(cls, raw: int | float | str | Decimal | None) -> MeasuredMoney:
        """Deserialize a persisted accounting-payload money value (VIB-5213).

        The inverse of :meth:`to_payload` and the read half of the
        accounting-payload boundary codec. Money is canonically ``Decimal``-/
        string-typed on the wire (never a JSON number — see blueprint 27 "All
        monetary fields use Decimal or string, never float"), so the canonical
        persisted input domain is ``str | None``:

        * non-empty ``str`` → measured (``"0"`` → measured zero)
        * ``""``            → absent
        * ``None``          → unmeasured

        Delegates to :meth:`from_raw`, inheriting its total, never-``""``→0
        contract. ``from_raw`` also accepts a ``Decimal`` so a value that was
        not JSON-decoded round-trips unchanged.

        This is the *read* boundary, so it is deliberately more liberal than
        :meth:`from_raw`: it also accepts legacy payloads that stored money as a
        raw JSON number (``int`` / ``float``) and converts via ``Decimal(str(x))``
        (avoiding float binary-repr artifacts) before measuring. The historical
        decode was ``Decimal(v)``, which accepted ``int`` / ``float``; rejecting
        them here would crash on reads of such legacy rows. ``from_raw``'s
        stricter in-memory contract (which rejects ``int`` / ``float`` / ``bool``)
        is preserved for construction sites; ``bool`` still falls through to
        ``from_raw`` and raises its typed ``TypeError``.
        """
        if isinstance(raw, bool):
            # bool is an int subclass but is never a money value — let from_raw
            # raise its typed TypeError rather than coercing True/False.
            return cls.from_raw(raw)  # type: ignore[arg-type]
        if isinstance(raw, int | float):
            return cls.from_raw(Decimal(str(raw)))
        return cls.from_raw(raw)

    # -- representation --------------------------------------------------------

    def __repr__(self) -> str:
        if self.state is MeasuredState.MEASURED:
            return f"MeasuredMoney.measured({self.amount!r})"
        return f"MeasuredMoney.{self.state.name.lower()}()"


# Alias: the type is conceptually a "measured Decimal". Both names refer to the
# same class so call sites can use whichever reads better in context.
MeasuredDecimal: Final = MeasuredMoney


def encode_money_payload(value: Decimal | None) -> str | None:
    """Serialize a legacy ``Decimal | None`` accounting money field to the
    persisted wire form *through* :class:`MeasuredMoney` (VIB-5213 / US-007).

    The accounting-payload boundary codec, write direction. The typed event
    models keep their ``Decimal | None`` field type (so no construction site
    changes), but every money field crosses the serialization seam as a
    :class:`MeasuredMoney`: a ``Decimal`` (incl. ``Decimal("0")``) becomes a
    measured value → ``str``; ``None`` becomes unmeasured → ``None``.

    Byte-identical to the historical ``str(v) if isinstance(v, Decimal) else
    None`` encoding for every finite Decimal, while routing the value through
    the Empty≠Zero type so a non-finite (NaN/Inf) money value fails closed at
    construction instead of silently persisting ``"NaN"`` into the books (the
    event builders already guarantee finiteness, so production write-bytes are
    unchanged).
    """
    return MeasuredMoney.from_raw(value).to_payload()


def decode_money_payload(raw: int | float | str | Decimal | None) -> Decimal | None:
    """Deserialize a persisted accounting money value back to the legacy
    ``Decimal | None`` field *through* :class:`MeasuredMoney` (VIB-5213).

    The accounting-payload boundary codec, read direction. A non-empty ``str``
    → measured ``Decimal``; ``None`` → unmeasured → ``None``; ``""``
    (parser-absent) → ``None``. The legacy ``Decimal | None`` field has no
    distinct "absent" slot, so absent collapses to ``None`` at the field — but
    it is NEVER coerced to ``Decimal("0")``, preserving Empty≠Zero. The full
    three-state distinction is preserved by :meth:`MeasuredMoney.from_payload`
    itself (proven by the boundary round-trip tests); this helper is the thin
    adapter onto the two-state legacy field.

    Byte-compatible with the historical ``Decimal(v) if v is not None else
    None`` decode for string inputs, and additionally robust to a stray ``""``
    (which the old decode crashed on with ``InvalidOperation``) and to legacy
    rows that stored money as a raw JSON number (``int`` / ``float``) — see
    :meth:`MeasuredMoney.from_payload`.
    """
    mm = MeasuredMoney.from_payload(raw)
    return mm.value if mm.is_measured else None


__all__ = [
    "MeasuredDecimal",
    "MeasuredMoney",
    "MeasuredState",
    "UnmeasuredValueError",
    "decode_money_payload",
    "encode_money_payload",
]
