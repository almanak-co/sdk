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

    # -- representation --------------------------------------------------------

    def __repr__(self) -> str:
        if self.state is MeasuredState.MEASURED:
            return f"MeasuredMoney.measured({self.amount!r})"
        return f"MeasuredMoney.{self.state.name.lower()}()"


# Alias: the type is conceptually a "measured Decimal". Both names refer to the
# same class so call sites can use whichever reads better in context.
MeasuredDecimal: Final = MeasuredMoney


__all__ = [
    "MeasuredDecimal",
    "MeasuredMoney",
    "MeasuredState",
    "UnmeasuredValueError",
]
