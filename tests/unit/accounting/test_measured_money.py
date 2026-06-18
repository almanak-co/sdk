"""Tests for MeasuredMoney — the Empty≠Zero-by-construction money type (VIB-5205)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from almanak.framework.accounting.measured import (
    MeasuredDecimal,
    MeasuredMoney,
    MeasuredState,
    UnmeasuredValueError,
)

# -- the three states are distinguishable and never equal ---------------------


def test_measured_zero_unmeasured_absent_all_distinct() -> None:
    """measured(0) ≠ unmeasured ≠ absent — the core Empty≠Zero guarantee."""
    measured_zero = MeasuredMoney.measured(Decimal("0"))
    unmeasured = MeasuredMoney.unmeasured()
    absent = MeasuredMoney.absent()

    assert measured_zero != unmeasured
    assert measured_zero != absent
    assert unmeasured != absent

    assert measured_zero.is_measured
    assert unmeasured.is_unmeasured
    assert absent.is_absent

    # measured zero is a true value, not "no data"
    assert measured_zero.value == Decimal("0")


def test_state_predicates_are_mutually_exclusive() -> None:
    for mm, expected in (
        (MeasuredMoney.measured(Decimal("1.5")), MeasuredState.MEASURED),
        (MeasuredMoney.unmeasured(), MeasuredState.UNMEASURED),
        (MeasuredMoney.absent(), MeasuredState.ABSENT),
    ):
        assert mm.state is expected
        assert [mm.is_measured, mm.is_unmeasured, mm.is_absent].count(True) == 1


def test_measured_zero_distinct_from_measured_nonzero() -> None:
    assert MeasuredMoney.measured(Decimal("0")) != MeasuredMoney.measured(Decimal("1"))


def test_immutable_and_hashable() -> None:
    mm = MeasuredMoney.measured(Decimal("3"))
    with pytest.raises(FrozenInstanceError):
        mm.amount = Decimal("4")  # type: ignore[misc]
    # hashable -> usable in sets/dicts
    assert {mm, MeasuredMoney.unmeasured(), MeasuredMoney.absent()}


# -- from_raw mapping ---------------------------------------------------------


def test_from_raw_empty_string_is_absent_not_measured_zero() -> None:
    mm = MeasuredMoney.from_raw("")
    assert mm.is_absent
    assert not mm.is_measured
    assert mm == MeasuredMoney.absent()
    # critically: NOT measured zero
    assert mm != MeasuredMoney.measured(Decimal("0"))


def test_from_raw_whitespace_only_string_is_absent() -> None:
    assert MeasuredMoney.from_raw("   ").is_absent


def test_from_raw_none_is_unmeasured() -> None:
    mm = MeasuredMoney.from_raw(None)
    assert mm.is_unmeasured
    assert mm == MeasuredMoney.unmeasured()
    assert mm != MeasuredMoney.measured(Decimal("0"))


def test_from_raw_decimal_zero_is_measured_zero() -> None:
    mm = MeasuredMoney.from_raw(Decimal("0"))
    assert mm.is_measured
    assert mm.value == Decimal("0")


def test_from_raw_decimal_nonzero_is_measured() -> None:
    mm = MeasuredMoney.from_raw(Decimal("123.45"))
    assert mm.is_measured
    assert mm.value == Decimal("123.45")


def test_from_raw_nonempty_string_parses_to_measured() -> None:
    mm = MeasuredMoney.from_raw("42.5")
    assert mm.is_measured
    assert mm.value == Decimal("42.5")


def test_from_raw_unparseable_string_raises() -> None:
    with pytest.raises(ValueError):
        MeasuredMoney.from_raw("not-a-number")


def test_from_raw_rejects_float_and_int() -> None:
    # money must be Decimal-typed; ambiguous numerics are rejected, not coerced.
    with pytest.raises(TypeError):
        MeasuredMoney.from_raw(0.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        MeasuredMoney.from_raw(0)  # type: ignore[arg-type]


# -- forbidden "" -> 0 coercion is structurally impossible --------------------


def test_measured_rejects_empty_string() -> None:
    """The classic bug: "" must never become Decimal("0")."""
    with pytest.raises(TypeError):
        MeasuredMoney.measured("")  # type: ignore[arg-type]


def test_measured_rejects_none() -> None:
    with pytest.raises(ValueError):
        MeasuredMoney.measured(None)  # type: ignore[arg-type]


def test_measured_rejects_float_and_int() -> None:
    with pytest.raises(TypeError):
        MeasuredMoney.measured(0.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        MeasuredMoney.measured(5)  # type: ignore[arg-type]


def test_measured_rejects_non_finite() -> None:
    with pytest.raises(ValueError):
        MeasuredMoney.measured(Decimal("NaN"))
    with pytest.raises(ValueError):
        MeasuredMoney.measured(Decimal("Infinity"))


def test_inconsistent_raw_construction_is_impossible() -> None:
    # MEASURED with no amount -> raise
    with pytest.raises(ValueError):
        MeasuredMoney(MeasuredState.MEASURED, None)
    # non-measured carrying an amount (the sentinel-Decimal smuggling bug) -> raise
    with pytest.raises(ValueError):
        MeasuredMoney(MeasuredState.UNMEASURED, Decimal("0"))
    with pytest.raises(ValueError):
        MeasuredMoney(MeasuredState.ABSENT, Decimal("0"))


# -- value access -------------------------------------------------------------


def test_value_raises_on_unmeasured_and_absent() -> None:
    with pytest.raises(UnmeasuredValueError):
        _ = MeasuredMoney.unmeasured().value
    with pytest.raises(UnmeasuredValueError):
        _ = MeasuredMoney.absent().value


def test_value_or_returns_default_for_non_measured() -> None:
    default = Decimal("-1")
    assert MeasuredMoney.unmeasured().value_or(default) == default
    assert MeasuredMoney.absent().value_or(default) == default
    assert MeasuredMoney.measured(Decimal("7")).value_or(default) == Decimal("7")


# -- arithmetic propagation ---------------------------------------------------


def test_add_measured_measured_is_measured_sum() -> None:
    result = MeasuredMoney.measured(Decimal("2.5")) + MeasuredMoney.measured(Decimal("4"))
    assert result.is_measured
    assert result.value == Decimal("6.5")


def test_add_measured_zero_preserves_measured() -> None:
    result = MeasuredMoney.measured(Decimal("3")) + MeasuredMoney.measured(Decimal("0"))
    assert result.is_measured
    assert result.value == Decimal("3")


def test_unmeasured_plus_measured_is_unmeasured() -> None:
    assert (MeasuredMoney.unmeasured() + MeasuredMoney.measured(Decimal("5"))).is_unmeasured
    assert (MeasuredMoney.measured(Decimal("5")) + MeasuredMoney.unmeasured()).is_unmeasured


def test_absent_dominates_unmeasured_and_measured() -> None:
    """absent is the strongest 'no data' — it propagates over unmeasured/measured."""
    assert (MeasuredMoney.absent() + MeasuredMoney.measured(Decimal("5"))).is_absent
    assert (MeasuredMoney.absent() + MeasuredMoney.unmeasured()).is_absent
    assert (MeasuredMoney.unmeasured() + MeasuredMoney.absent()).is_absent


def test_non_measured_results_carry_no_amount() -> None:
    result = MeasuredMoney.unmeasured() + MeasuredMoney.measured(Decimal("5"))
    assert result.amount is None
    with pytest.raises(UnmeasuredValueError):
        _ = result.value


def test_neg_negates_measured_and_preserves_state() -> None:
    assert (-MeasuredMoney.measured(Decimal("3"))).value == Decimal("-3")
    assert (-MeasuredMoney.measured(Decimal("0"))).value == Decimal("0")
    assert (-MeasuredMoney.unmeasured()).is_unmeasured
    assert (-MeasuredMoney.absent()).is_absent


def test_sub_measured_measured_is_difference() -> None:
    result = MeasuredMoney.measured(Decimal("10")) - MeasuredMoney.measured(Decimal("4"))
    assert result.value == Decimal("6")


def test_sub_propagates_non_measured() -> None:
    assert (MeasuredMoney.measured(Decimal("10")) - MeasuredMoney.unmeasured()).is_unmeasured
    assert (MeasuredMoney.measured(Decimal("10")) - MeasuredMoney.absent()).is_absent
    assert (MeasuredMoney.unmeasured() - MeasuredMoney.absent()).is_absent


def test_arithmetic_with_non_measuredmoney_is_typeerror() -> None:
    with pytest.raises(TypeError):
        _ = MeasuredMoney.measured(Decimal("1")) + Decimal("2")  # type: ignore[operator]


# -- alias --------------------------------------------------------------------


def test_measured_decimal_alias_is_same_class() -> None:
    assert MeasuredDecimal is MeasuredMoney
    assert MeasuredDecimal.measured(Decimal("1")).is_measured


# -- repr ---------------------------------------------------------------------


def test_repr_round_trips_intent() -> None:
    assert repr(MeasuredMoney.measured(Decimal("1.5"))) == "MeasuredMoney.measured(Decimal('1.5'))"
    assert repr(MeasuredMoney.unmeasured()) == "MeasuredMoney.unmeasured()"
    assert repr(MeasuredMoney.absent()) == "MeasuredMoney.absent()"
