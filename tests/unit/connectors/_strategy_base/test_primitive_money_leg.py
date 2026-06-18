"""Tests for the PrimitiveMoneyLeg extraction contract (VIB-5212, US-008).

Covers construction + validation, MeasuredMoney leg amounts, role/direction
correctness, and the Empty≠Zero state propagation when aggregating legs (seeded
with a measured zero — MeasuredMoney has no ``__radd__`` / ``__mul__``).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.primitive_money_leg import (
    MoneyLegRole,
    PrimitiveMoneyLeg,
    PrimitiveMoneyLegs,
)
from almanak.framework.accounting.measured import MeasuredMoney

# -- construction + classmethods ----------------------------------------------


def test_classmethods_set_the_right_role() -> None:
    """The intention-revealing constructors tag the correct role."""
    amt = MeasuredMoney.measured(Decimal("10"))
    assert PrimitiveMoneyLeg.input("USDC", amt).role is MoneyLegRole.INPUT
    assert PrimitiveMoneyLeg.output("WETH", amt).role is MoneyLegRole.OUTPUT
    assert PrimitiveMoneyLeg.principal("USDC", amt).role is MoneyLegRole.PRINCIPAL


def test_leg_carries_token_identity_and_measured_amount() -> None:
    leg = PrimitiveMoneyLeg.input("0xabc", MeasuredMoney.measured(Decimal("1.5")))
    assert leg.token == "0xabc"
    assert leg.amount.is_measured
    assert leg.amount.value == Decimal("1.5")


def test_empty_token_is_allowed_for_unknown_identity() -> None:
    """Empty≠Zero: an unknown token identity is '', never a fabricated symbol."""
    leg = PrimitiveMoneyLeg.input("", MeasuredMoney.unmeasured())
    assert leg.token == ""
    assert leg.amount.is_unmeasured


def test_leg_is_frozen() -> None:
    leg = PrimitiveMoneyLeg.input("USDC", MeasuredMoney.measured(Decimal("1")))
    with pytest.raises(FrozenInstanceError):
        leg.token = "WETH"  # type: ignore[misc]


# -- construction validation --------------------------------------------------


def test_amount_must_be_measured_money() -> None:
    """A bare Decimal / None / str is rejected — Empty≠Zero must be carried."""
    with pytest.raises(TypeError):
        PrimitiveMoneyLeg(MoneyLegRole.INPUT, "USDC", Decimal("1"))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        PrimitiveMoneyLeg.input("USDC", None)  # type: ignore[arg-type]


def test_role_must_be_enum() -> None:
    with pytest.raises(TypeError):
        PrimitiveMoneyLeg("input", "USDC", MeasuredMoney.measured(Decimal("1")))  # type: ignore[arg-type]


def test_token_must_be_str() -> None:
    with pytest.raises(TypeError):
        PrimitiveMoneyLeg.input(123, MeasuredMoney.measured(Decimal("1")))  # type: ignore[arg-type]


# -- role views ---------------------------------------------------------------


def _swap_legs() -> PrimitiveMoneyLegs:
    return PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input("USDC", MeasuredMoney.measured(Decimal("100"))),
        PrimitiveMoneyLeg.output("WETH", MeasuredMoney.measured(Decimal("0.03"))),
    )


def test_role_views_partition_by_role_in_order() -> None:
    legs = PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input("A", MeasuredMoney.measured(Decimal("1"))),
        PrimitiveMoneyLeg.input("B", MeasuredMoney.measured(Decimal("2"))),
        PrimitiveMoneyLeg.output("C", MeasuredMoney.measured(Decimal("3"))),
        PrimitiveMoneyLeg.principal("D", MeasuredMoney.measured(Decimal("4"))),
    )
    assert [leg.token for leg in legs.input_legs] == ["A", "B"]
    assert [leg.token for leg in legs.output_legs] == ["C"]
    assert [leg.token for leg in legs.principal_legs] == ["D"]


def test_absent_role_view_is_empty() -> None:
    assert _swap_legs().principal_legs == ()


def test_of_normalizes_to_tuple_and_validates_members() -> None:
    legs = PrimitiveMoneyLegs([PrimitiveMoneyLeg.input("USDC", MeasuredMoney.measured(Decimal("1")))])
    assert isinstance(legs.legs, tuple)
    with pytest.raises(TypeError):
        PrimitiveMoneyLegs.of("not-a-leg")  # type: ignore[arg-type]


# -- aggregation: Empty≠Zero propagation --------------------------------------


def test_total_of_all_measured_legs_is_the_measured_sum() -> None:
    legs = PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.principal("USDC", MeasuredMoney.measured(Decimal("10"))),
        PrimitiveMoneyLeg.principal("USDC", MeasuredMoney.measured(Decimal("2.5"))),
    )
    total = legs.total_principal()
    assert total.is_measured
    assert total.value == Decimal("12.5")


def test_empty_role_totals_to_measured_zero() -> None:
    """No legs to taint the seed → measured zero, not unmeasured."""
    total = _swap_legs().total_principal()
    assert total.is_measured
    assert total.value == Decimal("0")


def test_one_unmeasured_leg_makes_total_unmeasured() -> None:
    """A missing leg amount propagates — never masquerades as measured zero."""
    legs = PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input("USDC", MeasuredMoney.measured(Decimal("10"))),
        PrimitiveMoneyLeg.input("DAI", MeasuredMoney.unmeasured()),
    )
    total = legs.total_input()
    assert not total.is_measured
    assert total.is_unmeasured


def test_absent_leg_dominates_unmeasured_in_total() -> None:
    """absent > unmeasured in the information lattice — the join wins."""
    legs = PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input("USDC", MeasuredMoney.measured(Decimal("10"))),
        PrimitiveMoneyLeg.input("DAI", MeasuredMoney.unmeasured()),
        PrimitiveMoneyLeg.input("WETH", MeasuredMoney.absent()),
    )
    total = legs.total_input()
    assert total.is_absent


def test_total_only_sums_the_requested_role() -> None:
    legs = PrimitiveMoneyLegs.of(
        PrimitiveMoneyLeg.input("USDC", MeasuredMoney.measured(Decimal("10"))),
        PrimitiveMoneyLeg.output("WETH", MeasuredMoney.unmeasured()),
    )
    # The unmeasured OUTPUT leg must not taint the INPUT total.
    assert legs.total_input().value == Decimal("10")
    assert legs.total(MoneyLegRole.OUTPUT).is_unmeasured


def test_legs_is_frozen() -> None:
    legs = _swap_legs()
    with pytest.raises(FrozenInstanceError):
        legs.legs = ()  # type: ignore[misc]


# -- role views / aggregation fail loud on a non-enum role --------------------


def test_total_rejects_non_enum_role() -> None:
    """A typo'd string role must raise, never silently return measured zero."""
    legs = _swap_legs()
    with pytest.raises(TypeError):
        legs.total("input")  # type: ignore[arg-type]


def test_by_role_rejects_non_enum_role() -> None:
    """A typo'd string role must raise, never silently return an empty view."""
    legs = _swap_legs()
    with pytest.raises(TypeError):
        legs.by_role("output")  # type: ignore[arg-type]


# -- stake/mint generic constructor + serialization (VIB-5220) ----------------


def test_stake_mint_builds_input_output_pair() -> None:
    """The generic stake/mint constructor declares one INPUT (staked) + one OUTPUT
    (minted) leg in that order — the reusable shape for any stake-and-mint connector."""
    legs = PrimitiveMoneyLegs.stake_mint(
        staked_token="ETH",
        staked_amount=MeasuredMoney.measured(Decimal("1")),
        minted_token="stETH",
        minted_amount=MeasuredMoney.measured(Decimal("1")),
    )
    assert [(leg.role, leg.token) for leg in legs.legs] == [
        (MoneyLegRole.INPUT, "ETH"),
        (MoneyLegRole.OUTPUT, "stETH"),
    ]
    assert legs.total_input().value == Decimal("1")
    assert legs.total_output().value == Decimal("1")


def test_stake_mint_carries_unmeasured_output() -> None:
    """An unresolved mint amount stays unmeasured — never a fabricated measured zero."""
    legs = PrimitiveMoneyLegs.stake_mint(
        staked_token="ETH",
        staked_amount=MeasuredMoney.measured(Decimal("1")),
        minted_token="stETH",
        minted_amount=MeasuredMoney.unmeasured(),
    )
    assert legs.input_legs[0].amount.is_measured
    assert legs.output_legs[0].amount.is_unmeasured


def test_leg_to_dict_serializes_empty_neq_zero_states() -> None:
    """``to_dict`` maps the three MeasuredMoney states onto the payload form:
    measured → str, unmeasured → None, absent → ''."""
    measured = PrimitiveMoneyLeg.input("ETH", MeasuredMoney.measured(Decimal("0")))
    unmeasured = PrimitiveMoneyLeg.output("stETH", MeasuredMoney.unmeasured())
    absent = PrimitiveMoneyLeg.output("stETH", MeasuredMoney.absent())
    assert measured.to_dict() == {"role": "input", "token": "ETH", "amount": "0"}
    assert unmeasured.to_dict() == {"role": "output", "token": "stETH", "amount": None}
    assert absent.to_dict() == {"role": "output", "token": "stETH", "amount": ""}


def test_legs_to_dict_lists_each_leg() -> None:
    legs = PrimitiveMoneyLegs.stake_mint(
        staked_token="ETH",
        staked_amount=MeasuredMoney.measured(Decimal("1")),
        minted_token="wstETH",
        minted_amount=MeasuredMoney.measured(Decimal("0.85")),
    )
    assert legs.to_dict() == {
        "legs": [
            {"role": "input", "token": "ETH", "amount": "1"},
            {"role": "output", "token": "wstETH", "amount": "0.85"},
        ]
    }
