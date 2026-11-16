"""Checked Morpho arithmetic is compared with retained deployed contract transitions."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.morpho_blue.accounting import accrue
from almanak.connectors.morpho_blue.domain import Accounting, Position
from almanak.connectors.morpho_blue.liquidation import liquidate, liquidation_amounts
from almanak.connectors.morpho_blue.math import (
    WAD,
    adaptive_rate,
    compounded,
    div_to_zero,
    exponential,
    mul_div,
    signed,
    to_assets,
    to_shares,
    uint,
)
from tests.retained_fixtures import retained

ROOT = Path(__file__).parents[1]
EVIDENCE = retained("2026-09-07-rollout/morpho-blue-discovery")


def recorded(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".json.gz")).read_bytes()))


def market(values):
    return Accounting(**dict(zip(Accounting.model_fields, values, strict=True)))


def position(values):
    return Position(**dict(zip(Position.model_fields, values, strict=True)))


@pytest.mark.parametrize("chain", [1, 8453])
def test_all_retained_rate_views_match_checked_revision_math(chain):
    raw = recorded(f"{chain}-operations")
    for step in raw["steps"]:
        for state in (step["before"], step["after"]):
            values = state["market"]
            rate, _ = adaptive_rate(
                values[0],
                values[2],
                state["rate_at_target"],
                state["block"]["timestamp"] - values[4],
            )
            assert rate == state["borrow_rate"]


@pytest.mark.parametrize("chain", [1, 8453])
@pytest.mark.parametrize("scenario", ["full_shares", "partial_shares", "bad_debt"])
def test_liquidation_and_lender_losses_match_deployed_contract(chain, scenario):
    raw = recorded(f"{chain}-liquidation-{scenario}")
    step = raw["steps"][-1]
    before, after = step["before"], step["after"]
    owned = position(before["accounts"]["borrower"]["position"])
    accrued = accrue(
        market(before["market"]), before["rate_at_target"], after["block"]["timestamp"]
    )
    amount = liquidation_input(owned, scenario)
    result = liquidate(accrued.accounting, owned, raw["fixture_price"], raw["params"][4], **amount)
    assert result.accounting == market(after["market"])
    assert result.position == position(after["accounts"]["borrower"]["position"])
    assert accrued.rate_at_target == after["rate_at_target"]
    assert (
        before["accounts"]["lender"]["loan"] - result.repaid_assets
        == after["accounts"]["lender"]["loan"]
    )
    assert (
        before["accounts"]["lender"]["collateral"] + result.seized_collateral
        == after["accounts"]["lender"]["collateral"]
    )
    assert (result.bad_debt_assets > 0) == (scenario == "bad_debt")
    assert (
        accrued.accounting.supply_assets - result.accounting.supply_assets == result.bad_debt_assets
    )


def liquidation_input(owned, scenario):
    if scenario == "bad_debt":
        return {"seized": owned.collateral}
    return {
        "shares": owned.borrow_shares // 2 if scenario == "partial_shares" else owned.borrow_shares
    }


@pytest.mark.parametrize(
    "numerator,denominator,expected", [(-5, 2, -2), (5, -2, -2), (-5, -2, 2), (5, 2, 2)]
)
def test_signed_division_matches_solidity_rounding(numerator, denominator, expected):
    assert div_to_zero(numerator, denominator) == expected


@pytest.mark.parametrize("value", [-1, True, 2**256, "1"])
def test_uint_refuses_out_of_range_and_untyped_values(value):
    with pytest.raises(ValueError):
        uint(value)


def test_checked_overflow_and_zero_denominators():
    for call in (
        lambda: mul_div(2**255, 2, 2),
        lambda: mul_div(2**256 - 1, 1, 2, round_up=True),
        lambda: mul_div(1, 1, 0),
        lambda: div_to_zero(1, 0),
        lambda: div_to_zero(-(2**255), -1),
        lambda: signed(2**255),
        lambda: compounded(2**255, 2),
    ):
        with pytest.raises(ValueError):
            call()


def test_virtual_shares_rounding_and_empty_market():
    assert to_shares(3, 0, 0) == 3_000_000
    assert to_assets(3_000_000, 0, 0) == 3
    assert to_shares(1, 2, 1) == 333_333
    assert to_shares(1, 2, 1, round_up=True) == 333_334
    assert to_assets(333_334, 2, 1) == 1
    assert to_assets(333_334, 2, 1, round_up=True) == 2


def test_bounded_exponential_and_first_irm_interaction():
    assert exponential(-41446531673892822313) == 0
    assert exponential(93859467695000404319) == exponential(2**255 - 1)
    assert exponential(0) == WAD
    initial = 4 * 10**16 // 31536000
    assert adaptive_rate(0, 0, 0, 0) == (initial // 4, initial)
    assert adaptive_rate(100, 90, initial, 1) == (initial, initial)
    with pytest.raises(ValueError):
        adaptive_rate(1, 2, initial, 1)
    with pytest.raises(ValueError):
        adaptive_rate(100, 10, 1, 1)


def test_accrual_cannot_move_backwards_and_same_timestamp_is_identity():
    raw = recorded("1-operations")["steps"][0]["before"]
    accounting = market(raw["market"])
    unchanged = accrue(accounting, raw["rate_at_target"], accounting.last_update)
    assert unchanged.accounting == accounting and unchanged.interest == unchanged.fee_shares == 0
    with pytest.raises(ValueError, match="precede"):
        accrue(accounting, raw["rate_at_target"], accounting.last_update - 1)


def test_liquidation_refuses_healthy_and_ambiguous_amounts():
    accounting = Accounting(
        supply_assets=100,
        supply_shares=100_000_000,
        borrow_assets=10,
        borrow_shares=10_000_000,
        last_update=1,
        fee=0,
    )
    owned = Position(supply_shares=0, borrow_shares=1_000_000, collateral=10)
    with pytest.raises(ValueError, match="healthy"):
        liquidate(accounting, owned, 10**36, WAD // 2, shares=1)
    for seized, shares in ((0, 0), (1, 1)):
        with pytest.raises(ValueError, match="exactly one"):
            liquidation_amounts(accounting, 10**36, WAD // 2, seized, shares)
