"""Decrease settlement prioritizes realized collateral profit, then locked collateral."""

import pytest

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.economic_position_collateral import (
    CollateralBalances,
    pay_for_cost,
)
from almanak.connectors.gmx_v2.economic_spot_types import Price


@pytest.fixture
def prices():
    return (
        Price(
            asset=AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1"),
            minimum=3,
            maximum=4,
        ),
        Price(
            asset=AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
            minimum=2,
            maximum=3,
        ),
    )


@pytest.mark.parametrize(
    "cost,remaining,output,secondary,paid,paid_secondary,unpaid",
    [
        (0, 10, 4, 20, 0, 0, 0),
        (6, 10, 2, 20, 2, 0, 0),
        (15, 9, 0, 20, 5, 0, 0),
        (45, 0, 0, 19, 14, 1, 0),
        (90, 0, 0, 0, 14, 20, 8),
    ],
)
def test_cost_payment_order_and_secondary_conversion(
    prices, cost, remaining, output, secondary, paid, paid_secondary, unpaid
):
    balances, payment = pay_for_cost(
        CollateralBalances(remaining=10, output=4, secondary=20), *prices, cost, "fees"
    )
    assert balances == CollateralBalances(remaining=remaining, output=output, secondary=secondary)
    assert (payment.collateral_amount, payment.secondary_amount, payment.unpaid_usd_e30) == (
        paid,
        paid_secondary,
        unpaid,
    )


def test_primary_cost_rounds_up_before_secondary_conversion(prices):
    balances, payment = pay_for_cost(
        CollateralBalances(remaining=0, output=0, secondary=2), *prices, 1, "pnl"
    )
    assert balances.secondary == 1
    assert payment.secondary_amount == 1 and payment.unpaid_usd_e30 == 0


@pytest.fixture
def decrease_input(prices):
    from almanak.connectors.gmx_v2.economic_position_collateral import DecreaseCollateralInput
    from almanak.connectors.gmx_v2.economic_position_fees import PositionFees, ReferralFees

    referral = ReferralFees(**{field: 0 for field in ReferralFees.model_fields})
    fees = PositionFees(
        **{field: 0 for field in PositionFees.model_fields if field != "referral"},
        referral=referral,
    )
    fees = fees.model_copy(update={"funding_amount": 2, "total_excluding_funding": 3, "total": 5})
    return DecreaseCollateralInput(
        collateral_price=prices[0],
        pnl_price=prices[1],
        collateral_amount=10,
        base_pnl_usd_e30=18,
        impact_usd_e30=-6,
        deferred_impact_usd_e30=3,
        fees=fees,
        withdrawal=2,
        insolvent_close_allowed=False,
        holding_address_available=True,
    )


def test_solvent_decrease_retains_deferred_claim_and_caps_withdrawal(decrease_input):
    from almanak.connectors.gmx_v2.economic_position_collateral import decrease_collateral

    result = decrease_collateral(decrease_input)
    assert result.balances == CollateralBalances(remaining=1, output=1, secondary=6)
    assert result.deferred_collateral == 1 and result.deferred_secondary == 0
    assert result.fees_collected and result.insolvent_at is None
    assert [p.stage for p in result.payments] == ["funding", "pnl", "fees", "impact", "diff"]


def test_insolvent_manual_close_refuses_but_liquidation_retains_unpaid_cost(decrease_input):
    from almanak.connectors.gmx_v2.economic_position_collateral import decrease_collateral
    from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint

    state = decrease_input.model_copy(update={"collateral_amount": 0, "base_pnl_usd_e30": 0})
    with pytest.raises(SwapConstraint, match="insufficient funds for funding"):
        decrease_collateral(state)
    result = decrease_collateral(state.model_copy(update={"insolvent_close_allowed": True}))
    assert result.insolvent_at == "funding" and not result.fees_collected
    assert result.payments[0].unpaid_usd_e30 == 6
    assert result.balances == CollateralBalances(remaining=0, output=0, secondary=0)


def test_secondary_funding_payment_requires_protocol_holding_receiver(decrease_input):
    from almanak.connectors.gmx_v2.economic_position_collateral import decrease_collateral
    from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint

    state = decrease_input.model_copy(
        update={"collateral_amount": 0, "holding_address_available": False}
    )
    with pytest.raises(SwapConstraint, match="holding address"):
        decrease_collateral(state)
