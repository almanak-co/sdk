"""Fee rounding and discount precedence follow the position executor's collateral units."""

import pytest

from almanak.connectors.gmx_v2.economic_position_fees import FeePolicy, referral_fees
from almanak.connectors.gmx_v2.economic_precision import FLOAT


@pytest.fixture
def fee_policy():
    return FeePolicy(
        position_factor=FLOAT // 100,
        position_receiver_factor=FLOAT // 5,
        borrowing_receiver_factor=FLOAT // 5,
        liquidation_factor=FLOAT // 100,
        liquidation_receiver_factor=FLOAT // 5,
        ui_factor=0,
        referral_enabled=True,
        affiliate_factor=FLOAT // 10,
        referral_discount_factor=FLOAT // 10,
        minimum_affiliate_factor=FLOAT // 20,
        pro_discount_factor=0,
    )


@pytest.mark.parametrize(
    "pro,reward,discount,protocol",
    [(5, 10, 10, 80), (13, 7, 13, 80), (18, 5, 18, 77), (25, 5, 25, 70)],
)
def test_pro_and_referral_discount_precedence(fee_policy, pro, reward, discount, protocol):
    result = referral_fees(
        100, fee_policy.model_copy(update={"pro_discount_factor": pro * FLOAT // 100})
    )
    assert result.affiliate_amount == reward
    assert result.total_discount_amount == discount
    assert result.protocol_amount == protocol


def test_unregistered_referral_has_no_reward_or_referral_discount(fee_policy):
    result = referral_fees(100, fee_policy.model_copy(update={"referral_enabled": False}))
    assert result.affiliate_amount == result.total_discount_amount == 0
    assert result.protocol_amount == 100


@pytest.mark.parametrize("liquidation", [False, True])
def test_fee_totals_keep_funding_separate_and_use_full_previous_notional(fee_policy, liquidation):
    from almanak.caip import AssetType
    from almanak.connectors.gmx_v2.economic_position_fees import (
        FeeAccumulators,
        PositionFeeInput,
        position_fees,
    )
    from almanak.connectors.gmx_v2.economic_spot_types import Price

    zero = FeeAccumulators(
        borrowing_factor=0,
        funding_payment_per_size=0,
        funding_long_claim_per_size=0,
        funding_short_claim_per_size=0,
    )
    latest = FeeAccumulators(
        borrowing_factor=FLOAT // 100,
        funding_payment_per_size=10**15,
        funding_long_claim_per_size=10**15,
        funding_short_claim_per_size=1,
    )
    state = PositionFeeInput(
        collateral_price=Price(
            asset=AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1"),
            minimum=3 * 10**12,
            maximum=4 * 10**12,
        ),
        size_usd_e30=3 * FLOAT,
        size_delta_usd_e30=3 * FLOAT,
        position=zero,
        latest=latest,
        policy=fee_policy.model_copy(update={"referral_enabled": False}),
        liquidation=liquidation,
    )
    result = position_fees(state)
    assert result.position_amount == result.borrowing_amount == 10**16
    assert result.liquidation_amount == (10**16 if liquidation else 0)
    assert result.total == (3 if liquidation else 2) * 10**16 + 3
    assert result.funding_amount == result.long_funding_claim == 3
    assert result.short_funding_claim == 0
    assert result.receiver_amount + result.pool_amount == result.total_excluding_funding
    smaller = position_fees(state.model_copy(update={"size_delta_usd_e30": FLOAT}))
    assert smaller.borrowing_amount == result.borrowing_amount
    assert smaller.funding_amount == result.funding_amount
