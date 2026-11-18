"""Position fees retain collateral units, accumulator rounding and referral/pro discounts."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.economic_pnl import borrowing_fee_usd, funding_amount
from almanak.connectors.gmx_v2.economic_precision import apply_factor, ceil_div
from almanak.connectors.gmx_v2.economic_spot_types import Factor, Price
from almanak.models import Model, UInt


class FeePolicy(Model):
    position_factor: Factor
    position_receiver_factor: Factor
    borrowing_receiver_factor: Factor
    liquidation_factor: Factor
    liquidation_receiver_factor: Factor
    ui_factor: Factor
    referral_enabled: bool
    affiliate_factor: Factor
    referral_discount_factor: Factor
    minimum_affiliate_factor: Factor
    pro_discount_factor: Factor


class FeeAccumulators(Model):
    borrowing_factor: UInt
    funding_payment_per_size: UInt
    funding_long_claim_per_size: UInt
    funding_short_claim_per_size: UInt


class PositionFeeInput(Model):
    collateral_price: Price
    size_usd_e30: UInt
    size_delta_usd_e30: UInt
    position: FeeAccumulators
    latest: FeeAccumulators
    policy: FeePolicy
    liquidation: bool

    @model_validator(mode="after")
    def accrual(self):
        if any(
            getattr(self.latest, field) < getattr(self.position, field)
            for field in FeeAccumulators.model_fields
        ):
            raise ValueError("GMX position fee accumulators cannot move backwards")
        return self


class ReferralFees(Model):
    adjusted_affiliate_factor: Factor
    affiliate_amount: UInt
    referral_discount_amount: UInt
    pro_discount_amount: UInt
    total_discount_amount: UInt
    protocol_amount: UInt


class PositionFees(Model):
    position_amount: UInt
    referral: ReferralFees
    borrowing_usd_e30: UInt
    borrowing_amount: UInt
    borrowing_receiver_amount: UInt
    funding_amount: UInt
    long_funding_claim: UInt
    short_funding_claim: UInt
    liquidation_usd_e30: UInt
    liquidation_amount: UInt
    liquidation_receiver_amount: UInt
    ui_amount: UInt
    position_receiver_amount: UInt
    position_pool_amount: UInt
    receiver_amount: UInt
    pool_amount: UInt
    total_excluding_funding: UInt
    total: UInt


def referral_fees(amount, policy):
    affiliate = policy.affiliate_factor if policy.referral_enabled else 0
    if policy.referral_enabled and policy.pro_discount_factor > policy.referral_discount_factor:
        rebate = policy.affiliate_factor + policy.referral_discount_factor
        affiliate = max(policy.minimum_affiliate_factor, rebate - policy.pro_discount_factor)
    reward = apply_factor(amount, affiliate)
    referral_discount = (
        apply_factor(amount, policy.referral_discount_factor) if policy.referral_enabled else 0
    )
    pro_discount = apply_factor(amount, policy.pro_discount_factor)
    discount = max(pro_discount, referral_discount)
    if reward + discount > amount:
        raise ValueError("GMX referral reward and trader discount exceed position fees")
    return ReferralFees(
        adjusted_affiliate_factor=affiliate,
        affiliate_amount=reward,
        referral_discount_amount=referral_discount,
        pro_discount_amount=pro_discount,
        total_discount_amount=discount,
        protocol_amount=amount - reward - discount,
    )


def position_fees(state):
    policy, latest, saved = state.policy, state.latest, state.position
    price, size, delta = (
        state.collateral_price.minimum,
        state.size_usd_e30,
        state.size_delta_usd_e30,
    )
    position = apply_factor(delta, policy.position_factor) // price
    referral = referral_fees(position, policy)
    borrowing_usd = borrowing_fee_usd(latest.borrowing_factor, saved.borrowing_factor, size)
    borrowing = borrowing_usd // price
    borrowing_receiver = apply_factor(borrowing, policy.borrowing_receiver_factor)
    liquidation_usd = apply_factor(delta, policy.liquidation_factor) if state.liquidation else 0
    liquidation = ceil_div(liquidation_usd, price)
    liquidation_receiver = apply_factor(liquidation, policy.liquidation_receiver_factor)
    ui = apply_factor(delta, policy.ui_factor) // price
    receiver = apply_factor(referral.protocol_amount, policy.position_receiver_factor)
    pool = referral.protocol_amount - receiver
    funding = funding_amount(
        latest.funding_payment_per_size, saved.funding_payment_per_size, size, payment=True
    )
    excluding = position + borrowing + liquidation + ui - referral.total_discount_amount
    return PositionFees(
        position_amount=position,
        referral=referral,
        borrowing_usd_e30=borrowing_usd,
        borrowing_amount=borrowing,
        borrowing_receiver_amount=borrowing_receiver,
        funding_amount=funding,
        long_funding_claim=funding_amount(
            latest.funding_long_claim_per_size,
            saved.funding_long_claim_per_size,
            size,
            payment=False,
        ),
        short_funding_claim=funding_amount(
            latest.funding_short_claim_per_size,
            saved.funding_short_claim_per_size,
            size,
            payment=False,
        ),
        liquidation_usd_e30=liquidation_usd,
        liquidation_amount=liquidation,
        liquidation_receiver_amount=liquidation_receiver,
        ui_amount=ui,
        position_receiver_amount=receiver,
        position_pool_amount=pool,
        receiver_amount=receiver + borrowing_receiver + liquidation_receiver,
        pool_amount=pool + borrowing - borrowing_receiver + liquidation - liquidation_receiver,
        total_excluding_funding=excluding,
        total=excluding + funding,
    )
