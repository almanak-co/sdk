"""GM deposits and withdrawals retain their own historical controls and pool limits."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_precision import apply_factor
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.economic_swap_risk import SwapSideRisk, validate_pnl
from almanak.models import Block, Model, UInt


class RequestControls(Model):
    creation_disabled: bool
    execution_disabled: bool
    cancellation_disabled: bool


class LiquidityRiskState(Model):
    market: Market
    block: Block
    long: SwapSideRisk
    short: SwapSideRisk
    deposits: RequestControls
    withdrawals: RequestControls
    market_disabled: bool
    request_expiration_seconds: UInt
    maximum_oracle_age_seconds: UInt
    maximum_long_deposit_usd_e30: UInt
    maximum_short_deposit_usd_e30: UInt
    minimum_first_deposit_shares: UInt
    maximum_lent_impact_withdrawal_factor: UInt

    @model_validator(mode="after")
    def scope(self):
        if self.block.chain != self.market.chain:
            raise ValueError("GM liquidity risk requires its canonical historical market chain")
        return self


def validate_market(state, risk, *, deposit):
    if state.spot.market != risk.market or state.spot.block != risk.block:
        raise ValueError("GM liquidity accounting and risk require the same canonical market")
    control = risk.deposits if deposit else risk.withdrawals
    if risk.market_disabled or control.execution_disabled:
        raise SwapConstraint("GM liquidity execution is disabled at this historical time")


def validate_market_pnl(state, risk, *, deposit):
    for is_long, currency, side in zip(
        (True, False), (state.spot.long, state.spot.short), (risk.long, risk.short), strict=True
    ):
        validate_pnl(
            side,
            state.index_price,
            currency.pool_amount * currency.price.minimum,
            is_long,
            incoming=deposit,
        )


def validate_deposit(state, risk, result):
    validate_market(state, risk, deposit=True)
    if not state.share_supply and risk.minimum_first_deposit_shares:
        raise SwapConstraint("GM initial deposit requires the protocol's locked-share receiver")
    # The deployed deposit checks aggregate trader PnL before either asset enters the pool.
    validate_market_pnl(state, risk, deposit=True)
    for leg in result.legs:
        currencies = (leg.after.spot.long, leg.after.spot.short)
        for is_long, currency, side in zip(
            (True, False), currencies, (risk.long, risk.short), strict=True
        ):
            changed = is_long == leg.is_long or leg.output_impact_amount > 0
            if changed and currency.pool_amount > side.maximum_pool_amount:
                raise SwapConstraint("GM deposit exceeds the historical maximum pool amount")
        incoming = currencies[0] if leg.is_long else currencies[1]
        maximum = (
            risk.maximum_long_deposit_usd_e30 if leg.is_long else risk.maximum_short_deposit_usd_e30
        )
        if incoming.pool_amount * incoming.price.maximum > maximum:
            raise SwapConstraint("GM deposit exceeds the historical incoming-currency USD cap")


def withdrawal_pool(state, calculation):
    currencies = (state.spot.long, state.spot.short)
    fees = (calculation.long_fees, calculation.short_fees)
    updated = []
    for currency, fee in zip(currencies, fees, strict=True):
        # Receiver/UI fees leave pool accounting even while their tokens remain claimable.
        amount = currency.pool_amount - fee.after_fees - fee.receiver_amount - fee.ui_amount
        if amount < 0:
            raise SwapConstraint("GM withdrawal exceeds available pool inventory")
        updated.append(currency.model_copy(update={"pool_amount": amount}))
    return state.model_copy(
        update={"spot": state.spot.model_copy(update={"long": updated[0], "short": updated[1]})}
    )


def validate_withdrawal(state, risk, calculation):
    validate_market(state, risk, deposit=False)
    after = withdrawal_pool(state, calculation)
    pool_usd = sum(s.pool_amount * s.price.minimum for s in (after.spot.long, after.spot.short))
    if state.impact.lent_amount * state.index_price.maximum > apply_factor(
        pool_usd, risk.maximum_lent_impact_withdrawal_factor
    ):
        raise SwapConstraint("GM withdrawal violates the historical lent-impact limit")
    for is_long, currency, side in zip(
        (True, False), (after.spot.long, after.spot.short), (risk.long, risk.short), strict=True
    ):
        reserved = (
            side.open_interest_tokens * state.index_price.maximum
            if is_long
            else side.open_interest_usd_e30
        )
        if reserved > apply_factor(
            currency.pool_amount * currency.price.minimum, side.reserve_factor
        ):
            raise SwapConstraint("GM withdrawal violates the historical position reserve")
    validate_market_pnl(after, risk, deposit=False)
    return after
