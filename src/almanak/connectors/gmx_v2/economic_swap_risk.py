"""Swap admission applies inventory, reserve and trader-PnL limits after modeled deltas."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_position_math import signed_div
from almanak.connectors.gmx_v2.economic_precision import FLOAT, apply_factor
from almanak.connectors.gmx_v2.economic_spot_types import Price, SwapConstraint
from almanak.models import Block, Model, UInt


class SwapSideRisk(Model):
    maximum_pool_amount: UInt
    reserve_factor: UInt
    maximum_deposit_pnl_factor: UInt
    maximum_withdrawal_pnl_factor: UInt
    open_interest_usd_e30: UInt
    open_interest_tokens: UInt


class SwapRiskState(Model):
    market: Market
    block: Block
    long: SwapSideRisk
    short: SwapSideRisk
    market_disabled: bool
    creation_disabled: bool
    execution_disabled: bool
    cancellation_disabled: bool
    request_expiration_seconds: UInt
    maximum_oracle_age_seconds: UInt

    @model_validator(mode="after")
    def scope(self):
        if self.block.chain != self.market.chain:
            raise ValueError("GMX swap risk state requires the market's exact historical chain")
        return self


def pool_after_swap(spot, calculation):
    incoming = calculation.fees.after_fees + calculation.fees.pool_amount
    incoming += (
        calculation.input_impact_amount
        if calculation.price_impact_usd_e30 > 0
        else calculation.impact_amount
    )
    changes = (incoming, -calculation.pool_amount_out)
    if calculation.asset_in == spot.market.short_asset:
        changes = changes[::-1]
    return spot.long.pool_amount + changes[0], spot.short.pool_amount + changes[1]


def maximum_side_pnl(side, price: Price, is_long):
    if not side.open_interest_usd_e30 or not side.open_interest_tokens:
        return 0
    if is_long:
        return side.open_interest_tokens * price.maximum - side.open_interest_usd_e30
    return side.open_interest_usd_e30 - side.open_interest_tokens * price.minimum


def validate_pnl(side, index_price, pool_usd, is_long, *, incoming):
    # The deployed ratio returns zero for an empty pool; reserve checks remain separate.
    ratio = (
        signed_div(maximum_side_pnl(side, index_price, is_long) * FLOAT, pool_usd)
        if pool_usd
        else 0
    )
    maximum = side.maximum_deposit_pnl_factor if incoming else side.maximum_withdrawal_pnl_factor
    if ratio > maximum:
        raise SwapConstraint("GMX swap exceeds the configured post-swap trader-PnL factor")


def validate_swap_risk(spot, risk, index_price, calculation):
    if (
        spot.market != risk.market
        or spot.block != risk.block
        or index_price.asset != risk.market.index_asset
    ):
        raise ValueError("GMX swap pricing and risk require the same canonical market state")
    if (
        risk.market_disabled
        or risk.execution_disabled
        or risk.market.long_asset == risk.market.short_asset
    ):
        raise SwapConstraint("GMX swap execution is disabled for this historical market")
    if calculation.amount_out <= 0:
        raise SwapConstraint("GMX swap impact leaves no positive output")
    amounts = pool_after_swap(spot, calculation)
    if min(amounts) < 0:
        raise SwapConstraint("GMX swap output exceeds available pool inventory")
    for is_long, amount, currency, side in zip(
        (True, False), amounts, (spot.long, spot.short), (risk.long, risk.short), strict=True
    ):
        incoming = currency.price.asset == calculation.asset_in
        pool_usd = amount * currency.price.minimum
        validate_swap_side(side, index_price, amount, pool_usd, is_long, incoming=incoming)


def validate_swap_side(side, index_price, amount, pool_usd, is_long, *, incoming):
    if incoming and amount > side.maximum_pool_amount:
        raise SwapConstraint("GMX swap input exceeds the configured maximum pool amount")
    reserved = (
        side.open_interest_tokens * index_price.maximum if is_long else side.open_interest_usd_e30
    )
    if not incoming and reserved > apply_factor(pool_usd, side.reserve_factor):
        raise SwapConstraint("GMX swap output violates the configured position reserve")
    validate_pnl(side, index_price, pool_usd, is_long, incoming=incoming)
