"""Perpetual request switches and increase reserve limits retain canonical inputs."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_liquidity_risk import RequestControls
from almanak.connectors.gmx_v2.economic_precision import apply_factor
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.models import Block, Model, UInt


class PerpetualSideRisk(Model):
    maximum_open_interest_usd_e30: UInt
    reserve_factor: UInt
    open_interest_reserve_factor: UInt
    collateral_open_interest_multiplier: UInt


class PerpetualRiskState(Model):
    market: Market
    block: Block
    increases: RequestControls
    decreases: RequestControls
    market_disabled: bool
    funding_claim_disabled: bool
    liquidation_disabled: bool
    holding_address_available: bool
    request_expiration_seconds: UInt
    maximum_oracle_age_seconds: UInt
    minimum_position_usd_e30: UInt
    minimum_collateral_usd_e30: UInt
    minimum_collateral_factor: UInt
    liquidation_collateral_factor: UInt
    maximum_liquidation_impact_factor: UInt
    long: PerpetualSideRisk
    short: PerpetualSideRisk

    @model_validator(mode="after")
    def scope(self):
        if self.market.chain != self.block.chain:
            raise ValueError("GMX perpetual controls require their canonical market chain")
        return self


def validate_increase_reserves(risk, valuation, size_delta, token_delta, is_long):
    if risk.market != valuation.spot.market or risk.block != valuation.spot.block:
        raise ValueError("GMX increase reserves require one canonical accounting block")
    side = risk.long if is_long else risk.short
    interest = valuation.long if is_long else valuation.short
    currency = valuation.spot.long if is_long else valuation.spot.short
    next_usd = interest.open_interest_usd_e30 + size_delta
    next_tokens = interest.open_interest_tokens + token_delta
    if next_usd > side.maximum_open_interest_usd_e30:
        raise SwapConstraint("GMX increase exceeds the market's maximum open interest")
    reserved = next_tokens * valuation.index_price.maximum if is_long else next_usd
    pool_usd = currency.pool_amount * currency.price.minimum
    if reserved > apply_factor(pool_usd, side.reserve_factor):
        raise SwapConstraint("GMX increase exceeds the market's position reserve")
    if reserved > apply_factor(pool_usd, side.open_interest_reserve_factor):
        raise SwapConstraint("GMX increase exceeds the market's open-interest reserve")
    return next_collateral_factor(risk, valuation, size_delta, is_long)


def next_collateral_factor(risk, valuation, size_delta, is_long):
    """Leverage bound after a notional change: the market floor or the open-interest multiple."""
    side = risk.long if is_long else risk.short
    interest = valuation.long if is_long else valuation.short
    next_usd = max(0, interest.open_interest_usd_e30 + size_delta)
    return max(
        risk.minimum_collateral_factor,
        apply_factor(next_usd, side.collateral_open_interest_multiplier),
    )
