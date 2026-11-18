"""Liquidation eligibility values a full exit while excluding liquidation fees."""

from typing import Literal

from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_types import Price
from almanak.models import Model, UInt

RiskReason = Literal["min collateral", "< 0", "min collateral for leverage"]


class PositionRiskInput(Model):
    size_usd_e30: UInt
    collateral_amount: UInt
    collateral_price: Price
    index_price: Price
    capped_pnl_usd_e30: int
    close_impact_usd_e30: int
    pending_impact_amount: int
    maximum_positive_impact_factor: UInt
    maximum_liquidation_impact_factor: UInt
    close_cost_amount: UInt
    minimum_collateral_usd_e30: UInt
    minimum_collateral_factor: UInt
    validate_minimum_collateral: bool


class PositionRisk(Model):
    remaining_collateral_usd_e30: int
    minimum_leverage_collateral_usd_e30: UInt
    minimum_collateral_usd_e30: UInt
    impact_usd_e30: int
    reason: RiskReason | None

    @property
    def liquidatable(self):
        return self.reason is not None


def risk_impact(state):
    impact = min(
        state.close_impact_usd_e30,
        state.size_usd_e30 * state.maximum_positive_impact_factor // FLOAT,
    )
    impact += state.pending_impact_amount * (
        state.index_price.minimum if state.pending_impact_amount > 0 else state.index_price.maximum
    )
    maximum_loss = state.size_usd_e30 * state.maximum_liquidation_impact_factor // FLOAT
    return max(-maximum_loss, min(0, impact))


def position_risk(state):
    impact = risk_impact(state)
    remaining = (
        (state.collateral_amount - state.close_cost_amount) * state.collateral_price.minimum
        + state.capped_pnl_usd_e30
        + impact
    )
    minimum = state.minimum_collateral_usd_e30 if state.validate_minimum_collateral else 0
    leverage = state.size_usd_e30 * state.minimum_collateral_factor // FLOAT
    reason: RiskReason | None = None
    if state.validate_minimum_collateral and remaining < minimum:
        reason = "min collateral"
    elif remaining <= 0:
        reason = "< 0"
    elif remaining < leverage:
        reason = "min collateral for leverage"
    return PositionRisk(
        remaining_collateral_usd_e30=remaining,
        minimum_leverage_collateral_usd_e30=leverage,
        minimum_collateral_usd_e30=minimum,
        impact_usd_e30=impact,
        reason=reason,
    )
