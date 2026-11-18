"""Frozen position pricing inputs use GMX native token and USD precision."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_spot_types import Factor, Price
from almanak.models import Block, Model, UInt


class PositionImpactParameters(Model):
    positive_factor: UInt
    negative_factor: UInt
    positive_exponent: UInt
    negative_exponent: UInt
    maximum_positive_factor: Factor


class PositionPricingState(Model):
    market: Market
    block: Block
    index_price: Price
    impact: PositionImpactParameters
    use_open_interest_in_tokens: bool
    long_open_interest: UInt
    short_open_interest: UInt
    virtual_inventory: int | None

    @model_validator(mode="after")
    def scope(self):
        if (
            self.block.chain != self.market.chain
            or self.index_price.asset != self.market.index_asset
        ):
            raise ValueError("GMX prepared position pricing differs from its chain or index asset")
        return self


class PositionExecutionPrice(Model):
    price_impact_usd_e30: int
    price_impact_amount: int
    base_size_delta_tokens: UInt
    execution_price: UInt
    balance_improved: bool


class ImpactSettlementState(Model):
    pricing: PositionPricingState
    maximum_negative_factor: Factor
    impact_pool_amount: UInt
    pool_usd_e30: UInt
    maximum_lendable_factor: Factor
    maximum_lendable_usd_e30: UInt
    lent_impact_amount: UInt


class RealizedImpact(Model):
    proportional_pending_usd_e30: int
    total_usd_e30: int
    claimable_difference_usd_e30: UInt
