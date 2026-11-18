"""Frozen market inputs retain native units, oracle bounds and inventory impact state."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.models import Block, Model, UInt

Factor = Annotated[UInt, Field(le=FLOAT)]


class SwapConstraint(ValueError):
    """A modeled contract bound rejects the requested fill after valid Oracle inputs."""


class Price(Model):
    asset: AssetType
    minimum: Annotated[UInt, Field(gt=0)]
    maximum: Annotated[UInt, Field(gt=0)]

    @property
    def midpoint(self):
        return (self.minimum + self.maximum) // 2

    @model_validator(mode="after")
    def ordered(self):
        if self.minimum > self.maximum:
            raise ValueError("GMX oracle minimum exceeds maximum")
        return self


class Currency(Model):
    price: Price
    pool_amount: UInt
    impact_pool_amount: UInt


class ImpactParameters(Model):
    positive_factor: UInt
    negative_factor: UInt
    exponent: UInt


class FeeParameters(Model):
    positive_factor: Factor
    negative_factor: Factor
    receiver_factor: Factor
    ui_factor: Factor


class SpotState(Model):
    pricing_type: Literal["SWAP", "DEPOSIT", "WITHDRAWAL"]
    market: Market
    block: Block
    long: Currency
    short: Currency
    impact: ImpactParameters
    fees: FeeParameters
    virtual_inventory: tuple[UInt, UInt] | None

    @model_validator(mode="after")
    def scope(self):
        if (
            self.block.chain != self.market.chain
            or self.long.price.asset != self.market.long_asset
            or self.short.price.asset != self.market.short_asset
        ):
            raise ValueError("GMX prepared spot state differs from its exact chain and currencies")
        return self


class Impact(Model):
    usd_e30: int
    balance_improved: bool


class Fees(Model):
    receiver_amount: UInt
    pool_amount: UInt
    ui_amount: UInt
    after_fees: UInt


class SwapCalculation(Model):
    asset_in: AssetType
    asset_out: AssetType
    amount_in: UInt
    amount_out: UInt
    price_impact_usd_e30: int
    impact_amount: int
    input_impact_amount: UInt
    pool_amount_out: UInt
    fees: Fees
