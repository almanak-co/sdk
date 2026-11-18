"""Explicit SparkLend sample cadence, per-block risk acquisition bounds and cost policy."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.inventory import InventoryConfig
from almanak.backtest.strategy_prices import StrategyPriceData
from almanak.models import Model, UInt
from almanak.triggers import PositiveSeconds


class DataPolicy(StrategyPriceData):
    source: Literal["sparklend_archive"]
    risk_model: Literal["pool_oracle_each_block"]
    sample_seconds: PositiveSeconds
    max_age_seconds: PositiveSeconds
    max_accounting_samples: Annotated[UInt, Field(gt=0)]
    max_risk_blocks: Annotated[UInt, Field(gt=0)]
    max_risk_samples: Annotated[UInt, Field(gt=0)]
    max_liquidations_per_event: Annotated[UInt, Field(gt=0)]

    @model_validator(mode="after")
    def age(self):
        if self.max_age_seconds < self.sample_seconds:
            raise ValueError("maximum source age cannot be shorter than accounting sampling")
        return self


class Costs(Model):
    kind: Literal["fixed_native"]
    supply: UInt
    withdraw: UInt
    borrow: UInt
    repay: UInt
    collateral: UInt


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    data: DataPolicy
    costs: Annotated[ExcludedCosts | Costs, Field(discriminator="kind")]
    liquidation_model: Literal["immediate_underlying_sparklend_revision4"]

    @model_validator(mode="after")
    def payer_costs(self):
        if not self.payers and any(f.profile.submitter != f.fund for f in self.funds):
            raise ValueError("economic observations require explicit external payer inventory")
        return self
