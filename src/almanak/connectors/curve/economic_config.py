"""Explicit archive sampling and fixed external share/fee-index assumptions."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.inventory import InventoryConfig
from almanak.backtest.strategy_prices import StrategyPriceData
from almanak.models import Model, UInt
from almanak.triggers import PositiveSeconds


class DataPolicy(StrategyPriceData):
    source: Literal["curve_archive"]
    sample_seconds: PositiveSeconds
    max_age_seconds: PositiveSeconds
    max_samples: Annotated[UInt, Field(gt=0)]
    max_parameter_events: Annotated[UInt, Field(gt=0)]
    max_parameter_log_requests: Annotated[UInt, Field(gt=0)]

    @model_validator(mode="after")
    def age(self):
        if self.max_age_seconds < self.sample_seconds:
            raise ValueError("maximum pool age cannot be shorter than its sampling interval")
        return self


class Costs(Model):
    kind: Literal["fixed_native"]
    swap: UInt
    mint: UInt
    increase: UInt
    decrease: UInt
    close: UInt
    revoke: UInt


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: Literal["fixed_external_unit_balances"] | None
    costs: Annotated[ExcludedCosts | Costs, Field(discriminator="kind")]
    data: DataPolicy

    @model_validator(mode="after")
    def payer_scope(self):
        if not self.payers and any(f.profile.submitter != f.fund for f in self.funds):
            raise ValueError("economic observations require explicit external payer inventory")
        return self
