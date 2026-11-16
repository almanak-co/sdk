"""Explicit prepared V3 sources, coverage, continuity limits and economic costs."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.inventory import InventoryConfig
from almanak.connectors.concentrated_policy import PoolDataPolicy
from almanak.models import Model, UInt

# Banded allocations credit the fee growth continuously initialized ticks prove accrued
# inside each concrete range and report the upper bracket; fixed ranges stay exact.
BAND_LIQUIDITY_MODEL = "bracketed_historical_fee_growth"


class DataPolicy(PoolDataPolicy):
    max_liquidity_events: Annotated[UInt, Field(gt=0)]
    max_liquidity_log_requests: Annotated[UInt, Field(gt=0)]


class Costs(Model):
    kind: Literal["fixed_native"]
    swap: UInt
    mint: UInt
    increase: UInt
    decrease: UInt
    collect: UInt
    close: UInt
    revoke: UInt


class EconomicConfig[D: DataPolicy](InventoryConfig):
    market_model: Literal["economic"]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: (
        Literal["fixed_historical_fee_growth", "bracketed_historical_fee_growth"] | None
    )
    costs: Annotated[ExcludedCosts | Costs, Field(discriminator="kind")]
    data: D

    @model_validator(mode="after")
    def bounds(self):
        if not self.payers and any(f.profile.submitter != f.fund for f in self.funds):
            raise ValueError("economic observations require explicit external payer inventory")
        count = (self.schedule.deadline - self.schedule.start) // self.data.sample_seconds + 1
        if count * len(self.data.coverage) > self.data.max_samples:
            raise ValueError("requested pool sampling exceeds explicit max_samples")
        return self
