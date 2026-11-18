"""Explicit offline pool resolution, coverage bounds and economic operation costs."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.connectors.concentrated_policy import Coverage as Coverage
from almanak.connectors.concentrated_policy import PoolDataPolicy as PoolDataPolicy
from almanak.connectors.uniswap_v4.backtest_config import InventoryConfig
from almanak.models import Model, UInt


class EconomicCosts(Model):
    kind: Literal["fixed_native"]
    swap: UInt
    mint: UInt
    collect: UInt
    burn: UInt
    revoke: UInt


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    swap_model: Literal["prepared_integer_pool"]
    liquidity_model: (
        Literal["fixed_historical_fee_growth", "bracketed_historical_fee_growth"] | None
    ) = Field(default=None, exclude_if=lambda value: value is None)
    costs: Annotated[ExcludedCosts | EconomicCosts, Field(discriminator="kind")]
    data: PoolDataPolicy

    @model_validator(mode="after")
    def sample_budget(self):
        if not isinstance(self.costs, ExcludedCosts) and not self.payers:
            raise ValueError("fixed costs require explicit payer inventory")
        count = (self.schedule.deadline - self.schedule.start) // self.data.sample_seconds + 1
        if count * len(self.data.coverage) > self.data.max_samples:
            raise ValueError(
                f"pool acquisition requires {count * len(self.data.coverage)} samples; "
                f"configured max_samples is {self.data.max_samples}. "
                "Increase the explicit limit or choose a different window or sampling interval."
            )
        return self
