"""Explicit sample resolution and bounded tick coverage for concentrated pool models."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.strategy_prices import StrategyPriceData
from almanak.connectors.concentrated_pool import TickIndex
from almanak.connectors.identifiers import Name
from almanak.models import Model, UInt
from almanak.triggers import PositiveSeconds


class Coverage(Model):
    allocation: Name
    lower: TickIndex
    upper: TickIndex

    @model_validator(mode="after")
    def ordered(self):
        if self.lower >= self.upper:
            raise ValueError("prepared tick coverage must be increasing")
        return self


class PoolDataPolicy(StrategyPriceData):
    sample_seconds: PositiveSeconds
    max_age_seconds: PositiveSeconds
    max_samples: Annotated[UInt, Field(gt=0)]
    max_bitmap_words: Annotated[UInt, Field(gt=0)]
    max_initialized_ticks: Annotated[UInt, Field(gt=0)]
    coverage: Annotated[tuple[Coverage, ...], Field(min_length=1)]

    @model_validator(mode="before")
    @classmethod
    def arrays(cls, value):
        if isinstance(value, dict):
            value = dict(value)
            for name in ("coverage",):
                if isinstance(value.get(name), list):
                    value[name] = tuple(value[name])
        return value

    @model_validator(mode="after")
    def distinct(self):
        if len({c.allocation for c in self.coverage}) != len(self.coverage):
            raise ValueError("pool coverage requires distinct allocation identities")
        if self.max_age_seconds < self.sample_seconds:
            raise ValueError("maximum age cannot be shorter than pool sampling interval")
        return self

    def for_target(self, target):
        for item in self.coverage:
            if item.allocation == target.allocation:
                return item
        raise ValueError("pool has no explicit prepared tick coverage")
