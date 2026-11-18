"""Spacing identifies Slipstream pools; mutable fees belong to pinned observations."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.v3_pool_domain import (
    ConcentratedMarket,
    ConcentratedObservation,
    PriceRequest,
)
from almanak.connectors.v3_pool_domain import Contracts as Contracts
from almanak.connectors.v3_pool_domain import Liquidity as Liquidity
from almanak.connectors.v3_pool_domain import Tick as Tick
from almanak.connectors.v3_pool_domain import TickRange as TickRange
from almanak.connectors.v3_pool_domain import Uint256 as Uint256
from almanak.models import Address, UInt


class SlipstreamPool(ConcentratedMarket):
    kind: Literal["aerodrome_slipstream"]
    tick_spacing: Annotated[int, Field(strict=True, ge=1, lt=16384)]


class PoolPriceRequest(PriceRequest[SlipstreamPool]):
    pass


class PoolObservation(ConcentratedObservation[SlipstreamPool]):
    unlocked: bool
    swap_fee: Annotated[UInt, Field(le=100_000)]
    unstaked_fee: Annotated[UInt, Field(le=1_000_000)]
    staked_liquidity: Liquidity
    gauge: Address
    nft_manager: Address
    swap_fee_module: Address
    unstaked_fee_module: Address

    @property
    def is_unlocked(self) -> bool:
        return self.unlocked

    @model_validator(mode="after")
    def staked_scope(self):
        if self.staked_liquidity > self.liquidity:
            raise ValueError("Slipstream staked liquidity exceeds total active liquidity")
        return self
