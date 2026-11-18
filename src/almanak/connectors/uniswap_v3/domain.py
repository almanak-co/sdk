"""uniswap_v3 market identity and protocol-specific fee encoding."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.v3_pool_domain import ConcentratedPool, Observation, PriceRequest
from almanak.connectors.v3_pool_domain import Contracts as Contracts
from almanak.connectors.v3_pool_domain import Liquidity as Liquidity
from almanak.connectors.v3_pool_domain import Tick as Tick
from almanak.connectors.v3_pool_domain import TickRange as TickRange
from almanak.connectors.v3_pool_domain import Uint256 as Uint256
from almanak.models import UInt


class UniswapPool(ConcentratedPool):
    kind: Literal["uniswap_v3"]


class PoolPriceRequest(PriceRequest[UniswapPool]):
    pass


class PoolObservation(Observation[UniswapPool]):
    protocol_fee: Annotated[UInt, Field(lt=256)]

    @model_validator(mode="after")
    def fee_encoding(self):
        from almanak.connectors.uniswap_v3.swap_math import protocol_fee_divisor

        protocol_fee_divisor(self.protocol_fee, True)
        return self
