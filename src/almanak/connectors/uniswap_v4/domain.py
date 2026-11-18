"""Connector-owned protocol identities and pinned observations."""

from fractions import Fraction
from typing import Annotated, Literal, Self

from pydantic import AfterValidator, Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.uniswap_v4.hooks import DYNAMIC_FEE, admit_key
from almanak.gateway.evm import currency_address
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Chain, Model, UInt


def validate_fee(fee: int) -> int:
    if fee > 1_000_000 and fee != DYNAMIC_FEE:
        raise ValueError("PoolKey fee must be at most 1000000 pips or the dynamic-fee flag")
    return fee


# A static fee in pips, or exactly the dynamic-fee flag a reviewed hook profile requires.
PoolFee = Annotated[int, Field(strict=True, ge=0, le=DYNAMIC_FEE), AfterValidator(validate_fee)]


class UniswapPool(Model):
    kind: Literal["uniswap_v4"]
    chain: Chain
    pool_manager: Address
    state_view: Address
    currency0: AssetType
    currency1: AssetType
    fee: PoolFee
    tick_spacing: Annotated[int, Field(strict=True, ge=1, le=32767)]
    hooks: Address

    @model_validator(mode="after")
    def supported_pool(self) -> Self:
        if currency_address(self.currency0, self.chain.chain_id) >= currency_address(
            self.currency1, self.chain.chain_id
        ):
            raise ValueError("PoolKey currencies must be distinct and in canonical address order")
        admit_key(self.chain.chain_id, self.fee, self.hooks)
        if ZERO_ADDRESS in (self.pool_manager, self.state_view):
            raise ValueError("pool manager and state view must be contract addresses")
        return self


class PoolPriceRequest(Model):
    source: UniswapPool
    base: AssetType
    quote: AssetType

    @model_validator(mode="after")
    def pair(self) -> Self:
        if self.base == self.quote or {self.base, self.quote} != {
            self.source.currency0,
            self.source.currency1,
        }:
            raise ValueError("base and quote must be the two currencies of the requested pool")
        return self


class PoolObservation(Model):
    source: UniswapPool
    block: Block
    currency0: Asset
    currency1: Asset
    sqrt_price_x96: Annotated[int, Field(strict=True, gt=0, lt=2**160)]
    tick: Annotated[int, Field(strict=True, ge=-887272, le=887272)]
    liquidity: UInt
    protocol_fee: UInt
    lp_fee: UInt

    @model_validator(mode="after")
    def identity(self) -> Self:
        if (
            not self.source.chain
            == self.block.chain
            == self.currency0.chain
            == self.currency1.chain
        ):
            raise ValueError("pool observation has mixed chain/environment scope")
        if (self.currency0.identifier, self.currency1.identifier) != (
            self.source.currency0,
            self.source.currency1,
        ):
            raise ValueError("pool observation asset identity mismatch")
        return self

    def spot_price(self, request: PoolPriceRequest) -> Fraction:
        if request.source != self.source:
            raise ValueError("observation is not from the requested source")
        if self.liquidity == 0:
            raise ValueError(
                "pool has no active liquidity; its stored price is not a trading input"
            )
        ratio = Fraction(self.sqrt_price_x96**2, 2**192)
        ratio *= Fraction(10**self.currency0.decimals, 10**self.currency1.decimals)
        return ratio if request.base == self.currency0.identifier else 1 / ratio
