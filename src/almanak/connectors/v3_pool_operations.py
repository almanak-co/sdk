"""V3 calls settle decreases atomically with collection before economic completion."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetId, AssetType
from almanak.connectors.v3_pool_domain import Liquidity, TickRange, Uint256
from almanak.connectors.v3_pool_models import Target
from almanak.connectors.v3_pool_positions import validate_position_range
from almanak.models import Address, AssetAmount, Model, UInt


class Operation[T: Target](Model):
    target: T

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def account(self):
        return self.target.account


class Approval[T: Target](Operation[T]):
    kind: Literal["approve"] = "approve"
    asset: AssetType
    spender: Address
    amount: Uint256

    @model_validator(mode="after")
    def scope(self):
        if self.asset not in self.target.assets or self.spender not in (
            self.target.contracts.router,
            self.target.contracts.position_manager,
        ):
            raise ValueError("approval requires a configured pool currency and periphery spender")
        return self


class Swap[T: Target](Operation[T]):
    kind: Literal["swap"] = "swap"
    amount: AssetAmount
    minimum: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def scope(self):
        if {self.amount.asset, self.minimum.asset} != set(self.target.assets):
            raise ValueError("swap input and minimum must identify the two pool currencies")
        if min(self.amount.amount, self.minimum.amount) <= 0:
            raise ValueError("swap requires positive input and minimum output")
        return self


class Addition[T: Target](Operation[T]):
    kind: str
    maximum0: AssetAmount
    maximum1: AssetAmount
    minimum0: AssetAmount
    minimum1: AssetAmount
    minimum_liquidity: Liquidity
    deadline: UInt
    # A banded target's concrete range travels with the operation so calldata, receipts
    # and reconciliation compare against the same ticks; fixed targets leave it absent.
    range: TickRange | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        validate_amounts(self.target, (self.maximum0, self.maximum1))
        validate_amounts(self.target, (self.minimum0, self.minimum1))
        if not self.target.liquidity_enabled or not self.minimum_liquidity:
            raise ValueError("liquidity addition requires a range and positive minimum liquidity")
        validate_position_range(self.target, self.range)
        if (
            self.minimum0.amount > self.maximum0.amount
            or self.minimum1.amount > self.maximum1.amount
        ):
            raise ValueError("liquidity minima cannot exceed supplied budgets")
        return self


class Mint[T: Target](Addition[T]):
    kind: Literal["mint"] = "mint"


class Increase[T: Target](Addition[T]):
    kind: Literal["increase"] = "increase"
    nft: AssetId

    @model_validator(mode="after")
    def identity(self):
        validate_nft(self.target, self.nft)
        return self


class NftOperation[T: Target](Operation[T]):
    nft: AssetId
    range: TickRange | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def identity(self):
        validate_nft(self.target, self.nft)
        validate_position_range(self.target, self.range)
        return self


class Collect[T: Target](NftOperation[T]):
    kind: Literal["collect"] = "collect"
    deadline: UInt


class Removal[T: Target](NftOperation[T]):
    kind: str
    liquidity: Annotated[Liquidity, Field(gt=0)]
    minimum0: AssetAmount
    minimum1: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        validate_amounts(self.target, (self.minimum0, self.minimum1))
        return self


class Decrease[T: Target](Removal[T]):
    kind: Literal["decrease"] = "decrease"


class Close[T: Target](Removal[T]):
    kind: Literal["close"] = "close"


def validate_amounts(target, amounts):
    if tuple(value.asset for value in amounts) != target.assets:
        raise ValueError("liquidity quantities must retain the configured currency order")


def validate_nft(target, nft):
    expected = AssetType(f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}")
    if nft.asset_type != expected or not target.liquidity_enabled:
        raise ValueError("NFT operation requires the exact configured manager and range")


def operation_range(operation):
    """The concrete ticks a liquidity operation acts on, or None for a trading target."""
    selected = getattr(operation, "range", None)
    return selected if selected is not None else operation.target.range
