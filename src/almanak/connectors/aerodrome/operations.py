"""Aerodrome LP principal and fee claims have separate economic completion."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.aerodrome.domain import Uint256
from almanak.connectors.aerodrome.models import Target
from almanak.models import Address, AssetAmount, Model, UInt


class Operation(Model):
    target: Target

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def account(self):
        return self.target.account


class Approval(Operation):
    kind: Literal["approve"] = "approve"
    asset: AssetType
    spender: Address
    amount: Uint256

    @model_validator(mode="after")
    def scope(self):
        if (
            self.asset not in (*self.target.assets, self.target.pool.shares_asset)
            or self.spender != self.target.contracts.router
            or (
                self.asset == self.target.pool.shares_asset
                and self.amount > 0
                and not self.target.liquidity_enabled
            )
        ):
            raise ValueError("approval requires a configured pool currency and periphery spender")
        return self


class Swap(Operation):
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


class Addition(Operation):
    kind: str
    maximum0: AssetAmount
    maximum1: AssetAmount
    minimum0: AssetAmount
    minimum1: AssetAmount
    minimum_shares: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def scope(self):
        validate_amounts(self.target, (self.maximum0, self.maximum1))
        validate_amounts(self.target, (self.minimum0, self.minimum1))
        validate_shares(self.target, self.minimum_shares)
        if (
            self.minimum0.amount > self.maximum0.amount
            or self.minimum1.amount > self.maximum1.amount
        ):
            raise ValueError("liquidity minima cannot exceed supplied budgets")
        return self


class Mint(Addition):
    kind: Literal["mint"] = "mint"


class Increase(Addition):
    kind: Literal["increase"] = "increase"


class Collect(Operation):
    kind: Literal["collect"] = "collect"

    @model_validator(mode="after")
    def scope(self):
        if not self.target.liquidity_enabled:
            raise ValueError("fee claims require explicitly enabled pool liquidity")
        return self


class Removal(Operation):
    kind: str
    shares: AssetAmount
    minimum0: AssetAmount
    minimum1: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        validate_shares(self.target, self.shares)
        validate_amounts(self.target, (self.minimum0, self.minimum1))
        return self


class Decrease(Removal):
    kind: Literal["decrease"] = "decrease"


class Close(Removal):
    kind: Literal["close"] = "close"


def validate_amounts(target, amounts):
    if tuple(value.asset for value in amounts) != target.assets:
        raise ValueError("liquidity quantities must retain the configured currency order")


def validate_shares(target, shares):
    if (
        not target.liquidity_enabled
        or shares.asset != target.pool.shares_asset
        or not 0 < shares.amount < 2**256
    ):
        raise ValueError("liquidity operation requires positive configured pool shares")


ProtocolOperation = Annotated[
    Approval | Swap | Mint | Increase | Decrease | Collect | Close,
    Field(discriminator="kind"),
]
