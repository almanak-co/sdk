"""Curve approvals, pool calls and fungible share burns retain economic scope."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.connectors.curve.domain import Uint256
from almanak.connectors.curve.models import Target
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
        if self.asset not in self.target.assets or self.spender != self.target.pool.pool:
            raise ValueError("Curve approvals require a configured coin and its exact pool")
        return self


class Swap(Operation):
    kind: Literal["swap"] = "swap"
    amount: AssetAmount
    minimum: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def scope(self):
        pair = {self.amount.asset, self.minimum.asset}
        if len(pair) != 2 or not pair.issubset(self.target.assets):
            raise ValueError("Curve swap input and output must be different configured coins")
        if min(self.amount.amount, self.minimum.amount) <= 0:
            raise ValueError("Curve swaps require positive input and minimum output")
        return self


class Addition(Operation):
    kind: str
    amounts: AssetAmounts
    minimum_shares: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def scope(self):
        self.amounts.in_order(self.target.assets)
        self.amounts.require_inventory()
        validate_shares(self.target, self.minimum_shares)
        return self


class Mint(Addition):
    kind: Literal["mint"] = "mint"


class Increase(Addition):
    kind: Literal["increase"] = "increase"


class Removal(Operation):
    kind: str
    shares: AssetAmount
    minimums: AssetAmounts
    output_asset: AssetType | None = None
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        validate_shares(self.target, self.shares)
        self.minimums.in_order(self.target.assets)
        if self.output_asset is not None:
            if self.output_asset != self.target.single_coin_exit:
                raise ValueError("Curve single-coin output requires explicit configured authority")
            if any(
                value.amount for value in self.minimums.root if value.asset != self.output_asset
            ):
                raise ValueError("single-coin removal cannot require output in other assets")
        return self


class Decrease(Removal):
    kind: Literal["decrease"] = "decrease"


class Close(Removal):
    kind: Literal["close"] = "close"


def validate_shares(target, shares):
    if (
        not target.liquidity_enabled
        or shares.asset != target.pool.shares_asset
        or not 0 < shares.amount < 2**256
    ):
        raise ValueError("Curve liquidity operation requires positive configured LP shares")


ProtocolOperation = Annotated[
    Approval | Swap | Mint | Increase | Decrease | Close,
    Field(discriminator="kind"),
]
