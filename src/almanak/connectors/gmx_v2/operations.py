"""Concrete local GMX calls remain separate from economic intent and keeper settlement."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.exchange_operations import (
    CancelExchangeOrder,
    CreateSwap,
    ExchangeApproval,
)
from almanak.connectors.gmx_v2.liquidity_operations import (
    CancelLiquidityRequest,
    CreateDeposit,
    CreateWithdrawal,
)
from almanak.models import AssetAmount, Hash32, Model, UInt


class Operation(Model):
    target: Target

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def account(self):
        return self.target.account


class Approval(Operation):
    kind: Literal["gmx_approval"] = "gmx_approval"
    amount: AssetAmount

    @model_validator(mode="after")
    def asset_scope(self):
        if self.amount.asset != self.target.market.wrapped_native:
            raise ValueError("GMX approval must identify the configured wrapped-native input")
        return self


class CreateOrder(Operation):
    kind: Literal["gmx_create_order"] = "gmx_create_order"
    order_type: Literal[2, 4]
    collateral_delta: AssetAmount
    size_delta_usd_e30: UInt
    execution_fee: AssetAmount
    acceptable_price: UInt

    @model_validator(mode="after")
    def units(self):
        if self.collateral_delta.asset != self.target.collateral_asset:
            raise ValueError("GMX order collateral differs from its exact target")
        if (
            self.execution_fee.asset != self.target.market.wrapped_native
            or not self.execution_fee.amount
        ):
            raise ValueError("GMX order requires a nonzero wrapped-native execution fee")
        return self


class CancelOrder(Operation):
    kind: Literal["gmx_cancel_order"] = "gmx_cancel_order"
    request_key: Hash32


class ClaimFunding(Operation):
    kind: Literal["gmx_claim_funding"] = "gmx_claim_funding"
    amounts: tuple[AssetAmount, ...]

    @model_validator(mode="after")
    def scope(self):
        expected = (self.target.market.long_asset, self.target.market.short_asset)
        if tuple(value.asset for value in self.amounts) != expected:
            raise ValueError("GMX funding claim must cover both configured market assets in order")
        if not any(value.amount for value in self.amounts):
            raise ValueError("GMX funding cleanup requires a nonzero owned entitlement")
        return self


ProtocolOperation = Annotated[
    Approval
    | CreateOrder
    | CancelOrder
    | ClaimFunding
    | ExchangeApproval
    | CreateSwap
    | CancelExchangeOrder
    | CreateDeposit
    | CreateWithdrawal
    | CancelLiquidityRequest,
    Field(discriminator="kind"),
]
