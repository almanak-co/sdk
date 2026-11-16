"""Admitted exchange requests retain escrow, output limits and admission deadlines."""

from typing import Literal

from pydantic import model_validator

from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget, Swap
from almanak.connectors.gmx_v2.exchange_models import input_assets
from almanak.models import AssetAmount, Hash32, Model, UInt


class ExchangeOperation(Model):
    target: ExchangeTarget

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def account(self):
        return self.target.account


class ExchangeApproval(ExchangeOperation):
    kind: Literal["gmx_exchange_approval"] = "gmx_exchange_approval"
    amount: AssetAmount

    @model_validator(mode="after")
    def scope(self):
        assets = (*input_assets(self.target.market), self.target.market.share_asset)
        if self.amount.asset not in assets:
            raise ValueError("GMX exchange approval requires a configured input or GM share asset")
        return self


class CreateSwap(ExchangeOperation):
    kind: Literal["gmx_create_swap"] = "gmx_create_swap"
    amount: AssetAmount
    minimum_output: AssetAmount
    execution_fee: AssetAmount
    admission_deadline: UInt

    @model_validator(mode="after")
    def units(self):
        Swap(
            target=self.target,
            amount=self.amount,
            minimum_output=self.minimum_output,
            deadline=self.admission_deadline,
        )
        if (
            self.execution_fee.asset != self.target.market.wrapped_native
            or self.execution_fee.amount <= 0
        ):
            raise ValueError("GMX exchange request requires a positive wrapped-native fee")
        return self

    @property
    def inputs(self):
        if self.amount.asset == self.execution_fee.asset:
            return (
                self.amount.model_copy(
                    update={"amount": self.amount.amount + self.execution_fee.amount}
                ),
            )
        return self.amount, self.execution_fee


class CancelExchangeOrder(ExchangeOperation):
    kind: Literal["gmx_cancel_exchange_order"] = "gmx_cancel_exchange_order"
    request_key: Hash32
