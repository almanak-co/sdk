"""Admitted GM requests separate market-currency escrow from fungible share escrow."""

from typing import Literal

from pydantic import model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.gmx_v2.exchange_models import input_assets
from almanak.connectors.gmx_v2.exchange_operations import ExchangeOperation
from almanak.connectors.gmx_v2.liquidity_models import validate_shares
from almanak.models import AssetAmount, Hash32, UInt


class CreateLiquidity(ExchangeOperation):
    execution_fee: AssetAmount
    admission_deadline: UInt

    @property
    def inputs(self) -> tuple[AssetAmount, ...]:
        raise NotImplementedError("GM escrow inputs belong to the concrete request kind")

    @model_validator(mode="after")
    def fee_scope(self):
        if (
            self.execution_fee.asset != self.target.market.wrapped_native
            or self.execution_fee.amount <= 0
        ):
            raise ValueError("GM request requires a positive explicit wrapped-native keeper fee")
        return self


class CreateDeposit(CreateLiquidity):
    kind: Literal["gmx_create_deposit"] = "gmx_create_deposit"
    amounts: AssetAmounts
    minimum_market_tokens: AssetAmount

    @property
    def request_kind(self):
        return "deposit"

    @property
    def inputs(self):
        amounts = {value.asset: value.amount for value in self.amounts.root}
        fee = self.execution_fee
        amounts[fee.asset] = amounts.get(fee.asset, 0) + fee.amount
        return tuple(
            AssetAmount(asset=asset, amount=amounts.get(asset, 0))
            for asset in input_assets(self.target.market)
        )

    @model_validator(mode="after")
    def deposit_scope(self):
        self.amounts.in_order(self.target.assets)
        if not any(value.amount for value in self.amounts.root):
            raise ValueError("GM deposit requires positive configured market-currency input")
        validate_shares(self.target.market, self.minimum_market_tokens)
        return self


class CreateWithdrawal(CreateLiquidity):
    kind: Literal["gmx_create_withdrawal"] = "gmx_create_withdrawal"
    market_tokens: AssetAmount
    minimum_outputs: AssetAmounts

    @property
    def request_kind(self):
        return "withdrawal"

    @property
    def inputs(self):
        return self.market_tokens, self.execution_fee

    @model_validator(mode="after")
    def withdrawal_scope(self):
        validate_shares(self.target.market, self.market_tokens)
        self.minimum_outputs.in_order(self.target.assets)
        return self


class CancelLiquidityRequest(ExchangeOperation):
    kind: Literal["gmx_cancel_liquidity"] = "gmx_cancel_liquidity"
    request_kind: Literal["deposit", "withdrawal"]
    request_key: Hash32
