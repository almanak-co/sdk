"""Explicit GMX exchange inventory, request fees and asset-denominated economic amounts."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.connectors.gmx_v2.exchange_domain import ExchangeMarket, ExchangeTarget
from almanak.connectors.identifiers import Name
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount, Model, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION, PositiveSeconds

Bound = Annotated[UInt, Field(lt=2**128)]


def input_assets(market):
    return tuple(dict.fromkeys((market.long_asset, market.short_asset, market.wrapped_native)))


def validate_inputs(market, amounts):
    amounts.in_order(input_assets(market))


class ExchangeAllocation(Model):
    name: Name
    account: Name
    market: ExchangeMarket
    initial_inventory: AssetAmounts
    execution_fee: AssetAmount
    hold_seconds: PositiveSeconds | None = Field(
        default=None, exclude_if=lambda v: v is None, description=HOLD_SECONDS_DESCRIPTION
    )
    deadline_seconds: PositiveSeconds
    maximum_event_block_span: Annotated[UInt, Field(gt=0)]
    native_asset: AssetType
    minimum_native_balance: UInt

    @property
    def chain(self):
        return self.market.chain

    @model_validator(mode="after")
    def inventory_scope(self):
        validate_inputs(self.market, self.initial_inventory)
        return self

    @model_validator(mode="after")
    def fee_scope(self):
        if self.execution_fee.asset != self.market.wrapped_native:
            raise ValueError("GMX exchange request fee requires explicit wrapped-native units")
        fee_inventory = next(
            value.amount
            for value in self.initial_inventory.root
            if value.asset == self.execution_fee.asset
        )
        if not 0 < self.execution_fee.amount < min(fee_inventory, 2**128):
            raise ValueError("GMX exchange inventory must cover a positive bounded request fee")
        if self.native_asset != native_asset(self.market.chain.chain_id):
            raise ValueError("GMX exchange payer reserve must identify the chain native asset")
        return self


class SwapAllocation(ExchangeAllocation):
    kind: Literal["gmx_swap"] = "gmx_swap"
    entry_amount: AssetAmount
    entry_minimum_output: AssetAmount
    exit_minimum_output: AssetAmount

    @property
    def funding_asset(self):
        return self.entry_amount.asset

    @model_validator(mode="after")
    def swap_scope(self):
        assets = self.market.long_asset, self.market.short_asset
        if {self.entry_amount.asset, self.entry_minimum_output.asset} != set(
            assets
        ) or self.exit_minimum_output.asset != self.entry_amount.asset:
            raise ValueError(
                "GMX swap reference requires opposite market assets and return to its entry asset"
            )
        if (
            min(
                self.entry_amount.amount,
                self.entry_minimum_output.amount,
                self.exit_minimum_output.amount,
            )
            <= 0
        ):
            raise ValueError("GMX swap reference requires positive entry and output limits")
        initial = next(
            value.amount
            for value in self.initial_inventory.root
            if value.asset == self.entry_amount.asset
        )
        fee = (
            self.execution_fee.amount if self.entry_amount.asset == self.execution_fee.asset else 0
        )
        if self.entry_amount.amount + fee > initial:
            raise ValueError("GMX swap entry and fee exceed explicit initial inventory")
        return self


class ExchangeCapability(Model):
    target: ExchangeTarget
    maximum_execution_fee: Annotated[Bound, Field(gt=0)]
