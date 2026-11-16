"""Fungible Curve LP shares and cash belong to the configured fund account."""

from typing import Literal

from pydantic import model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.connectors.curve.domain import Uint256
from almanak.connectors.curve.models import Target
from almanak.models import Address, AssetAmount, Block, Model


class Position(Model):
    shares: AssetAmount

    def validate_target(self, target):
        if not target.liquidity_enabled or self.shares.asset != target.pool.shares_asset:
            raise ValueError("Curve LP shares require the configured pool's liquidity authority")
        if not 0 < self.shares.amount < 2**256:
            raise ValueError("an owned Curve position requires positive LP shares")


class Ownership(Model):
    target: Target
    inventory: AssetAmounts
    position: Position | None = None
    entry_complete: bool = False

    @model_validator(mode="after")
    def scope(self):
        self.inventory.in_order(self.target.assets)
        if self.position is not None:
            self.position.validate_target(self.target)
        return self

    def amount(self, asset):
        return next(value.amount for value in self.inventory.root if value.asset == asset)


class Allowance(Model):
    asset: AssetType
    spender: Address
    amount: Uint256


class Quote(Model):
    source: Target
    block: Block
    amount_in: AssetAmount
    amount_out: AssetAmount
    calculation: Literal["contract_pool", "prepared_integer_pool"]
    inputs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def units(self):
        pair = {self.amount_in.asset, self.amount_out.asset}
        if (
            self.source.fund.chain != self.block.chain
            or len(pair) != 2
            or not pair.issubset(self.source.assets)
        ):
            raise ValueError("Curve quote must retain its source, chain and two configured assets")
        # A zero output is a measurement: the whole input produced nothing. Snapshots
        # admit it only as an exit residual, never as a quote a swap can follow.
        if self.amount_in.amount <= 0:
            raise ValueError("Curve quote requires a positive input quantity")
        return self
