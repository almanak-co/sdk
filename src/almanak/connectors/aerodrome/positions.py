"""Fungible LP ownership retains principal shares and independent fee obligations."""

from typing import Literal

from pydantic import model_validator

from almanak.caip import AssetType
from almanak.connectors.aerodrome.domain import Uint256
from almanak.connectors.aerodrome.models import Target
from almanak.models import Address, AssetAmount, Block, Model


class Position(Model):
    """Stored fund claimables and supply indices, never a synthetic accrual checkpoint."""

    shares: AssetAmount
    fees0: AssetAmount
    fees1: AssetAmount
    index0: Uint256
    index1: Uint256

    def validate_target(self, target):
        if not target.liquidity_enabled or self.shares.asset != target.pool.shares_asset:
            raise ValueError("owned LP shares require the configured pool's liquidity authority")
        if (self.fees0.asset, self.fees1.asset) != target.assets:
            raise ValueError("LP fees must retain the configured currency order")

    @property
    def empty(self):
        return not (self.shares.amount or self.fees0.amount or self.fees1.amount)


class Ownership(Model):
    target: Target
    inventory0: AssetAmount
    inventory1: AssetAmount
    position: Position | None = None
    entry_complete: bool = False

    @model_validator(mode="after")
    def scope(self):
        if (self.inventory0.asset, self.inventory1.asset) != self.target.assets:
            raise ValueError("inventory must retain the configured currency order")
        if self.position is not None:
            self.position.validate_target(self.target)
        return self


class Allowance(Model):
    asset: AssetType
    spender: Address
    amount: Uint256


class Quote(Model):
    source: Target
    block: Block
    amount_in: AssetAmount
    amount_out: AssetAmount
    calculation: Literal["contract_router", "prepared_integer_pool"]
    inputs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def units(self):
        if self.source.fund.chain != self.block.chain or {
            self.amount_in.asset,
            self.amount_out.asset,
        } != set(self.source.assets):
            raise ValueError("quote must retain its exact source, execution chain and units")
        # A zero output is a measurement: the whole input produced nothing. Snapshots
        # admit it only as an exit residual, never as a quote a swap can follow.
        if self.amount_in.amount <= 0:
            raise ValueError("quote requires a positive measured input")
        return self
