"""GMX aggregate position state retains collateral, index units and fee accumulators."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Target
from almanak.models import AssetAmount, Block, Hash32, Model, UInt


class Position(Model):
    key: Hash32
    size_usd_e30: UInt
    index_amount: AssetAmount
    collateral: AssetAmount
    pending_impact_amount: int
    borrowing_factor: UInt
    funding_fee_amount_per_size: UInt
    long_claimable_funding_amount_per_size: UInt
    short_claimable_funding_amount_per_size: UInt
    increased_at: UInt
    decreased_at: UInt


class PositionObservation(Model):
    target: Target
    block: Block
    position: Position | None

    @model_validator(mode="after")
    def scope(self):
        if self.block.chain != self.target.fund.chain:
            raise ValueError("GMX position observation differs from its fund environment")
        if self.position is not None and (
            self.position.index_amount.asset,
            self.position.collateral.asset,
        ) != (self.target.market.index_asset, self.target.collateral_asset):
            raise ValueError("GMX position amounts differ from their configured assets")
        return self


class Ownership(Model):
    target: Target
    inventory: tuple[AssetAmount, ...]
    position: Position | None = None
    claimable_funding: tuple[AssetAmount, ...] = ()

    @model_validator(mode="after")
    def inventory_scope(self):
        if len({value.asset for value in self.inventory}) != len(self.inventory):
            raise ValueError("GMX owned inventory cannot duplicate an asset")
        if any(value.asset.chain != self.target.fund.chain.chain_id for value in self.inventory):
            raise ValueError("GMX owned inventory must retain the fund chain")
        return self

    @model_validator(mode="after")
    def funding_scope(self):
        assets = tuple(value.asset for value in self.claimable_funding)
        allowed = (self.target.market.long_asset, self.target.market.short_asset)
        if len(assets) != len(set(assets)) or any(asset not in allowed for asset in assets):
            raise ValueError("GMX owned funding must identify unique configured market assets")
        return self
