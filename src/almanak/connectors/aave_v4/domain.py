"""Connector-owned protocol identities and pinned observations."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.gateway.evm import erc20_address
from almanak.models import ZERO_ADDRESS, Account, Address, Asset, Block, Chain, Model, UInt


class AaveReserve(Model):
    kind: Literal["aave_v4"]
    chain: Chain
    spoke: Address
    reserve_id: Annotated[UInt, Field(lt=2**256)]
    hub: Address
    asset: AssetType

    @model_validator(mode="after")
    def contracts(self) -> Self:
        erc20_address(self.asset, self.chain.chain_id)
        if ZERO_ADDRESS in (self.spoke, self.hub):
            raise ValueError("Aave reserve references must be contract addresses")
        return self


class ReserveObservation(Model):
    source: AaveReserve
    block: Block
    asset: Asset
    hub_asset_id: UInt
    collateral_risk_bps: UInt
    dynamic_config_key: UInt
    paused: bool
    frozen: bool
    borrowable: bool
    receive_shares_enabled: bool

    @model_validator(mode="after")
    def identity(self) -> Self:
        if self.source.chain != self.block.chain or self.asset.chain != self.block.chain:
            raise ValueError("reserve observation has mixed chain/environment scope")
        if self.asset.identifier != self.source.asset:
            raise ValueError("reserve observation asset identity mismatch")
        return self


class PositionObservation(Model):
    reserve: ReserveObservation
    account: Account
    supplied_assets: UInt
    supplied_shares: UInt
    drawn_debt: UInt
    premium_debt: UInt
    total_debt: UInt

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.account.chain != self.reserve.block.chain:
            raise ValueError("position account has a different chain/environment")
        if self.total_debt != self.drawn_debt + self.premium_debt:
            raise ValueError("protocol debt components do not reconcile")
        return self
