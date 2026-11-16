"""Pool identity and fungible LP observations for Aerodrome's two invariants."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.identifiers import ProtocolVariant
from almanak.gateway.evm import erc20_address
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Chain, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]


class AerodromePool(Model):
    kind: Literal["aerodrome"]
    chain: Chain
    factory: Address
    pool: Address
    currency0: AssetType
    currency1: AssetType
    stable: bool

    @model_validator(mode="after")
    def identity(self):
        if erc20_address(self.currency0, self.chain.chain_id) >= erc20_address(
            self.currency1, self.chain.chain_id
        ):
            raise ValueError("Aerodrome currencies must be distinct ERC-20 assets in address order")
        if ZERO_ADDRESS in (self.factory, self.pool) or self.factory == self.pool:
            raise ValueError("Aerodrome factory and pool require distinct contract addresses")
        return self

    @property
    def variant(self) -> ProtocolVariant:
        return ProtocolVariant("stable" if self.stable else "volatile")

    @property
    def shares_asset(self) -> AssetType:
        return AssetType(f"{self.chain.chain_id}/erc20:{self.pool}")


class Contracts(Model):
    router: Address

    @model_validator(mode="after")
    def nonzero(self):
        if self.router == ZERO_ADDRESS:
            raise ValueError("Aerodrome requires an explicit nonzero router")
        return self


class PoolObservation(Model):
    source: AerodromePool
    block: Block
    currency0: Asset
    currency1: Asset
    shares: Asset
    reserve0: Uint256
    reserve1: Uint256
    balance0: Uint256
    balance1: Uint256
    deposited_shares: Uint256
    total_supply: Uint256
    fee_bps: Annotated[UInt, Field(le=300)]
    index0: Uint256
    index1: Uint256
    pool_fees: Address
    paused: bool

    @model_validator(mode="after")
    def scope(self):
        assets = (self.currency0, self.currency1, self.shares)
        if any(asset.chain != self.source.chain for asset in assets):
            raise ValueError("Aerodrome observation assets belong to another execution scope")
        if self.source.chain != self.block.chain:
            raise ValueError("Aerodrome observation block belongs to another execution scope")
        expected = (self.source.currency0, self.source.currency1, self.source.shares_asset)
        if tuple(asset.identifier for asset in assets) != expected:
            raise ValueError("Aerodrome observation currencies or LP shares differ from its source")
        if self.shares.decimals != 18 or self.pool_fees == ZERO_ADDRESS:
            raise ValueError("Aerodrome requires 18-decimal LP shares and a fee contract")
        return self
