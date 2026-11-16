"""Explicit GMX market and aggregate perpetual position identities."""

from typing import Literal

from pydantic import model_validator

from almanak.caip import AssetType
from almanak.connectors.identifiers import Name
from almanak.gateway.evm import erc20_address
from almanak.models import ZERO_ADDRESS, Account, Address, Chain, Model


class Contracts(Model):
    exchange_router: Address
    token_router: Address
    reader: Address
    data_store: Address
    event_emitter: Address
    oracle: Address
    order_handler: Address
    order_vault: Address
    increase_order_executor: Address
    decrease_order_executor: Address

    @model_validator(mode="after")
    def deployed_roles(self):
        addresses = tuple(self.model_dump().values())
        if ZERO_ADDRESS in addresses or len(set(addresses)) != len(addresses):
            raise ValueError("GMX contracts require distinct nonzero deployment addresses")
        return self


class Market(Model):
    kind: Literal["gmx_v2"] = "gmx_v2"
    chain: Chain
    share_asset: AssetType
    index_asset: AssetType
    long_asset: AssetType
    short_asset: AssetType
    wrapped_native: AssetType
    contracts: Contracts

    @model_validator(mode="after")
    def asset_scope(self):
        for asset in (
            self.share_asset,
            self.index_asset,
            self.long_asset,
            self.short_asset,
            self.wrapped_native,
        ):
            erc20_address(asset, self.chain.chain_id)
        if self.long_asset == self.short_asset:
            raise ValueError("GMX perpetual market requires distinct long and short assets")
        if self.share_asset in (self.index_asset, self.long_asset, self.short_asset):
            raise ValueError("GM market shares must differ from the underlying assets")
        return self


class Target(Model):
    allocation: Name
    account: Name
    fund: Account
    market: Market
    collateral_asset: AssetType
    side: Literal["long", "short"]

    @model_validator(mode="after")
    def scope(self):
        if self.market.chain != self.fund.chain:
            raise ValueError("GMX target must retain its fund chain and execution environment")
        if self.collateral_asset not in (self.market.long_asset, self.market.short_asset):
            raise ValueError("GMX position collateral must identify a configured market asset")
        return self

    @property
    def position_scope(self):
        """Allocation labels cannot split GMX's fund/market/collateral/side position."""
        return self.fund, self.market.share_asset, self.collateral_asset, self.side

    @property
    def assets(self):
        from almanak.gateway.evm import native_asset

        return tuple(
            dict.fromkeys(
                (
                    self.market.index_asset,
                    self.market.long_asset,
                    self.market.short_asset,
                    native_asset(self.fund.chain.chain_id),
                )
            )
        )
