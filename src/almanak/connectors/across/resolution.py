"""Resolve an authored Across route through trusted canonical reads on its source chain.

The sent asset and the SpokePool are read at the source chain's anchor. The received
asset is named by the author and is not read here: resolution reads one chain per
market, so its on-chain verification on the destination chain is still to be built,
and no run mode is declared before it is.
"""

import hashlib
from typing import Literal

from pydantic import model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.across.domain import FeeBps, Route, spoke_pool
from almanak.connectors.across.observations import PoolState, read_pool, require_pinned
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.models import Asset, Block, Model


class MarketSelection(Model):
    connector: Literal["across"]
    # The source chain and the asset sent from it.
    chain: ChainId
    asset: AssetType
    destination_chain: ChainId
    destination_asset: AssetType
    # The author's ceiling on the whole bridge fee, in basis points of the amount sent.
    max_fee_bps: FeeBps
    # The one source of the output amount; it is named, never defaulted.
    quote: Literal["across_api"]
    assertions: None = None

    @model_validator(mode="after")
    def scope(self):
        erc20_address(self.asset, self.chain)
        erc20_address(self.destination_asset, self.destination_chain)
        if self.chain == self.destination_chain:
            raise ValueError("an Across market requires two different chains")
        spoke_pool(self.chain)
        spoke_pool(self.destination_chain)
        return self


class ResolvedRoute(Model):
    resolver: Literal["across-route-resolution-v1"] = "across-route-resolution-v1"
    selection: MarketSelection
    block: Block
    asset: Asset
    pool: PoolState

    @model_validator(mode="after")
    def scope(self):
        if (
            self.selection.chain != self.block.chain.chain_id
            or self.asset.chain != self.block.chain
        ):
            raise ValueError("resolved Across route has mixed chain or anchor scope")
        if self.asset.identifier != self.selection.asset:
            raise ValueError("resolved Across route differs from its authored asset")
        return self

    @property
    def route(self) -> Route:
        selection, source = self.selection, self.block.chain
        return Route(
            chain=source,
            asset=selection.asset,
            destination_chain=selection.destination_chain,
            destination_asset=selection.destination_asset,
            decimals=self.asset.decimals,
            spoke_pool=self.pool.address,
            max_fee_bps=selection.max_fee_bps,
            quote=selection.quote,
        )

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def assets(self) -> tuple[Asset, ...]:
        return (self.asset,)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "spoke_pool": self.pool.address,
            "implementation": self.pool.implementation,
            "decimals": self.asset.decimals,
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedRoute:
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError("chain_mismatch", "Across resolution requires its source chain and anchor")
    pool = read_pool(reader, spoke_pool(selection.chain).address, block)
    require_pinned(pool, selection.chain)
    asset = reader.asset(selection.asset, block)
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Across resolution anchor is no longer canonical")
    return ResolvedRoute(selection=selection, block=block, asset=asset, pool=pool)
