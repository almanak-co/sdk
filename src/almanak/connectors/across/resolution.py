"""Resolve an authored Across route through trusted canonical reads on both its chains.

The sent asset and the SpokePool are read at the source chain's anchor. The received
asset is then read at the destination chain's anchor, with that chain's own reader:
a market resolves on its source chain first and is completed on its destination.
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
    # The received asset, read on the destination chain at that chain's anchor. Absent
    # until the market is completed there; a route is built only from a completed market.
    destination: Asset | None = None
    destination_block: Block | None = None

    @model_validator(mode="after")
    def scope(self):
        if (self.destination is None) != (self.destination_block is None):
            raise ValueError("a completed Across route retains its destination anchor")
        if self.destination is not None and self.destination_block is not None:
            self.destination_scope(self.destination, self.destination_block)
        if (
            self.selection.chain != self.block.chain.chain_id
            or self.asset.chain != self.block.chain
        ):
            raise ValueError("resolved Across route has mixed chain or anchor scope")
        if self.asset.identifier != self.selection.asset:
            raise ValueError("resolved Across route differs from its authored asset")
        return self

    def destination_scope(self, destination: Asset, anchor: Block):
        if (
            destination.chain != anchor.chain
            or anchor.chain.chain_id != self.selection.destination_chain
            or destination.identifier != self.selection.destination_asset
        ):
            raise ValueError("resolved Across route differs from its authored received asset")
        if destination.decimals != self.asset.decimals:
            # The fee ceiling compares base units of both assets.
            raise ValueError(
                f"Across route assets have different decimals: {self.asset.decimals} sent, "
                f"{destination.decimals} received"
            )

    @property
    def route(self) -> Route:
        if self.destination is None:
            raise ValueError("the received asset of an Across route is not verified on its chain")
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

    @property
    def destination_assets(self) -> tuple[Asset, ...]:
        """The received asset, verified on the destination chain; empty until completed."""
        return () if self.destination is None else (self.destination,)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "spoke_pool": self.pool.address,
            "implementation": self.pool.implementation,
            "decimals": self.asset.decimals,
            "block": self.block.number,
        }


def require_anchor(reader, block: Block) -> None:
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Across resolution anchor is no longer canonical")


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedRoute:
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError("chain_mismatch", "Across resolution requires its source chain and anchor")
    # The reader answers calls only at the block it pinned last.
    require_anchor(reader, block)
    pool = read_pool(reader, spoke_pool(selection.chain).address, block)
    require_pinned(pool, selection.chain)
    asset = reader.asset(selection.asset, block)
    require_anchor(reader, block)
    return ResolvedRoute(selection=selection, block=block, asset=asset, pool=pool)


def complete_market(reader, resolved: ResolvedRoute, block: Block) -> ResolvedRoute:
    """Verify the received asset on the destination chain, at that chain's anchor."""
    selection = resolved.selection
    if selection.destination_chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "Across completion requires its destination chain and anchor"
        )
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Across destination anchor is no longer canonical")
    destination = reader.asset(selection.destination_asset, block)
    return ResolvedRoute(
        **{**dict(resolved), "destination": destination, "destination_block": block}
    )
