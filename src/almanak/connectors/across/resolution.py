"""Resolve an authored Across route through trusted canonical reads on both its chains.

The sent asset and the SpokePool are read at the source chain's anchor. The received
asset and the destination SpokePool are then read at the destination chain's anchor,
with that chain's own reader: a market resolves on its source chain first and is
completed on its destination. Each SpokePool must run its reviewed implementation.
"""

import hashlib
from typing import Literal

from pydantic import model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.across.domain import FeeBps, Route, SpokePoolPin, spoke_pool
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
    # v2 retains the destination SpokePool; a route resolved under v1 is resolved again.
    resolver: Literal["across-route-resolution-v2"] = "across-route-resolution-v2"
    selection: MarketSelection
    block: Block
    asset: Asset
    pool: PoolState
    # The received asset, read on the destination chain at that chain's anchor. Absent
    # until the market is completed there; a route is built only from a completed market.
    destination: Asset | None = None
    destination_block: Block | None = None
    # The SpokePool that fills on the destination chain, read at the same anchor. Neither
    # pool is held to its pin when retained facts are loaded or replayed; see `admit_route`.
    destination_pool: PoolState | None = None

    @model_validator(mode="after")
    def scope(self):
        completed = (self.destination, self.destination_block, self.destination_pool)
        if len({part is None for part in completed}) != 1:
            raise ValueError("a completed Across route retains its destination anchor and pool")
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
        if self.destination is None or self.destination_pool is None:
            raise ValueError("the received asset of an Across route is not verified on its chain")
        filling = self.destination_pool
        selection, source = self.selection, self.block.chain
        return Route(
            chain=source,
            asset=selection.asset,
            destination_chain=selection.destination_chain,
            destination_asset=selection.destination_asset,
            decimals=self.asset.decimals,
            spoke_pool=self.pool.address,
            destination_pool=SpokePoolPin(
                address=filling.address,
                implementation=filling.implementation,
                implementation_code_hash=filling.implementation_code_hash,
            ),
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
        raise DataError("block_mismatch", "an Across resolution anchor is no longer canonical")


def resolve_market(
    reader, selection: MarketSelection, block: Block, pool_address: str | None = None
) -> ResolvedRoute:
    """Read the sending SpokePool and the sent asset at the source anchor.

    `pool_address` is the pool a retained route was resolved on, given on replay only.
    """
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError("chain_mismatch", "Across resolution requires its source chain and anchor")
    # The reader answers calls only at the block it pinned last.
    require_anchor(reader, block)
    pool = read_pool(reader, pool_address or spoke_pool(selection.chain).address, block)
    asset = reader.asset(selection.asset, block)
    require_anchor(reader, block)
    return ResolvedRoute(selection=selection, block=block, asset=asset, pool=pool)


def complete_market(
    reader, resolved: ResolvedRoute, block: Block, pool_address: str | None = None
) -> ResolvedRoute:
    """Read the received asset and the filling SpokePool at the destination's anchor.

    `pool_address` is the pool a retained route was completed on, given on replay only.
    """
    selection = resolved.selection
    if selection.destination_chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "Across completion requires its destination chain and anchor"
        )
    require_anchor(reader, block)
    address = pool_address or spoke_pool(selection.destination_chain).address
    pool = read_pool(reader, address, block)
    destination = reader.asset(selection.destination_asset, block)
    require_anchor(reader, block)
    return ResolvedRoute(
        **{
            **dict(resolved),
            "destination": destination,
            "destination_block": block,
            "destination_pool": pool,
        }
    )


def replay_market(reader, retained: ResolvedRoute) -> ResolvedRoute:
    """The source reads again, at the pool the retained route names.

    The pin in code may name another proxy by now. The deployment's transfers and its
    approval live in the pool it was resolved on, so replay asks the retained reads about
    that pool; reads of any other address are not in the evidence and refuse.
    """
    return resolve_market(reader, retained.selection, retained.block, retained.pool.address)


def replay_completion(reader, observed: ResolvedRoute, retained: ResolvedRoute) -> ResolvedRoute:
    filling = retained.destination_pool
    if retained.destination_block is None or filling is None:
        raise DataError("resolution_evidence", "a retained Across route was never completed")
    return complete_market(reader, observed, retained.destination_block, filling.address)


def admit_route(resolved: ResolvedRoute) -> None:
    """Hold a freshly resolved route's SpokePools to the reviewed pins.

    First resolution only. `resolve_market` and `complete_market` also replay a running
    deployment's retained reads, where they verify that the facts follow from the reads
    and nothing else: held there, a reviewed pin change would refuse the restart of a
    deployment with a transfer in flight or a standing approval, and trap both. After
    such a change an existing deployment still restarts, reconciles arrivals and
    refunds, revokes and closes, and `validate_entry` refuses its new deposits.
    """
    require_pinned(resolved.pool, resolved.selection.chain)
    if resolved.destination_pool is not None:
        require_pinned(resolved.destination_pool, resolved.selection.destination_chain)
