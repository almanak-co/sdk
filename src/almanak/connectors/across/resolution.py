"""Resolve an authored Across route through trusted canonical reads on both its chains.

The sent asset and the SpokePool are read at the source chain's anchor. The received
asset and the destination SpokePool are then read at the destination chain's anchor,
with that chain's own reader: a market resolves on its source chain first and is
completed on its destination. Each SpokePool must run its reviewed implementation.
"""

import hashlib
from typing import Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.connectors.across.domain import (
    NATIVE_DIRECT_ONLY,
    FeeBps,
    Route,
    SpokePoolPin,
    names_native,
    require_native_pair,
    spoke_pool,
)
from almanak.connectors.across.observations import PoolState, read_pool, require_pinned
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.models import Address, Asset, Block, Model


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
        self.require_assets()
        if self.chain == self.destination_chain:
            raise ValueError("an Across market requires two different chains")
        spoke_pool(self.chain)
        spoke_pool(self.destination_chain)
        return self

    def require_assets(self) -> None:
        """A strategy's market names two ERC-20 assets."""
        if names_native(self.asset, self.destination_asset):
            raise ValueError(NATIVE_DIRECT_ONLY)
        erc20_address(self.asset, self.chain)
        erc20_address(self.destination_asset, self.destination_chain)

    @property
    def native(self) -> bool:
        return names_native(self.asset)


class DirectSelection(MarketSelection):
    """The route an operator names for a direct command.

    It may name the native asset, on both chains and only where that is ETH. Nothing
    else builds one: an author file is read as a `MarketSelection`, which refuses it.
    """

    def require_assets(self) -> None:
        if not names_native(self.asset, self.destination_asset):
            return super().require_assets()
        require_native_pair(self.chain, self.asset, self.destination_chain, self.destination_asset)


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
    # A native route only: each SpokePool's own `wrappedNativeToken()`, read with the
    # pool at its chain's anchor and held to the network definition's wrapped native.
    wrapped_native: Address | None = Field(default=None, exclude_if=lambda v: v is None)
    destination_wrapped_native: Address | None = Field(default=None, exclude_if=lambda v: v is None)

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
        self.wrapped_scope()
        return self

    def wrapped_scope(self):
        """A native route retains each pool's wrapped native token with that pool, and a
        token route retains none: a record with one of the two would load as a token route."""
        native = self.selection.native
        expected = (native, native and self.destination_pool is not None)
        retained = (self.wrapped_native is not None, self.destination_wrapped_native is not None)
        if retained != expected:
            raise ValueError(
                "a resolved Across route retains a pool's wrapped native token exactly "
                "when it is a native route and that pool was read"
            )

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
        wrapped = None
        if self.wrapped_native is not None and self.destination_wrapped_native is not None:
            wrapped = (self.wrapped_native, self.destination_wrapped_native)
        return Route(
            wrapped=wrapped,
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


def wrapped_native(reader, pool: PoolState, selection: MarketSelection, block: Block):
    """The pool's own wrapped native token on a native route, held to the network's.

    The SpokePool wraps a deposit's value into this token and unwraps it for a plain
    recipient, so the route's token addresses are the pool's answer, read at the anchor
    like every other fact of the route. A pool that names another token than the
    network definition's is refused: one of the two is not what this route assumes.
    None for a route of two tokens, which reads nothing here.
    """
    if not selection.native:
        return None
    chain = block.chain.chain_id
    (named,) = reader.call(pool.address, "wrappedNativeToken()", block, outputs=("address",))
    expected = evm_network(chain).wrapped_native.reference
    if named.lower() != expected:
        raise DataError(
            "unsupported_route",
            f"the Across SpokePool on {chain} names {named.lower()} as its wrapped native "
            f"token, not the network's {expected}; no native route is resolved through it",
        )
    return expected


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
    wrapped = wrapped_native(reader, pool, selection, block)
    require_anchor(reader, block)
    return ResolvedRoute(
        selection=selection, block=block, asset=asset, pool=pool, wrapped_native=wrapped
    )


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
    wrapped = wrapped_native(reader, pool, selection, block)
    require_anchor(reader, block)
    return ResolvedRoute(
        **{
            **dict(resolved),
            "destination": destination,
            "destination_block": block,
            "destination_pool": pool,
            "destination_wrapped_native": wrapped,
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
