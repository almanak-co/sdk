"""Resolve an exact authored Comet market target through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.compound_v3.deployment import Deployment, read_deployment
from almanak.connectors.compound_v3.domain import Market, Reserve, Revision
from almanak.connectors.compound_v3.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, address

ASSET_INFO = "(uint8,address,address,uint64,uint64,uint64,uint64,uint128)"


class MarketAssertions(Model):
    base_asset: AssetType | None = None
    price_feed: Address | None = None
    decimals: Annotated[int, Field(ge=0, le=255)] | None = None

    def verify(self, reserve: Reserve, asset: Asset):
        observed = {
            "base_asset": reserve.market.base_asset,
            "price_feed": reserve.price_feed,
            "decimals": asset.decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(
                    f"Compound market assertion {field} differs from verified metadata"
                )


class MarketSelection(Model):
    connector: Literal["compound_v3"]
    chain: ChainId
    comet: Address
    asset: AssetType
    assertions: MarketAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.comet == ZERO_ADDRESS:
            raise ValueError("Compound market selection requires an EVM chain and nonzero Comet")
        erc20_address(self.asset, self.chain)
        if self.assertions is not None and self.assertions.base_asset is not None:
            if self.assertions.base_asset.chain != self.chain:
                raise ValueError("Compound base asset assertion belongs to another chain")
        return self


class ResolvedMarket(Model):
    resolver: Literal["compound-v3-market-resolution-v1"] = "compound-v3-market-resolution-v1"
    selection: MarketSelection
    block: Block
    reserve: Reserve
    asset: Asset
    revision: Revision
    identities: Annotated[tuple[ContractIdentity, ...], Field(min_length=4, max_length=4)]

    @model_validator(mode="after")
    def scope(self):
        if (
            self.selection.chain != self.block.chain.chain_id
            or self.reserve.chain != self.block.chain
            or self.asset.chain != self.block.chain
        ):
            raise ValueError("resolved Compound market has mixed chain or anchor scope")
        if (self.selection.comet, self.selection.asset) != (
            self.reserve.market.comet,
            self.reserve.asset,
        ) or self.asset.identifier != self.reserve.asset:
            raise ValueError("resolved Compound market differs from its authored target or asset")
        self.require_identities()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.reserve, self.asset)
        return self

    def require_identities(self):
        """A reloaded result must carry the four correlated Comet roles, not only the Comet."""
        proxy = self.identities[0]
        addresses = tuple(identity.address for identity in self.identities)
        if proxy.address != self.reserve.market.comet or len(set(addresses)) != 4:
            raise ValueError("resolved Compound identities differ from the verified Comet roles")
        if proxy.implementation != addresses[1]:
            raise ValueError("resolved Compound proxy names another verified implementation")

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def assets(self) -> tuple[Asset, ...]:
        return (self.asset,)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        market = self.reserve.market
        return {
            "selection": self.selection.model_dump(mode="json"),
            "comet": market.comet,
            "base_asset": str(market.base_asset),
            "base_price_feed": market.base_price_feed,
            "asset": str(self.reserve.asset),
            "price_feed": self.reserve.price_feed,
            "decimals": self.asset.decimals,
            "revision": str(self.revision),
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedMarket:
    require_scope(reader, selection, block)
    require_anchor(reader, block)
    market = market_from_comet(reader, selection, block)
    deployment = read_deployment(reader, market, block)
    reserve = Reserve(
        market=market,
        asset=selection.asset,
        price_feed=asset_price_feed(reader, market, deployment, block, selection.asset),
    )
    asset = reader.asset(selection.asset, block)
    require_anchor(reader, block)
    return ResolvedMarket(
        selection=selection,
        block=block,
        reserve=reserve,
        asset=asset,
        revision=deployment.revision,
        identities=deployment.contracts,
    )


def require_scope(reader, selection: MarketSelection, block: Block):
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "Compound resolution requires its selected chain and anchor"
        )
    if selection.chain not in {entry.chain for entry in candidate_support()}:
        raise DataError("unsupported_chain", "Compound resolution chain has no declared support")


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Compound resolution anchor is no longer canonical")


def market_from_comet(reader, selection: MarketSelection, block: Block) -> Market:
    """The Comet itself names its base asset and that asset's configured price source."""
    (base,) = reader.call(selection.comet, "baseToken()", block, outputs=("address",))
    (feed,) = reader.call(selection.comet, "baseTokenPriceFeed()", block, outputs=("address",))
    return Market(
        chain=block.chain,
        comet=selection.comet,
        base_asset=AssetType(f"{selection.chain}/erc20:{address(base)}"),
        base_price_feed=address(feed),
    )


def asset_price_feed(
    reader, market: Market, deployment: Deployment, block: Block, asset: AssetType
) -> str:
    if asset == market.base_asset:
        return market.base_price_feed
    listed = collateral_list(reader, market, deployment, block)
    if asset not in listed:
        raise DataError(
            "unsupported_asset",
            f"Comet {market.comet} lists neither {asset} as collateral nor as its base asset. "
            "Select an asset this Comet lists, or the Comet that carries the intended asset.",
        )
    return listed[asset]


def collateral_list(
    reader, market: Market, deployment: Deployment, block: Block
) -> dict[AssetType, str]:
    """The Comet's whole ordered asset list; the count must equal its verified immutable."""
    (count,) = reader.call(market.comet, "numAssets()", block, outputs=("uint8",))
    if count != deployment.configuration["numAssets"]:
        raise DataError(
            "deployment_changed", "Comet asset count differs from its verified implementation"
        )
    listed = dict(collateral_entry(reader, market, block, index) for index in range(count))
    if len(listed) != count or market.base_asset in listed:
        raise DataError(
            "deployment_changed", "Comet asset list repeats an asset or lists its own base asset"
        )
    return listed


def collateral_entry(reader, market: Market, block: Block, index: int) -> tuple[AssetType, str]:
    info = reader.call(
        market.comet,
        "getAssetInfo(uint8)",
        block,
        inputs=("uint8",),
        args=(index,),
        outputs=(ASSET_INFO,),
    )[0]
    if info[0] != index:
        raise DataError("deployment_changed", "Comet asset list returned another collateral offset")
    return AssetType(f"{market.chain.chain_id}/erc20:{address(info[1])}"), address(info[2])
