"""Resolve an exact authored GMX V2 market token through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.gmx_v2 import deployments
from almanak.connectors.gmx_v2.connector import candidate_support
from almanak.connectors.gmx_v2.exchange_domain import ExchangeContracts, ExchangeMarket
from almanak.connectors.gmx_v2.networks import addresses, roles_for
from almanak.connectors.gmx_v2.oracle_reader import is_deployed, stored, stream_decimals
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Asset, Block, Model, address

MARKET_ABI = "(address,address,address,address)"


class MarketAssertions(Model):
    """Author expectations the resolver rejects on; they never supply a fact."""

    index: AssetType | None = None
    long: AssetType | None = None
    short: AssetType | None = None
    index_decimals: Annotated[int, Field(ge=0, le=255)] | None = None
    long_decimals: Annotated[int, Field(ge=0, le=255)] | None = None
    short_decimals: Annotated[int, Field(ge=0, le=255)] | None = None

    def foreign(self, chain: ChainId) -> bool:
        return any(a is not None and a.chain != chain for a in (self.index, self.long, self.short))

    def verify(self, market: ExchangeMarket, assets: tuple[Asset, Asset, Asset]):
        observed = {
            "index": market.index_asset,
            "long": market.long_asset,
            "short": market.short_asset,
            "index_decimals": assets[0].decimals,
            "long_decimals": assets[1].decimals,
            "short_decimals": assets[2].decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(f"GMX V2 market assertion {field} differs from verified metadata")


class MarketSelection(Model):
    connector: Literal["gmx_v2"]
    chain: ChainId
    market: AssetType
    assertions: MarketAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155":
            raise ValueError("GMX V2 market selection requires an EVM chain")
        erc20_address(self.market, self.chain)
        if self.assertions is not None and self.assertions.foreign(self.chain):
            raise ValueError("GMX V2 market assertion belongs to another chain")
        return self


class ResolvedMarket(Model):
    resolver: Literal["gmx-v2-market-resolution-v1"] = "gmx-v2-market-resolution-v1"
    selection: MarketSelection
    block: Block
    market: ExchangeMarket
    index: Asset
    long: Asset
    short: Asset
    wrapped_native: Asset
    share: Asset
    identities: tuple[ContractIdentity, ...]

    @model_validator(mode="after")
    def scope(self):
        self.require_anchor_scope()
        self.require_target()
        self.require_identities()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.market, self.currencies)
        return self

    def require_anchor_scope(self):
        scopes = {self.block.chain, self.market.chain, *(asset.chain for asset in self.assets)}
        if scopes != {self.block.chain} or self.selection.chain != self.block.chain.chain_id:
            raise ValueError("resolved GMX V2 market has mixed chain or anchor scope")

    def require_target(self):
        if self.market.share_asset != self.selection.market:
            raise ValueError("resolved GMX V2 market differs from its authored share token")
        verified = (
            self.market.index_asset,
            self.market.long_asset,
            self.market.short_asset,
            self.market.wrapped_native,
            self.market.share_asset,
        )
        if tuple(asset.identifier for asset in self.retained) != verified:
            raise ValueError("resolved GMX V2 assets differ from the verified market tokens")

    def require_identities(self):
        """A reloaded result carries every verified role, not only the market token."""
        seen = {identity.address for identity in self.identities}
        missing = [role for role in addresses(self.market.contracts) if role not in seen]
        if missing or self.market.share_asset.reference not in seen:
            raise ValueError("resolved GMX V2 identities omit a verified deployment address")

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def retained(self) -> tuple[Asset, Asset, Asset, Asset, Asset]:
        """The five verified assets in the order the resolver reads them."""
        return (self.index, self.long, self.short, self.wrapped_native, self.share)

    @property
    def currencies(self) -> tuple[Asset, Asset, Asset]:
        return (self.index, self.long, self.short)

    @property
    def assets(self) -> tuple[Asset, ...]:
        """Index, long, short, wrapped native and the GM share, each named once.

        On Arbitrum the index, long and wrapped native are all WETH, so the
        constructed asset metadata must not repeat that identifier.
        """
        return tuple({asset.identifier: asset for asset in self.retained}.values())

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "share": str(self.share.identifier),
            "index": str(self.index.identifier),
            "long": str(self.long.identifier),
            "short": str(self.short.identifier),
            "wrapped_native": str(self.wrapped_native.identifier),
            "decimals": [asset.decimals for asset in self.retained],
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedMarket:
    require_scope(reader, selection, block)
    require_anchor(reader, block)
    contracts = roles_for(selection.chain)
    index, long, short = market_tokens(reader, contracts, selection, block)
    market = ExchangeMarket(
        kind="gmx_v2",
        chain=block.chain,
        share_asset=selection.market,
        index_asset=index,
        long_asset=long,
        short_asset=short,
        wrapped_native=erc20(
            selection.chain, stored(reader, block, contracts.data_store, "Address", "WNT")
        ),
        contracts=contracts,
    )
    identities = deployments.verify_deployments(reader, market, block)
    assets = (
        index_asset(reader, contracts, index, block),
        *(
            reader.asset(token, block)
            for token in (long, short, market.wrapped_native, selection.market)
        ),
    )
    require_anchor(reader, block)
    return ResolvedMarket(
        selection=selection,
        block=block,
        market=market,
        index=assets[0],
        long=assets[1],
        short=assets[2],
        wrapped_native=assets[3],
        share=assets[4],
        identities=identities,
    )


def require_scope(reader, selection: MarketSelection, block: Block):
    """Every scope refusal precedes the first provider read."""
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "GMX V2 resolution requires its selected chain and anchor"
        )
    if selection.chain not in {entry.chain for entry in candidate_support()}:
        raise DataError("unsupported_chain", "GMX V2 resolution chain has no declared support")


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "GMX V2 resolution anchor is no longer canonical")


def market_tokens(
    reader, contracts: ExchangeContracts, selection: MarketSelection, block: Block
) -> tuple[AssetType, AssetType, AssetType]:
    """The Reader's stored tuple names the three underlying tokens of the GM market."""
    token, index, long, short = reader.call(
        contracts.reader,
        "getMarket(address,address)",
        block,
        inputs=("address", "address"),
        args=(contracts.data_store, selection.market.reference),
        outputs=(MARKET_ABI,),
    )[0]
    if address(token) == ZERO_ADDRESS:
        raise DataError(
            "unavailable", "GMX V2 market token is not registered in the reviewed data store"
        )
    if address(token) != selection.market.reference:
        raise DataError("identity", "GMX V2 stored market differs from the authored share token")
    chain = selection.chain
    return erc20(chain, index), erc20(chain, long), erc20(chain, short)


def index_asset(reader, contracts: ExchangeContracts, token: AssetType, block: Block) -> Asset:
    """The index of a synthetic GMX market is priced, never held: no contract exists there."""
    if is_deployed(reader, block, token.reference):
        return reader.asset(token, block)
    decimals = stream_decimals(reader, block, contracts.data_store, token.reference)
    return Asset(chain=block.chain, identifier=token, decimals=decimals)


def erc20(chain: ChainId, value: str) -> AssetType:
    return AssetType(f"{chain}/erc20:{address(value)}")
