"""Resolve one exact authored Morpho Blue market leg through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.morpho_blue.deployment import read_deployment
from almanak.connectors.morpho_blue.domain import Market, MarketId, Reserve
from almanak.connectors.morpho_blue.networks import core_for
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, UInt, address

Leg = Literal["loan", "collateral"]
PARAMETERS = ("address", "address", "address", "address", "uint256")


class MarketAssertions(Model):
    loan_asset: AssetType | None = None
    collateral_asset: AssetType | None = None
    oracle: Address | None = None
    irm: Address | None = None
    lltv: Annotated[UInt, Field(gt=0, lt=10**18)] | None = None
    decimals: Annotated[int, Field(ge=0, le=255)] | None = None

    def foreign(self, chain: ChainId) -> bool:
        pair = (self.loan_asset, self.collateral_asset)
        return any(a is not None and a.chain != chain for a in pair)

    def verify(self, market: Market, asset: Asset):
        observed = {
            "loan_asset": market.loan_asset,
            "collateral_asset": market.collateral_asset,
            "oracle": market.oracle,
            "irm": market.irm,
            "lltv": market.lltv,
            "decimals": asset.decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(f"Morpho market assertion {field} differs from verified metadata")


class MarketSelection(Model):
    connector: Literal["morpho_blue"]
    chain: ChainId
    market_id: MarketId
    leg: Leg
    # An omitted core accepts the reviewed deployment; a named one must be it.
    core: Address | None = None
    assertions: MarketAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.core == ZERO_ADDRESS:
            raise ValueError("Morpho market selection requires an EVM chain and nonzero core")
        if self.assertions is not None and self.assertions.foreign(self.chain):
            raise ValueError("Morpho market asset assertion belongs to another chain")
        return self


class ResolvedMarket(Model):
    resolver: Literal["morpho-blue-market-resolution-v1"] = "morpho-blue-market-resolution-v1"
    selection: MarketSelection
    block: Block
    reserve: Reserve
    asset: Asset
    identities: tuple[ContractIdentity, ...]

    @property
    def market(self) -> Market:
        return self.reserve.market

    @model_validator(mode="after")
    def scope(self):
        self.require_anchor_scope()
        self.require_target()
        self.require_leg()
        self.require_identities()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.market, self.asset)
        return self

    def require_anchor_scope(self):
        if (
            self.selection.chain != self.block.chain.chain_id
            or self.reserve.chain != self.block.chain
            or self.asset.chain != self.block.chain
        ):
            raise ValueError("resolved Morpho market has mixed chain or anchor scope")

    def require_target(self):
        named = self.selection.core
        if self.selection.market_id != self.market.market_id or named not in (
            None,
            self.market.core,
        ):
            raise ValueError("resolved Morpho market differs from its authored target")

    def require_leg(self):
        if (self.selection.leg == "loan") != self.reserve.is_loan:
            raise ValueError("resolved Morpho leg differs from its authored side")
        if self.asset.identifier != self.reserve.asset:
            raise ValueError("resolved Morpho asset differs from its authored leg")

    def require_identities(self):
        expected = (self.market.core, self.market.irm, self.market.oracle)
        if tuple(identity.address for identity in self.identities) != expected:
            raise ValueError("resolved Morpho identities differ from the verified roles")

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def assets(self) -> tuple[Asset, ...]:
        return (self.asset,)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        market = self.market
        return {
            "selection": self.selection.model_dump(mode="json"),
            "core": market.core,
            "market_id": str(market.market_id),
            "loan_asset": str(market.loan_asset),
            "collateral_asset": str(market.collateral_asset),
            "oracle": market.oracle,
            "irm": market.irm,
            "lltv": market.lltv,
            "asset": str(self.asset.identifier),
            "decimals": self.asset.decimals,
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedMarket:
    core = require_scope(reader, selection, block)
    require_anchor(reader, block)
    market = market_from_parameters(reader, selection, core, block)
    # The shared deployment check re-reads the stored parameters of this market.
    deployment = read_deployment(reader, market, block)
    reserve = Reserve(market=market, asset=leg_asset(market, selection.leg))
    asset = reader.asset(reserve.asset, block)
    require_anchor(reader, block)
    return ResolvedMarket(
        selection=selection,
        block=block,
        reserve=reserve,
        asset=asset,
        identities=deployment.contracts,
    )


def require_scope(reader, selection: MarketSelection, block: Block) -> Address:
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "Morpho resolution requires its selected chain and anchor"
        )
    core = core_for(selection.chain)
    if selection.core is not None and selection.core != core:
        raise DataError(
            "deployment_mismatch", "Morpho selection names a core outside the reviewed deployment"
        )
    return core


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Morpho resolution anchor is no longer canonical")


def market_from_parameters(reader, selection: MarketSelection, core: Address, block: Block):
    """The core's own registry names the market's tokens, oracle, IRM and LLTV."""
    loan, collateral, oracle, irm, lltv = reader.call(
        core,
        "idToMarketParams(bytes32)",
        block,
        inputs=("bytes32",),
        args=(bytes.fromhex(str(selection.market_id)[2:]),),
        outputs=PARAMETERS,
    )
    if address(loan) == ZERO_ADDRESS:
        raise DataError("unknown_market", "Morpho core registers no market under this identifier")
    return Market(
        chain=block.chain,
        core=core,
        market_id=selection.market_id,
        loan_asset=erc20(selection.chain, loan),
        collateral_asset=erc20(selection.chain, collateral),
        oracle=address(oracle),
        irm=address(irm),
        lltv=lltv,
    )


def leg_asset(market: Market, leg: Leg) -> AssetType:
    return market.loan_asset if leg == "loan" else market.collateral_asset


def erc20(chain: ChainId, value: str) -> AssetType:
    return AssetType(f"{chain}/erc20:{address(value)}")
