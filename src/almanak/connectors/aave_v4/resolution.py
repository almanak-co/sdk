"""Resolve an exact authored Spoke/reserve through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v4.deployments import IMPLEMENTATIONS, verify_contract
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aave_v4.reader import observe_reserve, read_reserve_data
from almanak.connectors.support import aave_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, UInt, address


class ReserveAssertions(Model):
    asset: AssetType | None = None
    hub: Address | None = None
    decimals: Annotated[int, Field(ge=0, le=255)] | None = None

    def verify(self, reserve: AaveReserve, asset: Asset):
        observed = {"asset": reserve.asset, "hub": reserve.hub, "decimals": asset.decimals}
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(f"Aave reserve assertion {field} differs from verified metadata")


class MarketSelection(Model):
    connector: Literal["aave_v4"]
    chain: ChainId
    spoke: Address
    reserve_id: Annotated[UInt, Field(lt=2**256)]
    assertions: ReserveAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.spoke == ZERO_ADDRESS:
            raise ValueError("Aave reserve selection requires an EVM chain and nonzero Spoke")
        if self.assertions is not None and self.assertions.asset is not None:
            if self.assertions.asset.chain != self.chain:
                raise ValueError("Aave reserve asset assertion belongs to another chain")
        return self


class ResolvedReserve(Model):
    resolver: Literal["aave-v4-reserve-resolution-v1"] = "aave-v4-reserve-resolution-v1"
    selection: MarketSelection
    block: Block
    reserve: AaveReserve
    asset: Asset
    hub_asset_id: UInt
    contracts: tuple[ContractIdentity, ContractIdentity]

    @model_validator(mode="after")
    def scope(self):
        if (
            self.selection.chain != self.block.chain.chain_id
            or self.reserve.chain != self.block.chain
            or self.asset.chain != self.block.chain
        ):
            raise ValueError("resolved Aave reserve has mixed chain or anchor scope")
        if (self.selection.spoke, self.selection.reserve_id) != (
            self.reserve.spoke,
            self.reserve.reserve_id,
        ) or self.asset.identifier != self.reserve.asset:
            raise ValueError("resolved Aave reserve differs from its authored target or asset")
        if tuple(c.address for c in self.contracts) != (self.reserve.spoke, self.reserve.hub):
            raise ValueError("resolved Aave contract identities differ from the reserve")
        # A reloaded result must carry the verified revisions, not only the addresses.
        for role, identity in zip(("spoke", "hub"), self.contracts, strict=True):
            if identity.implementation_hash not in IMPLEMENTATIONS[role]:
                raise ValueError(
                    "resolved Aave contract identity is outside the verified revisions"
                )
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.reserve, self.asset)
        return self

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
            "hub": self.reserve.hub,
            "asset": str(self.asset.identifier),
            "decimals": self.asset.decimals,
            "hub_asset_id": self.hub_asset_id,
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedReserve:
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError("chain_mismatch", "Aave resolution requires its selected chain and anchor")
    if selection.chain not in {entry.chain for entry in aave_support()}:
        raise DataError("unsupported_chain", "Aave resolution chain has no declared support")
    require_anchor(reader, block)
    authority = AuthorityReader(reader, block)
    spoke_identity = verify_contract(authority, "spoke", selection.spoke)
    data = read_reserve_data(reader, selection.spoke, selection.reserve_id, block)
    reserve = AaveReserve(
        kind="aave_v4",
        chain=block.chain,
        spoke=selection.spoke,
        reserve_id=selection.reserve_id,
        hub=data.hub,
        asset=AssetType(f"{selection.chain}/erc20:{address(data.underlying)}"),
    )
    # Only a verified Spoke's exact returned Hub extends the dependency scope.
    hub_identity = verify_contract(authority, "hub", reserve.hub)
    observed = observe_reserve(reader, reserve, block, data)
    verify_hub_asset(reader, observed)
    require_anchor(reader, block)
    return ResolvedReserve(
        selection=selection,
        block=block,
        reserve=reserve,
        asset=observed.asset,
        hub_asset_id=observed.hub_asset_id,
        contracts=(spoke_identity, hub_identity),
    )


def require_anchor(reader, block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Aave resolution anchor is no longer canonical")


def verify_hub_asset(reader, reserve):
    underlying, decimals = reader.call(
        reserve.source.hub,
        "getAssetUnderlyingAndDecimals(uint256)",
        reserve.block,
        inputs=("uint256",),
        args=(reserve.hub_asset_id,),
        outputs=("address", "uint8"),
    )
    if (address(underlying), decimals) != (
        reserve.asset.identifier.reference,
        reserve.asset.decimals,
    ):
        raise DataError("source_mismatch", "Aave Hub asset differs from the verified Spoke reserve")
