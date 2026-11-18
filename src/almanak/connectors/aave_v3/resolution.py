"""Resolve an exact authored Aave V3 reserve through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Literal

from pydantic import model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v3 import reader as aave_reader
from almanak.connectors.aave_v3.domain import Reserve
from almanak.connectors.aave_v3.networks import pool_for
from almanak.connectors.pool_lending_resolution import (
    ReserveAssertions,
    require_anchor,
    require_pool,
    require_resolved_identities,
    require_resolved_scope,
    require_resolved_target,
    require_scope,
    require_selection_scope,
    reserve_summary,
    verified_reserve,
)
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import Address, Asset, Block, Model

NAME = "Aave V3"


class MarketSelection(Model):
    connector: Literal["aave_v3"]
    chain: ChainId
    asset: AssetType
    pool: Address | None = None
    assertions: ReserveAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        require_selection_scope(self)
        return self


class ResolvedReserve(Model):
    resolver: Literal["aave-v3-reserve-resolution-v1"] = "aave-v3-reserve-resolution-v1"
    selection: MarketSelection
    block: Block
    reserve: Reserve
    asset: Asset
    a_token: AssetType
    variable_debt_token: AssetType
    identities: tuple[ContractIdentity, ContractIdentity, ContractIdentity]

    @model_validator(mode="after")
    def scope(self):
        require_resolved_scope(self)
        require_resolved_target(self, pool_for(self.selection.chain))
        require_resolved_identities(self)
        return self

    @property
    def tokens(self) -> tuple[AssetType, AssetType]:
        return (self.a_token, self.variable_debt_token)

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def assets(self) -> tuple[Asset, ...]:
        return (self.asset,)

    def summary(self) -> dict:
        return reserve_summary(self)


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedReserve:
    require_scope(reader, selection, block, NAME)
    pool = require_pool(selection, pool_for(selection.chain), NAME)
    require_anchor(reader, block, NAME)
    source = Reserve(chain=block.chain, pool=pool, asset=selection.asset)
    tokens, identities = verified_reserve(reader, aave_reader, source, block)
    asset = reader.asset(selection.asset, block)
    require_anchor(reader, block, NAME)
    return ResolvedReserve(
        selection=selection,
        block=block,
        reserve=source,
        asset=asset,
        a_token=tokens[0],
        variable_debt_token=tokens[1],
        identities=identities,
    )
