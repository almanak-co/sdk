"""Resolve an exact authored Aerodrome pool through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.aerodrome import deployments
from almanak.connectors.aerodrome.domain import AerodromePool, Contracts
from almanak.connectors.aerodrome.networks import Deployment, deployment_for
from almanak.connectors.aerodrome.support import candidate_support
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, address

# The verification order of `verified_identities`, which the retained result replays.
ROLES = ("factory", "implementation", "pool", "pool_fees", "router", "factory_registry")


class PoolAssertions(Model):
    currency0: AssetType | None = None
    currency1: AssetType | None = None
    stable: bool | None = None
    decimals0: Annotated[int, Field(ge=0, le=255)] | None = None
    decimals1: Annotated[int, Field(ge=0, le=255)] | None = None

    def foreign(self, chain: ChainId) -> bool:
        pair = (self.currency0, self.currency1)
        return any(c is not None and c.chain != chain for c in pair)

    def verify(self, pool: AerodromePool, assets: tuple[Asset, Asset]):
        observed = {
            "currency0": pool.currency0,
            "currency1": pool.currency1,
            "stable": pool.stable,
            "decimals0": assets[0].decimals,
            "decimals1": assets[1].decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(f"Aerodrome pool assertion {field} differs from verified metadata")


class MarketSelection(Model):
    connector: Literal["aerodrome"]
    chain: ChainId
    pool: Address
    assertions: PoolAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.pool == ZERO_ADDRESS:
            raise ValueError("Aerodrome pool selection requires an EVM chain and nonzero pool")
        if self.assertions is not None and self.assertions.foreign(self.chain):
            raise ValueError("Aerodrome pool currency assertion belongs to another chain")
        return self


class ResolvedPool(Model):
    resolver: Literal["aerodrome-pool-resolution-v1"] = "aerodrome-pool-resolution-v1"
    selection: MarketSelection
    block: Block
    pool: AerodromePool
    contracts: Contracts
    asset0: Asset
    asset1: Asset
    share: Asset
    identities: tuple[ContractIdentity, ...]

    @model_validator(mode="after")
    def scope(self):
        self.require_anchor_scope()
        self.require_target()
        self.require_identities()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.pool, self.currencies)
        return self

    def require_anchor_scope(self):
        scopes = {self.block.chain, self.pool.chain, *(asset.chain for asset in self.assets)}
        if scopes != {self.block.chain} or self.selection.chain != self.block.chain.chain_id:
            raise ValueError("resolved Aerodrome pool has mixed chain or anchor scope")

    def require_target(self):
        if self.selection.pool != self.pool.pool:
            raise ValueError("resolved Aerodrome pool differs from its authored target")
        currencies = (self.pool.currency0, self.pool.currency1)
        if (self.asset0.identifier, self.asset1.identifier) != currencies:
            raise ValueError("resolved Aerodrome assets differ from the pool currencies")
        if self.share.identifier != erc20(self.selection.chain, self.pool.pool):
            raise ValueError("resolved Aerodrome share differs from the pool's own token")

    def require_identities(self):
        pairs = zip(ROLES, self.identities, strict=True)
        roles = {role: identity.address for role, identity in pairs}
        reviewed = {
            "factory": self.pool.factory,
            "pool": self.pool.pool,
            "router": self.contracts.router,
        }
        distinct = len(set(roles.values())) == len(ROLES)
        if not distinct or reviewed != {role: roles[role] for role in reviewed}:
            raise ValueError("resolved Aerodrome identities differ from the six verified roles")

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def currencies(self) -> tuple[Asset, Asset]:
        return (self.asset0, self.asset1)

    @property
    def assets(self) -> tuple[Asset, Asset, Asset]:
        """The two currencies and the pool's own share token, all with verified decimals."""
        return (self.asset0, self.asset1, self.share)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "pool": self.pool.pool,
            "currencies": [str(asset.identifier) for asset in self.currencies],
            "stable": self.pool.stable,
            "decimals": [asset.decimals for asset in self.currencies],
            "share_decimals": self.share.decimals,
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedPool:
    require_scope(reader, selection, block)
    require_anchor(reader, block)
    deployment = deployment_for(selection.chain)
    contracts = Contracts(router=deployment.router)
    pool = pool_from_storage(reader, selection, deployment, block)
    identities = verified_identities(reader, pool, contracts, block)
    asset0 = reader.asset(pool.currency0, block)
    asset1 = reader.asset(pool.currency1, block)
    share = reader.asset(erc20(selection.chain, pool.pool), block)
    require_anchor(reader, block)
    return ResolvedPool(
        selection=selection,
        block=block,
        pool=pool,
        contracts=contracts,
        asset0=asset0,
        asset1=asset1,
        share=share,
        identities=identities,
    )


def require_scope(reader, selection: MarketSelection, block: Block):
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "Aerodrome resolution requires its selected chain and anchor"
        )
    if selection.chain not in {entry.chain for entry in candidate_support()}:
        raise DataError("unsupported_chain", "Aerodrome resolution chain has no declared support")


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Aerodrome resolution anchor is no longer canonical")


def pool_from_storage(
    reader, selection: MarketSelection, deployment: Deployment, block: Block
) -> AerodromePool:
    """A minimal-proxy pool holds no immutables: its storage names factory, pair and invariant."""
    if stored(reader, selection.pool, "factory", block) != deployment.factory:
        raise DataError(
            "deployment_changed", "Aerodrome pool names a factory outside its reviewed deployment"
        )
    return AerodromePool(
        kind="aerodrome",
        chain=block.chain,
        factory=deployment.factory,
        pool=selection.pool,
        currency0=erc20(selection.chain, stored(reader, selection.pool, "token0", block)),
        currency1=erc20(selection.chain, stored(reader, selection.pool, "token1", block)),
        stable=reader.call(selection.pool, "stable()", block, outputs=("bool",))[0],
    )


def stored(reader, pool: Address, name: str, block: Block) -> str:
    return address(reader.call(pool, f"{name}()", block, outputs=("address",))[0])


def verified_identities(
    reader, pool: AerodromePool, contracts: Contracts, block: Block
) -> tuple[ContractIdentity, ...]:
    """`verify_pool` covers the factory, its pool implementation, the clone and its fees."""
    return (
        *deployments.verify_pool(reader, pool, block),
        *deployments.verify_router(reader, pool, contracts, block),
    )


def erc20(chain: ChainId, value: str) -> AssetType:
    return AssetType(f"{chain}/erc20:{value}")
