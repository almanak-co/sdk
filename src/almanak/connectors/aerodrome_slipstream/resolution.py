"""Resolve an exact authored Aerodrome Slipstream pool through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.aerodrome_slipstream import deployments
from almanak.connectors.aerodrome_slipstream.domain import SlipstreamPool
from almanak.connectors.aerodrome_slipstream.networks import Deployment, deployment_for
from almanak.connectors.aerodrome_slipstream.support import candidate_support
from almanak.connectors.v3_pool_domain import Contracts
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, address

# verify_pool retains the factory, the pool implementation and the pool before any
# optional fee module, and verify_periphery then retains the three Contracts roles.
POOL_ROLES = 3
PERIPHERY_ROLES = 3


class PoolAssertions(Model):
    currency0: AssetType | None = None
    currency1: AssetType | None = None
    tick_spacing: Annotated[int, Field(strict=True, ge=1, lt=16384)] | None = None
    decimals0: Annotated[int, Field(ge=0, le=255)] | None = None
    decimals1: Annotated[int, Field(ge=0, le=255)] | None = None

    def foreign(self, chain: ChainId) -> bool:
        pair = (self.currency0, self.currency1)
        return any(c is not None and c.chain != chain for c in pair)

    def verify(self, pool: SlipstreamPool, assets: tuple[Asset, Asset]):
        observed = {
            "currency0": pool.currency0,
            "currency1": pool.currency1,
            "tick_spacing": pool.tick_spacing,
            "decimals0": assets[0].decimals,
            "decimals1": assets[1].decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(
                    f"Slipstream pool assertion {field} differs from verified metadata"
                )


class MarketSelection(Model):
    connector: Literal["aerodrome_slipstream"]
    chain: ChainId
    pool: Address
    assertions: PoolAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.pool == ZERO_ADDRESS:
            raise ValueError("Slipstream pool selection requires an EVM chain and nonzero pool")
        if self.assertions is not None and self.assertions.foreign(self.chain):
            raise ValueError("Slipstream pool currency assertion belongs to another chain")
        return self


class ResolvedPool(Model):
    resolver: Literal["aerodrome-slipstream-pool-resolution-v1"] = (
        "aerodrome-slipstream-pool-resolution-v1"
    )
    selection: MarketSelection
    block: Block
    pool: SlipstreamPool
    contracts: Contracts
    asset0: Asset
    asset1: Asset
    identities: tuple[ContractIdentity, ...]

    @model_validator(mode="after")
    def scope(self):
        if (
            self.selection.chain != self.block.chain.chain_id
            or self.pool.chain != self.block.chain
            or self.asset0.chain != self.block.chain
            or self.asset1.chain != self.block.chain
        ):
            raise ValueError("resolved Slipstream pool has mixed chain or anchor scope")
        if self.selection.pool != self.pool.pool:
            raise ValueError("resolved Slipstream pool differs from its authored target")
        if (self.asset0.identifier, self.asset1.identifier) != (
            self.pool.currency0,
            self.pool.currency1,
        ):
            raise ValueError("resolved Slipstream assets differ from the pool currencies")
        self.require_identities()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.pool, self.assets)
        return self

    def require_identities(self):
        addresses = tuple(identity.address for identity in self.identities)
        if len(set(addresses)) != len(addresses):
            raise ValueError("resolved Slipstream identities repeat a contract address")
        self.require_roles(addresses)

    def require_roles(self, addresses: tuple[str, ...]):
        """The pool implementation sits between the factory and the clone; fees are optional."""
        if len(addresses) < POOL_ROLES + PERIPHERY_ROLES or (addresses[0], addresses[2]) != (
            self.pool.factory,
            self.pool.pool,
        ):
            raise ValueError("resolved Slipstream identities differ from the verified pool roles")
        periphery = (
            self.contracts.router,
            self.contracts.position_manager,
            self.contracts.quoter,
        )
        if addresses[-PERIPHERY_ROLES:] != periphery:
            raise ValueError("resolved Slipstream identities differ from the verified periphery")

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def assets(self) -> tuple[Asset, Asset]:
        return (self.asset0, self.asset1)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "pool": self.pool.pool,
            "currencies": [str(asset.identifier) for asset in self.assets],
            "tick_spacing": self.pool.tick_spacing,
            "decimals": [asset.decimals for asset in self.assets],
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedPool:
    require_scope(reader, selection, block)
    require_anchor(reader, block)
    deployment = deployment_for(selection.chain)
    contracts = Contracts(
        router=deployment.router,
        position_manager=deployment.position_manager,
        quoter=deployment.quoter,
    )
    pool = pool_from_storage(reader, selection, deployment, block)
    identities = verified_identities(reader, pool, contracts, block)
    asset0 = reader.asset(pool.currency0, block)
    asset1 = reader.asset(pool.currency1, block)
    require_anchor(reader, block)
    return ResolvedPool(
        selection=selection,
        block=block,
        pool=pool,
        contracts=contracts,
        asset0=asset0,
        asset1=asset1,
        identities=identities,
    )


def require_scope(reader, selection: MarketSelection, block: Block):
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "Slipstream resolution requires its selected chain and anchor"
        )
    if selection.chain not in {entry.chain for entry in candidate_support()}:
        raise DataError("unsupported_chain", "Slipstream resolution chain has no declared support")


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Slipstream resolution anchor is no longer canonical")


def pool_from_storage(
    reader, selection: MarketSelection, deployment: Deployment, block: Block
) -> SlipstreamPool:
    """A Slipstream pool is a clone, so its own storage names factory, pair and spacing."""
    named = word(reader, selection.pool, "factory()", block)
    if named != deployment.factory:
        raise DataError(
            "deployment_changed",
            f"Slipstream pool names factory {named}, outside its reviewed deployment"
            f" {deployment.factory}",
        )
    (spacing,) = reader.call(selection.pool, "tickSpacing()", block, outputs=("int24",))
    return SlipstreamPool(
        kind="aerodrome_slipstream",
        chain=block.chain,
        factory=deployment.factory,
        pool=selection.pool,
        currency0=erc20(selection.chain, word(reader, selection.pool, "token0()", block)),
        currency1=erc20(selection.chain, word(reader, selection.pool, "token1()", block)),
        tick_spacing=spacing,
    )


def verified_identities(
    reader, pool: SlipstreamPool, contracts: Contracts, block: Block
) -> tuple[ContractIdentity, ...]:
    identities = (
        *deployments.verify_pool(reader, pool, block),
        *deployments.verify_periphery(reader, pool, contracts, block),
    )
    deployments.require_no_discount(reader, pool, block)
    return identities


def word(reader, target: str, signature: str, block: Block) -> str:
    (value,) = reader.call(target, signature, block, outputs=("address",))
    return address(value)


def erc20(chain: ChainId, value: str) -> AssetType:
    return AssetType(f"{chain}/erc20:{value}")
