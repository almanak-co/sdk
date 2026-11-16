"""Resolve an exact authored Uniswap V3 pool through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.uniswap_v3 import deployments
from almanak.connectors.uniswap_v3.domain import UniswapPool
from almanak.connectors.uniswap_v3.networks import Deployment, deployment_for
from almanak.connectors.uniswap_v3.support import candidate_support
from almanak.connectors.v3_pool_domain import Contracts
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, UInt, address


class PoolAssertions(Model):
    currency0: AssetType | None = None
    currency1: AssetType | None = None
    fee: Annotated[UInt, Field(lt=1_000_000)] | None = None
    tick_spacing: Annotated[int, Field(strict=True, ge=1, lt=16384)] | None = None
    decimals0: Annotated[int, Field(ge=0, le=255)] | None = None
    decimals1: Annotated[int, Field(ge=0, le=255)] | None = None

    def foreign(self, chain: ChainId) -> bool:
        pair = (self.currency0, self.currency1)
        return any(c is not None and c.chain != chain for c in pair)

    def verify(self, pool: UniswapPool, assets: tuple[Asset, Asset]):
        observed = {
            "currency0": pool.currency0,
            "currency1": pool.currency1,
            "fee": pool.fee,
            "tick_spacing": pool.tick_spacing,
            "decimals0": assets[0].decimals,
            "decimals1": assets[1].decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(
                    f"Uniswap V3 pool assertion {field} differs from verified metadata"
                )


class MarketSelection(Model):
    connector: Literal["uniswap_v3"]
    chain: ChainId
    pool: Address
    assertions: PoolAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.pool == ZERO_ADDRESS:
            raise ValueError("Uniswap V3 pool selection requires an EVM chain and nonzero pool")
        if self.assertions is not None and self.assertions.foreign(self.chain):
            raise ValueError("Uniswap V3 pool currency assertion belongs to another chain")
        return self


class ResolvedPool(Model):
    resolver: Literal["uniswap-v3-pool-resolution-v1"] = "uniswap-v3-pool-resolution-v1"
    selection: MarketSelection
    block: Block
    pool: UniswapPool
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
            raise ValueError("resolved Uniswap V3 pool has mixed chain or anchor scope")
        if self.selection.pool != self.pool.pool:
            raise ValueError("resolved Uniswap V3 pool differs from its authored target")
        if (self.asset0.identifier, self.asset1.identifier) != (
            self.pool.currency0,
            self.pool.currency1,
        ):
            raise ValueError("resolved Uniswap V3 assets differ from the pool currencies")
        self.require_identities()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.pool, self.assets)
        return self

    def require_identities(self):
        expected = (
            self.pool.factory,
            self.pool.pool,
            self.contracts.router,
            self.contracts.position_manager,
            self.contracts.quoter,
        )
        if tuple(identity.address for identity in self.identities) != expected:
            raise ValueError("resolved Uniswap V3 identities differ from the verified roles")

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
            "fee": self.pool.fee,
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
    pool = pool_from_immutables(reader, selection, deployment, block)
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
            "chain_mismatch", "Uniswap V3 resolution requires its selected chain and anchor"
        )
    if selection.chain not in {entry.chain for entry in candidate_support()}:
        raise DataError("unsupported_chain", "Uniswap V3 resolution chain has no declared support")


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Uniswap V3 resolution anchor is no longer canonical")


def pool_from_immutables(
    reader, selection: MarketSelection, deployment: Deployment, block: Block
) -> UniswapPool:
    """The pool's own constructor immutables name its factory, pair, fee and spacing."""
    values, _ = deployments.read_code(reader, selection.pool, block, "pool")
    named = immutable_address(values["factory"])
    if named != deployment.factory:
        raise DataError(
            "deployment_changed",
            f"Uniswap V3 pool names factory {named}, outside its reviewed deployment"
            f" {deployment.factory}",
        )
    return UniswapPool(
        kind="uniswap_v3",
        chain=block.chain,
        factory=deployment.factory,
        pool=selection.pool,
        currency0=erc20(selection.chain, values["token0"]),
        currency1=erc20(selection.chain, values["token1"]),
        fee=values["fee"],
        tick_spacing=values["tickSpacing"],
    )


def verified_identities(
    reader, pool: UniswapPool, contracts: Contracts, block: Block
) -> tuple[ContractIdentity, ...]:
    return (
        *deployments.verify_pool(reader, pool, block),
        *deployments.verify_periphery(reader, pool, contracts, block),
    )


def immutable_address(value: int) -> str:
    return address(f"0x{value:040x}")


def erc20(chain: ChainId, value: int) -> AssetType:
    return AssetType(f"{chain}/erc20:{immutable_address(value)}")
