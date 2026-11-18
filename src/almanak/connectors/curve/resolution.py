"""Resolve an exact authored Curve pool through trusted canonical reads.

The author names a chain and a pool address. A bounded membership probe over the
chain's reviewed factories decides the family, the connector's own verification
decides the compiled revision, and both are retained as resolved facts. These
connector-owned inputs and results do not grant execution authority. The gateway
bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.curve import deployment
from almanak.connectors.curve.domain import CurvePool, TriCryptoRevision, protocol_variant
from almanak.connectors.curve.networks import (
    Family,
    Registration,
    factories_for,
    registered_family,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model

CurveRevision = Literal[
    "stableswap-ng-2023-11-15",
    "stableswap-ng-2023-12-12",
    "twocrypto-ng-v2.1.0",
    "tricrypto-ng-v2.0.0",
    "tricrypto-ng-2024-01-04",
]
Decimals = Annotated[int, Field(ge=0, le=255)]
Coins = Annotated[tuple[AssetType, ...], Field(min_length=2, max_length=8)]

TWOCRYPTO_REVISION: CurveRevision = "twocrypto-ng-v2.1.0"
TRICRYPTO_2024: TriCryptoRevision = "tricrypto-ng-2024-01-04"
TRICRYPTO_REVISIONS: tuple[TriCryptoRevision, ...] = ("tricrypto-ng-v2.0.0", TRICRYPTO_2024)
FAMILY_REVISIONS: dict[Family, tuple[CurveRevision, ...]] = {
    "stableswap_ng": ("stableswap-ng-2023-11-15", "stableswap-ng-2023-12-12"),
    "twocrypto_ng": (TWOCRYPTO_REVISION,),
    "tricrypto_ng": TRICRYPTO_REVISIONS,
}
# The verified roles each family's deployment reports, in its own identity order.
ROLES: dict[Family, tuple[str, ...]] = {
    "stableswap_ng": ("factory", "blueprint", "pool", "views"),
    "twocrypto_ng": ("factory", "blueprint", "pool", "math", "views"),
    "tricrypto_ng": ("factory", "pool", "math", "views"),
}


class Contracts(Model):
    """Verified role addresses; a role the family does not deploy stays absent."""

    factory: Address
    pool: Address
    views: Address
    blueprint: Address | None = None
    math: Address | None = None

    def ordered(self, family: Family) -> tuple[str | None, ...]:
        return tuple(getattr(self, role) for role in ROLES[family])


class PoolAssertions(Model):
    family: Family | None = None
    revision: CurveRevision | None = None
    coins: Coins | None = None
    decimals: Annotated[tuple[Decimals, ...], Field(min_length=2, max_length=8)] | None = None

    def foreign(self, chain: ChainId) -> bool:
        return any(coin.chain != chain for coin in self.coins or ())

    def verify(self, resolved: "ResolvedPool"):
        observed = {
            "family": resolved.pool.family,
            "revision": resolved.revision,
            "coins": resolved.pool.coins,
            "decimals": tuple(asset.decimals for asset in resolved.coins),
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(f"Curve pool assertion {field} differs from verified metadata")


class MarketSelection(Model):
    connector: Literal["curve"]
    chain: ChainId
    pool: Address
    assertions: PoolAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.pool == ZERO_ADDRESS:
            raise ValueError("Curve pool selection requires an EVM chain and nonzero pool")
        if self.assertions is not None and self.assertions.foreign(self.chain):
            raise ValueError("Curve pool coin assertion belongs to another chain")
        return self


class ResolvedPool(Model):
    resolver: Literal["curve-pool-resolution-v1"] = "curve-pool-resolution-v1"
    selection: MarketSelection
    block: Block
    pool: CurvePool
    revision: CurveRevision
    contracts: Contracts
    coins: tuple[Asset, ...]
    share: Asset
    identities: tuple[ContractIdentity, ...]

    @model_validator(mode="after")
    def scope(self):
        self.require_anchor_scope()
        self.require_authored_pool()
        self.require_revision()
        self.require_identities()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self)
        return self

    def require_anchor_scope(self):
        mixed = (
            self.selection.chain != self.block.chain.chain_id
            or self.pool.chain != self.block.chain
            or any(asset.chain != self.block.chain for asset in self.assets)
        )
        if mixed:
            raise ValueError("resolved Curve pool has mixed chain or anchor scope")

    def require_authored_pool(self):
        if self.selection.pool != self.pool.pool:
            raise ValueError("resolved Curve pool differs from its authored target")
        if tuple(asset.identifier for asset in self.coins) != self.pool.coins:
            raise ValueError("resolved Curve assets differ from the verified pool coins")
        if self.share.identifier != erc20(self.selection.chain, self.pool.pool):
            raise ValueError("resolved Curve share differs from the pool's own token")

    def require_revision(self):
        if self.revision not in FAMILY_REVISIONS[self.pool.family]:
            raise ValueError("resolved Curve revision does not belong to the verified family")
        if self.pool.variant != protocol_variant(self.pool.family, self.revision):
            raise ValueError("resolved Curve revision differs from the verified pool variant")

    def require_identities(self):
        addresses = tuple(identity.address for identity in self.identities)
        if addresses != self.contracts.ordered(self.pool.family):
            raise ValueError("resolved Curve identities differ from the verified roles")
        if (self.contracts.factory, self.contracts.pool) != (self.pool.factory, self.pool.pool):
            raise ValueError("resolved Curve contracts differ from the verified pool")

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def assets(self) -> tuple[Asset, ...]:
        """The verified coins and the pool's own LP token, all with verified decimals."""
        return (*self.coins, self.share)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "pool": self.pool.pool,
            "family": self.pool.family,
            "revision": self.revision,
            "factory": self.pool.factory,
            "coins": [str(asset.identifier) for asset in self.coins],
            "decimals": [asset.decimals for asset in self.coins],
            "share_decimals": self.share.decimals,
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedPool:
    require_scope(reader, selection, block)
    listed = factories_for(selection.chain)
    require_anchor(reader, block)
    entry = registered_family(reader, listed, selection.pool, block)
    pool, revision, verified = verified_pool(reader, selection, entry, block)
    coins = tuple(reader.asset(coin, block) for coin in pool.coins)
    share = reader.asset(erc20(selection.chain, pool.pool), block)
    require_anchor(reader, block)
    return ResolvedPool(
        selection=selection,
        block=block,
        pool=pool,
        revision=revision,
        contracts=contracts_of(pool.family, verified),
        coins=coins,
        share=share,
        identities=tuple(verified.identities),
    )


def require_scope(reader, selection: MarketSelection, block: Block):
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError("chain_mismatch", "Curve resolution requires its selected chain and anchor")


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Curve resolution anchor is no longer canonical")


def verified_pool(reader, selection: MarketSelection, entry: Registration, block: Block):
    """One bounded compiled-revision probe; only an unmatched revision retries."""
    for candidate in revision_candidates(entry.family):
        pool = candidate_pool(selection, entry, candidate, block)
        verified = attempt(reader, pool, block)
        if verified is not None:
            return pool, resolved_revision(entry.family, candidate, verified), verified
    raise DataError("unsupported_revision", "the Curve pool matches no reviewed compiled revision")


def attempt(reader, pool: CurvePool, block: Block):
    try:
        return deployment.verify_pool(reader, pool, block)
    except DataError as error:
        if error.kind != "revision":
            raise
        return None


def revision_candidates(family: Family) -> tuple[TriCryptoRevision | None, ...]:
    """Only TriCrypto NG ships more than one reviewed compiled file set."""
    return TRICRYPTO_REVISIONS if family == "tricrypto_ng" else (None,)


def candidate_pool(
    selection: MarketSelection, entry: Registration, candidate, block: Block
) -> CurvePool:
    # The connector spells the default TriCrypto file set as an absent revision.
    return CurvePool(
        kind="curve",
        family=entry.family,
        chain=block.chain,
        factory=entry.factory,
        pool=selection.pool,
        coins=tuple(erc20(selection.chain, coin) for coin in entry.coins),
        implementation_revision=candidate if candidate == TRICRYPTO_2024 else None,
    )


def resolved_revision(
    family: Family, candidate: TriCryptoRevision | None, verified
) -> CurveRevision:
    if family == "stableswap_ng":
        stable: CurveRevision = verified.revision
        return stable
    # TwoCrypto NG probes no alternative file set, so its absent candidate is v2.1.0.
    return candidate if candidate is not None else TWOCRYPTO_REVISION


def contracts_of(family: Family, verified) -> Contracts:
    roles = zip(ROLES[family], (identity.address for identity in verified.identities), strict=True)
    return Contracts.model_validate(dict(roles))


def erc20(chain: ChainId, value: str) -> AssetType:
    return AssetType(f"{chain}/erc20:{value}")
