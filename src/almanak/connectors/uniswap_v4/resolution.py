"""Resolve an exact authored Uniswap V4 pool key through trusted canonical reads.

These connector-owned inputs and results do not grant execution authority. The
gateway bootstrap binds their read scope; public author loading is unchanged.
"""

import hashlib
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType, ChainId
from almanak.connectors.support import candidate_uniswap_support
from almanak.connectors.uniswap_v4 import deployments
from almanak.connectors.uniswap_v4.custody import custody_asset, is_native
from almanak.connectors.uniswap_v4.domain import PoolFee, UniswapPool
from almanak.connectors.uniswap_v4.hook_terms import HookTerms, reviewed_terms
from almanak.connectors.uniswap_v4.hooks import admit_key, pool_hook, require_reviewed_code
from almanak.connectors.uniswap_v4.models import Deployment
from almanak.connectors.uniswap_v4.networks import NetworkRoles, roles_for
from almanak.connectors.uniswap_v4.pool import pool_id, read_pool
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import currency_address
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, address


class PoolAssertions(Model):
    """Author expectations the resolver rejects on; they never supply a fact."""

    pool_manager: Address | None = None
    state_view: Address | None = None
    decimals0: Annotated[int, Field(ge=0, le=255)] | None = None
    decimals1: Annotated[int, Field(ge=0, le=255)] | None = None

    def verify(self, pool: UniswapPool, assets: tuple[Asset, Asset]):
        observed = {
            "pool_manager": pool.pool_manager,
            "state_view": pool.state_view,
            "decimals0": assets[0].decimals,
            "decimals1": assets[1].decimals,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(
                    f"Uniswap V4 pool assertion {field} differs from verified metadata"
                )


class MarketSelection(Model):
    connector: Literal["uniswap_v4"]
    chain: ChainId
    currency0: AssetType
    currency1: AssetType
    fee: PoolFee
    tick_spacing: Annotated[int, Field(strict=True, ge=1, le=32767)]
    hooks: Address = ZERO_ADDRESS
    assertions: PoolAssertions | None = None

    @property
    def key(self):
        """The five PoolKey members the author states; their hash names the pool."""
        return (self.currency0, self.currency1, self.fee, self.tick_spacing, self.hooks)

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155":
            raise ValueError("Uniswap V4 pool selection requires an EVM chain")
        if any(currency.chain != self.chain for currency in (self.currency0, self.currency1)):
            raise ValueError("Uniswap V4 pool currencies must belong to the selected chain")
        if currency_address(self.currency0, self.chain) >= currency_address(
            self.currency1, self.chain
        ):
            raise ValueError("PoolKey currencies must be distinct and in canonical address order")
        return self


class HookAdmission(Model):
    """The reviewed profile and pinned code a hooked pool was admitted with, and its terms."""

    profile: str
    identity: ContractIdentity
    terms: HookTerms


class VerifiedRoles(Model):
    """The pool/contracts view the shared deployment verification consumes."""

    pool: UniswapPool
    contracts: Deployment


class ResolvedPool(Model):
    resolver: Literal["uniswap-v4-pool-resolution-v1"] = "uniswap-v4-pool-resolution-v1"
    selection: MarketSelection
    block: Block
    pool: UniswapPool
    contracts: Deployment
    asset0: Asset
    asset1: Asset
    # The wrapped token the fund holds for a native currency, with its read precision.
    custody0: Asset | None = Field(default=None, exclude_if=lambda v: v is None)
    custody1: Asset | None = Field(default=None, exclude_if=lambda v: v is None)
    identities: tuple[ContractIdentity, ...]
    hook: HookAdmission | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        self.require_target()
        self.require_custody()
        self.require_identities()
        self.require_hook()
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.pool, self.currencies)
        return self

    def require_target(self):
        chains = {self.pool.chain, self.asset0.chain, self.asset1.chain}
        if chains != {self.block.chain} or self.selection.chain != self.block.chain.chain_id:
            raise ValueError("resolved Uniswap V4 pool has mixed chain or anchor scope")
        if self.selection.key != pool_key(self.pool):
            raise ValueError("resolved Uniswap V4 pool differs from its authored key")
        if (self.asset0.identifier, self.asset1.identifier) != (
            self.pool.currency0,
            self.pool.currency1,
        ):
            raise ValueError("resolved Uniswap V4 assets differ from the pool currencies")

    def require_custody(self):
        for currency, held in ((self.asset0, self.custody0), (self.asset1, self.custody1)):
            identity = currency_type(currency)
            expected = custody_asset(identity) if is_native(identity) else None
            if (held and held.identifier) != expected or (
                held and held.decimals != currency.decimals
            ):
                raise ValueError("resolved Uniswap V4 custody differs from its native currency")

    @property
    def roles(self) -> VerifiedRoles:
        return VerifiedRoles(pool=self.pool, contracts=self.contracts)

    def require_identities(self):
        """A reloaded result carries the verified deployment code, not only addresses."""
        try:
            expected = tuple(sorted(deployments.infrastructure(self.roles)))
            deployments.verify_identities(self.roles, self.identities)
        except DataError as error:
            raise ValueError(f"resolved Uniswap V4 deployment is unverified: {error}") from error
        if tuple(identity.address for identity in self.identities) != expected:
            raise ValueError("resolved Uniswap V4 identities differ from the verified roles")

    def require_hook(self):
        """A reloaded result names the reviewed profile of its hook; the pin is not re-read."""
        entry, hook = pool_hook(self.pool), self.hook
        if entry is None and hook is None:
            return
        if (
            entry is None
            or hook is None
            or (hook.profile, hook.identity.address) != (entry.profile, self.pool.hooks)
        ):
            raise ValueError("resolved Uniswap V4 hook differs from its reviewed profile")

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def currencies(self) -> tuple[Asset, Asset]:
        return (self.asset0, self.asset1)

    @property
    def assets(self) -> tuple[Asset, Asset]:
        """What a fund holds: a native currency is held as its wrapped token."""
        return (self.custody0 or self.asset0, self.custody1 or self.asset1)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "pool_id": pool_id(self.pool),
            "pool_manager": self.pool.pool_manager,
            "currencies": [str(asset.identifier) for asset in self.currencies],
            "fee": self.pool.fee,
            "tick_spacing": self.pool.tick_spacing,
            "hooks": self.pool.hooks,
            "decimals": [asset.decimals for asset in self.assets],
            "block": self.block.number,
            **({} if self.hook is None else {"hook_profile": self.hook.profile}),
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedPool:
    require_scope(reader, selection, block)
    require_anchor(reader, block)
    roles = roles_for(selection.chain)
    pool = authored_pool(selection, roles, block)
    require_holdable(pool)
    observation = read_pool(reader, pool, block)
    identities = verified_identities(reader, pool, roles, block)
    hook = admitted_hook(reader, pool, block)
    require_anchor(reader, block)
    return ResolvedPool(
        selection=selection,
        block=block,
        pool=pool,
        contracts=roles.contracts,
        asset0=observation.currency0,
        asset1=observation.currency1,
        custody0=custody(reader, observation.currency0, block),
        custody1=custody(reader, observation.currency1, block),
        identities=identities,
        hook=hook,
    )


def currency_type(currency: Asset) -> AssetType:
    if not isinstance(currency.identifier, AssetType):
        raise ValueError("Uniswap V4 currencies are fungible asset types")
    return currency.identifier


def require_holdable(pool: UniswapPool):
    """A native currency the fund cannot hold as its wrapped token is refused before any read."""
    from almanak.connectors.uniswap_v4.native_router import routes

    held = []
    for currency in (pool.currency0, pool.currency1):
        try:
            held.append(custody_asset(currency))
        except ValueError as error:
            raise DataError("unsupported_pool", str(error)) from None
        if is_native(currency) and str(pool.chain.chain_id) not in routes():
            raise DataError(
                "unsupported_pool", "Uniswap V4 native pools have no reviewed route on this chain"
            )
    if held[0] == held[1]:
        raise DataError(
            "unsupported_pool",
            "a native/wrapped-native pool would hold one asset for both currencies",
        )


def custody(reader, currency: Asset, block: Block) -> Asset | None:
    """The wrapped token held for a native currency, refused unless it has the same precision."""
    identity = currency_type(currency)
    if not is_native(identity):
        return None
    try:
        wrapped = reader.asset(custody_asset(identity), block)
    except ValueError as error:
        raise DataError("unsupported_pool", str(error)) from None
    if wrapped.decimals != currency.decimals:
        raise DataError("unsupported_pool", "wrapped native precision differs from the native coin")
    return wrapped


def admitted_hook(reader, pool: UniswapPool, block: Block) -> HookAdmission | None:
    """A hooked pool is admitted only with its reviewed code and measurable terms."""
    entry = require_reviewed_code(AuthorityReader(reader, block), pool)
    if entry is None:
        return None
    return HookAdmission(
        profile=entry.profile,
        identity=entry.identity,
        terms=reviewed_terms(reader, pool, block, entry),
    )


def require_scope(reader, selection: MarketSelection, block: Block):
    """Every scope refusal precedes the first provider read."""
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError(
            "chain_mismatch", "Uniswap V4 resolution requires its selected chain and anchor"
        )
    if selection.chain not in supported_chains():
        raise DataError("unsupported_chain", "Uniswap V4 resolution chain has no declared support")
    try:
        admit_key(selection.chain, selection.fee, selection.hooks)
    except ValueError as error:
        raise DataError("unsupported_pool", str(error)) from None


def supported_chains():
    return {entry.chain for entry in candidate_uniswap_support()}


def require_anchor(reader, block: Block):
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "Uniswap V4 resolution anchor is no longer canonical")


def authored_pool(selection: MarketSelection, roles: NetworkRoles, block: Block) -> UniswapPool:
    """The authored key names the pool; only the deployment roles come from review."""
    return UniswapPool(
        kind="uniswap_v4",
        chain=block.chain,
        pool_manager=roles.pool_manager,
        state_view=roles.state_view,
        currency0=selection.currency0,
        currency1=selection.currency1,
        fee=selection.fee,
        tick_spacing=selection.tick_spacing,
        hooks=selection.hooks,
    )


def verified_identities(
    reader, pool: UniswapPool, roles: NetworkRoles, block: Block
) -> tuple[ContractIdentity, ...]:
    """`read_pool` already bound the StateView; the other periphery names it here."""
    authority = AuthorityReader(reader, block)
    for contract in (roles.quoter, roles.position_manager):
        if address(authority.call(contract, "poolManager()")[0]) != pool.pool_manager:
            raise DataError("authority", "Uniswap V4 periphery has another PoolManager")
    identities = tuple(authority.identity(target) for target in roles.addresses)
    deployments.verify_identities(VerifiedRoles(pool=pool, contracts=roles.contracts), identities)
    return identities


def pool_key(pool: UniswapPool):
    return (pool.currency0, pool.currency1, pool.fee, pool.tick_spacing, pool.hooks)
