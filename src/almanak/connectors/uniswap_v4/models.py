"""Validated Uniswap targets, fixed author parameters and scoped economic authority."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetId, AssetType
from almanak.capabilities import CapabilitySet
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.entry_swap import EntrySwapBps, require_trading_entry
from almanak.connectors.uniswap_v4.custody import custody_assets, native_index, require_custody
from almanak.connectors.uniswap_v4.domain import UniswapPool
from almanak.connectors.uniswap_v4.hooks import pool_hook
from almanak.connectors.v3_pool_domain import TickBand
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DexTarget,
    ExitExchange,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.production_config import ProductionExecution
from almanak.models import (
    Address,
    AssetAmount,
    AssetMetadata,
    Block,
    Hash32,
    Model,
    StrategyAccounts,
    UInt,
)
from almanak.triggers import HOLD_SECONDS_DESCRIPTION, IntervalTrigger

Positive128 = Annotated[UInt, Field(gt=0, lt=2**128)]
Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


class Deployment(Model):
    router: Address
    position_manager: Address
    quoter: Address
    permit2: Address


class Range(Model):
    lower: Annotated[int, Field(ge=-887272, le=887272)]
    upper: Annotated[int, Field(ge=-887272, le=887272)]

    @model_validator(mode="after")
    def increasing(self):
        if self.lower >= self.upper:
            raise ValueError("LP range must have increasing ticks")
        return self


class Target(DexTarget):
    pool: UniswapPool
    contracts: Deployment
    range: Range | None
    band: TickBand | None = Field(default=None, exclude_if=lambda v: v is None)
    funding_asset: AssetType | None = Field(default=None, exclude_if=lambda v: v is None)

    @property
    def liquidity_enabled(self):
        return self.range is not None or self.band is not None

    @property
    def funding_currency(self):
        return self.funding_asset or self.assets[0]

    @property
    def counter_currency(self):
        return next(asset for asset in self.assets if asset != self.funding_currency)

    @property
    def assets(self):
        """The two ERC-20s the fund holds, in PoolKey order; native is held wrapped."""
        return custody_assets(self.pool)

    @property
    def native_index(self):
        return native_index(self.pool)

    @model_validator(mode="after")
    def scope(self):
        require_custody(self.pool)
        if self.native_index is not None:
            from almanak.connectors.uniswap_v4.native_router import require_native_route

            require_native_route(self)
        if self.funding_currency not in self.assets:
            raise ValueError("funding currency must belong to the configured pool")
        if self.fund.chain != self.pool.chain:
            raise ValueError("pool and fund must share chain/environment scope")
        validate_liquidity_shape(self.pool, self.range, self.band)
        return self


class Adoption(Model):
    """What an operator deployment owns at its start, stated by a direct liquidity action.

    A strategy never states one: its deployment starts from funded capital alone.

    `counter` is base units of the held pool token that is not the capital asset. With
    the capital amount it is the inventory attributed to the deployment, and the wallet
    must cover both. `nft` is the PositionManager NFT the deployment takes over. Its
    owner, pool key and range are read from the chain at the deployment's first
    observation and must be the bound wallet, the allocation's pool and its range.
    """

    counter: Annotated[UInt, Field(lt=2**128)]
    nft: AssetId | None = Field(default=None, exclude_if=lambda v: v is None)


COLLECT_SECONDS_DESCRIPTION = (
    "Optional, liquidity only. Read only by strategy code to time its own fee collection; "
    "connectors never collect on a schedule. Omit when the strategy collects no fees "
    "before exit: closing the position takes the fees it accrued."
)


class Allocation(Model):
    kind: Literal["trading", "liquidity"] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    name: Name
    account: Name
    pool: UniswapPool
    contracts: Deployment
    range: Range | None = None
    band: TickBand | None = Field(default=None, exclude_if=lambda v: v is None)
    capital: AssetAmount
    # Absent on the wire unless stated, so a strategy allocation keeps its form.
    adopt: Adoption | None = Field(default=None, exclude_if=lambda v: v is None)
    # Each entry quote is sized from this share of capital, capped by the funding
    # inventory. Trading swaps all of it and repeats; liquidity swaps the part the mint
    # pairs with, offered only while no position is held, and the mint uses all inventory.
    # The default is omitted from serialized values so configurations that keep it read
    # as before.
    entry_swap_bps: EntrySwapBps = Field(default=10000, exclude_if=lambda v: v == 10000)
    hold_seconds: Positive128 | None = Field(
        default=None, exclude_if=lambda v: v is None, description=HOLD_SECONDS_DESCRIPTION
    )
    # Serialized as null when absent, as trading allocations always were.
    collect_seconds: Positive128 | None = Field(
        default=None, description=COLLECT_SECONDS_DESCRIPTION
    )
    slippage_bps: Annotated[UInt, Field(lt=10000)]
    deadline_seconds: Positive128
    native_asset: AssetType
    minimum_native_balance: UInt

    @property
    def liquidity_enabled(self):
        return self.range is not None or self.band is not None

    @property
    def chain(self):
        return self.pool.chain

    @model_validator(mode="after")
    def units(self):
        from almanak.gateway.evm import native_asset

        if self.native_asset != native_asset(self.pool.chain.chain_id):
            raise ValueError("gas must identify the chain native asset")
        require_custody(self.pool)
        if self.capital.asset not in custody_assets(self.pool):
            raise ValueError("capital must identify a configured funding currency")
        # Only an adopting allocation may state no capital: it can act on a position
        # without bringing inventory.
        least = 1 if self.adopt is None else 0
        if not least <= self.capital.amount < 2**128:
            raise ValueError("capital must fit positive uint128")
        if self.adopt is None:
            require_trading_entry(self.liquidity_enabled, self.entry_swap_bps)
        else:
            validate_adoption(self, self.adopt)
        if (
            self.collect_seconds is not None
            and self.hold_seconds is not None
            and self.collect_seconds >= self.hold_seconds
        ):
            raise ValueError("fee collection must precede scheduled closure")
        validate_liquidity_shape(self.pool, self.range, self.band)
        return self


def validate_adoption(allocation, adopt):
    """An adopting allocation is one fixed range that trades nothing.

    Its entry share must be stated as zero, since the default offers the whole capital
    as an entry swap, and it has no band, whose rebalancing is a swap as well.
    """
    if allocation.entry_swap_bps or allocation.band is not None or allocation.range is None:
        raise ValueError("an adopting allocation states a fixed range, no entry swap and no band")
    if adopt.nft is None:
        return
    chain = allocation.pool.chain.chain_id
    manager = AssetType(f"{chain}/erc721:{allocation.contracts.position_manager}")
    if adopt.nft.asset_type != manager:
        raise ValueError("an adopted NFT must be one of the allocation's PositionManager")


def validate_liquidity_shape(pool, range, band):
    entry = pool_hook(pool)
    if entry is not None and (range is not None or band is not None):
        if (refusal := entry.reviewed.liquidity_refusal) is not None:
            raise ValueError(f"{refusal}; this pool supports trading allocations only")
    if range is not None and band is not None:
        raise ValueError("a liquidity allocation declares either a fixed range or a band")
    if range is not None and any(t % pool.tick_spacing for t in (range.lower, range.upper)):
        raise ValueError("LP boundaries must be multiples of pool tick spacing")
    if band is not None:
        band.validate_pool(pool)


def validate_position_range(target, selected):
    """A banded position must retain a valid band range; a fixed one has no range of its own."""
    if target.band is not None:
        if selected is None:
            raise ValueError("banded liquidity requires the retained position range")
        target.band.validate_range(selected, target.pool.tick_spacing)
    elif selected is not None and selected != target.range:
        raise ValueError("position range differs from the configured fixed range")


class Parameters(AuthorGas):
    asset_metadata: dict[AssetType, AssetMetadata] = Field(
        default_factory=dict, exclude_if=lambda v: not v
    )
    decision_trigger: IntervalTrigger
    connector: Literal["uniswap_v4"] = "uniswap_v4"
    accounts: StrategyAccounts
    allocations: Annotated[tuple[Allocation, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def scope(self):
        if len({a.name for a in self.allocations}) != len(self.allocations):
            raise ValueError("allocation names must be unique")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("allocations must identify exactly the bound accounts")
        # One attributed inventory per account avoids pretending that shared wallet
        # balances independently belong to multiple allocations.
        if len({a.account for a in self.allocations}) != len(self.allocations):
            raise ValueError("this Uniswap slice supports one allocation per fund account")
        for item in self.allocations:
            self.target(item.name)
        return self

    @property
    def positions(self):
        return self.allocations

    def allocation(self, name):
        for item in self.allocations:
            if item.name == name:
                return item
        raise ValueError(f"unknown Uniswap allocation: {name}")

    def target(self, name):
        allocation = self.allocation(name)
        return Target(
            allocation=name,
            account=allocation.account,
            fund=self.accounts.binding(allocation.account).profile.fund,
            pool=allocation.pool,
            contracts=allocation.contracts,
            range=allocation.range,
            band=allocation.band,
            funding_asset=(
                allocation.capital.asset
                if allocation.capital.asset != custody_assets(allocation.pool)[0]
                else None
            ),
        )


class Config(Parameters, PreparedConfig):
    fork_bindings: tuple[ForkBinding, ...] = Field(default=(), exclude_if=lambda v: not v)
    fork_instance: Hash32 | None = Field(default=None, exclude_if=lambda v: v is None)
    fork_origin: Block | None = Field(default=None, exclude_if=lambda v: v is None)
    production: ProductionExecution | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def execution_scope(self):
        validate_environments(self)
        return self


class Capability(Model):
    """Scope only: the exact pool, contracts, fund and range or band of one allocation."""

    target: Target


Capabilities = CapabilitySet[Capability]
Intent = Annotated[
    SwapExactInput[Target]
    | OpenLiquidity[Target]
    | CollectLiquidityFees[Target]
    | CloseLiquidity[Target]
    | RevokeExchange[Target]
    | ExitExchange[Target],
    Field(discriminator="kind"),
]
