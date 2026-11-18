"""Aerodrome targets bind economic actions to a configured fungible LP market."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.capabilities import CapabilitySet
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.aerodrome.domain import AerodromePool, Contracts
from almanak.connectors.entry_swap import EntrySwapBps, require_trading_entry
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DecreaseLiquidity,
    DexTarget,
    ExitExchange,
    IncreaseLiquidity,
    Name,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.evm import native_asset
from almanak.gateway.production_config import ProductionExecution
from almanak.models import AssetAmount, AssetMetadata, Model, StrategyAccounts, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION, IntervalTrigger, PositiveSeconds


class Target(DexTarget):
    pool: AerodromePool
    contracts: Contracts
    funding_asset: AssetType
    liquidity_enabled: bool

    @property
    def assets(self):
        return self.pool.currency0, self.pool.currency1

    @property
    def funding_currency(self):
        return self.funding_asset

    @property
    def counter_currency(self):
        return next(asset for asset in self.assets if asset != self.funding_asset)

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.pool.chain or self.funding_asset not in self.assets:
            raise ValueError(
                "Aerodrome target requires its fund chain and explicit funding currency"
            )
        return self


class Allocation(Model):
    name: Name
    account: Name
    pool: AerodromePool
    contracts: Contracts
    liquidity_enabled: bool
    capital: AssetAmount
    entry_swap_bps: EntrySwapBps
    hold_seconds: PositiveSeconds | None = Field(
        default=None, exclude_if=lambda v: v is None, description=HOLD_SECONDS_DESCRIPTION
    )
    slippage_bps: Annotated[UInt, Field(lt=10000)]
    deadline_seconds: PositiveSeconds
    native_asset: AssetType
    minimum_native_balance: UInt

    @property
    def chain(self):
        return self.pool.chain

    @model_validator(mode="after")
    def units(self):
        if self.native_asset != native_asset(self.pool.chain.chain_id):
            raise ValueError("payer policy must identify the chain native asset")
        require_trading_entry(self.liquidity_enabled, self.entry_swap_bps)
        if self.capital.asset not in (self.pool.currency0, self.pool.currency1):
            raise ValueError("capital must identify the explicit funding currency")
        if not 0 < self.capital.amount < 2**127:
            raise ValueError("capital requires a positive supported int128 quantity")
        return self


class Parameters(AuthorGas):
    connector: Literal["aerodrome"] = "aerodrome"
    decision_trigger: IntervalTrigger
    asset_metadata: dict[AssetType, AssetMetadata] = Field(default_factory=dict)
    accounts: StrategyAccounts
    allocations: Annotated[tuple[Allocation, ...], Field(min_length=1)]

    @property
    def positions(self):
        return self.allocations

    def allocation(self, name):
        for allocation in self.allocations:
            if allocation.name == name:
                return allocation
        raise ValueError(f"unknown Aerodrome allocation: {name}")

    def target(self, name):
        allocation = self.allocation(name)
        return Target(
            allocation=name,
            account=allocation.account,
            fund=self.accounts.binding(allocation.account).profile.fund,
            pool=allocation.pool,
            contracts=allocation.contracts,
            liquidity_enabled=allocation.liquidity_enabled,
            funding_asset=allocation.capital.asset,
        )

    @model_validator(mode="after")
    def scope(self):
        if len({a.name for a in self.allocations}) != len(self.allocations):
            raise ValueError("Aerodrome allocation names must be unique")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("every named fund requires an explicit market allocation")
        if len({a.account for a in self.allocations}) != len(self.allocations):
            raise ValueError(
                "Aerodrome candidate inventory currently requires one allocation per fund"
            )
        for allocation in self.allocations:
            self.target(allocation.name)
        return self


class Config(Parameters, PreparedConfig):
    fork_bindings: tuple[ForkBinding, ...] = ()
    production: ProductionExecution | None = None

    @model_validator(mode="after")
    def execution_scope(self):
        validate_environments(self)
        return self


class Capability(Model):
    target: Target


Capabilities = CapabilitySet[Capability]
Intent = Annotated[
    SwapExactInput[Target]
    | OpenLiquidity[Target]
    | IncreaseLiquidity[Target]
    | DecreaseLiquidity[Target]
    | CollectLiquidityFees[Target]
    | CloseLiquidity[Target]
    | RevokeExchange[Target]
    | ExitExchange[Target],
    Field(discriminator="kind"),
]
