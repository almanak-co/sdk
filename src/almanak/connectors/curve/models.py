"""Curve economic targets retain ordered coins, fund scope and economic capital."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.capabilities import CapabilitySet
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.entry_swap import EntrySwapBps, require_trading_entry
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    DexTarget,
    ExitExchange,
    IncreasePoolLiquidity,
    Name,
    OpenPoolLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.evm import native_asset
from almanak.gateway.production_config import ProductionExecution
from almanak.models import AssetMetadata, Model, StrategyAccounts, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION, IntervalTrigger, PositiveSeconds


class Target(DexTarget):
    pool: CurvePool
    funding_asset: AssetType
    trading_asset: AssetType
    liquidity_enabled: bool
    single_coin_exit: AssetType | None = None

    @property
    def assets(self):
        return self.pool.coins

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.pool.chain:
            raise ValueError("Curve target fund must retain the configured pool chain")
        if len({self.funding_asset, self.trading_asset}) != 2 or not {
            self.funding_asset,
            self.trading_asset,
        }.issubset(self.assets):
            raise ValueError("Curve target requires two distinct configured trading assets")
        if self.single_coin_exit is not None and (
            not self.liquidity_enabled or self.single_coin_exit not in self.assets
        ):
            raise ValueError("Curve single-coin exit requires enabled liquidity and a pool asset")
        return self


class Allocation(Model):
    name: Name
    account: Name
    pool: CurvePool
    funding_asset: AssetType
    trading_asset: AssetType
    liquidity_enabled: bool
    single_coin_exit: AssetType | None = None
    capital: AssetAmounts
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
        self.capital.in_order(self.pool.coins)
        self.capital.require_inventory()
        if self.native_asset != native_asset(self.chain.chain_id):
            raise ValueError("Curve payer policy must identify the chain's native asset")
        require_trading_entry(self.liquidity_enabled, self.entry_swap_bps)
        # A sanity width for coin amounts, independent of any spending authority.
        if any(value.amount >= 2**127 for value in self.capital.root):
            raise ValueError("Curve capital must be below 2**127 base units per coin")
        return self


class Parameters(AuthorGas):
    connector: Literal["curve"] = "curve"
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
        raise ValueError(f"unknown Curve allocation: {name}")

    def target(self, name):
        allocation = self.allocation(name)
        return Target(
            allocation=name,
            account=allocation.account,
            fund=self.accounts.binding(allocation.account).profile.fund,
            pool=allocation.pool,
            funding_asset=allocation.funding_asset,
            trading_asset=allocation.trading_asset,
            liquidity_enabled=allocation.liquidity_enabled,
            single_coin_exit=allocation.single_coin_exit,
        )

    @model_validator(mode="after")
    def scope(self):
        if len({allocation.name for allocation in self.allocations}) != len(self.allocations):
            raise ValueError("Curve allocation names must be unique")
        names = {allocation.account for allocation in self.allocations}
        if names != {binding.name for binding in self.accounts.bindings}:
            raise ValueError("every named Curve fund requires an explicit market allocation")
        if len(names) != len(self.allocations):
            raise ValueError("Curve candidate inventory requires one pool allocation per fund")
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
    """Scope only: the exact pool, coins, fund and liquidity choices of one allocation."""

    target: Target


Capabilities = CapabilitySet[Capability]
Intent = Annotated[
    SwapExactInput[Target]
    | OpenPoolLiquidity[Target]
    | IncreasePoolLiquidity[Target]
    | DecreasePoolLiquidity[Target]
    | ClosePoolLiquidity[Target]
    | RevokeExchange[Target]
    | ExitExchange[Target],
    Field(discriminator="kind"),
]
