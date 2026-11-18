"""Author-selected GMX position economics and named fund routing."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.capabilities import CapabilitySet
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.gmx_v2.domain import Market, Target
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.exchange_models import (
    ExchangeCapability,
    SwapAllocation,
)
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation, LiquidityCapability
from almanak.connectors.identifiers import Name
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.evm import native_asset
from almanak.gateway.production_config import ProductionExecution
from almanak.models import AssetAmount, AssetMetadata, Model, StrategyAccounts, UInt
from almanak.triggers import HOLD_SECONDS_DESCRIPTION, IntervalTrigger, PositiveSeconds

Bound = Annotated[UInt, Field(gt=0, lt=2**128)]


class Allocation(Model):
    name: Name
    account: Name
    market: Market
    side: Literal["long", "short"]
    capital: AssetAmount
    entry_collateral: AssetAmount
    entry_size_usd_e30: Bound
    execution_fee: AssetAmount
    increase_acceptable_price: UInt
    decrease_acceptable_price: UInt
    hold_seconds: PositiveSeconds | None = Field(
        default=None, exclude_if=lambda v: v is None, description=HOLD_SECONDS_DESCRIPTION
    )
    maximum_event_block_span: Annotated[UInt, Field(gt=0)]
    native_asset: AssetType
    minimum_native_balance: UInt

    @property
    def chain(self):
        return self.market.chain

    @model_validator(mode="after")
    def units(self):
        if self.capital.asset not in (self.market.long_asset, self.market.short_asset):
            raise ValueError("GMX capital must identify the configured collateral asset")
        if self.entry_collateral.asset != self.capital.asset or not (
            0 < self.entry_collateral.amount <= self.capital.amount - self.execution_fee.amount
        ):
            raise ValueError("GMX entry collateral must fit the explicit owned capital after fees")
        if self.execution_fee.asset != self.market.wrapped_native:
            raise ValueError("GMX request fee must identify configured wrapped native units")
        if self.capital.asset != self.execution_fee.asset:
            raise ValueError("GMX first position inventory requires wrapped-native collateral")
        if self.native_asset != native_asset(self.market.chain.chain_id):
            raise ValueError("GMX payer reserve must identify its chain native asset")
        if not 0 < self.execution_fee.amount < self.capital.amount:
            raise ValueError("GMX capital must cover its nonzero request fee")
        return self


class Parameters(AuthorGas):
    connector: Literal["gmx_v2"] = "gmx_v2"
    decision_trigger: IntervalTrigger
    asset_metadata: dict[AssetType, AssetMetadata] = Field(default_factory=dict)
    accounts: StrategyAccounts
    allocations: Annotated[
        tuple[Allocation | SwapAllocation | LiquidityAllocation, ...], Field(min_length=1)
    ]

    @property
    def positions(self):
        return self.allocations

    def allocation(self, name):
        return next((a for a in self.allocations if a.name == name), None) or self.unknown(name)

    @staticmethod
    def unknown(name):
        raise ValueError(f"unknown GMX allocation: {name}")

    def target(self, name):
        allocation = self.allocation(name)
        if isinstance(allocation, (SwapAllocation, LiquidityAllocation)):
            return ExchangeTarget(
                allocation=name,
                account=allocation.account,
                fund=self.accounts.binding(allocation.account).profile.fund,
                market=allocation.market,
                funding_asset=allocation.funding_asset,
            )
        return Target(
            allocation=name,
            account=allocation.account,
            fund=self.accounts.binding(allocation.account).profile.fund,
            market=allocation.market,
            collateral_asset=allocation.capital.asset,
            side=allocation.side,
        )

    @model_validator(mode="after")
    def scope(self):
        if len({a.name for a in self.allocations}) != len(self.allocations):
            raise ValueError("GMX allocation names must be unique")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("every GMX named fund requires an explicit allocation")
        if len({a.account for a in self.allocations}) != len(self.allocations):
            raise ValueError("GMX candidate inventory requires one allocation per named fund")
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
    maximum_execution_fee: Bound


Capabilities = CapabilitySet[Capability | ExchangeCapability | LiquidityCapability]
