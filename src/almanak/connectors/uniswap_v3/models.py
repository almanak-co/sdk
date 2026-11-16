"""Fixed V3 economic parameters bind named funds to explicit configured markets."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.capabilities import CapabilitySet
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.uniswap_v3.domain import UniswapPool
from almanak.connectors.v3_pool_models import Allocation as FamilyAllocation
from almanak.connectors.v3_pool_models import Target as FamilyTarget
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DecreaseLiquidity,
    ExitExchange,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.production_config import ProductionExecution
from almanak.models import AssetMetadata, Model, StrategyAccounts
from almanak.triggers import IntervalTrigger


class Target(FamilyTarget[UniswapPool]):
    pass


class Allocation(FamilyAllocation[UniswapPool]):
    pass


class Parameters(AuthorGas):
    connector: Literal["uniswap_v3"] = "uniswap_v3"
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
        raise ValueError(f"unknown V3 allocation: {name}")

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
            funding_asset=allocation.capital.asset,
        )

    @model_validator(mode="after")
    def scope(self):
        if len({a.name for a in self.allocations}) != len(self.allocations):
            raise ValueError("V3 allocation names must be unique")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("every named fund requires an explicit market allocation")
        if len({a.account for a in self.allocations}) != len(self.allocations):
            raise ValueError("V3 candidate inventory currently requires one allocation per fund")
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
