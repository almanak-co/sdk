"""Scoped V3 reserve allocations and one owner per fund/Pool/asset balance."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.lending_health import (
    MINIMUM_HEALTH_FACTOR_DESCRIPTION,
    require_borrow_floor,
)
from almanak.connectors.sparklend.domain import Reserve, Target, Uint256
from almanak.connectors.sparklend.math import UINT256_MAX
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.evm import native_asset
from almanak.gateway.production_config import ProductionExecution
from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
    EconomicLendingIntent,
    Name,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import (
    Account,
    Address,
    AssetAmount,
    AssetMetadata,
    Block,
    Hash32,
    Model,
    StrategyAccounts,
)
from almanak.triggers import IntervalTrigger

FiniteAmount = Annotated[Uint256, Field(lt=UINT256_MAX)]
HealthFactor = Annotated[Uint256, Field(gt=10**18)]


class Allocation(Model):
    name: Name
    account: Name
    reserve: Reserve
    # Base units of the reserve asset the allocation starts with; the funding statement
    # requires it. It is not a limit: how much to supply is the strategy's decision.
    capital: FiniteAmount = 0
    # Authority for the reserve as collateral, including the Pool's automatic enablement of
    # a first supply when the reserve's LTV is above zero.
    collateral: bool
    # Borrow and repay authority for this reserve; supply and withdrawal are always granted.
    borrow: bool = False
    minimum_health_factor: HealthFactor | None = Field(
        default=None,
        exclude_if=lambda v: v is None,
        description=MINIMUM_HEALTH_FACTOR_DESCRIPTION,
    )
    oracle: Address
    price_source: Address
    native_asset: AssetType
    minimum_native_balance: Uint256

    @property
    def chain(self):
        return self.reserve.chain

    @model_validator(mode="after")
    def native_scope(self):
        if self.native_asset != native_asset(self.reserve.chain.chain_id):
            raise ValueError("native payer asset differs from the reserve chain")
        require_borrow_floor(self.borrow, self.minimum_health_factor)
        return self


class Parameters(AuthorGas):
    connector: Literal["sparklend"] = "sparklend"
    decision_trigger: IntervalTrigger
    asset_metadata: dict[AssetType, AssetMetadata] = Field(default_factory=dict)
    accounts: StrategyAccounts
    allocations: Annotated[tuple[Allocation, ...], Field(min_length=1)]

    @property
    def positions(self):
        return self.allocations

    def allocation(self, name: str) -> Allocation:
        for allocation in self.allocations:
            if allocation.name == name:
                return allocation
        raise ValueError(f"unknown SparkLend allocation: {name}")

    def target(self, name: str) -> Target:
        allocation = self.allocation(name)
        try:
            fund = self.accounts.binding(allocation.account).profile.fund
        except KeyError:
            raise ValueError("SparkLend allocation references an unbound account") from None
        return Target(
            allocation=name, account=allocation.account, fund=fund, reserve=allocation.reserve
        )

    @model_validator(mode="after")
    def scope(self):
        names = [a.name for a in self.allocations]
        if len(set(names)) != len(names):
            raise ValueError("SparkLend allocation names must be unique")
        targets = [self.target(name) for name in names]
        if len({target.identity for target in targets}) != len(targets):
            raise ValueError("allocations cannot alias a fund/Pool/asset balance")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("each bound account requires an explicit reserve allocation")
        self.validate_pool_policies()
        return self

    def validate_pool_policies(self):
        policies: dict[tuple[Account, Address], tuple[int | None, Address]] = {}
        for allocation in self.allocations:
            target = self.target(allocation.name)
            key = target.fund, target.reserve.pool
            policy = allocation.minimum_health_factor, allocation.oracle
            if policies.setdefault(key, policy) != policy:
                raise ValueError("fund/Pool allocations must share one risk policy and oracle")

    def supply(self, name: str, amount: int) -> SupplyIntent[Target]:
        """Supply `amount` base units of the reserve asset from fund inventory.

        A first supply into a reserve with an LTV enables collateral; needs `collateral: true`.
        """
        target = self.target(name)
        return SupplyIntent[Target](
            target=target, amount=AssetAmount(asset=target.asset, amount=amount)
        )

    def borrow(self, name: str, amount: int) -> BorrowIntent[Target]:
        """Borrow `amount` base units of the reserve asset; needs `borrow` authority."""
        target = self.target(name)
        return BorrowIntent[Target](
            target=target, amount=AssetAmount(asset=target.asset, amount=amount)
        )

    def withdraw(self, name: str) -> WithdrawIntent[Target]:
        """Withdraw the whole supplied balance, including accrued interest."""
        return WithdrawIntent[Target](target=self.target(name))

    def withdraw_amount(self, name: str, amount: int) -> WithdrawAmountIntent[Target]:
        """Withdraw exactly `amount` base units, leaving the rest supplied."""
        target = self.target(name)
        return WithdrawAmountIntent[Target](
            target=target, amount=AssetAmount(asset=target.asset, amount=amount)
        )

    def repay(self, name: str) -> RepayIntent[Target]:
        """Repay the whole debt with accrued interest; the fund must hold all of it."""
        return RepayIntent[Target](target=self.target(name))

    def repay_amount(self, name: str, amount: int) -> RepayAmountIntent[Target]:
        """Repay exactly `amount` base units; refuses an amount above the outstanding debt."""
        target = self.target(name)
        return RepayAmountIntent[Target](
            target=target, amount=AssetAmount(asset=target.asset, amount=amount)
        )

    def set_collateral(self, name: str, enabled: bool) -> CollateralIntent[Target]:
        """Enable or disable the supplied balance as collateral.

        Disabling with debt outstanding must keep the minimum health factor.
        """
        return CollateralIntent[Target](target=self.target(name), enabled=enabled)


Intent = EconomicLendingIntent[Target]


class Config(Parameters, PreparedConfig):
    fork_bindings: tuple[ForkBinding, ...] = Field(default=(), exclude_if=lambda v: not v)
    fork_instance: Hash32 | None = Field(default=None, exclude_if=lambda v: v is None)
    fork_origin: Block | None = Field(default=None, exclude_if=lambda v: v is None)
    production: ProductionExecution | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def execution_scope(self):
        validate_environments(self)
        return self
