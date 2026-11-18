"""Named Comet allocations share authoritative market and fund risk parameters."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.compound_v3.domain import Market, Reserve, Target, Uint256
from almanak.connectors.lending_health import (
    MINIMUM_HEALTH_FACTOR_DESCRIPTION,
    require_borrow_floor,
)
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.evm import native_asset
from almanak.gateway.production_config import ProductionExecution
from almanak.lending_intents import (
    BorrowIntent,
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
    Chain,
    Hash32,
    Model,
    StrategyAccounts,
)
from almanak.triggers import IntervalTrigger

FiniteAmount = Annotated[Uint256, Field(lt=2**256 - 1)]
HealthFactor = Annotated[Uint256, Field(gt=10**18)]


class Allocation(Model):
    name: Name
    account: Name
    reserve: Reserve
    # Base units of the reserve asset the allocation starts with; the funding statement
    # requires it. It is not a limit: how much to supply is the strategy's decision.
    capital: FiniteAmount = 0
    # Borrow and repay authority on a base allocation; supply and withdrawal are always granted.
    borrow: bool = False
    minimum_health_factor: HealthFactor | None = Field(
        default=None,
        exclude_if=lambda v: v is None,
        description=MINIMUM_HEALTH_FACTOR_DESCRIPTION,
    )
    native_asset: AssetType
    minimum_native_balance: Uint256

    @property
    def chain(self):
        return self.reserve.chain

    @model_validator(mode="after")
    def authority(self):
        if not self.reserve.is_base and self.borrow:
            raise ValueError("Compound collateral cannot carry borrowing or repayment authority")
        if self.native_asset != native_asset(self.reserve.chain.chain_id):
            raise ValueError("Compound payer asset differs from its configured chain")
        require_borrow_floor(self.borrow, self.minimum_health_factor)
        return self


class Parameters(AuthorGas):
    connector: Literal["compound_v3"] = "compound_v3"
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
        raise ValueError(f"unknown Compound allocation: {name}")

    def target(self, name: str) -> Target:
        allocation = self.allocation(name)
        try:
            fund = self.accounts.binding(allocation.account).profile.fund
        except KeyError:
            raise ValueError("Compound allocation references an unbound account") from None
        return Target(
            allocation=name, account=allocation.account, fund=fund, reserve=allocation.reserve
        )

    @model_validator(mode="after")
    def scope(self):
        names = [a.name for a in self.allocations]
        if len(set(names)) != len(names):
            raise ValueError("Compound allocation names must be unique")
        targets = [self.target(name) for name in names]
        if len({target.identity for target in targets}) != len(targets):
            raise ValueError("Compound allocations cannot alias a fund/Comet/asset balance")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("each Compound fund requires an explicit allocation")
        self.validate_markets()
        self.validate_borrowing()
        return self

    def validate_markets(self):
        markets: dict[tuple[Chain, Address], Market] = {}
        policies: dict[tuple[Account, Address], int | None] = {}
        for allocation in self.allocations:
            market = allocation.reserve.market
            target = self.target(allocation.name)
            if markets.setdefault((market.chain, market.comet), market) != market:
                raise ValueError(
                    "Compound market allocations require one base asset and price source"
                )
            scope = target.fund, market.comet
            policy = allocation.minimum_health_factor
            if policies.setdefault(scope, policy) != policy:
                raise ValueError("Compound fund/Comet allocations require one risk policy")

    def validate_borrowing(self):
        collateral_scopes = {
            (self.target(a.name).fund, a.reserve.market)
            for a in self.allocations
            if not a.reserve.is_base
        }
        for allocation in self.allocations:
            if (
                allocation.borrow
                and (self.target(allocation.name).fund, allocation.reserve.market)
                not in collateral_scopes
            ):
                raise ValueError(
                    "Compound borrowing requires collateral allocated to the same fund/Comet"
                )

    def supply(self, name: str, amount: int) -> SupplyIntent[Target]:
        """Supply `amount` base units of the allocation's asset to its Comet.

        The base asset earns interest; a collateral asset backs borrowing of the base asset.
        """
        target = self.target(name)
        return SupplyIntent[Target](
            target=target, amount=AssetAmount(asset=target.asset, amount=amount)
        )

    def borrow(self, name: str, amount: int) -> BorrowIntent[Target]:
        """Borrow `amount` of the Comet base asset against collateral in the same Comet.

        Needs `borrow` authority on the base allocation.
        """
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


Intent = Annotated[
    SupplyIntent[Target]
    | BorrowIntent[Target]
    | WithdrawIntent[Target]
    | WithdrawAmountIntent[Target]
    | RepayIntent[Target]
    | RepayAmountIntent[Target],
    Field(discriminator="kind"),
]


class Config(Parameters, PreparedConfig):
    fork_bindings: tuple[ForkBinding, ...] = Field(default=(), exclude_if=lambda v: not v)
    fork_instance: Hash32 | None = Field(default=None, exclude_if=lambda v: v is None)
    fork_origin: Block | None = Field(default=None, exclude_if=lambda v: v is None)
    production: ProductionExecution | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def execution_scope(self):
        validate_environments(self)
        return self
