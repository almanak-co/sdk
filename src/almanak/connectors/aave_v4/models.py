"""Named Aave supply positions, economic intents and strategy transitions."""

from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.capabilities import CapabilitySet
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aave_v4.observations import (
    CollateralObservation as CollateralObservation,
)
from almanak.connectors.aave_v4.observations import (
    LendingSnapshot as LendingSnapshot,
)
from almanak.connectors.aave_v4.observations import (
    SupplyTarget as SupplyTarget,
)
from almanak.connectors.decision_observations import Observation
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.production_config import ProductionExecution
from almanak.lending_intents import (
    BorrowIntent,
    OwnedCash,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.lending_intents import (
    Name as Name,
)
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
from almanak.strategy_api import (
    DecisionContext,
    OutcomeContext,
    SimulationEvidence,
    StrategyDecision,
)
from almanak.strategy_api import (
    Memory as Memory,
)
from almanak.triggers import IntervalTrigger, TriggerEvent
from almanak.work import ObservationScope


class Position(Model):
    """A named fund position in one Spoke reserve: a supply allocation or a debt."""

    name: Name
    account: Name
    reserve: AaveReserve
    native_asset: AssetType
    minimum_native_balance: UInt = 0
    hold_seconds: UInt | None = None

    @property
    def chain(self):
        return self.reserve.chain

    @model_validator(mode="after")
    def native_scope(self) -> Self:
        from almanak.gateway.evm import native_asset

        if self.native_asset != native_asset(self.reserve.chain.chain_id):
            raise ValueError("native gas asset differs from the allocation network")
        return self


class Allocation(Position):
    """Supply and full withdrawal of one reserve; how much to supply is the strategy's."""

    # Base units of the reserve asset the allocation starts with; the funding statement
    # requires it. It is not a limit.
    capital: Annotated[UInt, Field(lt=2**256)] = 0


class DebtAllocation(Position):
    """Borrowing and full repayment of one reserve against named supply collateral."""

    collateral: Annotated[tuple[Name, ...], Field(min_length=1)]
    minimum_health_factor: Annotated[UInt, Field(gt=10**18)]
    oracle: Address
    collateral_price_sources: tuple[Address, ...]
    debt_price_source: Address

    @model_validator(mode="after")
    def limits(self) -> Self:
        if len(set(self.collateral)) != len(self.collateral):
            raise ValueError("collateral references must be unique")
        if len(self.collateral_price_sources) != len(self.collateral):
            raise ValueError("each collateral requires its explicit oracle source")
        return self


class LendingParameters(AuthorGas):
    asset_metadata: dict[AssetType, AssetMetadata] = Field(
        default_factory=dict, exclude_if=lambda v: not v
    )
    decision_trigger: IntervalTrigger
    accounts: StrategyAccounts
    allocations: Annotated[tuple[Allocation, ...], Field(min_length=1)]
    debts: tuple[DebtAllocation, ...] = ()

    @model_validator(mode="after")
    def scope(self) -> Self:
        names = [allocation.name for allocation in self.positions]
        if len(set(names)) != len(names):
            raise ValueError("allocation names must be unique")
        targets = [self.target(name) for name in names]
        if len({target.identity for target in targets}) != len(targets):
            raise ValueError("allocations cannot alias the same fund/Spoke/reserve position")
        if {a.account for a in self.allocations} != {b.name for b in self.accounts.bindings}:
            raise ValueError("each bound account must have an explicit allocation")
        self.validate_debts()
        return self

    @property
    def positions(self) -> tuple[Position, ...]:
        return (*self.allocations, *self.debts)

    def validate_debts(self):
        scopes = []
        for debt in self.debts:
            scopes.append((debt.account, debt.reserve.chain, debt.reserve.spoke))
            self.validate_collateral(debt)
        if len(set(scopes)) != len(scopes):
            raise ValueError("this Aave profile admits one debt reserve per fund/Spoke")

    def validate_collateral(self, debt):
        for name in debt.collateral:
            collateral = self.allocation(name)
            if isinstance(collateral, DebtAllocation) or (
                collateral.account,
                collateral.reserve.chain,
                collateral.reserve.spoke,
            ) != (debt.account, debt.reserve.chain, debt.reserve.spoke):
                raise ValueError("collateral must be supplied in the debt fund and Spoke")

    def allocation(self, name: str) -> Position:
        for allocation in self.positions:
            if allocation.name == name:
                return allocation
        raise ValueError(f"unknown allocation: {name}")

    def target(self, name: str) -> SupplyTarget:
        allocation = self.allocation(name)
        try:
            fund = self.accounts.binding(allocation.account).profile.fund
        except KeyError:
            raise ValueError("allocation references an unbound account") from None
        return SupplyTarget(
            allocation=name, account=allocation.account, fund=fund, reserve=allocation.reserve
        )

    def supply(self, name: str, amount: int) -> SupplyIntent[SupplyTarget]:
        """Supply `amount` base units of the reserve asset from fund inventory."""
        target = self.target(name)
        return SupplyIntent[SupplyTarget](
            target=target,
            amount=AssetAmount(asset=target.reserve.asset, amount=amount),
        )

    def withdraw(self, name: str) -> WithdrawIntent[SupplyTarget]:
        """Withdraw the whole supplied balance, including accrued interest."""
        return WithdrawIntent[SupplyTarget](target=self.target(name))

    def borrow(self, name: str, amount: int) -> BorrowIntent[SupplyTarget]:
        """Borrow `amount` base units on a debt allocation.

        Borrowing is backed by the debt allocation's named collateral allocations.
        """
        return BorrowIntent[SupplyTarget](
            target=self.target(name),
            amount=AssetAmount(asset=self.target(name).asset, amount=amount),
        )

    def repay(self, name: str) -> RepayIntent[SupplyTarget]:
        """Repay the whole debt with accrued interest; the fund must hold all of it."""
        return RepayIntent[SupplyTarget](target=self.target(name))


class OraclePrice(Model):
    block: Block
    oracle: Address
    feed: Address
    asset: AssetType
    quote_unit: Literal["aave_oracle_base_currency"] = "aave_oracle_base_currency"
    decimals: UInt
    value: UInt


class BorrowProjection(Model):
    block: Block
    health_factor: UInt
    prices: tuple[OraclePrice, ...]


class LendingConfig(LendingParameters, PreparedConfig):
    fork_bindings: tuple[ForkBinding, ...] = Field(default=(), exclude_if=lambda v: not v)
    fork_instance: Hash32 | None = Field(default=None, exclude_if=lambda v: v is None)
    fork_origin: Block | None = Field(default=None, exclude_if=lambda v: v is None)
    production: ProductionExecution | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def execution_scope(self):
        validate_environments(self)
        return self


EffectiveLendingConfig = LendingConfig | LendingParameters


class Ownership(Model):
    target: SupplyTarget
    shares: UInt
    drawn_shares: UInt = 0
    premium_shares: UInt = 0
    premium_offset_ray: int = 0
    collateral_enabled: bool = False
    inventory: OwnedCash = None


Intent = Annotated[
    SupplyIntent[SupplyTarget]
    | WithdrawIntent[SupplyTarget]
    | BorrowIntent[SupplyTarget]
    | RepayIntent[SupplyTarget],
    Field(discriminator="kind"),
]


class SupplyCapability(Model):
    kind: Literal["aave_supply"] = "aave_supply"
    target: SupplyTarget


class BorrowCapability(Model):
    kind: Literal["aave_borrow"] = "aave_borrow"
    target: SupplyTarget
    collateral: tuple[SupplyTarget, ...]


AaveCapability = Annotated[SupplyCapability | BorrowCapability, Field(discriminator="kind")]
AaveCapabilities = CapabilitySet[AaveCapability]


class DecisionInput(DecisionContext[EffectiveLendingConfig, Ownership, LendingSnapshot]):
    pass


class Decision(StrategyDecision[Intent]):
    pass


class DecisionRecord(Model):
    trigger: TriggerEvent
    identity: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    intent: Intent
    observations: tuple[Observation, ...]
    required_observations: tuple[ObservationScope, ...] = Field(
        default=(), exclude_if=lambda v: not v
    )
    memory: Memory


class ActionResult(Model):
    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    @property
    def effective_at(self) -> datetime:
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    intent: Intent
    success: bool
    complete: bool
    assets: AssetAmount
    shares: UInt
    block: Block
    tx_hash: Hash32 | None = None
    simulation: SimulationEvidence | None = None

    @model_validator(mode="after")
    def evidence(self) -> Self:
        if (self.tx_hash is None) == (self.simulation is None):
            raise ValueError("outcome requires exactly one on-chain or simulated evidence source")
        return self


class OutcomeInput(OutcomeContext[EffectiveLendingConfig, ActionResult]):
    pass
