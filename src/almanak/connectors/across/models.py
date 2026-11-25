"""Named bridge allocations between two bound fund accounts of one strategy."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.bridge_intents import BridgeIntent, Name, RevokeBridgeIntent
from almanak.caip import AssetType
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.across.domain import Route, Target, Uint256, refuse_native_output
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.evm import native_asset
from almanak.gateway.production_config import ProductionExecution
from almanak.models import Account, AssetAmount, AssetMetadata, Model, StrategyAccounts
from almanak.triggers import IntervalTrigger


class Recipient(Model):
    """The receiving fund an operator states directly: an address on the destination
    chain that the deployment binds no account for and never reads or signs with."""

    fund: Account


class Allocation(Model):
    name: Name
    # The source fund, which deposits, and the fund on the destination chain that receives.
    account: Name
    destination_account: Name
    route: Route
    # Base units of the sent asset the allocation starts with; the funding statement
    # requires it. It is not a limit: how much to bridge is the strategy's decision.
    capital: Uint256 = 0
    native_asset: AssetType
    minimum_native_balance: Uint256
    # A direct action's operator deployment only: the recipient, stated as a fund. Its
    # `destination_account` is then a label bound to no account, so the deployment has
    # one chain, the one it sends from. A strategy allocation names a bound account.
    adopt: Recipient | None = Field(default=None, exclude_if=lambda v: v is None)

    @property
    def chain(self):
        return self.route.chain

    @model_validator(mode="after")
    def scope(self):
        if self.native_asset != native_asset(self.route.chain.chain_id):
            raise ValueError("Across payer asset differs from its source chain")
        if self.account == self.destination_account:
            raise ValueError("an Across allocation requires two different named accounts")
        if self.adopt is not None and (
            self.adopt.fund.chain.chain_id != self.route.destination_chain
            or self.adopt.fund.chain.environment != self.route.chain.environment
        ):
            raise ValueError("a stated Across recipient is on the route's destination chain")
        return self


class Parameters(AuthorGas):
    connector: Literal["across"] = "across"
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
        raise ValueError(f"unknown Across allocation: {name}")

    def profile(self, account: str):
        try:
            return self.accounts.binding(account).profile
        except KeyError:
            raise ValueError(f"Across allocation references unbound account {account}") from None

    def recipient(self, allocation: Allocation) -> Account:
        """The fund that receives: the bound destination account's, or the stated one."""
        if allocation.adopt is not None:
            return allocation.adopt.fund
        return self.profile(allocation.destination_account).fund

    def target(self, name: str) -> Target:
        allocation = self.allocation(name)
        return Target(
            allocation=name,
            account=allocation.account,
            fund=self.profile(allocation.account).fund,
            destination_account=allocation.destination_account,
            destination_fund=self.recipient(allocation),
            route=allocation.route,
        )

    @model_validator(mode="after")
    def scope(self):
        names = [a.name for a in self.allocations]
        if len(set(names)) != len(names):
            raise ValueError("Across allocation names must be unique")
        used, sources = set(), set()
        for allocation in self.allocations:
            self.target(allocation.name)
            self.require_profiles(allocation)
            used.add(allocation.account)
            if allocation.adopt is None:
                used.add(allocation.destination_account)
            # A refund names the fund and the token, never the deposit it pays back.
            source = (allocation.account, allocation.route.asset)
            if source in sources:
                raise ValueError(
                    "Across takes one allocation per source fund and sent asset: a refund "
                    "could not be told apart between two"
                )
            sources.add(source)
        if used != {b.name for b in self.accounts.bindings}:
            raise ValueError("each bound account must send or receive an Across allocation")
        return self

    def require_profiles(self, allocation: Allocation):
        source = self.profile(allocation.account)
        # The Roles grants of a bridge are not built yet; without them a Safe cannot deposit.
        # A backtest's hypothetical account signs nothing and stands for an EOA.
        if source.kind not in ("eoa", "simulation"):
            raise ValueError("Across deposits are supported from an EOA account only")
        kind = self.recipient_kind(allocation)
        # An operator's allocation may only revoke, which pays nobody: its payout rule is
        # held where a deposit is asked for and again where the gateway admits one.
        if allocation.adopt is None:
            refuse_native_output(allocation.route, kind)

    def recipient_kind(self, allocation: Allocation) -> str:
        # A stated recipient is an address of unknown kind, held to the stricter rule.
        if allocation.adopt is not None:
            if allocation.destination_account in {b.name for b in self.accounts.bindings}:
                raise ValueError("a stated Across recipient names no bound account")
            return "eoa"
        kind = self.profile(allocation.destination_account).kind
        return "eoa" if kind == "simulation" else kind

    def bridge(self, name: str, amount: int) -> BridgeIntent[Target]:
        """Send `amount` base units of the allocation's asset to its destination account.

        The amount received is the venue's quote, never above the amount sent and never
        below it by more than the market's `max_fee_bps`. It arrives later, on the
        destination chain; until then the fund holds a transfer, not a balance.
        """
        target = self.target(name)
        self.require_payout(name)
        return BridgeIntent[Target](
            target=target, amount=AssetAmount(asset=target.asset, amount=amount)
        )

    def require_payout(self, name: str) -> None:
        """Refuse a deposit whose recipient Across would pay in the native asset."""
        allocation = self.allocation(name)
        refuse_native_output(allocation.route, self.recipient_kind(allocation))

    def revoke(self, name: str) -> RevokeBridgeIntent[Target]:
        """Clear the SpokePool approval an unfinished bridge left on the source account.

        A bridge revokes its own approval once its deposit is mined. This is for an
        approval whose deposit never was, which teardown must clear before it can close.
        """
        return RevokeBridgeIntent[Target](target=self.target(name))


Intent = Annotated[BridgeIntent[Target] | RevokeBridgeIntent[Target], Field(discriminator="kind")]


class Config(Parameters, PreparedConfig):
    fork_bindings: tuple[ForkBinding, ...] = Field(default=(), exclude_if=lambda v: not v)
    production: ProductionExecution | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def execution_scope(self):
        validate_environments(self)
        return self
