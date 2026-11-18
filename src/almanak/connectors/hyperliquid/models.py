"""HyperCore perp allocations and the intents a strategy expresses (blueprint 30, 12 and 13).

One allocation trades one first-dex market from one fund's HyperCore account, which is
the fund's own address on `eip155:999`. Its capital is USDC on HyperEVM. The strategy
moves it in and out (`deposit`, `withdraw`), sets the configured cross leverage while
flat (`set_leverage`), opens or adds with an `Ioc` order (`increase_position`) and closes
the whole position with a reduce-only `Ioc` order (`close_position`), as the phase 3
reference strategy does.

Backtests simulate these intents (section 12). Fork runs take the same intents through
the runtime (section 13): the prepared `Config` adds the fork bindings, and connector
planning turns each intent into its custody transactions and agent-signed venue
actions (`planning.py`). Production waits for D7.
"""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.chains import HYPEREVM
from almanak.configuration.gas_parameters import AuthorGas, PreparedConfig
from almanak.connectors.hyperliquid.assets import USDC
from almanak.connectors.hyperliquid.resolution import PerpMarket
from almanak.connectors.identifiers import Name
from almanak.gateway.environments import ForkBinding, validate_environments
from almanak.gateway.production_config import ProductionExecution
from almanak.models import (
    Account,
    AssetAmount,
    AssetMetadata,
    Chain,
    Model,
    StrategyAccounts,
    UInt,
)
from almanak.triggers import HOLD_SECONDS_DESCRIPTION, IntervalTrigger, PositiveSeconds

# Positions above this notional are liquidated in 20% steps on HyperCore, which the
# model does not simulate, so an entry that would reach it is refused.
MAXIMUM_NOTIONAL = Decimal(100_000)
# HyperCore refuses an order worth less than this many USDC (`MinTradeNtl`).
MINIMUM_ORDER_VALUE = Decimal(10)
Size = Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)(\.[0-9]+)?$")]
Positive = Annotated[int, Field(strict=True, gt=0)]


class Target(Model):
    allocation: Name
    account: Name
    fund: Account
    market: PerpMarket
    margin_asset: AssetType = USDC

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain.chain_id != HYPEREVM.chain or self.margin_asset != USDC:
            raise ValueError("a HyperCore target is a USDC account on eip155:999")
        return self

    @property
    def assets(self) -> tuple[AssetType, ...]:
        """The fund's HyperEVM assets a fork run moves: USDC, and HYPE for the gas of its
        custody transactions."""
        return self.margin_asset, HYPEREVM.native_asset


class Allocation(Model):
    name: Name
    account: Name
    chain: Chain
    market: PerpMarket
    # USDC base units the fund starts with on HyperEVM; how much to deposit and trade is
    # the strategy's decision.
    capital: AssetAmount
    # The fixed cross leverage every entry needs, set while flat.
    leverage: Annotated[int, Field(strict=True, ge=1)]
    # The author's slippage: an `Ioc` limit is the reference price moved by this much.
    slippage_bps: Annotated[int, Field(strict=True, gt=0, le=1_000)]
    hold_seconds: PositiveSeconds | None = Field(
        default=None, exclude_if=lambda v: v is None, description=HOLD_SECONDS_DESCRIPTION
    )
    # HYPE base units the operator keeps for the custody transactions' gas: below it, a
    # fork run takes no new exposure. Zero by default; backtests exclude gas costs.
    minimum_native_balance: UInt = Field(default=0, exclude_if=lambda v: not v)

    @property
    def native_asset(self) -> AssetType:
        """HYPE, which pays the operator's custody transactions on HyperEVM."""
        return HYPEREVM.native_asset

    @model_validator(mode="after")
    def scope(self):
        if self.chain.chain_id != HYPEREVM.chain or self.capital.asset != USDC:
            raise ValueError("HyperCore capital is USDC on eip155:999")
        if self.leverage > self.market.max_leverage:
            raise ValueError("the configured leverage exceeds the market's maximum")
        return self


class Parameters(AuthorGas):
    connector: Literal["hyperliquid"] = "hyperliquid"
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
        raise ValueError(f"unknown HyperCore allocation: {name}")

    def target(self, name: str) -> Target:
        allocation = self.allocation(name)
        try:
            fund = self.accounts.binding(allocation.account).profile.fund
        except KeyError:
            raise ValueError("HyperCore allocation references an unbound account") from None
        return Target(
            allocation=name, account=allocation.account, fund=fund, market=allocation.market
        )

    @model_validator(mode="after")
    def scope(self):
        names = [a.name for a in self.allocations]
        if len(set(names)) != len(names):
            raise ValueError("HyperCore allocation names must be unique")
        # Cross margin pools one account's positions, so one fund holds one allocation.
        accounts = [a.account for a in self.allocations]
        if len(set(accounts)) != len(accounts):
            raise ValueError("one allocation per fund: a HyperCore account margins one market")
        if set(accounts) != {b.name for b in self.accounts.bindings}:
            raise ValueError("each HyperCore fund requires an explicit allocation")
        for name in names:
            self.target(name)
        return self


class Config(Parameters, PreparedConfig):
    """The prepared configuration of a fork or production run. Production is admitted only
    once the connector declares it, after D7 (blueprint 30, section 14.4)."""

    fork_bindings: tuple[ForkBinding, ...] = ()
    production: ProductionExecution | None = None

    @model_validator(mode="after")
    def execution_scope(self):
        # The checkpoint holds one Safe's custody, trading and book documents.
        if len(self.allocations) != 1:
            raise ValueError("a Hyperliquid deployment trades through exactly one Safe")
        for allocation in self.allocations:
            if allocation.chain != self.target(allocation.name).fund.chain:
                raise ValueError(
                    "Hyperliquid allocation must retain its fund chain and execution environment"
                )
        validate_environments(self)
        return self


class Deposit(Model):
    """Move USDC from the fund on HyperEVM to its HyperCore perp account."""

    kind: Literal["deposit"] = "deposit"
    target: Target
    amount: Positive


class SetLeverage(Model):
    kind: Literal["set_leverage"] = "set_leverage"
    target: Target
    leverage: Annotated[int, Field(strict=True, ge=1)]


class IncreasePosition(Model):
    """An `Ioc` order that opens or adds to the position on one side."""

    kind: Literal["increase_position"] = "increase_position"
    target: Target
    side: Literal["long", "short"]
    # Coins, rounded down to the market's size decimals when the order is sized.
    size: Size

    @model_validator(mode="after")
    def nonzero(self):
        if Decimal(self.size) <= 0:
            raise ValueError("a HyperCore entry needs a positive size")
        return self


class ClosePosition(Model):
    """A reduce-only `Ioc` order for the whole owned position."""

    kind: Literal["close_position"] = "close_position"
    target: Target


class Withdraw(Model):
    """Move perp USDC back to the fund on HyperEVM; everything withdrawable when unset."""

    kind: Literal["withdraw"] = "withdraw"
    target: Target
    amount: Positive | None = Field(default=None, exclude_if=lambda v: v is None)


Intent = Annotated[
    Deposit | SetLeverage | IncreasePosition | ClosePosition | Withdraw,
    Field(discriminator="kind"),
]
