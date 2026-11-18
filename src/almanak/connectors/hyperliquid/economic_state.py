"""A backtest's hypothetical HyperCore account, its observations and its results.

Amounts are USDC base units (micro-USDC, the unit HyperCore's perp balance is kept in);
sizes, prices and rates stay decimal strings, as the venue writes them.
"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.engine import RunState
from almanak.backtest.types import UnixTime
from almanak.connectors.hyperliquid.models import Intent, Parameters, Target
from almanak.connectors.hyperliquid.positions import Holding
from almanak.connectors.identifiers import Name
from almanak.gateway.hypercore_shapes import DecimalText
from almanak.models import AssetAmount, Model, UInt
from almanak.strategy_api import (
    DecisionContext,
    OutcomeContext,
    SimulationEvidence,
    StrategyDecision,
)
from almanak.triggers import UtcInstant


class HyperCoreAccount(Model):
    """The fund's HyperCore perp account as the model keeps it."""

    target: Target
    # HyperCore holds an account for the fund once a deposit paid its activation fee.
    activated: bool = False
    # Perp USDC before unrealized PnL; it can fall below zero while a position is open.
    perp_usdc: int = 0
    # The cross leverage set for the market; None until the strategy sets one.
    leverage: Annotated[int, Field(strict=True, ge=1)] | None = None
    holding: Holding | None = None
    # Running totals for reports, all in USDC base units.
    deposited: UInt = 0
    withdrawn: UInt = 0
    activation_fee: UInt = 0
    fees: UInt = 0
    funding: int = 0
    realized_pnl: int = 0
    liquidations: UInt = 0

    @property
    def allocation(self):
        return self.target.allocation


class Reference(Model):
    """The latest trade candle available at a time: its close stands in for the book,
    the mark and the oracle, none of which `/info` keeps history of (D4)."""

    price: DecimalText
    candle_open: UnixTime
    available_at: UnixTime


class Snapshot(Model):
    allocation: Name
    target: Target
    effective_at: UtcInstant
    # USDC held by the fund on HyperEVM.
    wallet: AssetAmount
    owned: HyperCoreAccount
    reference: Reference
    account_value: int
    unrealized_pnl: int
    initial_margin: UInt
    maintenance_margin: UInt
    withdrawable: UInt

    @property
    def position(self) -> Holding | None:
        return self.owned.holding


class OrderFill(Model):
    side: Literal["buy", "sell"]
    size: DecimalText
    price: DecimalText
    # The reference trade price the order was priced from, and its `Ioc` limit.
    reference: DecimalText
    limit: DecimalText | None
    fee: UInt
    closed_pnl: int


class HyperCoreResult(Model):
    intent: Intent
    success: bool
    complete: Literal[True] = True
    effective_at: UtcInstant
    simulation: SimulationEvidence
    before: HyperCoreAccount
    after: HyperCoreAccount
    fill: OrderFill | None = Field(default=None, exclude_if=lambda v: v is None)
    # Why the venue would refuse the action; a refusal leaves the account unchanged.
    error: str | None = Field(default=None, exclude_if=lambda v: v is None)
    unavailable: InputGap | None = Field(default=None, exclude_if=lambda v: v is None)

    @property
    def applied(self) -> bool:
        """Whether the action took effect, as a fork run's `ActionResult.applied` says; a
        backtest refuses what does not apply, so a successful result always applied."""
        return self.success

    @model_validator(mode="after")
    def outcome(self):
        if self.success == (self.error is not None):
            raise ValueError("a HyperCore result either applies or names its refusal")
        if not self.success and self.after != self.before:
            raise ValueError("a refused HyperCore action changes nothing")
        return self


class FundingPayment(Model):
    kind: Literal["funding"] = "funding"
    allocation: Name
    effective_at: UtcInstant
    rate: DecimalText
    # The trade close standing in for the oracle price the venue charges funding at.
    price: DecimalText
    szi: DecimalText
    # USDC base units credited to the account; negative when it pays.
    payment: int
    simulation: SimulationEvidence


class Liquidation(Model):
    kind: Literal["liquidation"] = "liquidation"
    allocation: Name
    effective_at: UtcInstant
    # The price at which account value meets maintenance margin.
    trigger: DecimalText
    fill: OrderFill
    before: HyperCoreAccount
    after: HyperCoreAccount
    simulation: SimulationEvidence


class HyperCoreEvents(Model):
    """Effects the venue applies without an intent: funding and liquidation."""

    kind: Literal["events"] = "events"
    results: Annotated[
        tuple[Annotated[FundingPayment | Liquidation, Field(discriminator="kind")], ...],
        Field(min_length=1),
    ]


Result = HyperCoreResult | HyperCoreEvents
Payload = HyperCoreAccount
State = RunState[HyperCoreAccount, Intent, Result]


class DecisionInput(DecisionContext[Parameters, HyperCoreAccount, Snapshot]):
    pass


class OutcomeInput(OutcomeContext[Parameters, HyperCoreResult]):
    pass


class Decision(StrategyDecision[Intent]):
    pass
