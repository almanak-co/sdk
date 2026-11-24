"""Hypothetical transfers in flight and modeled arrivals in the shared journal."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.engine import RunState
from almanak.backtest.types import UnixTime
from almanak.connectors.across.domain import Target, Uint256
from almanak.connectors.across.models import Intent, Parameters
from almanak.connectors.across.observations import Ownership
from almanak.connectors.across.settlement_types import ArrivalResult
from almanak.gateway.evm import native_asset
from almanak.models import Account, AssetAmount, Model
from almanak.strategy_api import DecisionContext, OutcomeContext, SimulationEvidence
from almanak.transfers import Refunded, Transfer
from almanak.triggers import UtcInstant


class Flight(Model):
    """One modeled transfer with the simulation seconds of its deposit and its arrival."""

    transfer: Transfer
    sent_at: UnixTime
    # The deposit's second plus the stated latency. The arrival is credited then, never before.
    arrives_at: UnixTime

    @model_validator(mode="after")
    def order(self):
        if self.arrives_at <= self.sent_at:
            raise ValueError("a modeled transfer arrives after its deposit")
        refunded = isinstance(self.transfer.resolution, Refunded)
        if self.transfer.unfilled or refunded:
            raise ValueError("the bridge model fills every transfer; it models no refund")
        return self


class Position(Model):
    target: Target
    # Every transfer the allocation sent, in order. An arrived one stays as its record.
    flights: tuple[Flight, ...] = ()

    @model_validator(mode="after")
    def scope(self):
        if any(f.transfer.allocation != self.target.allocation for f in self.flights):
            raise ValueError("a modeled transfer belongs to its own allocation")
        references = [f.transfer.reference for f in self.flights]
        if len(set(references)) != len(references):
            raise ValueError("a modeled transfer is retained once")
        return self

    @property
    def in_flight(self) -> tuple[Flight, ...]:
        return tuple(f for f in self.flights if f.transfer.open)

    @property
    def owned(self) -> Ownership:
        return Ownership(target=self.target, in_flight=tuple(f.transfer for f in self.in_flight))


class Snapshot(Model):
    target: Target
    wallet_balance: Uint256
    # The model keeps no approval: a modeled bridge approves exactly what it deposits.
    allowance: Literal[0] = 0
    native_balance: AssetAmount
    payer: Account
    effective_at: UtcInstant
    simulated: Literal[True] = True

    @property
    def allocation(self):
        return self.target.allocation

    @model_validator(mode="after")
    def scope(self):
        chain = self.target.fund.chain
        if self.payer.chain != chain or self.native_balance.asset != native_asset(chain.chain_id):
            raise ValueError("modeled bridge gas evidence differs from its payer chain")
        return self


class ActionResult(Model):
    kind: Literal["action"] = "action"
    intent: Intent
    success: bool
    complete: Literal[True] = True
    # What left the source fund. The amount received is credited at the modeled arrival.
    sent: AssetAmount
    transfer: Transfer | None = None
    effective_at: UtcInstant
    simulation: SimulationEvidence
    error: str | None = None

    @property
    def status(self) -> Literal["completed", "failed"]:
        return "completed" if self.success else "failed"

    @model_validator(mode="after")
    def scope(self):
        if self.sent.asset != self.intent.target.asset:
            raise ValueError("modeled bridge result has another asset")
        if self.success == (self.error is not None):
            raise ValueError("a modeled bridge failure must stay visible")
        if self.success != (self.transfer is not None) or self.success != bool(self.sent.amount):
            raise ValueError("exactly a modeled deposit moves the asset and opens a transfer")
        return self


class ModeledArrival(ArrivalResult):
    """A modeled arrival: the amount sent less the author's whole fee ceiling."""

    # What the transfer sent and the fee the model charged, both in the sent asset's units.
    sent: AssetAmount
    modeled_fee: AssetAmount
    effective_at: UtcInstant
    simulation: SimulationEvidence
    measurement_status: Literal["modeled"] = "modeled"

    @model_validator(mode="after")
    def scope(self):
        if self.received != self.offered:
            raise ValueError("a modeled arrival pays exactly its offer")
        if self.sent.amount - self.modeled_fee.amount != self.received.amount:
            raise ValueError("a modeled arrival is the amount sent less the modeled fee")
        return self


class ArrivalEvent(Model):
    kind: Literal["bridge_arrivals"] = "bridge_arrivals"
    effective_at: UtcInstant
    arrivals: Annotated[tuple[ModeledArrival, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def time(self):
        if any(a.effective_at != self.effective_at for a in self.arrivals):
            raise ValueError("modeled arrivals of one event share its timestamp")
        return self


Result = Annotated[ActionResult | ArrivalEvent, Field(discriminator="kind")]
State = RunState[Position, Intent, Result]
DecisionInput = DecisionContext[Parameters, Ownership, Snapshot]
OutcomeInput = OutcomeContext[Parameters, ActionResult | ModeledArrival]
