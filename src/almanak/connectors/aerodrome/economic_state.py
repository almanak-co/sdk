"""Hypothetical pool shares remain explicit simulation state in the shared journal."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.engine import RunState
from almanak.backtest.types import UnixTime
from almanak.connectors.aerodrome.models import Intent, Parameters, Target
from almanak.connectors.aerodrome.observations import Snapshot
from almanak.connectors.aerodrome.positions import Ownership, Quote
from almanak.connectors.dex_fill_timing import OnDecisionSample, StateAge
from almanak.models import AssetAmount, Model, UInt
from almanak.strategy_api import DecisionContext, OutcomeContext, SimulationEvidence
from almanak.triggers import UtcInstant


class EconomicSnapshot(Snapshot):
    simulation_time: UnixTime
    simulated: Literal[True] = True

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.simulation_time, UTC)

    @model_validator(mode="after")
    def economic_scope(self):
        if self.position != self.owned.position:
            raise ValueError("simulated observations must retain hypothetical position ownership")
        if self.block.timestamp > self.simulation_time:
            raise ValueError("economic observations cannot use future pool state")
        return self


class EconomicPosition(Model):
    target: Target
    owned: Ownership
    swap0: UInt = 0
    swap1: UInt = 0
    deposit0: UInt = 0
    deposit1: UInt = 0
    fees0: UInt = 0
    fees1: UInt = 0

    @model_validator(mode="after")
    def scope(self):
        if self.target != self.owned.target:
            raise ValueError("economic pool ownership requires its exact target")
        return self


class EconomicResult(Model):
    kind: Literal["action"] = "action"
    intent: Intent
    success: Literal[True] = True
    complete: bool
    effective_at: UtcInstant
    simulation: SimulationEvidence
    before: EconomicSnapshot
    after: Ownership
    delta0: int
    delta1: int
    shares_delta: int
    fees0: UInt
    fees1: UInt
    gas_paid: AssetAmount
    fill: Quote | None
    steps: tuple[Literal["swap", "mint", "increase", "decrease", "collect", "close", "revoke"], ...]
    # Absent for a step that reads no pool sample, and in results retained before
    # these fields existed.
    fill_state_age_seconds: StateAge = None
    fill_on_decision_sample: OnDecisionSample = None

    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        return "completed" if self.complete else "intermediate"

    @model_validator(mode="after")
    def scope(self):
        if self.before.target != self.intent.target or self.after.target != self.intent.target:
            raise ValueError("economic result requires its exact execution target")
        return self


class EconomicFailure(Model):
    kind: Literal["failure"] = "failure"
    intent: Intent
    success: Literal[False] = False
    complete: Literal[True] = True
    effective_at: UtcInstant
    simulation: SimulationEvidence
    error: str
    unavailable: InputGap | None = None

    @property
    def status(self) -> Literal["failed"]:
        return "failed"


Result = Annotated[EconomicResult | EconomicFailure, Field(discriminator="kind")]
DecisionInput = DecisionContext[Parameters, Ownership, EconomicSnapshot]
OutcomeInput = OutcomeContext[Parameters, Result]
State = RunState[EconomicPosition, Intent, Result]
