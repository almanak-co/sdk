"""Hypothetical liquidity retains simulation identity and shared portfolio ownership."""

from datetime import UTC, datetime
from typing import Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.types import UnixTime
from almanak.connectors.dex_fill_timing import OnDecisionSample, StateAge
from almanak.connectors.v3_pool_domain import ConcentratedObservation
from almanak.connectors.v3_pool_models import Target
from almanak.connectors.v3_pool_observations import Snapshot
from almanak.connectors.v3_pool_positions import Ownership, Position, Quote
from almanak.dex_intents import DexIntent
from almanak.models import AssetAmount, Model, UInt
from almanak.strategy_api import SimulationEvidence
from almanak.triggers import UtcInstant


class EconomicSnapshot[T: Target, P: ConcentratedObservation, W: Ownership, Q: Quote](
    Snapshot[T, P, W, Q]
):
    simulation_time: UnixTime
    simulated: Literal[True] = True

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.simulation_time, UTC)

    @model_validator(mode="after")
    def economic_scope(self):
        if isinstance(self.position, Position) or isinstance(self.owned.position, Position):
            raise ValueError("hypothetical liquidity cannot claim an on-chain NFT")
        if self.position != self.owned.position:
            raise ValueError("simulated observation must retain its hypothetical position")
        if self.block.timestamp > self.simulation_time:
            raise ValueError("economic observations cannot use future pool state")
        return self


class EconomicPosition[T: Target, W: Ownership](Model):
    target: T
    owned: W
    swap0: UInt = 0
    swap1: UInt = 0
    deposit0: UInt = 0
    deposit1: UInt = 0
    fees0: UInt = 0
    fees1: UInt = 0
    # Collected growth the upper band bracket admits beyond the credited lower bracket.
    unattributed0: UInt = Field(default=0, exclude_if=lambda v: v == 0)
    unattributed1: UInt = Field(default=0, exclude_if=lambda v: v == 0)

    @model_validator(mode="after")
    def scope(self):
        if self.target != self.owned.target or isinstance(self.owned.position, Position):
            raise ValueError("economic position requires its exact simulated ownership")
        validate_simulated(self.target, self.owned.position)
        if self.target.band is None and (self.unattributed0 or self.unattributed1):
            raise ValueError("fixed-range fee growth is exact and has no unattributed bound")
        return self


def validate_simulated(target, position):
    if position is None:
        return
    if target.band is None:
        if position.range is not None or position.bound_growth is not None:
            raise ValueError("fixed-range simulated liquidity uses its configured exact range")
        return
    if position.range is None or position.bound_growth is None:
        raise ValueError("banded simulated liquidity requires its range and growth bounds")
    target.band.validate_range(position.range, target.pool.tick_spacing)


class EconomicResult[I: DexIntent, S: EconomicSnapshot, W: Ownership, Q: Quote](Model):
    kind: Literal["action"] = "action"
    intent: I
    success: Literal[True] = True
    complete: bool
    effective_at: UtcInstant
    simulation: SimulationEvidence
    before: S
    after: W
    delta0: int
    delta1: int
    liquidity_delta: int
    fees0: UInt
    fees1: UInt
    gas_paid: AssetAmount
    fill: Q | None
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
        if isinstance(self.after.position, Position):
            raise ValueError("economic result cannot claim an on-chain NFT")
        return self


class EconomicFailure[I: DexIntent](Model):
    kind: Literal["failure"] = "failure"
    intent: I
    success: Literal[False] = False
    complete: Literal[True] = True
    effective_at: UtcInstant
    simulation: SimulationEvidence
    error: str
    unavailable: InputGap | None = None

    @property
    def status(self) -> Literal["failed"]:
        return "failed"
