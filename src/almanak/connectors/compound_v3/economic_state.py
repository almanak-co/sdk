"""Hypothetical Comet principal and absorption evidence in the shared journal."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.engine import RunState
from almanak.connectors.compound_v3.domain import AccountObservation, Target, Uint64, Uint256
from almanak.connectors.compound_v3.liquidation import Absorption
from almanak.connectors.compound_v3.math import present_value
from almanak.connectors.compound_v3.models import Intent, Parameters
from almanak.connectors.compound_v3.observations import Ownership, target_asset
from almanak.gateway.evm import native_asset
from almanak.models import Account, AssetAmount, Model
from almanak.strategy_api import DecisionContext, Memory, OutcomeContext, SimulationEvidence
from almanak.triggers import UtcInstant


class Position(Ownership):
    collateral_enabled: bool = False
    supplied: Uint256 = 0
    withdrawn: Uint256 = 0
    borrowed: Uint256 = 0
    repaid: Uint256 = 0
    seized_collateral: Uint256 = 0
    absorption_credit: Uint256 = 0
    absorbed_deficit: Uint256 = 0

    @model_validator(mode="after")
    def membership(self):
        if self.collateral_enabled and (self.target.reserve.is_base or self.collateral == 0):
            raise ValueError("economic Comet membership requires nonzero collateral")
        return self


class ModeledIndices(Model):
    supply: Annotated[Uint64, Field(gt=0)]
    borrow: Annotated[Uint64, Field(gt=0)]
    scale: Literal[1000000000000000] = 1000000000000000


class Snapshot(Model):
    target: Target
    account: AccountObservation
    indices: ModeledIndices
    wallet_balance: Uint256
    native_balance: AssetAmount
    payer: Account
    effective_at: UtcInstant
    simulation: SimulationEvidence
    allowance: Literal[0] = 0
    allowance_model: Literal["excluded"] = "excluded"
    simulated: Literal[True] = True

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def asset(self):
        return target_asset(self.target, self.account)

    @model_validator(mode="after")
    def scope(self):
        target_asset(self.target, self.account)
        if self.effective_at.timestamp() < self.account.market.block.timestamp:
            raise ValueError("economic Comet snapshot cannot use future market evidence")
        balance = present_value(self.account.principal, self.indices.supply, self.indices.borrow)
        if (self.account.supplied_base, self.account.borrowed_base) != (
            max(0, balance),
            max(0, -balance),
        ):
            raise ValueError("economic Comet balances differ from their modeled indices")
        if self.payer.chain != self.target.fund.chain or self.native_balance.asset != native_asset(
            self.payer.chain.chain_id
        ):
            raise ValueError("economic Comet gas evidence differs from its payer scope")
        if not self.simulation.inputs:
            raise ValueError("economic Comet snapshot requires retained input evidence")
        return self


class ActionResult(Model):
    kind: Literal["action"] = "action"
    intent: Intent
    success: bool
    complete: Literal[True] = True
    assets: AssetAmount | None
    principal_delta: int
    collateral_delta: int
    effective_at: UtcInstant
    simulation: SimulationEvidence
    error: str | None = None
    unavailable: InputGap | None = None

    @property
    def status(self) -> Literal["completed", "failed"]:
        return "completed" if self.success else "failed"

    @model_validator(mode="after")
    def scope(self):
        if self.assets is not None and self.assets.asset != self.intent.target.asset:
            raise ValueError("economic Comet result has another asset")
        if self.success == (self.error is not None) or (self.success and self.unavailable):
            raise ValueError("economic Comet failure and missing-input evidence must stay visible")
        if self.success and not self.simulation.inputs:
            raise ValueError("successful economic Comet results require retained input evidence")
        return self


class AbsorptionEffect(Model):
    absorption: Absorption
    indices: ModeledIndices


class FundInputGap(Model):
    fund: Account
    gap: InputGap

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.gap.key.chain:
            raise ValueError("economic Comet input gap differs from its affected fund chain")
        return self


class Event(Model):
    kind: Literal["liquidation", "input_unavailable"]
    effective_at: UtcInstant
    absorptions: tuple[AbsorptionEffect, ...]
    unavailable: tuple[InputGap, ...]
    affected_funds: tuple[FundInputGap, ...] = ()
    simulation: SimulationEvidence
    blocked: tuple[str, ...] = ()

    @model_validator(mode="after")
    def gaps(self):
        if set(self.unavailable) != {affected.gap for affected in self.affected_funds}:
            raise ValueError("economic Comet input gaps require their affected fund scopes")
        return self


Result = Annotated[ActionResult | Event, Field(discriminator="kind")]
State = RunState[Position, Intent, Result]
DecisionInput = DecisionContext[Parameters, Ownership, Snapshot]
OutcomeInput = OutcomeContext[Parameters, ActionResult]


class EventInput(Model):
    config: Parameters
    memory: Memory
    event: Event
