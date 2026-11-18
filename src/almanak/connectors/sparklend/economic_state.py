"""Hypothetical SparkLend ownership and explicit simulation evidence in the shared journal."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.engine import RunState
from almanak.connectors.sparklend.domain import (
    PositionObservation,
    ScaledBalance,
    Target,
    Uint256,
)
from almanak.connectors.sparklend.models import Intent, Parameters
from almanak.connectors.sparklend.observations import Ownership
from almanak.gateway.evm import native_asset
from almanak.models import Account, AssetAmount, Model
from almanak.strategy_api import (
    DecisionContext,
    Memory,
    OutcomeContext,
    SimulationEvidence,
)
from almanak.triggers import UtcInstant


class Position(Ownership):
    supply_scaled: ScaledBalance = 0
    debt_scaled: ScaledBalance = 0
    collateral_enabled: bool = False
    supplied: Uint256 = 0
    withdrawn: Uint256 = 0
    borrowed: Uint256 = 0
    repaid: Uint256 = 0
    liquidated_collateral: Uint256 = 0
    liquidated_debt: Uint256 = 0
    liquidation_fees: Uint256 = 0


class Snapshot(Model):
    target: Target
    position: PositionObservation
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

    @model_validator(mode="after")
    def scope(self):
        if (self.position.fund, self.position.reserve.source) != (
            self.target.fund,
            self.target.reserve,
        ):
            raise ValueError("economic snapshot differs from its configured fund and reserve")
        if self.effective_at.timestamp() < self.position.reserve.block.timestamp:
            raise ValueError("economic snapshot cannot use a future reserve observation")
        if self.payer.chain != self.target.fund.chain or self.native_balance.asset != native_asset(
            self.payer.chain.chain_id
        ):
            raise ValueError("economic gas evidence differs from its payer scope")
        if not self.simulation.inputs:
            raise ValueError("economic snapshot requires retained input evidence")
        return self


class ActionResult(Model):
    kind: Literal["action"] = "action"
    intent: Intent
    success: bool
    complete: Literal[True] = True
    assets: AssetAmount | None
    supply_delta: int
    debt_delta: int
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
            raise ValueError("economic result has another reserve asset")
        if self.success == (self.error is not None) or (
            self.success and self.unavailable is not None
        ):
            raise ValueError("economic failure and missing-input evidence must remain visible")
        if self.success and not self.simulation.inputs:
            raise ValueError("successful economic results require retained input evidence")
        return self


class LiquidationEffect(Model):
    collateral: Target
    debt: Target
    collateral_removed: AssetAmount
    debt_restored: AssetAmount
    protocol_fee: AssetAmount

    @model_validator(mode="after")
    def scope(self):
        if (self.collateral.fund, self.collateral.reserve.pool) != (
            self.debt.fund,
            self.debt.reserve.pool,
        ):
            raise ValueError("liquidation effects require one fund and Pool")
        if (
            self.collateral_removed.asset != self.collateral.asset
            or self.protocol_fee.asset != self.collateral.asset
        ):
            raise ValueError("liquidation collateral and fee require their reserve asset")
        if self.debt_restored.asset != self.debt.asset:
            raise ValueError("liquidation repayment requires its debt reserve asset")
        return self


class Event(Model):
    kind: Literal["liquidation", "input_unavailable"]
    effective_at: UtcInstant
    liquidations: tuple[LiquidationEffect, ...]
    unavailable: tuple[InputGap, ...]
    simulation: SimulationEvidence
    blocked: tuple[str, ...] = ()


Result = Annotated[ActionResult | Event, Field(discriminator="kind")]
State = RunState[Position, Intent, Result]
DecisionInput = DecisionContext[Parameters, Ownership, Snapshot]
OutcomeInput = OutcomeContext[Parameters, ActionResult]


class EventInput(Model):
    config: Parameters
    memory: Memory
    event: Event
