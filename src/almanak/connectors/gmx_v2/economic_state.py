"""Simulated GMX request escrow is durable and distinct from transaction evidence."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.engine import RunState
from almanak.backtest.types import UnixTime
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicLiquidationBatch,
    EconomicPerpetualFailure,
    EconomicPerpetualPosition,
    EconomicPerpetualResult,
    EconomicPerpetualSnapshot,
    PerpetualOwnership,
)
from almanak.connectors.gmx_v2.exchange_domain import ExchangeIntent, ExchangeTarget
from almanak.connectors.gmx_v2.exchange_observations import (
    ExchangeOwnership,
    ExchangeSnapshot,
    allowance_assets,
)
from almanak.connectors.gmx_v2.intents import Intent
from almanak.connectors.gmx_v2.liquidity_operations import CreateDeposit, CreateWithdrawal
from almanak.connectors.gmx_v2.models import Parameters
from almanak.dex_intents import RevokeExchange
from almanak.effects import AssetDelta
from almanak.models import AssetAmount, Model
from almanak.strategy_api import DecisionContext, OutcomeContext, SimulationEvidence
from almanak.triggers import UtcInstant


class Escrow(Model):
    intent: ExchangeIntent
    identity: str
    accepted_at: UnixTime
    inputs: tuple[AssetAmount, ...]
    execution_fee: AssetAmount

    @model_validator(mode="after")
    def scope(self):
        target = self.intent.target
        if not self.identity:
            raise ValueError("simulated GMX escrow requires a durable admission identity")
        if self.execution_fee.asset != target.market.wrapped_native:
            raise ValueError("simulated GMX request fee remains wrapped-native fund escrow")
        assets = tuple(value.asset for value in self.inputs)
        if len(set(assets)) != len(assets) or set(assets) - set(target.accounting_assets):
            raise ValueError("simulated GMX escrow requires distinct configured input assets")
        return self


class LiquidityEscrow(Escrow):
    request: CreateDeposit | CreateWithdrawal

    @model_validator(mode="after")
    def request_scope(self):
        if (
            self.request.target != self.intent.target
            or self.request.execution_fee != self.execution_fee
        ):
            raise ValueError("GM modeled escrow differs from its exact retained request")
        inputs = (
            self.request.amounts.root
            if isinstance(self.request, CreateDeposit)
            else (self.request.market_tokens,)
        )
        if inputs != self.inputs:
            raise ValueError("GM modeled escrow differs from its retained principal inputs")
        return self


class EconomicPosition(Model):
    target: ExchangeTarget
    owned: ExchangeOwnership
    spent: tuple[AssetAmount, ...]
    allowances: tuple[AssetAmount, ...]
    escrow: Escrow | LiquidityEscrow | None

    @model_validator(mode="after")
    def scope(self):
        if self.target != self.owned.target or (
            self.escrow is not None and self.escrow.intent.target != self.target
        ):
            raise ValueError("simulated GMX ownership and escrow require the same exact target")
        assets = tuple(value.asset for value in self.spent)
        if len(set(assets)) != len(assets) or set(assets) - set(self.target.accounting_assets):
            raise ValueError("simulated GMX spending requires distinct configured assets")
        if tuple(value.asset for value in self.allowances) != allowance_assets(self.target):
            raise ValueError(
                "simulated GMX authority must identify every configured allowance asset"
            )
        return self


class EconomicSnapshot(ExchangeSnapshot):
    simulation_time: UnixTime
    simulated: Literal[True] = True
    escrow: Escrow | LiquidityEscrow | None

    @property
    def request_pending(self):
        return self.escrow is not None

    @property
    def needs_cleanup(self):
        return self.request_pending or super().needs_cleanup

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.simulation_time, UTC)

    @model_validator(mode="after")
    def hypothetical(self):
        if (
            self.request is not None
            or self.pending_order is not None
            or self.pending_liquidity is not None
        ):
            raise ValueError("simulated GMX observations cannot claim a native request")
        if self.escrow is not None and self.escrow.intent.target != self.target:
            raise ValueError("simulated GMX observation has another escrow target")
        if self.block.timestamp > self.simulation_time:
            raise ValueError("simulated GMX observations cannot use future canonical state")
        return self


class EconomicResult(Model):
    kind: Literal["action"] = "action"
    intent: ExchangeIntent
    success: bool = True
    complete: bool
    phase: Literal["accepted", "waiting", "settled", "revoked", "cancelled"]
    reason: str | None = None
    effective_at: UtcInstant
    simulation: SimulationEvidence
    before: EconomicSnapshot
    after: EconomicPosition
    assets: tuple[AssetDelta, ...]
    gas_paid: AssetAmount
    keeper_paid: AssetAmount
    fund_refund: AssetAmount

    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    @model_validator(mode="after")
    def scope(self):
        if self.before.target != self.intent.target or self.after.target != self.intent.target:
            raise ValueError("simulated GMX result requires its exact configured target")
        outstanding = self.after.escrow is not None or (
            isinstance(self.intent, RevokeExchange)
            and any(value.amount for value in self.after.allowances)
        )
        if self.complete == outstanding or (
            self.complete and self.phase in ("accepted", "waiting")
        ):
            raise ValueError("simulated GMX completion must agree with its remaining escrow")
        expected_success = self.phase != "cancelled" or isinstance(self.intent, RevokeExchange)
        if self.success != expected_success or (self.reason is None) == (self.phase == "cancelled"):
            raise ValueError("simulated GMX cancellation must preserve its unsuccessful reason")
        native = self.before.native_balance.asset
        if any(
            value.asset != native for value in (self.gas_paid, self.keeper_paid, self.fund_refund)
        ):
            raise ValueError(
                "simulated GMX costs and native fund refunds require exact native units"
            )
        return self


class EconomicFailure(Model):
    kind: Literal["failure"] = "failure"
    intent: ExchangeIntent
    success: Literal[False] = False
    complete: bool
    effective_at: UtcInstant
    simulation: SimulationEvidence
    error: str
    unavailable: InputGap | None

    @property
    def status(self) -> Literal["failed"]:
        return "failed"


Result = Annotated[
    EconomicResult
    | EconomicFailure
    | EconomicPerpetualResult
    | EconomicPerpetualFailure
    | EconomicLiquidationBatch,
    Field(discriminator="kind"),
]
DecisionInput = DecisionContext[
    Parameters,
    ExchangeOwnership | PerpetualOwnership,
    EconomicSnapshot | EconomicPerpetualSnapshot,
]
OutcomeInput = OutcomeContext[Parameters, Result]
Payload = EconomicPosition | EconomicPerpetualPosition
State = RunState[Payload, Intent, Result]
