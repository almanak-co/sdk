"""Economic observations and journal payloads retain modeled market generations."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import InputGap
from almanak.backtest.engine import RunState
from almanak.connectors.morpho_blue.domain import Market, Target, Uint256
from almanak.connectors.morpho_blue.economic_account import AccountView
from almanak.connectors.morpho_blue.economic_data import key
from almanak.connectors.morpho_blue.economic_liquidation import LiquidationChange
from almanak.connectors.morpho_blue.economic_market import MarketChange
from almanak.connectors.morpho_blue.economic_positions import Position
from almanak.connectors.morpho_blue.models import Intent, Parameters
from almanak.connectors.morpho_blue.observations import Ownership
from almanak.gateway.evm import native_asset
from almanak.models import Account, AssetAmount, Model
from almanak.strategy_api import DecisionContext, Memory, OutcomeContext, SimulationEvidence
from almanak.triggers import UtcInstant


class Snapshot(Model):
    target: Target
    account: AccountView
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
        return (
            self.account.market.loan_asset
            if self.target.reserve.is_loan
            else self.account.market.collateral_asset
        )

    @model_validator(mode="after")
    def scope(self):
        if (
            self.target.fund != self.account.fund
            or self.target.reserve.market != self.account.market.source
        ):
            raise ValueError("modeled Morpho snapshot differs from its fund/market target")
        if self.effective_at.timestamp() != self.account.ledger.effective_at:
            raise ValueError("modeled Morpho snapshot differs from its accounting time")
        if self.payer.chain != self.target.fund.chain or self.native_balance.asset != native_asset(
            self.payer.chain.chain_id
        ):
            raise ValueError("modeled Morpho gas evidence differs from its payer scope")
        if not self.simulation.inputs:
            raise ValueError("modeled Morpho snapshot requires retained input evidence")
        return self


class ActionResult(Model):
    kind: Literal["action"] = "action"
    intent: Intent
    success: bool
    complete: Literal[True] = True
    assets: AssetAmount | None
    market_change: MarketChange | None
    effective_at: UtcInstant
    simulation: SimulationEvidence
    error: str | None = None
    unavailable: InputGap | None = None

    @property
    def status(self) -> Literal["completed", "failed"]:
        return "completed" if self.success else "failed"

    @model_validator(mode="after")
    def scope(self):
        if self.success == (self.error is not None) or (self.success and self.unavailable):
            raise ValueError("modeled Morpho failures and input gaps must remain visible")
        if self.success != (self.assets is not None and self.market_change is not None):
            raise ValueError("modeled Morpho success requires its assets and market change")
        if not self.success and (self.assets is not None or self.market_change is not None):
            raise ValueError("failed modeled Morpho action cannot retain an economic effect")
        if self.assets is not None and self.assets.asset != self.intent.target.asset:
            raise ValueError("modeled Morpho result differs from its target asset")
        self.validate_market()
        return self

    def validate_market(self):
        if self.market_change is None:
            return
        after = self.market_change.after
        if (
            after.source != self.intent.target.reserve.market
            or after.effective_at != self.effective_at.timestamp()
        ):
            raise ValueError("modeled Morpho result differs from its market or logical time")
        if not self.simulation.inputs:
            raise ValueError("successful modeled Morpho result requires retained evidence")


class MarketInputGap(Model):
    source: Market
    gap: InputGap
    fund: Account | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.fund is not None and self.fund.chain != self.source.chain:
            raise ValueError("modeled Morpho input gap fund has another chain scope")
        if self.gap.key not in (key(self.source, "accounting"), key(self.source, "risk")):
            raise ValueError("modeled Morpho input gap differs from its affected market")
        return self


class Event(Model):
    kind: Literal["accounting", "liquidation", "input_unavailable"]
    effective_at: UtcInstant
    market_changes: tuple[MarketChange, ...]
    liquidations: tuple[LiquidationChange, ...]
    unavailable: tuple[InputGap, ...]
    affected_markets: tuple[MarketInputGap, ...]
    simulation: SimulationEvidence

    @model_validator(mode="after")
    def scope(self):
        expected = (
            "liquidation"
            if self.liquidations
            else "input_unavailable"
            if self.unavailable
            else "accounting"
        )
        if self.kind != expected:
            raise ValueError("modeled Morpho event kind differs from its economic effects")
        if set(self.unavailable) != {affected.gap for affected in self.affected_markets}:
            raise ValueError("modeled Morpho input gaps require their affected market scopes")
        if any(change.market not in self.market_changes for change in self.liquidations):
            raise ValueError("modeled Morpho liquidation lacks its shared market change")
        if any(
            change.after.effective_at != self.effective_at.timestamp()
            for change in self.market_changes
        ):
            raise ValueError("modeled Morpho event changes differ from its logical time")
        if self.market_changes and not self.simulation.inputs:
            raise ValueError("modeled Morpho market events require retained evidence")
        return self


Result = Annotated[ActionResult | Event, Field(discriminator="kind")]
State = RunState[Position, Intent, Result]
DecisionInput = DecisionContext[Parameters, Ownership, Snapshot]
OutcomeInput = OutcomeContext[Parameters, ActionResult]


class EventInput(Model):
    config: Parameters
    memory: Memory
    event: Event
