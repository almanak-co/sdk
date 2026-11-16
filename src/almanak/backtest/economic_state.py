"""Typed economic lending views and obligations; no fabricated chain execution."""

from typing import Annotated, Literal

from pydantic import Field

from almanak.backtest.dataset import InputGap
from almanak.backtest.dependencies import PriceObservation
from almanak.backtest.engine import RunState
from almanak.backtest.types import Name, UnixTime
from almanak.connectors.aave_v4.domain import PositionObservation
from almanak.effects import Effects
from almanak.lending import CollateralObservation, LendingParameters, Ownership, SupplyTarget
from almanak.lending_intents import EconomicLendingIntent
from almanak.models import Account, Address, AssetAmount, Model, UInt
from almanak.price_series import PriceSeries
from almanak.strategy_api import (
    DecisionContext,
    Memory,
    OutcomeContext,
    SimulationEvidence,
    StrategyDecision,
)
from almanak.triggers import UtcInstant


class EconomicParameters(LendingParameters):
    def validate_debts(self):
        for debt in self.debts:
            self.validate_collateral(debt)


class RefreshIntent(Model):
    kind: Literal["aave_refresh"] = "aave_refresh"
    target: SupplyTarget
    dynamic_config: bool


Intent = Annotated[EconomicLendingIntent[SupplyTarget] | RefreshIntent, Field(discriminator="kind")]


class EconomicPosition(Model):
    target: SupplyTarget
    shares: Annotated[UInt, Field(lt=2**120)] = 0
    drawn_shares: Annotated[UInt, Field(lt=2**120)] = 0
    premium_shares: Annotated[UInt, Field(lt=2**120)] = 0
    premium_offset_ray: Annotated[int, Field(ge=-(2**199), lt=2**199)] = 0
    collateral: bool = False
    dynamic_config_key: UInt = 0
    risk_premium_bps: Annotated[UInt, Field(lt=2**24)] = 0
    supplied: UInt = 0
    withdrawn: UInt = 0
    borrowed: UInt = 0
    repaid: UInt = 0
    liquidated_collateral: UInt = 0
    liquidated_debt: UInt = 0
    deficit: UInt = 0


class RiskView(Model):
    fund: Account
    spoke: Address
    collateral_value: UInt
    weighted_collateral_value: UInt
    debt_value_ray: UInt
    health_factor_wad: UInt | None
    risk_premium_bps: UInt
    active_collateral_count: UInt
    borrow_count: UInt
    value_decimals: Literal[26] = 26
    quote: Literal["USD"] = "USD"


class EconomicSnapshot(Model):
    allocation: Name
    account: Name
    position: PositionObservation
    wallet_balance: UInt
    native_balance: AssetAmount
    effective_at: UtcInstant
    collateral_enabled: bool
    collateral: tuple[CollateralObservation, ...] = ()
    drawn_shares: UInt = 0
    premium_shares: UInt = 0
    premium_offset_ray: int = 0
    dynamic_config_key: UInt
    risk_premium_bps: UInt
    risk: RiskView
    input_times: tuple[UnixTime, ...]
    evidence: tuple[str, ...]
    allowance: Literal[0] = 0
    allowance_model: Literal["excluded"] = "excluded"
    simulated: Literal[True] = True

    @property
    def target(self) -> SupplyTarget:
        return SupplyTarget(
            allocation=self.allocation,
            account=self.account,
            fund=self.position.account,
            reserve=self.position.reserve.source,
        )

    @property
    def health_factor(self) -> int | None:
        return self.risk.health_factor_wad


class EconomicDecisionInput(DecisionContext[EconomicParameters, Ownership, EconomicSnapshot]):
    # A single strategy price (window 1, coin price or protocol oracle) arrives as one
    # observation, a pool candle series as its window.
    prices: tuple[PriceObservation | PriceSeries, ...] = Field(  # type: ignore[assignment]
        default=(), exclude_if=lambda v: not v
    )


class EconomicDecision(StrategyDecision[Intent]):
    pass


class EconomicActionResult(Model):
    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    kind: Literal["action"] = "action"
    intent: Intent
    success: bool
    complete: Literal[True] = True
    effective_at: UtcInstant
    assets: AssetAmount | None
    shares: UInt
    simulation: SimulationEvidence
    error: str | None = None
    unavailable: InputGap | None = None


class LiquidationEffect(Model):
    collateral: SupplyTarget
    debt: SupplyTarget
    collateral_removed: AssetAmount
    debt_restored: AssetAmount
    fee_shares: UInt


class UnavailableGroup(Model):
    targets: Annotated[tuple[SupplyTarget, ...], Field(min_length=1)]
    input: InputGap


class EconomicEvent(Model):
    kind: Literal["liquidation", "input_unavailable"] = "liquidation"
    effective_at: UtcInstant
    liquidations: tuple[LiquidationEffect, ...]
    deficits: tuple[AssetAmount, ...]
    simulation: SimulationEvidence
    unavailable: tuple[UnavailableGroup, ...] = ()


Result = Annotated[EconomicActionResult | EconomicEvent, Field(discriminator="kind")]
EconomicState = RunState[EconomicPosition, Intent, Result]


class EconomicOutcomeInput(OutcomeContext[EconomicParameters, EconomicActionResult]):
    pass


class EconomicEventInput(Model):
    config: EconomicParameters
    memory: Memory
    event: EconomicEvent
    effects: Effects[EconomicPosition]
