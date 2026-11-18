"""Economic execution time is separate from the archival pool source block."""

from datetime import UTC, datetime
from typing import Literal

from almanak.backtest.engine import RunState
from almanak.backtest.types import UnixTime
from almanak.connectors.uniswap_v4.backtest import HistoricalPosition
from almanak.connectors.uniswap_v4.backtest_config import OperationKind
from almanak.connectors.uniswap_v4.models import Intent, Parameters
from almanak.connectors.uniswap_v4.reader import Quote
from almanak.connectors.uniswap_v4.state import Ownership, Snapshot
from almanak.models import AssetAmount, Model, UInt
from almanak.strategy_api import DecisionContext, OutcomeContext, SimulationEvidence
from almanak.triggers import UtcInstant


class EconomicSnapshot(Snapshot):
    simulation_time: UnixTime
    simulated: Literal[True] = True

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.simulation_time, UTC)


class EconomicResult(Model):
    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    intent: Intent
    success: Literal[True] = True
    complete: bool = True
    effective_at: UtcInstant
    simulation: SimulationEvidence
    before: EconomicSnapshot
    after: Ownership
    steps: tuple[OperationKind, ...]
    delta0: int
    delta1: int
    fees0: UInt
    fees1: UInt
    gas_paid: AssetAmount
    fill: Quote | None


DecisionInput = DecisionContext[Parameters, Ownership, EconomicSnapshot]
OutcomeInput = OutcomeContext[Parameters, EconomicResult]
State = RunState[HistoricalPosition, Intent, EconomicResult]
