"""One simulator journal retains native positions, results and shared fund balances."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest import economic_state as aave_v4
from almanak.backtest.engine import RunState, Transition
from almanak.connectors.aave_v3 import economic_state as lending
from almanak.connectors.aerodrome import economic_state as aerodrome
from almanak.connectors.aerodrome_slipstream import economic_state as slipstream
from almanak.connectors.composed.models import OwnedParameters
from almanak.connectors.composed.state import Intent, Ownership
from almanak.connectors.compound_v3 import economic_state as compound
from almanak.connectors.curve import economic_state as curve
from almanak.connectors.gmx_v2 import economic_perpetual_state as gmx_perpetual
from almanak.connectors.gmx_v2 import economic_state as gmx
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue import economic_state as morpho
from almanak.connectors.pancakeswap_v3 import economic_state as pancake
from almanak.connectors.sparklend import economic_state as spark
from almanak.connectors.uniswap_v3 import economic_state as exchange
from almanak.connectors.uniswap_v4 import economic_state as v4
from almanak.connectors.uniswap_v4.backtest import HistoricalPosition as V4Position
from almanak.models import Model
from almanak.strategy_api import DecisionContext, Memory, OutcomeContext
from almanak.triggers import UtcInstant

Position = (
    V4Position
    | aave_v4.EconomicPosition
    | lending.Position
    | spark.Position
    | exchange.EconomicPosition
    | pancake.EconomicPosition
    | aerodrome.EconomicPosition
    | slipstream.EconomicPosition
    | morpho.Position
    | compound.Position
    | curve.EconomicPosition
    | gmx.EconomicPosition
    | gmx_perpetual.EconomicPerpetualPosition
)
Observation = (
    v4.EconomicSnapshot
    | aave_v4.EconomicSnapshot
    | lending.Snapshot
    | spark.Snapshot
    | exchange.EconomicSnapshot
    | pancake.EconomicSnapshot
    | aerodrome.EconomicSnapshot
    | slipstream.EconomicSnapshot
    | morpho.Snapshot
    | compound.Snapshot
    | curve.EconomicSnapshot
    | gmx.EconomicSnapshot
    | gmx_perpetual.EconomicPerpetualSnapshot
)
ActionResult = (
    v4.EconomicResult
    | aave_v4.EconomicActionResult
    | lending.ActionResult
    | spark.ActionResult
    | exchange.Result
    | pancake.Result
    | aerodrome.Result
    | slipstream.Result
    | morpho.ActionResult
    | compound.ActionResult
    | curve.Result
    | gmx.EconomicResult
    | gmx.EconomicFailure
    | gmx_perpetual.EconomicPerpetualResult
    | gmx_perpetual.EconomicPerpetualFailure
)
NativeEvent = (
    aave_v4.EconomicEvent
    | lending.Event
    | spark.Event
    | morpho.Event
    | compound.Event
    | gmx_perpetual.EconomicLiquidationBatch
)


class ComponentEvent(Model):
    connector: ConnectorId
    transition: Transition[Position, NativeEvent]

    @model_validator(mode="after")
    def scope(self):
        expected = {
            ConnectorId("aave_v3"): lending.Event,
            ConnectorId("aave_v4"): aave_v4.EconomicEvent,
            ConnectorId("sparklend"): spark.Event,
            ConnectorId("morpho_blue"): morpho.Event,
            ConnectorId("compound_v3"): compound.Event,
            ConnectorId("gmx_v2"): gmx_perpetual.EconomicLiquidationBatch,
        }
        if type(self.transition.result) is not expected.get(self.connector):
            raise ValueError("composed event differs from its connector identity")
        if not self.transition.complete:
            raise ValueError("composed external events require complete atomic effects")
        for change in self.transition.effects.positions:
            position = change.after or change.before
            if position is None or position.key.connector != self.connector:
                raise ValueError("native event cannot mutate another connector position")
        return self


class Event(Model):
    kind: Literal["composed_event"] = "composed_event"
    effective_at: UtcInstant
    components: Annotated[tuple[ComponentEvent, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def scope(self):
        if len({c.connector for c in self.components}) != len(self.components):
            raise ValueError("composed event can retain each connector only once")
        if any(c.transition.result.effective_at != self.effective_at for c in self.components):
            raise ValueError("composed events require one logical timestamp")
        return self


Result = ActionResult | Event
State = RunState[Position, Intent, Result]
EconomicOwnership = Ownership | gmx_perpetual.PerpetualOwnership
DecisionInput = DecisionContext[OwnedParameters, EconomicOwnership, Observation]
OutcomeInput = OutcomeContext[OwnedParameters, ActionResult]


class EventInput(Model):
    config: OwnedParameters
    memory: Memory
    event: Event
