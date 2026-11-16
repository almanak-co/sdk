"""Concrete economic state keeps connector-specific sources and intents."""

from typing import Annotated

from pydantic import Field

from almanak.backtest.engine import RunState
from almanak.connectors import v3_economic_state as family
from almanak.connectors.aerodrome_slipstream.domain import PoolObservation
from almanak.connectors.aerodrome_slipstream.models import Intent, Parameters, Target
from almanak.connectors.aerodrome_slipstream.positions import Ownership, Quote
from almanak.strategy_api import DecisionContext, OutcomeContext


class EconomicSnapshot(family.EconomicSnapshot[Target, PoolObservation, Ownership, Quote]):
    pass


class EconomicPosition(family.EconomicPosition[Target, Ownership]):
    pass


class EconomicResult(family.EconomicResult[Intent, EconomicSnapshot, Ownership, Quote]):
    pass


class EconomicFailure(family.EconomicFailure[Intent]):
    pass


Result = Annotated[EconomicResult | EconomicFailure, Field(discriminator="kind")]
DecisionInput = DecisionContext[Parameters, Ownership, EconomicSnapshot]
OutcomeInput = OutcomeContext[Parameters, Result]
State = RunState[EconomicPosition, Intent, Result]
