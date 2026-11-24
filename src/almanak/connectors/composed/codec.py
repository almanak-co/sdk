"""Decode native connector records without losing their concrete protocol types."""

from functools import cache
from typing import Any

from pydantic import TypeAdapter

from almanak.configuration.composed import Parameters
from almanak.connectors.aave_v3.observations import Snapshot as LendingSnapshot
from almanak.connectors.aave_v4.models import LendingSnapshot as AaveV4Snapshot
from almanak.connectors.across.observations import Snapshot as AcrossSnapshot
from almanak.connectors.aerodrome.observations import Snapshot as AerodromeSnapshot
from almanak.connectors.aerodrome_slipstream.observations import Snapshot as SlipstreamSnapshot
from almanak.connectors.composed import state
from almanak.connectors.composed.api import Actions
from almanak.connectors.composed.authority import capability_codec
from almanak.connectors.composed.models import Config, OwnedParameters
from almanak.connectors.compound_v3.observations import Snapshot as CompoundSnapshot
from almanak.connectors.curve.observations import Snapshot as CurveSnapshot
from almanak.connectors.gmx_v2.state import SnapshotType as GmxSnapshot
from almanak.connectors.interfaces import Codec
from almanak.connectors.morpho_blue.observations import Snapshot as MorphoSnapshot
from almanak.connectors.pancakeswap_v3.observations import Snapshot as PancakeSnapshot
from almanak.connectors.sparklend.observations import Snapshot as SparkSnapshot
from almanak.connectors.uniswap_v3.observations import Snapshot as ExchangeSnapshot
from almanak.connectors.uniswap_v4.state import Snapshot as V4Snapshot
from almanak.models import Model


class UnionModel:
    def __init__(self, model):
        self.adapter: TypeAdapter[Model] = TypeAdapter(model)

    def __call__(self, **values: Any) -> Model:
        return self.adapter.validate_python(values)

    def model_validate_json(self, value: str) -> Model:
        return self.adapter.validate_json(value)


@cache
def codec():
    # Building these adapters generates schemas for every connector's records. The frozen
    # codec is immutable, and every checkpoint decode and connector selection resolves it.
    return Codec(
        parameters=TypeAdapter(Config | OwnedParameters | Parameters),
        parameter_type=Config | OwnedParameters | Parameters,
        config=Config,
        checkpoint=state.Checkpoint,
        snapshot=UnionModel(
            LendingSnapshot
            | AaveV4Snapshot
            | SparkSnapshot
            | V4Snapshot
            | ExchangeSnapshot
            | PancakeSnapshot
            | AerodromeSnapshot
            | SlipstreamSnapshot
            | MorphoSnapshot
            | CompoundSnapshot
            | CurveSnapshot
            | GmxSnapshot
            | AcrossSnapshot
        ),
        intent=state.Intent,
        decision=state.Decision,
        decision_input=state.DecisionInput,
        outcome_input=state.OutcomeInput,
        decision_record=UnionModel(state.DecisionRecord),
        plan=UnionModel(state.TransactionPlan),
        pending=UnionModel(state.Pending),
        outcome=UnionModel(state.Outcome),
        capability=capability_codec(),
        schema_version=1,
        actions=Actions,
    )
