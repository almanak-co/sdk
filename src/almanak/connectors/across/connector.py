"""Across wire binding: deposit, arrival and refund of a bridge, with no declared run mode yet."""

from pydantic import TypeAdapter

from almanak.connectors.across.asset_effects import effects
from almanak.connectors.across.authority import capability_codec
from almanak.connectors.across.execution import AcrossExecution
from almanak.connectors.across.lifecycle import AcrossLifecycle
from almanak.connectors.across.models import Config, Intent, Parameters
from almanak.connectors.across.observations import Snapshot
from almanak.connectors.across.permissions import AcrossPermissions
from almanak.connectors.across.settlement import ExternalEffects
from almanak.connectors.across.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.across.support import candidate_support
from almanak.connectors.interfaces import Codec, Connector


class AcrossProjection:
    """Reports state each transfer and how it ended; a dashboard follows with the
    reference strategy."""

    @staticmethod
    def dashboard(state, *, captured_at=None):
        raise ValueError("Across has no dashboard projection yet")

    @staticmethod
    def report(state):
        return {"transfers": [t.model_dump(mode="json") for t in state.transfers]}


def connector():
    return Connector(
        chain_support=candidate_support(),
        execution_effects=effects,
        codec=Codec(
            parameters=TypeAdapter(Config | Parameters),
            parameter_type=Config | Parameters,
            config=Config,
            checkpoint=Checkpoint,
            snapshot=Snapshot,
            intent=Intent,
            decision=Decision,
            decision_input=DecisionInput,
            outcome_input=OutcomeInput,
            decision_record=DecisionRecord,
            plan=TransactionPlan,
            pending=Pending,
            outcome=Outcome,
            capability=capability_codec(),
            schema_version=1,
            actions=lambda context: context.config,
        ),
        lifecycle=AcrossLifecycle(),
        execution=AcrossExecution(),
        permissions=AcrossPermissions(),
        projection=AcrossProjection(),
        external_effects=ExternalEffects(),
        # The fork lifecycle fixture arrives with the reference strategy.
        fork=None,
    )
