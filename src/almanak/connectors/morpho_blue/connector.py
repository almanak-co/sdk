"""Verified Morpho native and prepared binding for Ethereum and Base."""

from pydantic import TypeAdapter

from almanak.connectors.interfaces import Codec, Connector
from almanak.connectors.morpho_blue.asset_effects import effects
from almanak.connectors.morpho_blue.authority import capability_codec
from almanak.connectors.morpho_blue.execution import MorphoExecution
from almanak.connectors.morpho_blue.fork import MorphoFork
from almanak.connectors.morpho_blue.lifecycle import MorphoLifecycle
from almanak.connectors.morpho_blue.models import Config, Intent, Parameters
from almanak.connectors.morpho_blue.observations import Snapshot
from almanak.connectors.morpho_blue.permissions import MorphoPermissions
from almanak.connectors.morpho_blue.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.morpho_blue.support import candidate_support


class MorphoProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.morpho_blue.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.lending_dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.morpho_blue.reporting import report

        return report(state)


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
        lifecycle=MorphoLifecycle(),
        execution=MorphoExecution(),
        permissions=MorphoPermissions(),
        projection=MorphoProjection(),
        fork=MorphoFork(),
        economic="almanak.connectors.morpho_blue.economic_binding:MorphoEconomic",
    )
