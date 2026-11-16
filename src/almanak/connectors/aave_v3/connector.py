"""V3 wire and execution binding for independently verified chain profiles."""

from pydantic import TypeAdapter

from almanak.connectors.aave_v3.asset_effects import effects
from almanak.connectors.aave_v3.authority import capability_codec
from almanak.connectors.aave_v3.execution import AaveExecution
from almanak.connectors.aave_v3.fork import AaveFork
from almanak.connectors.aave_v3.lifecycle import AaveLifecycle
from almanak.connectors.aave_v3.models import Config, Intent, Parameters
from almanak.connectors.aave_v3.observations import Snapshot
from almanak.connectors.aave_v3.permissions import AavePermissions
from almanak.connectors.aave_v3.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.aave_v3.support import verified_support
from almanak.connectors.interfaces import Codec, Connector


class AaveProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.aave_v3.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.lending_dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.aave_v3.reporting import report

        return report(state)


def connector():
    return Connector(
        chain_support=verified_support(),
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
        lifecycle=AaveLifecycle(),
        execution=AaveExecution(),
        permissions=AavePermissions(),
        projection=AaveProjection(),
        fork=AaveFork(),
        economic="almanak.connectors.aave_v3.economic_binding:AaveEconomic",
    )
