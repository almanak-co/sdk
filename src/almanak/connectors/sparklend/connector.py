"""SparkLend execution and prepared economic binding with explicit Ethereum scope."""

from pydantic import TypeAdapter

from almanak.connectors.interfaces import Codec, Connector
from almanak.connectors.sparklend.asset_effects import effects
from almanak.connectors.sparklend.authority import capability_codec
from almanak.connectors.sparklend.execution import SparkExecution
from almanak.connectors.sparklend.fork import SparkFork
from almanak.connectors.sparklend.lifecycle import SparkLifecycle
from almanak.connectors.sparklend.models import Config, Intent, Parameters
from almanak.connectors.sparklend.observations import Snapshot
from almanak.connectors.sparklend.permissions import SparkPermissions
from almanak.connectors.sparklend.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.sparklend.support import candidate_support


class SparkProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.sparklend.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.lending_dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.sparklend.reporting import report

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
        lifecycle=SparkLifecycle(),
        execution=SparkExecution(),
        permissions=SparkPermissions(),
        projection=SparkProjection(),
        fork=SparkFork(),
        economic="almanak.connectors.sparklend.economic_binding:SparkEconomic",
    )
