"""V3 dispatch with revision-bound execution, authority and prepared model support."""

from pydantic import TypeAdapter

from almanak.connectors.interfaces import Codec, Connector
from almanak.connectors.pancakeswap_v3.api import Actions
from almanak.connectors.pancakeswap_v3.asset_effects import effects
from almanak.connectors.pancakeswap_v3.authority import capability_codec
from almanak.connectors.pancakeswap_v3.execution import PancakeExecution
from almanak.connectors.pancakeswap_v3.fork import PancakeFork
from almanak.connectors.pancakeswap_v3.lifecycle import PancakeLifecycle
from almanak.connectors.pancakeswap_v3.models import Config, Intent, Parameters
from almanak.connectors.pancakeswap_v3.observations import Snapshot
from almanak.connectors.pancakeswap_v3.permissions import PancakePermissions
from almanak.connectors.pancakeswap_v3.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.pancakeswap_v3.support import verified_support


class PancakeProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.pancakeswap_v3.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.dex_dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.pancakeswap_v3.reporting import report

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
            actions=Actions,
        ),
        lifecycle=PancakeLifecycle(),
        execution=PancakeExecution(),
        permissions=PancakePermissions(),
        projection=PancakeProjection(),
        fork=PancakeFork(),
        economic="almanak.connectors.pancakeswap_v3.economic_binding:PancakeEconomic",
    )
