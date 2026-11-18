"""V3 dispatch with revision-bound execution, authority and prepared model support."""

from pydantic import TypeAdapter

from almanak.connectors.aerodrome_slipstream.api import Actions
from almanak.connectors.aerodrome_slipstream.asset_effects import effects
from almanak.connectors.aerodrome_slipstream.authority import capability_codec
from almanak.connectors.aerodrome_slipstream.execution import SlipstreamExecution
from almanak.connectors.aerodrome_slipstream.fork import SlipstreamFork
from almanak.connectors.aerodrome_slipstream.lifecycle import SlipstreamLifecycle
from almanak.connectors.aerodrome_slipstream.models import Config, Intent, Parameters
from almanak.connectors.aerodrome_slipstream.observations import Snapshot
from almanak.connectors.aerodrome_slipstream.permissions import SlipstreamPermissions
from almanak.connectors.aerodrome_slipstream.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.aerodrome_slipstream.support import verified_support
from almanak.connectors.interfaces import Codec, Connector


class SlipstreamProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.aerodrome_slipstream.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.dex_dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.aerodrome_slipstream.reporting import report

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
        lifecycle=SlipstreamLifecycle(),
        execution=SlipstreamExecution(),
        permissions=SlipstreamPermissions(),
        projection=SlipstreamProjection(),
        fork=SlipstreamFork(),
        economic="almanak.connectors.aerodrome_slipstream.economic_binding:SlipstreamEconomic",
    )
