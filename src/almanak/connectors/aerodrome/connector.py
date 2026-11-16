"""Aerodrome dispatch with verified execution, authority and prepared model support."""

from pydantic import TypeAdapter

from almanak.connectors.aerodrome.api import Actions
from almanak.connectors.aerodrome.asset_effects import effects
from almanak.connectors.aerodrome.authority import capability_codec
from almanak.connectors.aerodrome.execution import AerodromeExecution
from almanak.connectors.aerodrome.fork import AerodromeFork
from almanak.connectors.aerodrome.lifecycle import AerodromeLifecycle
from almanak.connectors.aerodrome.models import Config, Intent, Parameters
from almanak.connectors.aerodrome.observations import Snapshot
from almanak.connectors.aerodrome.permissions import AerodromePermissions
from almanak.connectors.aerodrome.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.aerodrome.support import verified_support
from almanak.connectors.interfaces import Codec, Connector


class AerodromeProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.aerodrome.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.aerodrome.dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.aerodrome.reporting import report

        return report(state)


def connector():
    return Connector(
        chain_support=verified_support(),
        variant=lambda target: target.pool.variant,
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
        lifecycle=AerodromeLifecycle(),
        execution=AerodromeExecution(),
        permissions=AerodromePermissions(),
        projection=AerodromeProjection(),
        fork=AerodromeFork(),
        economic="almanak.connectors.aerodrome.economic_binding:AerodromeEconomic",
    )
