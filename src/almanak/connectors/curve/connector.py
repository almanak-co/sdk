"""Verified Curve NG execution and prepared-model bindings."""

from pydantic import TypeAdapter

from almanak.connectors.curve.api import Actions
from almanak.connectors.curve.asset_effects import effects
from almanak.connectors.curve.authority import capability_codec
from almanak.connectors.curve.execution import CurveExecution
from almanak.connectors.curve.fork import CurveFork
from almanak.connectors.curve.lifecycle import CurveLifecycle
from almanak.connectors.curve.models import Config, Intent, Parameters
from almanak.connectors.curve.observations import Snapshot
from almanak.connectors.curve.permissions import CurvePermissions
from almanak.connectors.curve.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.curve.support import verified_support
from almanak.connectors.interfaces import Codec, Connector


class CurveProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.curve.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.curve.dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.curve.reporting import report

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
        lifecycle=CurveLifecycle(),
        execution=CurveExecution(),
        permissions=CurvePermissions(),
        projection=CurveProjection(),
        fork=CurveFork(),
        economic="almanak.connectors.curve.economic_binding:CurveEconomic",
    )
