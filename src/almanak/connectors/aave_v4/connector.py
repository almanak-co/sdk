"""Explicit Aave binding for shared orchestration and validated wire contracts."""

from pydantic import TypeAdapter

from almanak.connectors.aave_v4.asset_effects import effects
from almanak.connectors.aave_v4.authority import capability_codec
from almanak.connectors.aave_v4.execution import AaveExecution
from almanak.connectors.aave_v4.fork import AaveFork
from almanak.connectors.aave_v4.lifecycle import AaveLifecycle
from almanak.connectors.aave_v4.models import (
    Decision,
    DecisionInput,
    DecisionRecord,
    EffectiveLendingConfig,
    Intent,
    LendingConfig,
    LendingSnapshot,
    OutcomeInput,
)
from almanak.connectors.aave_v4.permission_adapter import AavePermissions
from almanak.connectors.aave_v4.state import Checkpoint, Outcome, Pending, TransactionPlan
from almanak.connectors.interfaces import Codec, Connector
from almanak.connectors.support import aave_support


class AaveProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.aave_v4.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.lending_dashboard import history_row

        return history_row(state, observed)

    @staticmethod
    def report(state):
        from almanak.connectors.aave_v4.reporting import report

        return report(state)


def connector():
    return Connector(
        execution_effects=effects,
        codec=Codec(
            parameters=TypeAdapter(EffectiveLendingConfig),
            parameter_type=EffectiveLendingConfig,
            config=LendingConfig,
            checkpoint=Checkpoint,
            snapshot=LendingSnapshot,
            intent=Intent,
            decision=Decision,
            decision_input=DecisionInput,
            outcome_input=OutcomeInput,
            decision_record=DecisionRecord,
            plan=TransactionPlan,
            pending=Pending,
            outcome=Outcome,
            capability=capability_codec(),
            schema_version=5,
            actions=lambda context: context.config,
            legacy_schemas={1: False, 2: False, 3: True, 4: True},
        ),
        lifecycle=AaveLifecycle(),
        execution=AaveExecution(),
        permissions=AavePermissions(),
        projection=AaveProjection(),
        fork=AaveFork(),
        historical="almanak.connectors.aave_v4.backtest_binding:AaveHistorical",
        economic="almanak.connectors.aave_v4.economic_binding:AaveEconomic",
        chain_support=aave_support(),
    )
