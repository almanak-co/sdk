"""Explicit trusted binding for Uniswap execution, authority and persistence."""

from pydantic import TypeAdapter

from almanak.connectors.dex_effects import effects
from almanak.connectors.interfaces import Codec, Connector
from almanak.connectors.support import uniswap_support
from almanak.connectors.uniswap_v4.authority import capability_codec
from almanak.connectors.uniswap_v4.execution import UniswapExecution
from almanak.connectors.uniswap_v4.fork import UniswapFork
from almanak.connectors.uniswap_v4.lifecycle import UniswapLifecycle
from almanak.connectors.uniswap_v4.models import Config, Intent, Parameters
from almanak.connectors.uniswap_v4.permissions import UniswapPermissions
from almanak.connectors.uniswap_v4.reporting import report
from almanak.connectors.uniswap_v4.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    Snapshot,
    TransactionPlan,
)


class UniswapProjection:
    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.uniswap_v4.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.uniswap_v4.dashboard import history_row

        return history_row(state, observed)

    report = staticmethod(report)


def connector():
    from almanak.connectors.uniswap_v4.api import Actions

    return Connector(
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
            schema_version=2,
            actions=Actions,
        ),
        lifecycle=UniswapLifecycle(),
        execution=UniswapExecution(),
        permissions=UniswapPermissions(),
        projection=UniswapProjection(),
        fork=UniswapFork(),
        historical="almanak.connectors.uniswap_v4.backtest_binding:UniswapHistorical",
        economic="almanak.connectors.uniswap_v4.economic_binding:UniswapEconomic",
        chain_support=uniswap_support(),
    )
