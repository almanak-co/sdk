"""GMX Arbitrum production dispatch with private Avalanche qualification."""

from datetime import UTC, datetime
from typing import Any

from pydantic import TypeAdapter

from almanak.caip import ChainId
from almanak.connectors.gmx_v2 import state
from almanak.connectors.gmx_v2.api import Actions
from almanak.connectors.gmx_v2.authority import capability_codec
from almanak.connectors.gmx_v2.economic_data import LIQUIDITY_MODEL
from almanak.connectors.gmx_v2.execution import Execution
from almanak.connectors.gmx_v2.fork import Fork
from almanak.connectors.gmx_v2.intents import Intent
from almanak.connectors.gmx_v2.lifecycle import Lifecycle
from almanak.connectors.gmx_v2.liquidation_binding import ExternalEffects
from almanak.connectors.gmx_v2.models import Config, Parameters
from almanak.connectors.gmx_v2.permissions import Permissions
from almanak.connectors.gmx_v2.reporting import Projection
from almanak.connectors.gmx_v2.request_binding import Requests
from almanak.connectors.interfaces import Codec, Connector
from almanak.connectors.support import (
    ONE_ALLOCATION_PER_FUND,
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
)
from almanak.models import Model


class OutcomeCodec:
    def __init__(self):
        self.adapter: TypeAdapter = TypeAdapter(state.Outcome)

    def __call__(self, **values: Any) -> Model:
        return self.adapter.validate_python(values)

    def model_validate_json(self, value: str) -> Model:
        return self.adapter.validate_json(value)


class SnapshotCodec(OutcomeCodec):
    def __init__(self):
        self.adapter = TypeAdapter(state.SnapshotType)


def candidate_support():
    return tuple(
        ConnectorChainSupport(
            chain=ChainId(chain),
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
            if chain == "eip155:42161"
            else (RunMode.FORK,),
            actions=tuple(
                EconomicAction(action)
                for action in (
                    "increase_position",
                    "decrease_position",
                    "close_position",
                    "revoke_position",
                    "swap_exact_input",
                    "open_liquidity",
                    "increase_liquidity",
                    "decrease_liquidity",
                    "close_liquidity",
                    "revoke_exchange",
                )
            ),
            account_profiles=("eoa", "safe_roles"),
            historical=historical(chain),
            restrictions=restrictions(chain),
        )
        for chain in ("eip155:42161", "eip155:43114")
    )


def restrictions(chain):
    if chain == "eip155:42161":
        # Promoted by the 16 September 2026 scoped-promotion amendment. Native
        # insolvency stays excluded under its dated external disposition.
        return (
            "Verified market and compiled deployment closure; wrapped-native collateral.",
            "Solvent liquidation reconciles as an external effect; native insolvency is "
            "not advertised (16 September 2026 disposition).",
            "Dedicated Safe with accepted broad cancellation grants and retained-key checks.",
            ONE_ALLOCATION_PER_FUND,
        )
    return (
        "Candidate identified-fork request lifecycle; not promoted SDK support.",
        "Verified market and compiled deployment closure; wrapped-native collateral.",
        "Position and swap candidate implementation; "
        "GM liquidity candidate implementation; offline models unproven.",
        "Dedicated Safe with accepted broad cancellation grants and retained-key checks.",
        ONE_ALLOCATION_PER_FUND,
    )


def historical(chain):
    if chain != "eip155:42161":
        return ()
    return (
        HistoricalSupport(
            model=LIQUIDITY_MODEL,
            mode="economic",
            actions=tuple(
                EconomicAction(action)
                for action in (
                    "open_liquidity",
                    "increase_liquidity",
                    "decrease_liquidity",
                    "close_liquidity",
                    "revoke_exchange",
                )
            ),
            backtest_start_at=datetime(2026, 9, 8, 0, 36, 52, tzinfo=UTC),
        ),
    )


def effects(outcome):
    return outcome.assets


def connector():
    return Connector(
        codec=Codec(
            parameters=TypeAdapter(Config | Parameters),
            parameter_type=Config | Parameters,
            config=Config,
            checkpoint=state.Checkpoint,
            snapshot=SnapshotCodec(),
            intent=Intent,
            decision=state.Decision,
            decision_input=state.DecisionInput,
            outcome_input=state.OutcomeInput,
            decision_record=state.DecisionRecord,
            plan=state.TransactionPlan,
            pending=state.Pending,
            outcome=OutcomeCodec(),
            capability=capability_codec(),
            schema_version=1,
            actions=Actions,
        ),
        lifecycle=Lifecycle(),
        execution=Execution(),
        permissions=Permissions(),
        projection=Projection(),
        fork=Fork(),
        requests=Requests(),
        external_effects=ExternalEffects(),
        execution_effects=effects,
        economic="almanak.connectors.gmx_v2.economic_binding:GmxEconomic",
        chain_support=candidate_support(),
    )
