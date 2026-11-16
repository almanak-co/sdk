"""The `hyperliquid` connector: HyperCore perps through a Safe on HyperEVM, in production,
fork runs and backtests (blueprint 30, sections 12 to 14).

One connector serves both modes with one strategy surface: the same `Actions` and the
same five intents (`models.py`). A backtest simulates them in the shared economic plane
(`economic_binding.py`, section 12), which keeps its own state types
(`economic_state.py`). A fork run takes them through the runtime (section 13): this
codec's checkpoint, decisions, plans, pending work and outcomes are the live types of
`state.py`, custody requests and HyperCore external effects bind as its `Requests` and
`ExternalEffects`, and the HyperCore gateway adapter (`gateway/hypercore_gateway.py`)
plans, signs and reconciles their custody transactions and agent-signed venue actions.

Production was declared with D7 (section 14.4): the production adapter
(`HyperCoreProductionGateway`) trades through Hyperliquid's own venue, rehearsed on a fork
against the synthesized model (section 14.3). The model is never Fork Qualification
evidence, so hosted Fork Qualification leaves the cell out
(`scripts/ci/fork_qualification_matrix.py`).
"""

from datetime import UTC, datetime
from typing import Any

from pydantic import TypeAdapter

from almanak.chains import HYPEREVM
from almanak.connectors.hyperliquid import state
from almanak.connectors.hyperliquid.api import Actions
from almanak.connectors.hyperliquid.authority import capability_codec
from almanak.connectors.hyperliquid.economic_data import HOURLY_FUNDING_FROM, MODEL
from almanak.connectors.hyperliquid.execution import Execution
from almanak.connectors.hyperliquid.external import ExternalEffects
from almanak.connectors.hyperliquid.fork import Fork
from almanak.connectors.hyperliquid.lifecycle import Lifecycle
from almanak.connectors.hyperliquid.models import Config, Intent, Parameters
from almanak.connectors.hyperliquid.observations import Snapshot
from almanak.connectors.hyperliquid.permissions import Permissions
from almanak.connectors.hyperliquid.reporting import Projection
from almanak.connectors.hyperliquid.requests import Requests
from almanak.connectors.interfaces import Codec, Connector
from almanak.connectors.support import (
    ONE_ALLOCATION_PER_FUND,
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
)
from almanak.models import Model

ACTIONS = tuple(
    EconomicAction(action)
    for action in ("deposit", "set_leverage", "increase_position", "close_position", "withdraw")
)


class UnionCodec:
    """Constructs or decodes one member of a discriminated union of trusted models."""

    def __init__(self, union):
        self.adapter: TypeAdapter = TypeAdapter(union)

    def __call__(self, **values: Any) -> Model:
        return self.adapter.validate_python(values)

    def model_validate_json(self, value: str) -> Model:
        return self.adapter.validate_json(value)


class PlanCodec(UnionCodec):
    """The EVM gateway prepares a custody transaction without naming its kind."""

    def __call__(self, **values: Any) -> Model:
        return super().__call__(**{"kind": "hyperliquid_transaction", **values})


RESTRICTIONS = (
    "Production (D7): T1 (an agent cannot move value out of the account) and the open half "
    "of T4 (an unknownOid after expiry is final) are production assumptions; an Ioc order "
    "whose answer is lost and which shows nothing past its expiry halts with its evidence.",
    "Production: the provider must serve HyperEVM's head exactly; Alchemy runs reads pinned "
    "near its head at head + 1, which the gateway refuses (blueprint 29).",
    "Backtests: HyperCore /info trade candles and hourly funding (D4); "
    "the venue serves only its latest 5000 candles of an interval.",
    "Cross margin on first-dex perps at the configured leverage; Ioc entries and "
    "reduce-only Ioc exits. Backtests refuse positions above the first margin tier or "
    "100,000 USDC of notional.",
    "Backtests model book, mark and oracle prices from trade candles.",
    "A dedicated Safe whose HyperCore account the strategy owns; the agent can "
    "trade every market of it (D2), and the gateway alone enforces the configured market "
    "and sizes.",
    "The Safe owners fund the HyperCore HYPE that pays each withdrawal's system "
    "transaction; a withdrawal it cannot pay for stops before anything is sent.",
    ONE_ALLOCATION_PER_FUND,
)


def support() -> tuple[ConnectorChainSupport, ...]:
    return (
        ConnectorChainSupport(
            chain=HYPEREVM.chain,
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
            actions=ACTIONS,
            account_profiles=("safe_roles",),
            historical=(
                HistoricalSupport(
                    model=MODEL,
                    mode="economic",
                    actions=ACTIONS,
                    backtest_start_at=datetime.fromtimestamp(HOURLY_FUNDING_FROM, UTC),
                ),
            ),
            restrictions=RESTRICTIONS,
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
            snapshot=Snapshot,
            intent=Intent,
            decision=state.Decision,
            decision_input=state.DecisionInput,
            outcome_input=state.OutcomeInput,
            decision_record=state.DecisionRecord,
            plan=PlanCodec(state.Plan),
            pending=UnionCodec(state.PendingWork),
            outcome=UnionCodec(state.Outcome),
            capability=capability_codec(),
            schema_version=1,
            actions=Actions,
        ),
        lifecycle=Lifecycle(),
        execution=Execution(),
        permissions=Permissions(),
        projection=Projection(),
        fork=Fork(),
        execution_effects=effects,
        requests=Requests(),
        external_effects=ExternalEffects(),
        economic="almanak.connectors.hyperliquid.economic_binding:HyperliquidEconomic",
        chain_support=support(),
    )
