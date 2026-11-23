"""Across wire binding: deposit, arrival and refund of a bridge, declared for fork runs."""

from pydantic import TypeAdapter

from almanak.connectors.across.asset_effects import effects
from almanak.connectors.across.authority import capability_codec
from almanak.connectors.across.execution import AcrossExecution
from almanak.connectors.across.fork import AcrossFork
from almanak.connectors.across.lifecycle import AcrossLifecycle
from almanak.connectors.across.models import Config, Intent, Parameters
from almanak.connectors.across.observations import Snapshot
from almanak.connectors.across.permissions import AcrossPermissions
from almanak.connectors.across.settlement import ExternalEffects
from almanak.connectors.across.state import (
    Checkpoint,
    Decision,
    DecisionInput,
    DecisionRecord,
    Outcome,
    OutcomeInput,
    Pending,
    TransactionPlan,
)
from almanak.connectors.across.support import candidate_support
from almanak.connectors.interfaces import Codec, Connector


def run_facts(state) -> dict:
    """What every report states of a run: its mode, its evidence and what is unfinished.

    `almanak shutdown` reads the mode: without it a closed run looks unfinished.
    """
    issues = [
        {"tx_hash": o.tx_hash, "issues": list(o.accounting_issues)}
        for o in state.outcomes
        if o.accounting_issues
    ]
    return {
        "schema_version": state.schema_version,
        "connector": "across",
        "deployment_id": state.deployment_id,
        "strategy": {"filename": state.strategy.filename, "sha256": state.strategy.sha256},
        "mode": state.mode,
        "execution_environment": "production" if state.config.production else "fork",
        "evidence_status": "invalidated" if state.integrity_error else "recorded",
        "integrity_error": state.integrity_error,
        "transaction_accounting_complete": not issues,
        "accounting_issues": issues,
        "memory": state.memory.model_dump(mode="json"),
        "undelivered_outcomes": len(state.outcomes) - state.delivered,
    }


class AcrossProjection:
    """Reports and dashboards state the run and each transfer with how it ended and
    which source quoted its offered amount."""

    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.across.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def report(state):
        # A refund-leaf payment that matched no transfer is listed with its evidence:
        # it is in the wallet, attributed to nothing.
        unattributed = [
            {"block": page.end.number, **page.unsolicited.model_dump(mode="json")}
            for page in getattr(state, "external_scans", ())
            if page.unsolicited is not None
        ]
        # The source of each transfer's offered amount, from its retained deposit. A
        # fork-constructed quote is named as one; an unretained deposit states none.
        sources = {
            o.transfer.reference: o.operation.quote.source
            for o in state.outcomes
            if getattr(o, "operation", None) is not None and o.transfer is not None
        }
        return {
            **run_facts(state),
            "transfers": [
                {**t.model_dump(mode="json"), "quote_source": sources.get(t.reference)}
                for t in state.transfers
            ],
            "unattributed_refund_payments": unattributed,
        }


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
        lifecycle=AcrossLifecycle(),
        execution=AcrossExecution(),
        permissions=AcrossPermissions(),
        projection=AcrossProjection(),
        external_effects=ExternalEffects(),
        fork=AcrossFork(),
    )
