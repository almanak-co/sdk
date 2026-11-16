"""Read-only GMX reports separate accepted escrow, fund positions and submitter gas."""

from almanak.connectors.dex_reporting import (
    accounting_evidence,
    execution_evidence,
    pending_evidence,
)
from almanak.connectors.gmx_v2.state import TransactionOutcome
from almanak.gateway.evm import native_asset


def payer_reports(state):
    result = []
    for payer in dict.fromkeys(b.profile.submitter for b in state.config.accounts.bindings):
        transactions = [
            o
            for o in state.outcomes
            if isinstance(o, TransactionOutcome) and o.execution.submitter == payer
        ]
        result.append(
            {
                "submitter": payer.model_dump(mode="json"),
                "gas_paid": {
                    "asset": str(native_asset(payer.chain.chain_id)),
                    "unit": "base_units",
                    "amount": str(sum(o.gas_paid.amount for o in transactions))
                    if state.initialized
                    else None,
                },
                "nonces": [o.nonce for o in transactions],
            }
        )
    return result


def report(state):
    return {
        "schema_version": state.schema_version,
        "connector": "gmx_v2",
        "deployment_id": state.deployment_id,
        "mode": state.mode,
        "strategy": {"filename": state.strategy.filename, "sha256": state.strategy.sha256},
        **execution_evidence(state),
        **accounting_evidence(state),
        **pending_evidence(state),
        "memory": state.memory.model_dump(mode="json"),
        "positions": [o.model_dump(mode="json") for o in state.owned],
        "requests": [r.model_dump(mode="json") for r in state.requests],
        "unresolved_requests": sum(r.unresolved for r in state.requests),
        "gas_payers": payer_reports(state),
        "outcomes": [
            {
                "source": "submitter" if isinstance(o, TransactionOutcome) else "external_keeper",
                "hash": o.tx_hash,
                "result": o.strategy_result().model_dump(mode="json"),
            }
            for o in state.outcomes
        ],
        "closure": [o.model_dump(mode="json") for o in state.closure],
        "valuation": (
            "Explicit asset base units and protocol USD-e30 notional; "
            "no marked equity or gas conversion."
        ),
    }


class Projection:
    report = staticmethod(report)

    @staticmethod
    def dashboard(state, *, captured_at=None):
        from almanak.connectors.gmx_v2.dashboard import project

        return project(state, captured_at=captured_at)

    @staticmethod
    def history(state, observed):
        from almanak.connectors.gmx_v2.dashboard_history import history_row

        return history_row(state, observed)
