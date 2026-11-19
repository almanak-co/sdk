"""Retained exchange ownership, accounting completeness and independently scoped payer costs."""

from almanak.gateway.evm import native_asset
from almanak.transaction_family import family_report


def observations(state, target):
    return (
        [o for o in state.baseline if o.target == target]
        + [o.after for o in state.outcomes if o.operation.target == target]
        + [o for o in state.closure if o.target == target]
    )


def quantity(asset, amount):
    return {
        "asset": str(asset),
        "amount": None if amount is None else str(amount),
        "unit": "base_units",
    }


def residual_report(observed):
    """Counter inventory left as measured residual dust, reported only when measured.

    The dust stays in owned inventory and valuation; this entry retains the
    zero-output exit quotes that let the exit and closure complete with it.
    """
    # Connectors without exit swaps, such as GMX, have no exit residual measurement.
    residuals = getattr(observed, "residuals", ())
    return {"exit_residuals": [q.model_dump(mode="json") for q in residuals]} if residuals else {}


def position_report(state, owned):
    target = owned.target
    events = [o for o in state.outcomes if o.operation.target == target]
    history = observations(state, target)
    latest = history[-1] if history else None
    fees = tuple(
        None
        if not state.initialized or any(getattr(o, f"fees{i}") is None for o in events if o.success)
        else sum(getattr(o, f"fees{i}") for o in events if o.success)
        for i in range(2)
    )
    return {
        "target": target.model_dump(mode="json"),
        "ownership": owned.model_dump(mode="json"),
        "last_observation": latest.model_dump(mode="json") if latest else None,
        "measurement_status": "retained" if latest else "unobserved",
        **residual_report(latest),
        "collected_fees": [quantity(a, f) for a, f in zip(target.assets, fees, strict=True)],
        "transactions": [
            {"kind": o.operation.kind, "hash": o.tx_hash, "nonce": o.nonce, "success": o.success}
            for o in events
        ],
    }


def payer_reports(state):
    result = []
    for payer in dict.fromkeys(b.profile.submitter for b in state.config.accounts.bindings):
        outcomes = [o for o in state.outcomes if o.execution.submitter == payer]
        result.append(
            {
                "submitter": payer.model_dump(mode="json"),
                "accounts": [
                    b.name for b in state.config.accounts.bindings if b.profile.submitter == payer
                ],
                "gas_paid": quantity(
                    native_asset(payer.chain.chain_id),
                    sum(o.gas_paid.amount for o in outcomes) if state.initialized else None,
                ),
                "nonces": [o.nonce for o in outcomes],
            }
        )
    return result


def report(state, *, position_reporter=position_report):
    return {
        "schema_version": state.schema_version,
        "connector": state.config.connector,
        "deployment_id": state.deployment_id,
        # An operator deployment retains no strategy.
        "strategy": {"filename": state.strategy.filename, "sha256": state.strategy.sha256}
        if state.strategy is not None
        else None,
        "mode": state.mode,
        **execution_evidence(state),
        **accounting_evidence(state),
        "memory": state.memory.model_dump(mode="json"),
        **pending_evidence(state),
        "positions": [position_reporter(state, owned) for owned in state.owned],
        "funds": [
            {"account": b.name, "fund": b.profile.fund.model_dump(mode="json")}
            for b in state.config.accounts.bindings
        ],
        "gas_payers": payer_reports(state),
        "closure": [o.model_dump(mode="json") for o in state.closure],
        "valuation": "asset quantities only; no common-currency valuation or gas conversion",
    }


def execution_evidence(state):
    return {
        "execution_environment": "production" if state.config.production else "fork",
        "receipt_confirmation": "canonical_mined" if state.config.production else "fork_mined",
        "evidence_status": "invalidated" if state.integrity_error else "recorded",
        "integrity_error": state.integrity_error,
    }


def accounting_evidence(state):
    issues = [
        {"tx_hash": getattr(o, "tx_hash", None), "issues": list(o.accounting_issues)}
        for o in state.outcomes
        if o.accounting_issues
    ]
    return {"transaction_accounting_complete": not issues, "accounting_issues": issues}


def pending_evidence(state):
    return {
        "undelivered_outcomes": len(state.outcomes) - state.delivered,
        # A venue action signed by an agent has no transaction hash (blueprint 30, 13.3).
        "pending_hash": getattr(state.pending, "tx_hash", None),
        "reserved_attempt": state.plan.attempt_id if state.plan else None,
        "transaction_families": family_report(state),
    }
