"""Retained supply, debt, missing valuation and independently attributed payer costs."""

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
        "amount": str(amount) if amount is not None else None,
        "unit": "base_units",
    }


def position_report(state, owned):
    target = owned.target
    events = [o for o in state.outcomes if o.operation.target == target]
    totals = {
        kind: sum(o.assets for o in events if o.success and o.operation.kind == kind)
        for kind in ("supply", "withdraw", "borrow", "repay")
    }
    history = observations(state, target)
    latest = history[-1] if history else None
    return {
        "target": target.model_dump(mode="json"),
        "ownership": owned.model_dump(mode="json"),
        "supply_share_unit": "aave_v3_a_token_scaled_units",
        "debt_share_unit": "aave_v3_variable_debt_scaled_units",
        "effects": {kind: quantity(target.asset, amount) for kind, amount in totals.items()},
        "realized_supply_delta": quantity(target.asset, totals["withdraw"] - totals["supply"])
        if totals["supply"] and not owned.supply_scaled
        else None,
        "realized_borrowing_cost": quantity(target.asset, totals["repay"] - totals["borrow"])
        if totals["borrow"] and not owned.debt_scaled
        else None,
        "last_observation": latest.model_dump(mode="json") if latest else None,
        "measurement_status": "retained" if latest else "unobserved",
        "risk_error": latest.pool_risk_error if latest else "no retained observation",
        "missing_data": [
            error
            for error in (
                latest.pool_risk_error,
                latest.position.risk_error,
                latest.position.reserve.oracle_error,
            )
            if error
        ]
        if latest
        else ["no retained observation"],
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


def report(state):
    return {
        "schema_version": state.schema_version,
        "connector": "aave_v3",
        "deployment_id": state.deployment_id,
        "strategy": {"filename": state.strategy.filename, "sha256": state.strategy.sha256}
        if state.strategy is not None
        else None,
        "mode": state.mode,
        "execution_environment": "production" if state.config.production else "fork",
        "receipt_confirmation": "canonical_mined" if state.config.production else "fork_mined",
        "evidence_status": "invalidated" if state.integrity_error else "recorded",
        "integrity_error": state.integrity_error,
        "transaction_accounting_complete": not any(o.accounting_issues for o in state.outcomes),
        "accounting_issues": [
            {"tx_hash": o.tx_hash, "issues": list(o.accounting_issues)}
            for o in state.outcomes
            if o.accounting_issues
        ],
        "memory": state.memory.model_dump(mode="json"),
        "undelivered_outcomes": len(state.outcomes) - state.delivered,
        "pending_hash": state.pending.tx_hash if state.pending else None,
        "reserved_attempt": state.plan.attempt_id if state.plan else None,
        "transaction_families": family_report(state),
        "positions": [position_report(state, owned) for owned in state.owned],
        "funds": [
            {"account": b.name, "fund": b.profile.fund.model_dump(mode="json")}
            for b in state.config.accounts.bindings
        ],
        "gas_payers": payer_reports(state),
        "closure": [o.model_dump(mode="json") for o in state.closure],
    }
