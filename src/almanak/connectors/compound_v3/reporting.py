"""Retained Comet balances, principal units, missing prices and separate payer costs."""

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


def position_report(state, retained):
    target = retained.target
    events = [o for o in state.outcomes if o.operation.target == target]
    totals = {
        kind: sum(o.assets for o in events if o.success and o.operation.kind == kind)
        for kind in ("supply", "withdraw", "borrow", "repay")
    }
    history = observations(state, target)
    latest = history[-1] if history else None
    measured = latest is not None
    complete = measured and retained.empty and not any(o.accounting_issues for o in events)
    return {
        "target": target.model_dump(mode="json"),
        "ownership": retained.model_dump(mode="json"),
        "ownership_unit": "compound_signed_base_principal"
        if target.reserve.is_base
        else "collateral_base_units",
        "effects": {
            kind: quantity(target.asset, amount if measured else None)
            for kind, amount in totals.items()
        },
        "realized_supply_delta": quantity(target.asset, totals["withdraw"] - totals["supply"])
        if complete and totals["supply"]
        else None,
        "realized_borrowing_cost": quantity(target.asset, totals["repay"] - totals["borrow"])
        if complete and totals["borrow"]
        else None,
        "last_observation": latest.model_dump(mode="json") if latest else None,
        "measurement_status": "retained" if measured else "unobserved",
        "missing_data": missing_data(latest),
        "transactions": [
            {"kind": o.operation.kind, "hash": o.tx_hash, "nonce": o.nonce, "success": o.success}
            for o in events
        ],
    }


def missing_data(observation):
    if observation is None:
        return ["no retained observation"]
    market = observation.account.market
    errors = [f"{c.asset_type}: {c.price_error}" for c in market.collaterals if c.price_error]
    if market.base_price_error:
        errors.insert(0, f"{market.source.base_asset}: {market.base_price_error}")
    return errors


def payer_reports(state):
    result = []
    for payer in dict.fromkeys(b.profile.submitter for b in state.config.accounts.bindings):
        outcomes = [o for o in state.outcomes if o.execution.submitter == payer]
        measured = any(o.payer == payer for o in state.baseline)
        result.append(
            {
                "submitter": payer.model_dump(mode="json"),
                "accounts": [
                    b.name for b in state.config.accounts.bindings if b.profile.submitter == payer
                ],
                "gas_paid": quantity(
                    native_asset(payer.chain.chain_id),
                    sum(o.gas_paid.amount for o in outcomes) if measured else None,
                ),
                "nonces": [o.nonce for o in outcomes],
            }
        )
    return result


def report(state):
    return {
        "schema_version": state.schema_version,
        "connector": "compound_v3",
        "deployment_id": state.deployment_id,
        # An operator deployment retains no strategy.
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
        "positions": [position_report(state, retained) for retained in state.owned],
        "funds": [
            {"account": b.name, "fund": b.profile.fund.model_dump(mode="json")}
            for b in state.config.accounts.bindings
        ],
        "gas_payers": payer_reports(state),
        "closure": [o.model_dump(mode="json") for o in state.closure],
    }
