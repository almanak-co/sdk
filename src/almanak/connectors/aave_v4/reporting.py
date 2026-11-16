"""Rebuildable position effects and fund costs from retained execution facts."""

from almanak.connectors.aave_v4.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.aave_v4.state import Checkpoint
from almanak.transaction_family import family_report


def report(state: Checkpoint) -> dict:
    return {
        "schema_version": state.schema_version,
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
        "transaction_families": family_report(state),
        "reserved_attempt": state.plan.attempt_id if state.plan else None,
        "positions": [
            position_report(state, allocation) for allocation in state.config.allocations
        ],
        "funds": [fund_report(state, binding) for binding in state.config.accounts.bindings],
        "debts": [debt_report(state, debt) for debt in state.config.debts],
        "gas_payers": payer_reports(state),
        "closure": [item.model_dump(mode="json") for item in state.closure],
    }


def position_report(state, allocation):
    target = state.config.target(allocation.name)
    outcomes = [item for item in state.outcomes if item.operation.target == target]
    supplied = sum(
        item.assets
        for item in outcomes
        if item.success and isinstance(item.operation, SupplyOperation)
    )
    withdrawn = sum(
        item.assets
        for item in outcomes
        if item.success and isinstance(item.operation, WithdrawOperation)
    )
    shares = next(item.shares for item in state.owned if item.target == target)
    closed = bool(withdrawn) and not shares
    return {
        "target": target.model_dump(mode="json"),
        "supplied": {"asset": str(allocation.reserve.asset), "amount": str(supplied)},
        "withdrawn": {"asset": str(allocation.reserve.asset), "amount": str(withdrawn)},
        "realized_token_delta": {
            "asset": str(allocation.reserve.asset),
            "amount": str(withdrawn - supplied),
        }
        if closed or state.mode == "closed"
        else None,
        "owned_shares": str(shares),
        "share_unit": "aave_v4_supply_shares",
        "amount_unit": "base_units",
        "transactions": [transaction_report(item) for item in outcomes],
    }


def transaction_report(outcome):
    return {
        "kind": outcome.operation.kind,
        "hash": outcome.tx_hash,
        "nonce": outcome.nonce,
        "success": outcome.success,
    }


def fund_report(state, binding):
    outcomes = [item for item in state.outcomes if item.operation.account == binding.name]
    allocations = [item for item in state.config.allocations if item.account == binding.name]
    return {
        "account": binding.name,
        "fund": binding.profile.fund.model_dump(mode="json"),
        "positions": [item.name for item in allocations],
        "gas_paid": {
            "asset": str(allocations[0].native_asset),
            "amount": str(sum(item.gas_paid.amount for item in outcomes)),
            "payer": binding.profile.submitter.model_dump(mode="json"),
        },
    }


def debt_report(state, debt):
    target = state.config.target(debt.name)
    owned = next(o for o in state.owned if o.target == target)
    outcomes = [o for o in state.outcomes if o.operation.target == target]
    borrowed = sum(
        o.assets for o in outcomes if o.success and isinstance(o.operation, BorrowOperation)
    )
    repaid = sum(
        o.assets for o in outcomes if o.success and isinstance(o.operation, RepayOperation)
    )
    observations = [o for o in state.baseline if o.target == target]
    observations.extend(o.after for o in outcomes)
    observations.extend(o for o in state.closure if o.target == target)
    latest = observations[-1] if observations else None
    closed = (
        bool(borrowed)
        and latest is not None
        and not (
            owned.drawn_shares
            or owned.premium_shares
            or owned.premium_offset_ray
            or latest.position.total_debt
        )
    )
    return {
        "target": target.model_dump(mode="json"),
        "borrowed": {"asset": str(target.asset), "amount": str(borrowed)},
        "repaid": {"asset": str(target.asset), "amount": str(repaid)},
        "realized_borrowing_cost": {"asset": str(target.asset), "amount": str(repaid - borrowed)}
        if closed
        else None,
        "ownership": owned.model_dump(mode="json"),
        "last_observation": latest.model_dump(mode="json") if latest else None,
        "collateral": [state.config.target(n).model_dump(mode="json") for n in debt.collateral],
        "transactions": [transaction_report(o) for o in outcomes],
    }


def payer_reports(state):
    payers = tuple(dict.fromkeys(b.profile.submitter for b in state.config.accounts.bindings))
    result = []
    for payer in payers:
        outcomes = [
            o
            for o in state.outcomes
            if state.config.accounts.binding(o.operation.account).profile.submitter == payer
        ]
        accounts = [b.name for b in state.config.accounts.bindings if b.profile.submitter == payer]
        allocation = next(a for a in state.config.allocations if a.account in accounts)
        result.append(
            {
                "submitter": payer.model_dump(mode="json"),
                "accounts": accounts,
                "gas_paid": {
                    "asset": str(allocation.native_asset),
                    "amount": str(sum(o.gas_paid.amount for o in outcomes)),
                },
                "nonces": [o.nonce for o in outcomes],
            }
        )
    return result
