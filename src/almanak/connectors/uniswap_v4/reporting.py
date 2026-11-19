"""Pool-marked fund value excludes operator gas and counts idle inventory once."""

from fractions import Fraction

from almanak.connectors.dex_reporting import residual_report
from almanak.connectors.uniswap_v4.math import accrued, amounts
from almanak.connectors.uniswap_v4.reader import sqrt_bounds
from almanak.connectors.uniswap_v4.state import Mint
from almanak.transaction_family import family_report


def quantity(asset, value):
    if value is None:
        return {"asset": str(asset), "amount": None, "complete": False}
    value = Fraction(value)
    return {
        "asset": str(asset),
        "numerator": str(value.numerator),
        "denominator": str(value.denominator),
        "unit": "base_units",
        "complete": True,
    }


def mark(amount0, amount1, observed):
    ratio = Fraction(observed.pool.sqrt_price_x96**2, 2**192)
    if observed.target.funding_currency == observed.target.assets[1]:
        return Fraction(amount1) + amount0 * ratio
    return Fraction(amount0) + Fraction(amount1) / ratio


def position_values(owned, observed):
    if owned.position is None:
        return (0, 0), (0, 0)
    lower, upper = sqrt_bounds(owned.target, position=owned.position)
    principal = amounts(owned.position.liquidity, observed.pool.sqrt_price_x96, lower, upper)
    growth = observed.fee_growth
    previous = owned.position.fee_growth
    if growth is None or previous is None:
        return principal, (None, None)
    fees = tuple(
        accrued(owned.position.liquidity, old, new)
        for old, new in zip(previous, growth, strict=True)
    )
    return principal, fees


def allocation_report(state, owned):
    target = owned.target
    outcomes = [o for o in state.outcomes if o.operation.target == target]
    observed = next((o for o in state.closure if o.target == target), None)
    if observed is None:
        observed = (
            outcomes[-1].after
            if outcomes
            else next((o for o in state.baseline if o.target == target), None)
        )
    result = {
        "target": target.model_dump(mode="json"),
        "owned": owned.model_dump(mode="json"),
        "valuation_source": target.pool.model_dump(mode="json"),
        "gas_included": False,
    }
    if observed is None:
        return {**result, "complete": False, "missing": ["no retained observation"]}
    principal, fees = position_values(owned, observed)
    total = None
    if None not in fees:
        total = mark(
            owned.inventory0.amount + principal[0] + fees[0],
            owned.inventory1.amount + principal[1] + fees[1],
            observed,
        )
    initial = state.config.allocation(target.allocation).capital.amount
    mint = next((o for o in outcomes if o.success and isinstance(o.operation, Mint)), None)
    benchmark = (
        None
        if mint is None
        else mark(
            mint.before.owned.inventory0.amount, mint.before.owned.inventory1.amount, observed
        )
    )
    return {
        **result,
        "block": observed.block.model_dump(mode="json"),
        "complete": total is not None,
        "wallet": [
            observed.wallet0.model_dump(mode="json"),
            observed.wallet1.model_dump(mode="json"),
        ],
        "principal": [quantity(a, v) for a, v in zip(target.assets, principal, strict=True)],
        "uncollected_fees": [quantity(a, v) for a, v in zip(target.assets, fees, strict=True)],
        "fund_value_excluding_gas": quantity(target.funding_currency, total),
        "pnl_excluding_gas": quantity(
            target.funding_currency, total - initial if total is not None else None
        ),
        "premint_hold_benchmark": quantity(target.funding_currency, benchmark),
        "lp_minus_hold": quantity(
            target.funding_currency,
            total - benchmark if total is not None and benchmark is not None else None,
        ),
        "fee_measurement_error": observed.fee_error,
        **residual_report(observed),
        "transactions": [
            {
                "kind": o.operation.kind,
                "hash": o.tx_hash,
                "success": o.success,
                "delta0": str(o.delta0),
                "delta1": str(o.delta1),
                "fees": [quantity(target.assets[0], o.fees0), quantity(target.assets[1], o.fees1)],
            }
            for o in outcomes
        ],
    }


def gas_payers(state):
    result = []
    payers = dict.fromkeys(b.profile.submitter for b in state.config.accounts.bindings)
    for payer in payers:
        outcomes = [o for o in state.outcomes if o.execution.submitter == payer]
        result.append(
            {
                "submitter": payer.model_dump(mode="json"),
                "amounts": [
                    quantity(
                        asset, sum(o.gas_paid.amount for o in outcomes if o.gas_paid.asset == asset)
                    )
                    for asset in dict.fromkeys(o.gas_paid.asset for o in outcomes)
                ],
                "nonces": [o.nonce for o in outcomes],
            }
        )
    return result


def report(state):
    issues = [issue for o in state.outcomes for issue in o.accounting_issues]
    return {
        "schema_version": state.schema_version,
        "deployment_id": state.deployment_id,
        "connector": "uniswap_v4",
        "mode": state.mode,
        # An operator deployment retains no strategy.
        "strategy": {"filename": state.strategy.filename, "sha256": state.strategy.sha256}
        if state.strategy is not None
        else None,
        "pending_hash": state.pending.tx_hash if state.pending else None,
        "transaction_families": family_report(state),
        "reserved_attempt": state.plan.attempt_id if state.plan else None,
        "undelivered_outcomes": len(state.outcomes) - state.delivered,
        "integrity_error": state.integrity_error,
        "transaction_accounting_complete": not issues,
        "accounting_issues": issues,
        "positions": [allocation_report(state, o) for o in state.owned],
        "gas_payers": gas_payers(state),
        "closure": [o.model_dump(mode="json") for o in state.closure],
    }
