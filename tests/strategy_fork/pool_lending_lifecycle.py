"""Shared assertions for explicit Pool reference phases and attributed balances."""


def entered(state):
    return (
        state.memory.values.get("phase") == 4
        and state.pending is None
        and state.plan is None
        and state.decision is None
        and state.delivered == len(state.outcomes)
    )


def assert_successful_outcomes(state):
    assert all((o.success, o.accounting_issues) == (True, ()) for o in state.outcomes)
    assert len({o.tx_hash for o in state.outcomes}) == len(state.outcomes)


def assert_zero_owned(state):
    for owned in state.owned:
        assert (owned.supply_scaled, owned.debt_scaled, owned.collateral_enabled) == (0, 0, False)


def assert_lifecycle_actions(state):
    completed = [o for o in state.outcomes if o.strategy_result().status == "completed"]
    assert [o.decision.intent.kind for o in completed] == [
        "supply",
        "collateral",
        "collateral",
        "borrow",
        "repay_amount",
        "withdraw_amount",
        "repay",
        "withdraw",
    ]


def wallet_movement(state, target, kinds):
    return sum(
        o.assets
        for o in state.outcomes
        if o.operation.target == target and o.operation.kind in kinds
    )


def assert_wallet_effects(state, before, after):
    incoming = wallet_movement(state, after.target, ("withdraw", "borrow"))
    outgoing = wallet_movement(state, after.target, ("supply", "repay"))
    assert after.wallet_balance == before.wallet_balance + incoming - outgoing


def assert_payer_effects(state, before, after):
    effects = [o for o in state.outcomes if o.execution.submitter == after.payer]
    assert before.native_balance.amount - after.native_balance.amount == sum(
        o.gas_paid.amount for o in effects
    )
    assert len({o.nonce for o in effects}) == len(effects)


def assert_zero_obligations(after):
    assert (
        after.position.supplied_scaled,
        after.position.debt_scaled,
        after.position.collateral_enabled,
        after.allowance,
    ) == (0, 0, False, 0)


def borrowed(state, name):
    """Base units the strategy borrowed for `name`, from its successful outcomes.

    The amount is the strategy's decision, not a configured limit, so holding checks
    compare proceeds with what was actually borrowed.
    """
    return sum(
        o.assets
        for o in state.outcomes
        if o.success and o.operation.kind == "borrow" and o.operation.target.allocation == name
    )
